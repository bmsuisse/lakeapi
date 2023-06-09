import inspect
from pathlib import Path
from typing import Any, Callable, List, Literal, Optional, Type, Union, cast
from typing_extensions import TypedDict, NotRequired, Required
import duckdb
import polars as pl
import pyarrow as pa
import pypika
import pypika.queries as fn
from aiocache import Cache, cached
from aiocache.serializers import PickleSerializer
from deltalake import DeltaTable, Metadata
from deltalake.exceptions import DeltaError
from fastapi import (
    APIRouter,
    BackgroundTasks,
    Depends,
    Header,
    HTTPException,
    Query,
    Request,
)
from pydantic import BaseModel
from pypika.queries import QueryBuilder

from bmsdna.lakeapi.context import get_context_by_engine
from bmsdna.lakeapi.context.df_base import ResultData, get_sql
from bmsdna.lakeapi.core.config import BasicConfig, Config, Configs, Param, SearchConfig
from bmsdna.lakeapi.core.dataframe import (
    Dataframe,
    filter_df_based_on_params,
    filter_partitions_based_on_params,
)
from bmsdna.lakeapi.core.log import get_logger
from bmsdna.lakeapi.core.model import create_parameter_model, create_response_model
from bmsdna.lakeapi.core.partition_utils import should_hide_colname
from bmsdna.lakeapi.core.response import create_response
from bmsdna.lakeapi.core.types import (
    MetadataDetailResult,
    MetadataSchemaField,
    MetadataSchemaFieldType,
    OutputFileType,
    Engines,
)
from bmsdna.lakeapi.core.env import CACHE_EXPIRATION_TIME_SECONDS

cache = cached(ttl=CACHE_EXPIRATION_TIME_SECONDS, cache=Cache.MEMORY, serializer=PickleSerializer())

logger = get_logger(__name__)


async def get_partitions(dataframe: Dataframe, params: BaseModel, config: Config) -> Optional[list]:
    parts = (
        await filter_partitions_based_on_params(
            DeltaTable(dataframe.uri).metadata(),
            params.dict(exclude_unset=True) if params else {},
            config.params or [],
        )
        if config.datasource.file_type == "delta"
        else None
    )
    return parts


def remove_search(prm_dict: dict, config: Config):
    if not config.search or len(prm_dict) == 0:
        return prm_dict
    search_cols = [s.name.lower() for s in config.search]
    return {k: v for k, v in prm_dict.items() if k.lower() not in search_cols}


async def get_params_filter_expr(columns: List[str], config: Config, params: BaseModel) -> Optional[pypika.Criterion]:
    expr = await filter_df_based_on_params(
        remove_search(params.dict(exclude_unset=True) if params else {}, config),
        config.params if config.params else [],
        columns,
    )
    return expr


def get_response_model(config: Config, metamodel: ResultData) -> Optional[Type[BaseModel]]:
    response_model: Optional[type[BaseModel]] = None
    try:
        response_model = create_response_model(config.tag + "_" + config.name, metamodel)
    except Exception as err:
        logger.warning(f"Could not get response type for f{config.route}. Error:{err}")
        response_model = None
    return response_model


def _to_dict(tblmeta: Optional[Metadata]):
    if not tblmeta:
        return None
    return {
        "name": tblmeta.name,
        "description": tblmeta.description,
        "partition_columns": tblmeta.partition_columns,
        "configuration": tblmeta.configuration,
    }


def create_detailed_meta_endpoint(
    metamodel: Optional[ResultData], config: Config, configs: Configs, router: APIRouter, basic_config: BasicConfig
):
    route = config.route + "/metadata_detail"
    has_complex = True
    if metamodel is not None:
        has_complex = any((pa.types.is_nested(t) for t in metamodel.arrow_schema().types))

    @router.get(
        route,
        tags=["metadata", "metadata:" + config.tag],
        operation_id=config.tag + "_" + config.name,
        name=config.name + "_metadata",
    )
    def get_detailed_metadata(
        req: Request,
        jsonify_complex: bool = Query(title="jsonify_complex", include_in_schema=has_complex, default=False),
    ) -> MetadataDetailResult:
        import json

        req.state.lake_api_basic_config = basic_config
        from bmsdna.lakeapi.context.df_duckdb import DuckDbExecutionContext

        with DuckDbExecutionContext() as context:
            realdataframe = Dataframe(
                config.version_str, config.tag, config.name, config.datasource, context, basic_config=basic_config
            )

            if not realdataframe.file_exists():
                raise HTTPException(404)
            partition_columns = []
            partition_values = None
            delta_tbl = None
            df = realdataframe.get_df(None)
            if config.datasource.file_type == "delta":
                delta_tbl = DeltaTable(realdataframe.uri)
                partition_columns = delta_tbl.metadata().partition_columns
                partition_columns = [
                    c for c in partition_columns if not should_hide_colname(c)
                ]  # also hide those from metadata detail
                if len(partition_columns) > 0:
                    qb: QueryBuilder = (
                        df.query_builder().select(*[pypika.Field(c) for c in partition_columns]).distinct()
                    )
                    partition_values = context.execute_sql(qb).to_arrow_table().to_pylist()
            schema = df.arrow_schema()
            str_cols = [
                name
                for name in schema.names
                if pa.types.is_string(schema.field(name).type) or pa.types.is_large_string(schema.field(name).type)
            ]
            complex_str_cols = (
                [
                    name
                    for name in schema.names
                    if pa.types.is_struct(schema.field(name).type) or pa.types.is_list(schema.field(name).type)
                ]
                if jsonify_complex
                else []
            )
            str_lengths_df = (
                (
                    context.execute_sql(
                        df.query_builder().select(
                            *(
                                [fn.Function("MAX", fn.Function("LEN", fn.Field(sc))).as_(sc) for sc in str_cols]
                                + [
                                    fn.Function(
                                        "MAX",
                                        fn.Function("LEN", context.json_function(fn.Field(sc), assure_string=True)),
                                    ).as_(sc)
                                    for sc in complex_str_cols
                                ]
                            )
                        )
                    )
                    .to_arrow_table()
                    .to_pylist()
                )
                if len(str_cols) > 0 or len(complex_str_cols) > 0
                else [{}]
            )
            str_lengths = str_lengths_df[0]

            def _recursive_get_type(t: pa.DataType) -> MetadataSchemaFieldType:
                is_complex = pa.types.is_nested(t)

                return MetadataSchemaFieldType(
                    type_str=str(pa.string()) if is_complex and jsonify_complex else str(t),
                    orig_type_str=str(t),
                    fields=[
                        MetadataSchemaField(name=f.name, type=_recursive_get_type(f.type))
                        for f in [t.field(find) for find in range(0, cast(pa.StructType, t).num_fields)]
                    ]
                    if pa.types.is_struct(t) and not jsonify_complex
                    else None,
                    inner=_recursive_get_type(t.value_type)
                    if pa.types.is_list(t)
                    or pa.types.is_large_list(t)
                    or pa.types.is_fixed_size_list(t)
                    and t.value_type is not None
                    and not jsonify_complex
                    else None,
                )

            schema = df.arrow_schema()
            return MetadataDetailResult(
                partition_values=partition_values,
                partition_columns=partition_columns,
                max_string_lengths=str_lengths,
                data_schema=[
                    MetadataSchemaField(name=n, type=_recursive_get_type(schema.field(n).type)) for n in schema.names
                ],
                delta_meta=_to_dict(delta_tbl.metadata() if delta_tbl else None),
                delta_schema=json.loads(delta_tbl.schema().to_json()) if delta_tbl else None,
                parameters=config.params,  # type: ignore
                search=config.search,
            )


def exclude_cols(columns: List[str]) -> List[str]:
    columns = [c for c in columns if not should_hide_colname(c)]
    return columns


def is_complex_type(schema: pa.Schema, col_name: str):
    f = schema.field(col_name)
    return pa.types.is_nested(f.type)


def create_config_endpoint(
    metamodel: Optional[ResultData],
    apimethod: Literal["get", "post"],
    config: Config,
    router: APIRouter,
    response_model: Type,
    basic_config: BasicConfig,
    configs: Configs,
):
    route = config.route

    query_model = create_parameter_model(
        metamodel, config.tag + "_" + config.name + "_" + apimethod, config.params, config.search, apimethod
    )

    api_method_mapping = {
        "post": router.post(
            route,
            tags=[config.tag],
            response_model=response_model,
            operation_id=config.tag + "_" + config.name + "_" + apimethod,
            name=config.name,
        ),
        "get": router.get(
            route,
            tags=[config.tag],
            response_model=response_model,
            operation_id=config.tag + "_" + config.name + "_" + apimethod,
            name=config.name,
        ),
    }

    api_method = api_method_mapping[apimethod]
    has_complex = True
    if metamodel is not None:
        has_complex = any((pa.types.is_struct(t) or pa.types.is_list(t) for t in metamodel.arrow_schema().types))

    @api_method
    async def data(
        request: Request,
        params: query_model = (Depends() if apimethod == "get" else None),  # type: ignore
        Accept: Union[str, None] = Header(default=None),
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        select: Union[str, None] = Query(title="$select", alias="$select", default=None, include_in_schema=False),
        distinct: bool = Query(title="$distinct", alias="$distinct", default=False, include_in_schema=False),
        engine: Engines = Query(title="$engine", alias="$engine", default="duckdb", include_in_schema=False),
        format: Optional[OutputFileType] = "json",
        jsonify_complex: bool = Query(title="jsonify_complex", include_in_schema=has_complex, default=False),
    ):  # type: ignore
        logger.info(f"{params.dict(exclude_unset=True) if params else None}Union[ ,  ]{request.url.path}")

        engine = engine or basic_config.default_engine

        logger.info(f"Engine: {engine}")

        with get_context_by_engine(engine) as context:
            realdataframe = Dataframe(
                config.version_str, config.tag, config.name, config.datasource, context, basic_config=basic_config
            )
            parts = await get_partitions(realdataframe, params, config)
            df = realdataframe.get_df(parts or None)

            expr = await get_params_filter_expr(df.columns(), config, params)
            base_schema = df.arrow_schema()
            new_query = df.query_builder()
            new_query = new_query.where(expr) if expr is not None else new_query

            searches = {}
            if config.search is not None and params is not None:
                search_dict = {c.name.lower(): c for c in config.search}
                searches = {
                    k: (v, search_dict[k.lower()])
                    for k, v in params.dict(exclude_unset=True).items()
                    if k.lower() in search_dict and v is not None and len(v) >= basic_config.min_search_length
                }

            import pypika

            columns = exclude_cols(df.columns())
            if select:
                columns = [c for c in columns if c in select.split(",")]
            if config.datasource.exclude and len(config.datasource.exclude) > 0:
                columns = [c for c in columns if c not in config.datasource.exclude]
            if config.datasource.sortby and len(searches) == 0:
                for s in config.datasource.sortby:
                    new_query = new_query.orderby(
                        s.by,
                        order=pypika.Order.desc if s.direction and s.direction.lower() == "desc" else pypika.Order.asc,
                    )
            if has_complex and format in ["csv", "excel", "scsv", "csv4excel"]:
                jsonify_complex = True
            if jsonify_complex:
                new_query = new_query.select(
                    *[
                        pypika.Field(c)
                        if not is_complex_type(base_schema, c)
                        else context.json_function(pypika.Field(c)).as_(c)
                        for c in columns
                    ]
                )
            else:
                new_query = new_query.select(*columns)

            if distinct:
                assert len(columns) <= 3  # reduce complexity here
                new_query = new_query.distinct()

            if not (limit == -1 and config.allow_get_all_pages):
                limit = 1000 if limit == -1 else limit
                new_query = new_query.offset(offset or 0).limit(limit)

            if len(searches) > 0 and config.search is not None:
                import pypika.queries
                import pypika.terms

                source_view = realdataframe.tablename
                context.init_search(source_view, config.search)
                score_sum = None
                for search_key, (search_val, search_cfg) in searches.items():
                    score_sum = (
                        context.search_score_function(source_view, search_val, search_cfg, alias=None)
                        if score_sum is None
                        else score_sum + context.search_score_function(source_view, search_val, search_cfg, alias=None)
                    )
                assert score_sum is not None
                new_query = new_query.select(score_sum.as_("search_score"))
                new_query = new_query.where(pypika.terms.NotNullCriterion(pypika.queries.Field("search_score")))

                new_query = new_query.orderby(pypika.Field("search_score"), order=pypika.Order.desc)

            logger.info(f"Query: {get_sql(new_query)}")

            df2 = context.execute_sql(new_query)

            try:
                return await create_response(
                    request.url,
                    format or request.headers["Accept"],
                    df2,
                    context,
                    basic_config=basic_config,
                    close_context=True,
                )
            except Exception as err:
                logger.error("Error in creating response", exc_info=err)
                raise HTTPException(status_code=500)
