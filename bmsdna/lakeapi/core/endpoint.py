from typing import List, Literal, Optional, Type, Union
import pyarrow as pa
import pypika
import pypika.functions as fn
from deltalake import DeltaTable
from aiocache import Cache, cached
from aiocache.serializers import PickleSerializer
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

from bmsdna.lakeapi.context import get_context_by_engine
from bmsdna.lakeapi.context.df_base import ResultData, get_sql
from bmsdna.lakeapi.core.config import BasicConfig, Config, Configs
from bmsdna.lakeapi.core.datasource import (
    Datasource,
    filter_df_based_on_params,
    filter_partitions_based_on_params,
)
from bmsdna.lakeapi.core.log import get_logger
from bmsdna.lakeapi.core.model import create_parameter_model, create_response_model
from bmsdna.lakeapi.core.partition_utils import should_hide_colname
from bmsdna.lakeapi.core.response import create_response
from bmsdna.lakeapi.core.types import (
    OutputFileType,
    Engines,
)
from bmsdna.lakeapi.core.env import CACHE_EXPIRATION_TIME_SECONDS

cache = cached(ttl=CACHE_EXPIRATION_TIME_SECONDS, cache=Cache.MEMORY, serializer=PickleSerializer())

logger = get_logger(__name__)


async def get_partitions(datasource: Datasource, params: BaseModel, config: Config) -> Optional[list]:
    parts = (
        await filter_partitions_based_on_params(
            DeltaTable(datasource.uri).metadata(),
            params.model_dump(exclude_unset=True) if params else {},
            config.params or [],
        )
        if not config.datasource or config.datasource.file_type == "delta"
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
        remove_search(params.model_dump(exclude_unset=True) if params else {}, config),
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


def exclude_cols(columns: List[str]) -> List[str]:
    columns = [c for c in columns if not should_hide_colname(c)]
    return columns


def is_complex_type(schema: pa.Schema, col_name: str):
    f = schema.field(col_name)
    return pa.types.is_nested(f.type)


def split_csv(csv_str: str) -> list[str]:
    import csv

    reader = csv.reader([csv_str], delimiter=",", quotechar='"')
    for i in reader:
        return i
    raise ValueError("cannot happen")


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
        engine: Engines | None = Query(title="$engine", alias="$engine", default=None, include_in_schema=False),
        format: Optional[OutputFileType] = "json",
        jsonify_complex: bool = Query(title="jsonify_complex", include_in_schema=has_complex, default=False),
        chunk_size: int | None = Query(title="$chunk_size", include_in_schema=False, default=None),
    ):  # type: ignore
        logger.debug(f"{params.model_dump(exclude_unset=True) if params else None}Union[ ,  ]{request.url.path}")

        engine = engine or config.engine or basic_config.default_engine

        logger.debug(f"Engine: {engine}")
        real_chunk_size = chunk_size or config.chunk_size or basic_config.default_chunk_size
        with get_context_by_engine(engine, chunk_size=real_chunk_size) as context:
            assert config.datasource is not None
            realdataframe = Datasource(
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
                    for k, v in params.model_dump(exclude_unset=True).items()
                    if k.lower() in search_dict and v is not None and len(v) >= basic_config.min_search_length
                }

            import pypika

            columns = exclude_cols(df.columns())
            if select:
                columns = [
                    c for c in columns if c in split_csv(select)
                ]  # split , is a bit naive, we might want to support real CSV with quotes here
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

            logger.debug(f"Query: {get_sql(new_query)}")

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
