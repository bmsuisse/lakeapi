from pathlib import Path
from typing import List, Literal, Optional, Type, Union, cast

import duckdb
import polars as pl
import pyarrow as pa
import pypika
import pypika.queries as fn
from aiocache import Cache, cached
from aiocache.serializers import PickleSerializer
from deltalake import DeltaTable, Metadata, PyDeltaTableError
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
from bmsdna.lakeapi.context.df_base import ResultData
from bmsdna.lakeapi.core.config import Config
from bmsdna.lakeapi.core.dataframe import (
    Dataframe,
    filter_df_based_on_params,
    filter_partitions_based_on_params,
)
from bmsdna.lakeapi.core.log import get_logger
from bmsdna.lakeapi.core.model import (
    create_parameter_model,
    create_response_model,
    should_hide_colname,
)
from bmsdna.lakeapi.core.response import create_response
from bmsdna.lakeapi.core.types import OutputFileType, Engines
from bmsdna.lakeapi.core.uservalidation import get_current_username
from bmsdna.lakeapi.core.env import CACHE_EXPIRATION_TIME_SECONDS, DATA_PATH

cache = cached(ttl=CACHE_EXPIRATION_TIME_SECONDS, cache=Cache.MEMORY, serializer=PickleSerializer())

logger = get_logger(__name__)


def get_delta_folders(rootdir) -> dict[str, Path]:
    paths = [p.parent for p in Path(rootdir).rglob("*/_delta_log/")]
    return {p.parent.name + "_" + p.name: p for p in paths}


def register_duckdb_views(con: duckdb.DuckDBPyConnection, paths: dict[str, Path]):
    for name, path in paths.items():
        try:
            dt = DeltaTable(path.absolute())
            con.register(name, dt.to_pyarrow_dataset())
        except PyDeltaTableError as e:
            logger.warning(f"Could not register view for {name}. Error: {e}")
            pass


async def get_partitions(dataframe: Dataframe, params: BaseModel, config: Config) -> Optional[list]:
    parts = (
        await filter_partitions_based_on_params(
            DeltaTable(dataframe.uri).metadata(),
            params.dict(exclude_unset=True) if params else {},
            config.params or [],
        )
        if config.dataframe.file_type == "delta"
        else None
    )
    return parts


def remove_search(prm_dict: dict, config: Config):
    if not config.search or len(prm_dict) == 0:
        return prm_dict
    search_cols = [s.name.lower() for s in config.search]
    return {k: v for k, v in prm_dict.items() if k.lower() not in search_cols}


async def get_expr(columns: List[str], config: Config, params: BaseModel) -> Optional[pypika.Criterion]:
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
    metamodel: Optional[ResultData],
    config: Config,
    router: APIRouter,
):
    route = config.real_route + "/metadata_detail"

    @router.get(
        route,
        tags=["metadata", "metadata:" + config.tag],
        operation_id=config.tag + "_" + config.name,
        name=config.name + "_metadata",
    )
    def get_detailed_metadata():
        import json

        from bmsdna.lakeapi.context.df_duckdb import DuckDbExecutionContext

        with DuckDbExecutionContext() as context:
            realdataframe = Dataframe(
                config.tag,
                config.name,
                config.dataframe,
                context,
            )
            partition_columns = []
            partition_values = None
            delta_tbl = None
            df = realdataframe.get_df(None)
            if config.dataframe.file_type == "delta":
                delta_tbl = DeltaTable(realdataframe.uri)
                partition_columns = delta_tbl.metadata().partition_columns
                if len(partition_columns) > 0:
                    qb: QueryBuilder = (
                        df.query_builder().select(*[pypika.Field(c) for c in partition_columns]).distinct()
                    )
                    partition_values = context.execute_sql(qb).to_arrow_table().to_pylist()
            schema = df.arrow_schema()
            str_cols = [name for name in schema.names if pa.types.is_string(schema.field(name).type)]

            str_lengths_df = (
                context.execute_sql(
                    df.query_builder().select(
                        *[fn.Function("MAX", fn.Function("LEN", fn.Field(sc))).as_(sc) for sc in str_cols]
                    )
                )
                .to_arrow_table()
                .to_pylist()
            )
            str_lengths = str_lengths_df[0]

            def _recursive_get_type(t: Optional[pa.DataType]):
                if t is None:
                    return None
                return {
                    "type_str": str(t),
                    "base_type": str(t),  # legacy
                    "fields": [
                        {"name": f.name, "type": _recursive_get_type(f.type)}
                        for f in [t.field(find) for find in range(0, cast(pa.StructType, t).num_fields)]
                    ]
                    if pa.types.is_struct(t)
                    else None,
                    "inner": _recursive_get_type(t.value_type) if pa.types.is_list(t) else None,
                }

            schema = df.arrow_schema()
            return {
                "partition_values": partition_values,
                "partition_columns": partition_columns,
                "max_string_lengths": str_lengths,
                "schema": [{"name": n, "type": _recursive_get_type(schema.field(n).type)} for n in schema.names],
                "delta_meta": _to_dict(delta_tbl.metadata() if delta_tbl else None),
                "delta_schema": json.loads(delta_tbl.schema().to_json()) if delta_tbl else None,
                "parameters": config.params,
                "search": config.search,
            }


def exclude_cols(columns: List[str]) -> List[str]:
    columns = [c for c in columns if not should_hide_colname(c)]
    return columns


def create_config_endpoint(
    metamodel: Optional[ResultData],
    apimethod: Literal["get", "post"],
    config: Config,
    router: APIRouter,
    response_model: Type,
):
    route = config.real_route

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
        username=Depends(get_current_username),
    ):  # type: ignore
        logger.info(f"{params.dict(exclude_unset=True) if params else None}Union[ ,  ]{request.url.path}")

        engine = engine or config.engine

        logger.info(f"Engine: {engine}")

        ExecutionContext = get_context_by_engine(engine)

        with ExecutionContext() as context:
            realdataframe = Dataframe(
                config.tag,
                config.name,
                config.dataframe,
                context,
            )

            parts = await get_partitions(realdataframe, params, config)
            df = realdataframe.get_df(parts or None)

            expr = await get_expr(df.columns(), config, params)

            new_query = df.query_builder()
            new_query = new_query.where(expr) if expr is not None else new_query

            columns = exclude_cols(df.columns())

            if columns:
                new_query = new_query.select(*columns)
            if select:
                import pypika

                new_query = new_query.select(*[pypika.Field(s) for s in select.split(",")])
            if distinct:
                new_query = new_query.distinct()

            if not (limit == -1 and config.allow_get_all_pages):
                limit = 1000 if limit == -1 else limit
                new_query = new_query.offset(offset or 0).limit(limit)

            if config.search is not None and params is not None:
                search_dict = {c.name.lower(): c for c in config.search}
                searches = {
                    k: (v, search_dict[k.lower()])
                    for k, v in params.dict(exclude_unset=True).items()
                    if k.lower() in search_dict
                }
                if len(searches) > 0:
                    import pypika.queries
                    import pypika.terms

                    source_view = realdataframe.tablename
                    context.init_search(source_view, config.search)
                    score_sum = None
                    for search_key, (search_val, search_cfg) in searches.items():
                        score_sum = (
                            context.search_score_function(source_view, search_val, search_cfg, alias=None)
                            if score_sum is None
                            else score_sum
                            + context.search_score_function(source_view, search_val, search_cfg, alias=None)
                        )
                    assert score_sum is not None
                    new_query = new_query.select(score_sum.as_("search_score"))
                    new_query = new_query.where(pypika.terms.NotNullCriterion(pypika.queries.Field("search_score")))
                    new_query = new_query.orderby(pypika.queries.Field("search_score"), order=pypika.Order.desc)

            logger.info(f"Query: {new_query.get_sql()}")
            df2 = context.execute_sql(new_query)

            try:
                return await create_response(
                    username,
                    request.url,
                    format or request.headers["Accept"],
                    df2,
                    context,
                )
            except Exception as err:
                logger.error("Error in creating response", exc_info=err)
                raise HTTPException(status_code=500)


duckcon: Optional[duckdb.DuckDBPyConnection] = None


def create_sql_endpoint(router: APIRouter):
    paths = get_delta_folders(DATA_PATH)

    @router.on_event("shutdown")
    async def startup_event():
        global duckcon
        if duckcon is not None:
            duckcon.close()

    @router.get("/api/sql/tables", tags=["sql"], operation_id="get_sql_tables")
    async def get_sql_tables(
        request: Request,
        background_tasks: BackgroundTasks,
        Accept: Union[str, None] = Header(default=None),
        format: Optional[OutputFileType] = "json",
        username=Depends(get_current_username),
    ):
        try:
            return [table for table in paths]
        except Exception as err:
            raise HTTPException(status_code=500, detail=str(err))

    @router.post(
        "/api/sql",
        tags=["sql"],
        operation_id="post_sql_endpoint",
    )
    async def get_sql_post(
        request: Request,
        background_tasks: BackgroundTasks,
        Accept: Union[str, None] = Header(default=None),
        format: Optional[OutputFileType] = "json",
        username=Depends(get_current_username),
    ):
        try:
            body = await request.body()
            from bmsdna.lakeapi.context.df_duckdb import DuckDbExecutionContextBase

            global duckcon
            if duckcon is None:
                duckcon = duckdb.connect()
                register_duckdb_views(duckcon, paths)
            context = DuckDbExecutionContextBase(duckcon)
            df = context.execute_sql(body.decode("utf-8"))

            return await create_response(username, request.url, format or request.headers["Accept"], df, context)
        except Exception as err:
            logger.error(err)
            raise HTTPException(status_code=500, detail=str(err))

    @router.get(
        "/api/sql",
        tags=["sql"],
        operation_id="get_sql_endpoint",
    )
    async def get_sql_get(
        request: Request,
        background_tasks: BackgroundTasks,
        sql: str,
        Accept: Union[str, None] = Header(default=None),
        format: Optional[OutputFileType] = "json",
        username=Depends(get_current_username),
    ):
        try:
            from bmsdna.lakeapi.context.df_duckdb import DuckDbExecutionContextBase

            global duckcon
            if duckcon is None:
                duckcon = duckdb.connect()
                register_duckdb_views(duckcon, paths)
            context = DuckDbExecutionContextBase(duckcon)

            df = context.execute_sql(sql)
            return await create_response(username, request.url, format or request.headers["Accept"], df, context)
        except Exception as err:
            logger.error(err)
            raise HTTPException(status_code=500, detail=str(err))
