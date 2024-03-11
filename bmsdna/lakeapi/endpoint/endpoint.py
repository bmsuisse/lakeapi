from typing import List, Literal, Optional, Type, Union

import pyarrow as pa
import pypika
import pypika.functions as fn
import pypika.queries
import pypika.terms
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError
from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException, Query, Request
from pydantic import BaseModel

from bmsdna.lakeapi.context import get_context_by_engine
from bmsdna.lakeapi.context.source_uri import SourceUri
from bmsdna.lakeapi.context.df_base import ExecutionContext, ResultData, get_sql
from bmsdna.lakeapi.core.config import BasicConfig, Config, Configs
from bmsdna.lakeapi.core.datasource import Datasource, filter_df_based_on_params, filter_partitions_based_on_params
from bmsdna.lakeapi.core.log import get_logger
from bmsdna.lakeapi.core.model import create_parameter_model, create_response_model
from bmsdna.lakeapi.core.partition_utils import should_hide_colname
from bmsdna.lakeapi.core.response import create_response
from bmsdna.lakeapi.core.types import Engines, OutputFileType
from bmsdna.lakeapi.endpoint.endpoint_search import handle_search_request
from bmsdna.lakeapi.endpoint.endpoint_nearby import handle_nearby_request
from starlette.responses import Response

logger = get_logger(__name__)


async def get_partitions(
    datasource: Datasource,
    uri: SourceUri,
    params: BaseModel,
    config: Config,
) -> Optional[list]:
    try:
        df_uri, df_opts = uri.get_uri_options(flavor="object_store")
        parts = (
            await filter_partitions_based_on_params(
                DeltaTable(df_uri, storage_options=df_opts).metadata(),
                params.model_dump(exclude_unset=True) if params else {},
                config.params or [],
            )
            if not config.datasource or config.datasource.file_type == "delta"
            else None
        )
    except (TableNotFoundError, FileNotFoundError) as err:
        logger.warning(f"Could not get partitions for {uri}", exc_info=err)
        parts = None
    return parts


def remove_search_nearby(
    prm_dict: dict,
    config: Config,
):
    if (not config.search and not config.nearby) or len(prm_dict) == 0:
        return prm_dict
    search_nearby_cols = [s.name.lower() for s in (config.search or [])] + [
        s.name.lower() for s in (config.nearby or [])
    ]
    return {k: v for k, v in prm_dict.items() if k.lower() not in search_nearby_cols}


async def get_params_filter_expr(
    context: ExecutionContext,
    columns: List[str],
    config: Config,
    params: BaseModel,
) -> Optional[pypika.Criterion]:
    expr = await filter_df_based_on_params(
        context,
        remove_search_nearby(params.model_dump(exclude_unset=True) if params else {}, config),
        config.params if config.params else [],
        columns,
    )
    return expr


def get_response_model(
    config: Config,
    schema: pa.Schema,
) -> Optional[Type[BaseModel]]:
    response_model: Optional[type[BaseModel]] = None
    try:
        response_model = create_response_model(config.tag + "_" + config.name, schema)
    except Exception as err:
        logger.warning(f"Could not get response type for f{config.route}. Error:{err}")
        response_model = None
    return response_model


def exclude_cols(
    columns: List[str],
) -> List[str]:
    columns = [c for c in columns if not should_hide_colname(c)]
    return columns


def split_csv(csv_str: str) -> list[str]:
    import csv

    reader = csv.reader([csv_str], delimiter=",", quotechar='"')
    for i in reader:
        return i
    raise ValueError("cannot happen")


def is_json_response(
    result,
    args,
    kwargs,
    key=None,
):
    return (
        kwargs.get("format") == "json"
        or kwargs.get("format") == "ndjson"
        or kwargs.get("request").headers.get("Accept") == "application/json"
        or kwargs.get("request").headers.get("Accept") == "application/x-ndjson"
    )


def is_complex_type(
    schema: pa.Schema,
    col_name: str,
):
    f = schema.field(col_name)
    return pa.types.is_nested(f.type)


def create_config_endpoint(
    schema: pa.Schema | None,
    apimethod: Literal["get", "post"],
    config: Config,
    router: APIRouter,
    response_model: Type[BaseModel] | None,
    basic_config: BasicConfig,
    configs: Configs,
):
    route = config.route

    query_model = create_parameter_model(
        schema,
        config.tag + "_" + config.name + "_" + apimethod,
        config.params,
        config.search,
        config.nearby,
        apimethod,
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
    if schema is not None:
        has_complex = any((pa.types.is_struct(t) or pa.types.is_list(t) for t in schema.types))

    @api_method
    async def data(
        request: Request,
        params: query_model = (Depends() if apimethod == "get" else None),  # type: ignore
        Accept: Union[str, None] = Header(default=None),
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        select: Union[str, None] = Query(
            title="$select",
            alias="$select",
            default=None,
            include_in_schema=False,
        ),
        distinct: bool = Query(
            title="$distinct",
            alias="$distinct",
            default=False,
            include_in_schema=False,
        ),
        engine: Engines | None = Query(
            title="$engine",
            alias="$engine",
            default=None,
            include_in_schema=False,
        ),
        format: Optional[OutputFileType] = "json",
        jsonify_complex: bool = Query(
            title="jsonify_complex",
            include_in_schema=has_complex,
            default=False,
        ),
        chunk_size: int | None = Query(
            title="$chunk_size",
            include_in_schema=False,
            default=None,
        ),
    ):  # type: ignore
        logger.debug(f"{params.model_dump(exclude_unset=True) if params else None}Union[ ,  ]{request.url.path}")

        engine = engine or config.engine or basic_config.default_engine

        logger.debug(f"Engine: {engine}")
        real_chunk_size = chunk_size or config.chunk_size or basic_config.default_chunk_size
        context = get_context_by_engine(
            engine,
            chunk_size=real_chunk_size,
        )
        assert config.datasource is not None
        realdataframe = Datasource(
            config.version_str,
            config.tag,
            config.name,
            config=config.datasource,
            sql_context=context,
            basic_config=basic_config,
            accounts=configs.accounts,
        )
        df = realdataframe.get_df()

        expr = await get_params_filter_expr(
            context,
            df.columns(),
            config,
            params,
        )
        base_schema = df.arrow_schema()
        new_query = df.query_builder()
        new_query = new_query.where(expr) if expr is not None else new_query
        columns = exclude_cols(df.columns())
        if select:
            columns = [
                c for c in columns if c in split_csv(select)
            ]  # split , is a bit naive, we might want to support real CSV with quotes here
        if config.datasource.exclude and len(config.datasource.exclude) > 0:
            columns = [c for c in columns if c not in config.datasource.exclude]
        if config.datasource.sortby:
            for s in config.datasource.sortby:
                new_query = new_query.orderby(
                    s.by,
                    order=pypika.Order.desc if s.direction and s.direction.lower() == "desc" else pypika.Order.asc,
                )
        if has_complex and format in ["csv", "excel", "scsv", "csv4excel"]:
            jsonify_complex = True
        if jsonify_complex:
            complex_cols = [c for c in columns if is_complex_type(base_schema, c)]

            new_query = context.jsonify_complex(new_query, complex_cols, columns)
        else:
            new_query = new_query.select(*columns)

        if distinct:
            assert len(columns) <= 3  # reduce complexity here
            new_query = new_query.distinct()

        if not (limit == -1 and config.allow_get_all_pages):
            limit = 1000 if limit == -1 else limit
            new_query = new_query.offset(offset or 0).limit(limit)

        new_query = handle_search_request(
            context,
            config,
            params,
            basic_config,
            source_view=realdataframe.tablename,
            query=new_query,
        )
        new_query = handle_nearby_request(
            context,
            config,
            params,
            basic_config,
            source_view=realdataframe.tablename,
            query=new_query,
        )
        logger.debug(f"Query: {get_sql(new_query)}")

        try:
            return await create_response(
                request.url,
                format or request.headers["Accept"],
                context=context,
                sql=new_query,
                basic_config=basic_config,
                close_context=True,
            )
        except Exception as err:
            logger.error("Error in creating response", exc_info=err)
            raise HTTPException(status_code=500)
