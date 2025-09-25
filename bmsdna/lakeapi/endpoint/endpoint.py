from typing import List, Literal, Optional, Type, Union, cast, Any

import pyarrow as pa


import sqlglot.expressions as ex

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request
from pydantic import BaseModel

from bmsdna.lakeapi.context import get_context_by_engine
from bmsdna.lakeapi.context.source_uri import SourceUri
from bmsdna.lakeapi.context.df_base import ExecutionContext, get_sql
from bmsdna.lakeapi.core.config import BasicConfig, Config, Configs
from bmsdna.lakeapi.core.datasource import (
    Datasource,
    filter_df_based_on_params,
    get_filters,
    filter_partitions_based_on_params,
)
from bmsdna.lakeapi.core.log import get_logger
from bmsdna.lakeapi.core.model import create_parameter_model, create_response_model
from bmsdna.lakeapi.core.response import create_response
from bmsdna.lakeapi.core.types import Engines, OperatorType, OutputFileType
from bmsdna.lakeapi.endpoint.endpoint_search import handle_search_request
from bmsdna.lakeapi.endpoint.endpoint_nearby import handle_nearby_request
from starlette.concurrency import run_in_threadpool
from bmsdna.lakeapi.utils.async_utils import _async


logger = get_logger(__name__)


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


def get_params_filter_expr(
    context: ExecutionContext,
    columns: List[str],
    config: Config,
    params: BaseModel,
) -> Optional[ex.Condition | ex.Binary]:
    expr = filter_df_based_on_params(
        context,
        remove_search_nearby(
            params.model_dump(exclude_unset=True) if params else {}, config
        ),
        config.params if config.params else [],
        columns,
    )
    return expr


def get_response_model(
    config: Config,
    schema: pa.Schema,
    basic_config: BasicConfig,
) -> Optional[Type[BaseModel]]:
    response_model: Optional[type[BaseModel]] = None
    try:
        response_model = create_response_model(
            config.tag + "_" + config.name, schema, basic_config=basic_config
        )
    except Exception as err:
        logger.warning(f"Could not get response type for f{config.route}. Error:{err}")
        response_model = None
    return response_model


def exclude_cols(columns: List[str], config: BasicConfig) -> List[str]:
    columns = [c for c in columns if not config.should_hide_col_name(c)]
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
        has_complex = any(
            (pa.types.is_struct(t) or pa.types.is_list(t) for t in schema.types)
        )

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
        logger.debug(
            f"{params.model_dump(exclude_unset=True) if params else None}Union[ ,  ]{request.url.path}"
        )

        engine = engine or config.engine or basic_config.default_engine

        logger.debug(f"Engine: {engine}")
        real_chunk_size = (
            chunk_size or config.chunk_size or basic_config.default_chunk_size
        )
        context = get_context_by_engine(
            engine,
            chunk_size=real_chunk_size,
        )
        if not (limit == -1 and config.allow_get_all_pages):
            limit = (1000 if limit == -1 else limit) or 1000
        assert config.datasource is not None
        with Datasource(
            config.version_str,
            config.tag,
            config.name,
            config=config.datasource,
            sql_context=context,
            basic_config=basic_config,
            accounts=configs.accounts,
        ) as realdataframe:
            if params:
                pre_filter, _ = get_filters(
                    params.model_dump(exclude_unset=True),
                    config.params if config.params else [],
                    None,
                )
            else:
                pre_filter = None
            if config.datasource.file_type == "delta" and config.params is not None:
                st = realdataframe.get_delta_table(True)
                if st is not None and params is not None:
                    part_filter = filter_partitions_based_on_params(
                        st, params.model_dump(exclude_unset=True), config.params
                    )
                    if part_filter:
                        pre_filter = list(pre_filter or []) + part_filter
            df = realdataframe.get_df(
                filters=pre_filter,
                limit=limit
                if not config.datasource.sortby and not offset and not limit == -1
                else None,  # if sorted we need all data to sort correctly
            )
            df_cols = df.columns()
            expr = get_params_filter_expr(  # this supports all kinds of filters, while the prefilter only supports equality
                context,
                df_cols,
                config,
                params,
            )
            new_query = df.query_builder()
            new_query = new_query.where(expr) if expr is not None else new_query
            columns = exclude_cols(df_cols, basic_config)
            if select:
                columns = [
                    c for c in columns if c in split_csv(select)
                ]  # split , is a bit naive, we might want to support real CSV with quotes here
            if config.datasource.exclude and len(config.datasource.exclude) > 0:
                columns = [c for c in columns if c not in config.datasource.exclude]
            if config.datasource.sortby:
                for s in config.datasource.sortby:
                    new_query = cast(ex.Select, new_query).order_by(
                        ex.column(s.by, quoted=True).desc()
                        if s.direction and s.direction.lower() == "desc"
                        else ex.column(s.by, quoted=True),
                        copy=False,
                    )
            if has_complex and format in ["csv", "excel", "scsv", "csv4excel"]:
                jsonify_complex = True
            if jsonify_complex:
                base_schema = df.arrow_schema()

                complex_cols = [c for c in columns if is_complex_type(base_schema, c)]

                new_query = context.jsonify_complex(new_query, complex_cols, columns)
            else:
                new_query = new_query.select(
                    *[ex.column(c, quoted=True) for c in columns], append=False
                )

            if distinct:
                assert len(columns) <= 3  # reduce complexity here
                new_query = cast(ex.Select, new_query).distinct()

            if not (limit == -1 and config.allow_get_all_pages):
                limit = (1000 if limit == -1 else limit) or 1000
                new_query = new_query.limit(limit)
                if offset:
                    new_query.offset(offset, copy=False)

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
            logger.debug(f"Query: {get_sql(new_query, dialect='duckdb')}")

            try:
                return await create_response(
                    request.url,
                    request.query_params,
                    format or request.headers["Accept"],
                    context=context,
                    sql=new_query,
                    basic_config=basic_config,
                    close_context=True,
                    charset=request.query_params.get("$encoding"),
                )
            except Exception as err:
                logger.error("Error in creating response", exc_info=err)
                raise HTTPException(status_code=500)
