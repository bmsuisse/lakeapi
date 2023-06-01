from typing import Any, Optional, Callable, Union
import duckdb
from fastapi import APIRouter, BackgroundTasks, Header, Query, Request
from bmsdna.lakeapi.context.df_base import ExecutionContext
from bmsdna.lakeapi.core.config import BasicConfig, Config, Configs, Param, SearchConfig
from bmsdna.lakeapi.core.dataframe import Dataframe
from bmsdna.lakeapi.core.types import OutputFileType
from bmsdna.lakeapi.core.response import create_response
from bmsdna.lakeapi.context import get_context_by_engine, Engines

sql_contexts: dict[str, ExecutionContext] = {}


def init_duck_con(con: ExecutionContext, basic_config: BasicConfig, configs: Configs):
    for cfg in configs:
        df = Dataframe(cfg.version_str, cfg.tag, cfg.name, cfg.datasource, con, basic_config)
        if df.file_exists():
            con.register_dataframe(df.tablename, df.uri, df.config.file_type, None)


def get_sql_context(engine: Engines, basic_config: BasicConfig, configs: Configs):
    global sql_contexts
    if not engine in sql_contexts:
        sql_contexts[engine] = get_context_by_engine(engine)
        init_duck_con(sql_contexts[engine], basic_config, configs)
        if basic_config.prepare_sql_db_hook is not None:
            basic_config.prepare_sql_db_hook(sql_contexts[engine])

    return sql_contexts[engine]


def create_sql_endpoint(
    router: APIRouter,
    basic_config: BasicConfig,
    configs: Configs,
):
    @router.on_event("shutdown")
    async def shutdown_event():
        global sql_contexts
        for item in sql_contexts.values():
            item.__exit__()
        sql_contexts = {}

    @router.get("/api/sql/tables", tags=["sql"], operation_id="get_sql_tables")
    async def get_sql_tables(
        request: Request,
        background_tasks: BackgroundTasks,
        Accept: Union[str, None] = Header(default=None),
        format: Optional[OutputFileType] = "json",
        engine: Engines = Query(title="$engine", alias="$engine", default="duckdb", include_in_schema=False),
    ):
        con = get_sql_context(engine, basic_config, configs)
        return con.list_tables().to_arrow_table().to_pylist()

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
        engine: Engines = Query(title="$engine", alias="$engine", default="duckdb", include_in_schema=False),
    ):
        body = await request.body()
        from bmsdna.lakeapi.context.df_duckdb import DuckDbExecutionContextBase

        con = get_sql_context(engine, basic_config, configs)
        df = con.execute_sql(body.decode("utf-8"))

        return await create_response(
            request.url, format or request.headers["Accept"], df, con, basic_config=basic_config, close_context=False
        )

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
        engine: Engines = Query(title="$engine", alias="$engine", default="duckdb", include_in_schema=False),
    ):
        con = get_sql_context(engine, basic_config, configs)

        df = con.execute_sql(sql)
        return await create_response(
            request.url, format or request.headers["Accept"], df, con, basic_config=basic_config, close_context=False
        )
