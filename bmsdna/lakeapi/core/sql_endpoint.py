from typing import Any, Optional, Callable, Union
import duckdb
from fastapi import APIRouter, BackgroundTasks, Header, Request
from bmsdna.lakeapi.context.df_base import ExecutionContext
from bmsdna.lakeapi.core.config import BasicConfig, Config, Configs, Param, SearchConfig
from bmsdna.lakeapi.context.df_duckdb import DuckDbExecutionContextBase
from bmsdna.lakeapi.core.dataframe import Dataframe
from bmsdna.lakeapi.core.types import OutputFileType
from bmsdna.lakeapi.core.response import create_response

duckcon: Optional[duckdb.DuckDBPyConnection] = None


def init_duck_con(con: DuckDbExecutionContextBase, basic_config: BasicConfig, configs: Configs):
    for cfg in configs:
        df = Dataframe(cfg.version_str, cfg.tag, cfg.name, cfg.datasource, con, basic_config)
        if df.file_exists():
            con.register_dataframe(df.tablename, df.uri, df.config.file_type, None)


def get_duckdb_con(basic_config: BasicConfig, configs: Configs):
    global duckcon
    if duckcon is None:
        duckcon = duckdb.connect()
        context = DuckDbExecutionContextBase(duckcon)
        init_duck_con(context, basic_config, configs)
        if basic_config.prepare_sql_db_hook is not None:
            basic_config.prepare_sql_db_hook(context)

    else:
        context = DuckDbExecutionContextBase(duckcon)
    return context


def create_sql_endpoint(
    router: APIRouter,
    basic_config: BasicConfig,
    configs: Configs,
):
    @router.on_event("shutdown")
    async def shutdown_event():
        global duckcon
        if duckcon is not None:
            duckcon.close()

    @router.get("/api/sql/tables", tags=["sql"], operation_id="get_sql_tables")
    async def get_sql_tables(
        request: Request,
        background_tasks: BackgroundTasks,
        Accept: Union[str, None] = Header(default=None),
        format: Optional[OutputFileType] = "json",
    ):
        con = get_duckdb_con(basic_config, configs)
        return (
            con.execute_sql("SELECT table_name, table_type from information_schema.tables where table_schema='main'")
            .to_arrow_table()
            .to_pylist()
        )

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
    ):
        body = await request.body()
        from bmsdna.lakeapi.context.df_duckdb import DuckDbExecutionContextBase

        con = get_duckdb_con(basic_config, configs)
        df = con.execute_sql(body.decode("utf-8"))

        return await create_response(
            request.url, format or request.headers["Accept"], df, con, basic_config=basic_config
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
    ):
        from bmsdna.lakeapi.context.df_duckdb import DuckDbExecutionContextBase

        con = get_duckdb_con(basic_config, configs)

        df = con.execute_sql(sql)
        return await create_response(
            request.url, format or request.headers["Accept"], df, con, basic_config=basic_config
        )
