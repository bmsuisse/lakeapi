from typing import Optional, Union
from fastapi import APIRouter, BackgroundTasks, Header, Query, Request
from bmsdna.lakeapi.context.df_base import ExecutionContext, FileTypeNotSupportedError
from bmsdna.lakeapi.core.config import BasicConfig, Config, Configs, Param, SearchConfig
from bmsdna.lakeapi.core.datasource import Datasource
from bmsdna.lakeapi.core.log import get_logger
from bmsdna.lakeapi.core.types import OutputFileType, FileTypes
from bmsdna.lakeapi.core.response import create_response
from bmsdna.lakeapi.context import get_context_by_engine, Engines
from deltalake.exceptions import TableNotFoundError

sql_contexts: dict[str, ExecutionContext] = {}

logger = get_logger(__name__)


def init_duck_con(
    con: ExecutionContext,
    basic_config: BasicConfig,
    configs: Configs,
):
    for cfg in configs:
        assert cfg.datasource is not None
        df = Datasource(
            cfg.version_str,
            cfg.tag,
            cfg.name,
            config=cfg.datasource,
            sql_context=con,
            accounts=configs.accounts,
            basic_config=basic_config,
        )

        if cfg.engine != "odbc" and df.file_exists():
            try:
                con.register_datasource(
                    df.unique_table_name,
                    df.tablename,
                    df.execution_uri,
                    df.config.file_type,
                    None,
                )
            except (FileTypeNotSupportedError, TableNotFoundError, FileNotFoundError) as err:
                logger.warning(f"Cannot query {df.tablename}")


def _get_sql_context(
    engine: Engines,
    basic_config: BasicConfig,
    configs: Configs,
):
    assert engine not in ["odbc", "sqlite"]
    global sql_contexts
    if not engine in sql_contexts:
        sql_contexts[engine] = get_context_by_engine(
            engine,
            basic_config.default_chunk_size,
        )
        init_duck_con(
            sql_contexts[engine],
            basic_config,
            configs,
        )
        if basic_config.prepare_sql_db_hook is not None:
            basic_config.prepare_sql_db_hook(
                sql_contexts[engine],
            )

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
        con = _get_sql_context(engine, basic_config, configs)
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
        engine: Engines = Query(
            title="$engine",
            alias="$engine",
            default="duckdb",
            include_in_schema=False,
        ),
    ):
        body = await request.body()
        from bmsdna.lakeapi.context.df_duckdb import DuckDbExecutionContextBase

        con = _get_sql_context(engine, basic_config, configs)

        sql = body.decode("utf-8")

        return await create_response(
            request.url,
            format or request.headers["Accept"],
            con,
            sql,
            basic_config=basic_config,
            close_context=False,
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
        engine: Engines = Query(
            title="$engine",
            alias="$engine",
            default="duckdb",
            include_in_schema=False,
        ),
    ):
        con = _get_sql_context(engine, basic_config, configs)

        df = con.execute_sql(sql)
        return await create_response(
            request.url,
            format or request.headers["Accept"],
            con,
            sql,
            basic_config=basic_config,
            close_context=False,
        )
