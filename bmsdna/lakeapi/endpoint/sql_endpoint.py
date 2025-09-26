from typing import Optional, Union
from fastapi import APIRouter, BackgroundTasks, Header, Query, Request, Response
from bmsdna.lakeapi.context.df_base import ExecutionContext, FileTypeNotSupportedError
from bmsdna.lakeapi.core.config import BasicConfig, Configs
from bmsdna.lakeapi.core.datasource import Datasource
from bmsdna.lakeapi.core.log import get_logger
from bmsdna.lakeapi.core.types import OutputFileType
from bmsdna.lakeapi.core.response import create_response
from bmsdna.lakeapi.context import get_context_by_engine, Engines


logger = get_logger(__name__)


def _register_tables(
    con: ExecutionContext,
    basic_config: BasicConfig,
    configs: Configs,
    tables: list[str],
):
    for cfg in configs:
        assert cfg.datasource is not None
        with Datasource(
            cfg.version_str,
            cfg.tag,
            cfg.name,
            config=cfg.datasource,
            sql_context=con,
            accounts=configs.accounts,
            basic_config=basic_config,
        ) as df:
            if cfg.engine != "odbc" and df.file_exists():
                try:
                    if df.unique_table_name in tables:
                        con.register_datasource(
                            df.unique_table_name,
                            df.tablename,
                            df.get_execution_uri(False),
                            df.config.file_type,
                            None,
                        )
                except (FileTypeNotSupportedError, FileNotFoundError):
                    logger.warning(f"Cannot query {df.tablename}")


def create_sql_endpoint(
    router: APIRouter,
    basic_config: BasicConfig,
    configs: Configs,
):
    @router.get("/api/sql/tables", tags=["sql"], operation_id="get_sql_tables")
    async def get_sql_tables(
        request: Request,
        background_tasks: BackgroundTasks,
        Accept: Union[str, None] = Header(default=None),
        format: Optional[OutputFileType] = "json",
        engine: Engines = Query(
            title="$engine", alias="$engine", default="duckdb", include_in_schema=False
        ),
    ):
        engine = engine or basic_config.default_engine
        if engine == "odbc":
            with get_context_by_engine(
                engine,
                basic_config.default_chunk_size,
            ) as con:
                return await con.list_tables().to_pylist()
        else:
            tbls = []
            with get_context_by_engine(
                engine,
                basic_config.default_chunk_size,
            ) as con:
                for cfg in configs:
                    assert cfg.datasource is not None
                    with Datasource(
                        cfg.version_str,
                        cfg.tag,
                        cfg.name,
                        config=cfg.datasource,
                        sql_context=con,
                        accounts=configs.accounts,
                        basic_config=basic_config,
                    ) as df:
                        if cfg.engine != "odbc" and df.file_exists():
                            tbls.append(df.unique_table_name)
            return tbls

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
        sql = body.decode("utf-8")
        con = None
        try:
            con = get_context_by_engine(
                engine,
                basic_config.default_chunk_size,
            )
            import sqlglot as sg
            import sqlglot.expressions as exp

            expr = sg.parse_one(sql, dialect=con.dialect)
            if not isinstance(
                expr, (exp.Select, exp.Union, exp.Intersect, exp.Except, exp.CTE)
            ):
                return Response(
                    status_code=400, content="Only a single SELECT statement is allowed"
                )
            table_names = [t.name for t in expr.find_all(exp.Table)]
            _register_tables(con, basic_config, configs, table_names)

            return await create_response(
                request.url,
                request.query_params,
                format or request.headers["Accept"],
                con,
                sql,
                basic_config=basic_config,
                close_context=True,
            )
        except Exception as e:
            if con:
                con.close()
            raise e

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
        con = None
        try:
            con = get_context_by_engine(
                engine,
                basic_config.default_chunk_size,
            )
            import sqlglot as sg
            import sqlglot.expressions as exp

            expr = sg.parse_one(sql, dialect=con.dialect)
            if not isinstance(
                expr, (exp.Select, exp.Union, exp.Intersect, exp.Except, exp.CTE)
            ):
                return Response(
                    status_code=400, content="Only a single SELECT statement is allowed"
                )
            table_names = [t.name for t in expr.find_all(exp.Table)]
            _register_tables(con, basic_config, configs, table_names)

            return await create_response(
                request.url,
                request.query_params,
                format or request.headers["Accept"],
                con,
                sql,
                basic_config=basic_config,
                close_context=True,
            )
        except Exception as e:
            if con:
                con.close()
            raise e
