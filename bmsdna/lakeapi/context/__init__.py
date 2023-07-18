from bmsdna.lakeapi.context.df_base import ExecutionContext
from bmsdna.lakeapi.core.types import Engines


def get_context_by_engine(engine: Engines, chunk_size: int) -> ExecutionContext:
    match engine.lower():
        case "duckdb":
            from bmsdna.lakeapi.context.df_duckdb import DuckDbExecutionContext

            return DuckDbExecutionContext(chunk_size=chunk_size)
        case "polars":
            from bmsdna.lakeapi.context.df_polars import PolarsExecutionContext

            return PolarsExecutionContext(chunk_size=chunk_size)
        case "odbc":
            from bmsdna.lakeapi.context.df_odbc import ODBCExecutionContext

            return ODBCExecutionContext(chunk_size=chunk_size)

        case "sqlite":
            from bmsdna.lakeapi.context.df_sqlite import SqliteExecutionContext

            return SqliteExecutionContext(chunk_size=chunk_size)

        case _:
            raise Exception(f"Unknown engine {engine}")


class ExecutionContextManager:
    def __init__(self, default_engine: str):
        self.default_engine = default_engine
        self.contexts: [str, ExecutionContext] = dict()

    def get_context(self, engine: str | None, chunk_size: int):
        real_engine = engine or self.default_engine
        if real_engine not in self.contexts:
            self.contexts[real_engine] = get_context_by_engine(real_engine, chunk_size)
        return self.contexts[real_engine]

    def __enter__(self, *args, **kwargs):
        return self

    def __exit__(self, *args, **kwargs):
        for _, ctx in self.contexts.items():
            ctx.__exit__(*args, **kwargs)
        self.contexts = dict()
