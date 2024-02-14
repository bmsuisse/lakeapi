from typing import Callable
from bmsdna.lakeapi.context.df_base import ExecutionContext
from bmsdna.lakeapi.core.types import Engines


def _duckdb(chunk_size: int):
    from bmsdna.lakeapi.context.df_duckdb import DuckDbExecutionContext

    return DuckDbExecutionContext(chunk_size=chunk_size)


def _polars(chunk_size: int):
    from bmsdna.lakeapi.context.df_polars import PolarsExecutionContext

    return PolarsExecutionContext(chunk_size=chunk_size)


def _odbc(chunk_size: int):
    from bmsdna.lakeapi.context.df_odbc import ODBCExecutionContext

    return ODBCExecutionContext(chunk_size=chunk_size)


engine_registry = {"duckdb": _duckdb, "polars": _polars, "odbc": _odbc}


def register_engine(engine: Engines, factory: Callable[[int], ExecutionContext]):
    engine_registry[engine] = factory


def get_context_by_engine(
    engine: Engines,
    chunk_size: int,
) -> ExecutionContext:
    return engine_registry[engine](chunk_size)


class ExecutionContextManager:
    default_engine: Engines

    def __init__(
        self,
        default_engine: Engines,
        default_chunk_size: int,
    ):
        self.default_engine = default_engine
        self.contexts: dict[str, ExecutionContext] = dict()
        self.default_chunk_size = default_chunk_size

    def get_context(
        self,
        engine: Engines | None,
        chunk_size: int | None = None,
    ):
        real_engine: Engines = engine or self.default_engine
        if real_engine not in self.contexts:
            self.contexts[real_engine] = get_context_by_engine(real_engine, chunk_size or self.default_chunk_size)
        return self.contexts[real_engine]

    def __enter__(self, *args, **kwargs):
        return self

    def __exit__(self, *args, **kwargs):
        for _, ctx in self.contexts.items():
            ctx.__exit__(*args, **kwargs)
        self.contexts = dict()
