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
        case _:
            raise Exception(f"Unknown engine {engine}")
