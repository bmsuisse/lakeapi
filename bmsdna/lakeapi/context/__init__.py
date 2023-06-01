from bmsdna.lakeapi.context.df_base import ExecutionContext
from bmsdna.lakeapi.core.types import Engines


def get_context_by_engine(engine: Engines) -> ExecutionContext:
    match engine.lower():
        case "duckdb":
            from bmsdna.lakeapi.context.df_duckdb import DuckDbExecutionContext

            return DuckDbExecutionContext()
        case "polars":
            from bmsdna.lakeapi.context.df_polars import PolarsExecutionContext

            return PolarsExecutionContext()
        case _:
            raise Exception(f"Unknown engine {engine}")
