from bmsdna.lakeapi.core.types import Engines
from bmsdna.lakeapi.context.df_duckdb import DuckDbExecutionContext
from bmsdna.lakeapi.context.df_polars import PolarsExecutionContext


def get_context_by_engine(engine: Engines):
    match engine.lower():
        case "duckdb":
            from bmsdna.lakeapi.context.df_duckdb import DuckDbExecutionContext

            ExecutionContext = DuckDbExecutionContext
        case "polars":
            from bmsdna.lakeapi.context.df_polars import PolarsExecutionContext

            ExecutionContext = PolarsExecutionContext
        case _:
            raise Exception(f"Unknown engine {engine}")
    return ExecutionContext
