from bmsdna.lakeapi.core.types import Engines
from bmsdna.lakeapi.context.df_duckdb import DuckDbExecutionContext
from bmsdna.lakeapi.context.df_polars import PolarsExecutionContext
from bmsdna.lakeapi.context.df_datafusion import DatafusionDbExecutionContext


def get_context_by_engine(engine: Engines):
    match engine.lower():
        case "duckdb":
            from bmsdna.lakeapi.context.df_duckdb import DuckDbExecutionContext

            ExecutionContext = DuckDbExecutionContext
        case "polars":
            from bmsdna.lakeapi.context.df_polars import PolarsExecutionContext

            ExecutionContext = PolarsExecutionContext
        case "datafusion":
            from bmsdna.lakeapi.context.df_datafusion import DatafusionDbExecutionContext

            ExecutionContext = DatafusionDbExecutionContext
        case _:
            raise Exception(f"Unknown engine {engine}")
    return ExecutionContext
