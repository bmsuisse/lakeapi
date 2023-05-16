from typing import Literal
from datetime import datetime, date, time, timedelta
from decimal import Decimal
from typing import Type
from polars.datatypes.convert import DataTypeMappings


Engines = Literal["duckdb", "polars", "datafusion"]


FileTypes = Literal[
    "delta",
    "parquet",
    "arrow",
    "arrow-stream",
    "avro",
    "csv",
    "json",
    "ndjson",
    "parquet_withdecimal",
]
OutputFileType = Literal[
    "json",
    "ndjson",
    "parquet",
    "csv",
    "csv4excel",
    "xlsx",
    "feather",
    "html",
    "scsv",
    "xml",
    "ipc",
    "arrow",
    "arrow-stream",
]
OperatorType = Literal[
    "<",
    "=",
    ">",
    ">=",
    "<=",
    "<>",
    "contains",
    "in",
    "not contains",
    "not in",
    "not null",
    "null",
    "between",
    "not between",
]
DeltaOperatorTypes = Literal["<", "=", ">", ">=", "<=", "in", "not in"]
PolaryTypeFunction = Literal[
    "sum",
    "count",
    "mean",
    "median",
    "min",
    "max",
    "first",
    "std",
    "n_unique",
    "distinct",
    "mode",
    "null_count",
]

CONFIG_DTYPE_MAP: dict[str, Type] = {
    "struct": dict,
    "string": str,
    "integer": int,
    "float": float,
    "array": list,
    "double": Decimal,
    "timesamp": datetime,
    "date": date,
    "time": time,
    "duration": timedelta,
    "str": str,
    "int": int,
    "bool": bool,
    "boolean": bool,
}


DTYPE_MAP = {
    "struct": dict,
    "string": str,
    "integer": int,
    "float": float,
    "array": list,
    "double": Decimal,
    "timesamp": datetime,
    "date": date,
    "time": time,
}
