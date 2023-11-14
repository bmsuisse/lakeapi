from dataclasses import dataclass
from typing import Any, List, Literal, cast
from datetime import datetime, date, time, timedelta
from decimal import Decimal
from typing import Type, Optional
from pydantic import BaseModel


Engines = Literal["duckdb", "polars", "odbc"]


FileTypes = Literal[
    "delta", "parquet", "arrow", "arrow-stream", "avro", "csv", "json", "ndjson", "odbc", "sqlite", "duckdb"
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
    "has",
    "startswith",
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


class MetadataSchemaFieldType(BaseModel):
    type_str: str
    orig_type_str: str
    fields: "Optional[list[MetadataSchemaField]]"
    inner: "Optional[MetadataSchemaFieldType]"


class MetadataSchemaField(BaseModel):
    name: str
    type: MetadataSchemaFieldType
    max_str_length: Optional[int] = None


@dataclass
class SearchConfig:
    name: str
    columns: List[str]


@dataclass
class NearbyConfig:
    name: str
    lat_col: str
    lon_col: str


@dataclass
class Param:
    name: str
    combi: Optional[Optional[List[str]]] = None
    default: Optional[str] = None
    required: Optional[bool] = False
    operators: Optional[list[OperatorType]] = None
    colname: Optional[str] = None

    @property
    def real_default(self) -> str:
        import ast

        self._real_default = ast.literal_eval(self.default) if self.default else None
        return cast(str, self._real_default)


class MetadataDetailResult(BaseModel):
    partition_values: Optional[list[dict[str, Any]]] = None
    partition_columns: List[str]
    max_string_lengths: dict[str, Optional[int]]
    data_schema: list[MetadataSchemaField]
    delta_meta: Optional[dict] = None
    delta_schema: Any = None
    parameters: Optional[List[Any]] = None
    search: Optional[List[Any]] = None
    nearby: Optional[List[Any]] = None
    modified_date: datetime | None = None


MetadataSchemaFieldType.model_rebuild()
MetadataSchemaField.model_rebuild()
MetadataDetailResult.model_rebuild()
