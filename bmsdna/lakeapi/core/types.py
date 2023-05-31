from dataclasses import dataclass
from typing import Any, List, Literal, cast
from datetime import datetime, date, time, timedelta
from decimal import Decimal
from typing import Type, Optional, TYPE_CHECKING
from typing_extensions import TypedDict
from polars.datatypes.convert import DataTypeMappings
from pydantic import BaseModel


Engines = Literal["duckdb", "polars"]


FileTypes = Literal[
    "delta",
    "parquet",
    "arrow",
    "arrow-stream",
    "avro",
    "csv",
    "json",
    "ndjson",
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


class MetadataSchemaFieldType(BaseModel):
    type_str: str
    orig_type_str: str
    fields: "Optional[list[MetadataSchemaField]]"
    inner: "Optional[MetadataSchemaFieldType]"


class MetadataSchemaField(BaseModel):
    name: str
    type: MetadataSchemaFieldType


@dataclass
class SearchConfig:
    name: str
    columns: List[str]


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
    partition_values: Optional[list[dict[str, Any]]]
    partition_columns: List[str]
    max_string_lengths: dict[str, Optional[int]]
    data_schema: list[MetadataSchemaField]
    delta_meta: Optional[dict]
    delta_schema: Any
    parameters: Optional[List[Any]]
    search: Optional[List[Any]]


MetadataSchemaFieldType.update_forward_refs()
MetadataSchemaField.update_forward_refs()
MetadataDetailResult.update_forward_refs()
