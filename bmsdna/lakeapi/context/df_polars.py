from datetime import datetime
from bmsdna.lakeapi.context.df_base import ExecutionContext, ResultData
import polars as pl
from bmsdna.lakeapi.polars_extensions.delta import *
from polars.datatypes.convert import DataTypeMappings, py_type_to_arrow_type
import pyarrow as pa
from typing import List, Optional, Tuple
import threading
from bmsdna.lakeapi.core.types import FileTypes
import pyarrow.dataset
import pypika.queries
import pypika
from uuid import uuid4


class PolarsResultData(ResultData):
    def __init__(self, df: pl.Union[DataFrame, pl].LazyFrame, sql_context: pl.SQLContext):
        self.df = df
        self.random_name = "tbl_" + str(uuid4())
        self.registred_df = False
        self.sql_context = sql_context

    def columns(self):
        return self.df.columns

    def query_builder(self) -> pypika.queries.QueryBuilder:
        if not self.registred_df:
            if isinstance(self.df, pl.DataFrame):
                self.df = self.df.lazy()

            self.sql_context.register(self.random_name, cast(pl.LazyFrame, self.df))
            self.registred_df = True
        return pypika.Query.from_(self.random_name)

    def _to_arrow_type(self, t: pl.PolarsDataType):
        if isinstance(t, pl.Struct):
            return pa.struct({f.name: self._to_arrow_type(f.dtype) for f in t.fields})
        if isinstance(t, pl.List):
            return pa.list_(self._to_arrow_type(t.inner) if t.inner else pa.string)
        if t in DataTypeMappings.PY_TYPE_TO_ARROW_TYPE:
            return py_type_to_arrow_type(t)
        raise ValueError("not supported")

    def arrow_schema(self) -> pa.Schema:
        if isinstance(self.df, pl.LazyFrame):
            return pa.Schema([pa.field(k, self._to_arrow_type(v)) for k, v in self.df.schema.items()])
        else:
            return self.df.limit(0).to_arrow().schema

    def to_pandas(self):
        if isinstance(self.df, pl.LazyFrame):
            self.df = self.df.collect(streaming=True)
        return self.df.to_pandas()

    def to_arrow_table(self):
        if isinstance(self.df, pl.LazyFrame):
            self.df = self.df.collect()
        assert isinstance(self.df, pl.DataFrame)
        return self.df.to_arrow()

    def to_arrow_recordbatch(self, chunk_size: int = 10000):
        if isinstance(self.df, pl.LazyFrame):
            self.df = self.df.collect()
        assert isinstance(self.df, pl.DataFrame)
        pat = self.df.to_arrow()
        return pat.to_reader(max_chunksize=chunk_size)

    def write_parquet(self, file_name: str):
        if isinstance(self.df, pl.LazyFrame):
            self.df = self.df.collect(streaming=True)
        self.df.write_parquet(file_name, use_pyarrow=True)

    def write_json(self, file_name: str):
        if isinstance(self.df, pl.LazyFrame):
            self.df = self.df.collect(streaming=True)
        self.df.write_json(file_name, pretty=False, row_oriented=True)

    def write_nd_json(self, file_name: str):
        if isinstance(self.df, pl.LazyFrame):
            self.df = self.df.collect(streaming=True)
        self.df.write_ndjson(file_name)


class PolarsExecutionContext(ExecutionContext):
    def __init__(self, sql_context: pl.Optional[SQLContext] = None):
        super().__init__()
        self.sql_context = sql_context or pl.SQLContext()

    def register_arrow(self, name: str, ds: pyarrow.dataset.Union[Dataset, pyarrow].Table):
        ds = pl.scan_pyarrow_dataset(ds) if isinstance(ds, pyarrow.dataset.Dataset) else pl.from_arrow(ds)

        if isinstance(ds, pl.DataFrame):
            ds = ds.lazy()

        ds = pl.scan_ds(ds)
        self.sql_context.register(name, ds)

    def close(self):
        pass

    def register_dataframe(
        self,
        name: str,
        uri: str,
        file_type: FileTypes,
        partitions: Optional[List[Tuple[str, str, Any]]],
    ):
        self.modified_dates[name] = self.get_modified_date(uri, file_type)
        match file_type:
            case "delta":
                df = pl.scan_delta2(  # type: ignore
                    uri,
                    pyarrow_options={"partitions": partitions},
                )
            case "parquet_withdecimal":  # differentiation no longer needed
                df = pl.scan_parquet(uri)
            case "parquet":
                df = pl.scan_parquet(uri)
            case "arrow":
                df = pl.scan_ipc(uri)
            case "avro":
                df = cast(pl.LazyFrame, pl.read_avro(uri))
            case "csv":
                df = pl.scan_csv(uri)
            case "json":
                df = cast(pl.LazyFrame, pl.read_json(uri))
            case "ndjson":
                df = pl.scan_ndjson(uri)
            case _:
                raise Exception(f"Not supported file type {file_type}")
        self.sql_context.register(name, df)

    def execute_sql(
        self,
        sql: pypika.queries.Union[
            QueryBuilder,
            str,
        ],
    ) -> PolarsResultData:
        df = self.sql_context.execute(sql.get_sql() if not isinstance(sql, str) else sql)
        if isinstance(df, pl.LazyFrame):
            df = df.collect()
        return PolarsResultData(df, self.sql_context)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


local = threading.local()
local.exec_context = None


def get_polars_context() -> ExecutionContext:
    if local.exec_context is None:
        local.exec_context = PolarsExecutionContext()
    return local.exec_context
