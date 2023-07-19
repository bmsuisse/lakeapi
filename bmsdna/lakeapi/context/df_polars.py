from bmsdna.lakeapi.context.df_base import ExecutionContext, FileTypeNotSupportedError, ResultData, get_sql

import os
import pyarrow as pa
from typing import List, Optional, Tuple, Union, cast, TYPE_CHECKING, Any
from bmsdna.lakeapi.core.types import FileTypes
import pyarrow.dataset
import pypika.queries
import pypika.terms
import pypika
from uuid import uuid4

if TYPE_CHECKING:
    import polars  # we want to lazy import in case we one day no longer rely on polars if only duckdb is needed


class PolarsResultData(ResultData):
    def __init__(
        self, df: "Union[polars.DataFrame, polars.LazyFrame]", sql_context: "polars.SQLContext", chunk_size: int
    ):
        super().__init__(chunk_size=chunk_size)
        self.df = df
        self.random_name = "tbl_" + str(uuid4())
        self.registred_df = False
        self.sql_context = sql_context

    def columns(self):
        return self.df.columns

    def query_builder(self) -> pypika.queries.QueryBuilder:
        import polars as pl

        if not self.registred_df:
            if isinstance(self.df, pl.DataFrame):
                self.df = self.df.lazy()

            self.sql_context.register(self.random_name, cast(pl.LazyFrame, self.df))
            self.registred_df = True
        return pypika.Query.from_(self.random_name)

    def _to_arrow_type(self, t: "polars.PolarsDataType"):
        import polars as pl
        from polars.datatypes.convert import DataTypeMappings, py_type_to_arrow_type

        if isinstance(t, pl.Struct):
            return pa.struct({f.name: self._to_arrow_type(f.dtype) for f in t.fields})
        if isinstance(t, pl.List):
            return pa.list_(self._to_arrow_type(t.inner) if t.inner else pa.string)

        if t in DataTypeMappings.PY_TYPE_TO_ARROW_TYPE:
            return py_type_to_arrow_type(t)
        raise ValueError("not supported")

    def arrow_schema(self) -> pa.Schema:
        import polars as pl

        if isinstance(self.df, pl.LazyFrame):
            return pa.Schema([pa.field(k, self._to_arrow_type(v)) for k, v in self.df.schema.items()])
        else:
            return self.df.limit(0).to_arrow().schema

    def to_pandas(self):
        import polars as pl

        if isinstance(self.df, pl.LazyFrame):
            self.df = self.df.collect()
        return self.df.to_pandas()

    def to_arrow_table(self):
        import polars as pl

        if isinstance(self.df, pl.LazyFrame):
            self.df = self.df.collect()
        assert isinstance(self.df, pl.DataFrame)
        return self.df.to_arrow()

    def to_arrow_recordbatch(self, chunk_size: int = 10000):
        import polars as pl

        if isinstance(self.df, pl.LazyFrame):
            self.df = self.df.collect()
        assert isinstance(self.df, pl.DataFrame)
        pat = self.df.to_arrow()
        return pat.to_reader(max_chunksize=chunk_size)

    def write_parquet(self, file_name: str):
        import polars as pl

        if isinstance(self.df, pl.LazyFrame):
            self.df = self.df.collect()
        self.df.write_parquet(file_name, use_pyarrow=True)

    def write_json(self, file_name: str):
        import polars as pl

        if isinstance(self.df, pl.LazyFrame):
            self.df = self.df.collect()
        self.df.write_json(file_name, pretty=False, row_oriented=True)

    def write_nd_json(self, file_name: str):
        import polars as pl

        if isinstance(self.df, pl.LazyFrame):
            self.df = self.df.collect()
        self.df.write_ndjson(file_name)


class PolarsExecutionContext(ExecutionContext):
    def __init__(self, chunk_size: int, sql_context: "Optional[polars.SQLContext]" = None):
        super().__init__(chunk_size=chunk_size)
        import polars as pl

        self.len_func = "length"
        self.sql_context = sql_context or pl.SQLContext()

    def register_arrow(self, name: str, ds: Union[pyarrow.dataset.Dataset, pyarrow.Table]):
        import polars as pl

        ds = pl.scan_pyarrow_dataset(ds) if isinstance(ds, pyarrow.dataset.Dataset) else pl.from_arrow(ds)

        self.sql_context.register(name, ds)

    def close(self):
        pass

    def json_function(self, term: pypika.terms.Term, assure_string=False):
        raise NotImplementedError()

    def register_datasource(
        self,
        name: str,
        uri: str,
        file_type: FileTypes,
        partitions: Optional[List[Tuple[str, str, Any]]],
    ):
        import polars as pl

        if os.path.exists(uri):
            self.modified_dates[name] = self.get_modified_date(uri, file_type)
        match file_type:
            case "delta":
                from bmsdna.lakeapi.polars_extensions.delta import scan_delta2

                df = pl.scan_delta2(  # type: ignore
                    uri,
                    pyarrow_options={
                        "partitions": partitions,
                        "parquet_read_options": {"coerce_int96_timestamp_unit": "us"},
                    },
                )
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
                raise FileTypeNotSupportedError(f"Not supported file type {file_type}")
        self.sql_context.register(name, df)

    def execute_sql(
        self,
        sql: Union[
            pypika.queries.QueryBuilder,
            str,
        ],
    ) -> PolarsResultData:
        import polars as pl

        df = self.sql_context.execute(get_sql(sql))
        if isinstance(df, pl.LazyFrame):
            df = df.collect()
        return PolarsResultData(df, self.sql_context, self.chunk_size)

    def list_tables(self) -> ResultData:
        return self.execute_sql("SHOW TABLES")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass
