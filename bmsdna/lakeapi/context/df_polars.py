from deltalake import DeltaTable
from bmsdna.lakeapi.context.df_base import ExecutionContext, FileTypeNotSupportedError, ResultData, get_sql

import os
import pyarrow as pa
from typing import List, Optional, Tuple, Union, cast, TYPE_CHECKING, Any
from bmsdna.lakeapi.core.types import FileTypes
import pyarrow.dataset
import pypika.queries
from pypika.terms import Term
import pypika
from uuid import uuid4

from bmsdna.lakeapi.delta.colmapping import only_fixed_supported
from .source_uri import SourceUri
from deltalake.exceptions import DeltaProtocolError, TableNotFoundError

if TYPE_CHECKING:
    import polars  # we want to lazy import in case we one day no longer rely on polars if only duckdb is needed


class PolarsResultData(ResultData):
    def __init__(
        self,
        df: "Union[polars.DataFrame, polars.LazyFrame]",
        sql_context: "polars.SQLContext",
        chunk_size: int,
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
    def __init__(
        self,
        chunk_size: int,
        sql_context: "Optional[polars.SQLContext]" = None,
    ):
        super().__init__(chunk_size=chunk_size)
        import polars as pl

        self.len_func = "length"
        self.sql_context = sql_context or pl.SQLContext()

    @property
    def supports_view_creation(self) -> bool:
        return True

    def register_arrow(
        self,
        name: str,
        ds: Union[pyarrow.dataset.Dataset, pyarrow.Table],
    ):
        import polars as pl

        ds = pl.scan_pyarrow_dataset(ds) if isinstance(ds, pyarrow.dataset.Dataset) else pl.from_arrow(ds)
        self.sql_context.register(name, ds)

    def close(self):
        pass

    def json_function(self, term: Term, assure_string=False):
        raise NotImplementedError()

    def distance_m_function(
        self,
        lat1: Term,
        lon1: Term,
        lat2: Term,
        lon2: Term,
    ):
        raise NotImplementedError("Not implemented")

    def register_datasource(
        self,
        target_name: str,
        source_table_name: Optional[str],
        uri: SourceUri,
        file_type: FileTypes,
        partitions: Optional[List[Tuple[str, str, Any]]],
    ):
        import polars as pl

        fs, fs_uri = uri.get_fs_spec()
        ab_uri, uri_opts = uri.get_uri_options(flavor="object_store")

        self.modified_dates[target_name] = self.get_modified_date(uri, file_type)
        match file_type:
            case "delta":
                try:
                    dt = DeltaTable(ab_uri, storage_options=uri_opts)
                    if only_fixed_supported(dt):
                        from bmsdna.lakeapi.polars_extensions.scan_delta_union import scan_delta_union

                        df = scan_delta_union(dt)
                    else:
                        df = pl.scan_delta(
                            ab_uri,
                            storage_options=uri_opts,
                            pyarrow_options={
                                "partitions": partitions,
                                "parquet_read_options": {"coerce_int96_timestamp_unit": "us"},
                            },
                        )
                except DeltaProtocolError as de:
                    raise FileTypeNotSupportedError(f"Delta table version {ab_uri} not supported") from de
            case "parquet":
                df = pl.scan_parquet(ab_uri, storage_options=uri_opts)
            case "arrow":
                df = pl.scan_ipc(ab_uri, storage_options=uri_opts)
            case "avro" if uri_opts is None:
                df = cast(pl.LazyFrame, pl.read_avro(ab_uri))
            case "csv" if uri_opts is None:
                df = pl.scan_csv(ab_uri)
            case "csv" if uri_opts is not None:
                df = pl.read_csv(ab_uri)
            case "json" if uri_opts is None:
                df = cast(pl.LazyFrame, pl.read_json(ab_uri))
            case "ndjson" if uri_opts is None:
                df = pl.scan_ndjson(ab_uri)
            case "sqlite" if uri_opts is None:
                query = "SELECT * FROM " + (source_table_name or target_name)

                df = pl.read_database_uri(query=query, uri="sqlite://" + ab_uri, engine="adbc")
            # case "odbc": to be tested, attention on security!
            #             #    query = "SELECT * FROM " + (source_table_name or target_name)
            #    df = pl.read_database(query=query, connection=uri.uri)
            case _:
                raise FileTypeNotSupportedError(f"Not supported file type {file_type}")
        self.sql_context.register(target_name, df)

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
