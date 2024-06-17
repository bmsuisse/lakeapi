from bmsdna.lakeapi.context.df_base import (
    ExecutionContext,
    FileTypeNotSupportedError,
    ResultData,
    get_sql,
)

import pyarrow as pa
from typing import List, Optional, Tuple, Union, cast, Any, TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl
from bmsdna.lakeapi.core.types import FileTypes, OperatorType
import pyarrow.dataset
import sqlglot.expressions as ex
from sqlglot import from_, parse_one


from uuid import uuid4
import json
from .source_uri import SourceUri
from deltalake.exceptions import DeltaProtocolError
from sqlglot.dialects.postgres import Postgres


class Polars(Postgres):
    class Generator(Postgres.Generator):
        NULL_ORDERING_SUPPORTED = None


polars_dialect = Polars()

try:
    import polars as pl  # we want to lazy import in case we one day no longer rely on polars if only duckdb is needed

    PL_TO_ARROW = {
        pl.Int8: pa.int8(),
        pl.Int16: pa.int16(),
        pl.Int32: pa.int32(),
        pl.Int64: pa.int64(),
        pl.UInt8: pa.uint8(),
        pl.UInt16: pa.uint16(),
        pl.UInt32: pa.uint32(),
        pl.UInt64: pa.uint64(),
        pl.Float32: pa.float32(),
        pl.Float64: pa.float64(),
        pl.Date: pa.date32(),
        pl.Boolean: pa.bool_(),
        pl.Utf8: pa.large_string(),
        pl.Binary: pa.binary(),
        pl.Categorical: pa.large_string(),
    }
except ImportError:
    PL_TO_ARROW = {}
    pass


class PolarsResultData(ResultData):
    def __init__(
        self,
        sql: Union[ex.Query, str],
        sql_context: "pl.SQLContext",
        chunk_size: int,
    ):
        super().__init__(chunk_size=chunk_size)
        self.sql = sql
        self._df = None
        self.random_name = "tbl_" + str(uuid4()).replace("-", "")
        self.registred_df = False
        self.sql_context = sql_context

    def get_df(self):
        if self._df is None:
            real_sql = get_sql(self.sql, dialect=polars_dialect)
            self._df = self.sql_context.execute(real_sql)
        return self._df

    def get_df_collected(self) -> "pl.DataFrame":
        _df = self.get_df()
        if isinstance(_df, pl.LazyFrame):
            _df = _df.collect()
            self._df = _df
        return _df

    def columns(self):
        if self._df is None:
            _df = pl.DataFrame(
                [],
                schema=self.sql_context.execute(
                    get_sql(self.sql, limit=0, dialect=polars_dialect)
                ).collect_schema(),
            )
            return _df.columns
        return self._df.columns

    def query_builder(self) -> ex.Query:
        if not isinstance(self.sql, str):
            return from_(self.sql.subquery(alias="s1"))
        else:
            return from_(
                cast(ex.Select, parse_one(self.sql, dialect=polars_dialect)).subquery(
                    alias="s1"
                )
            )

    def arrow_schema(self) -> pa.Schema:
        if self._df is None:
            _df = pl.DataFrame(
                [],
                schema=self.sql_context.execute(
                    get_sql(self.sql, limit=0, dialect=polars_dialect)
                ).collect_schema(),
            )
        else:
            _df = pl.DataFrame([], self._df.collect_schema())
        return _df.to_arrow().schema

    def to_pandas(self):
        return self.get_df_collected().to_pandas()

    def to_arrow_table(self):
        return self.get_df_collected().to_arrow()

    def to_arrow_recordbatch(self, chunk_size: int = 10000):
        return self.get_df_collected().to_arrow().to_reader(max_chunksize=chunk_size)

    def write_parquet(self, file_name: str):
        self.get_df_collected().write_parquet(file_name, use_pyarrow=True)

    def write_json(self, file_name: str):
        self.get_df_collected().write_json(file_name)

    def write_csv(self, file_name: str, *, separator: str):
        self.get_df_collected().write_csv(file_name, separator=separator)

    def write_nd_json(self, file_name: str):
        self.get_df_collected().write_ndjson(file_name)


class PolarsExecutionContext(ExecutionContext):
    def __init__(
        self,
        chunk_size: int,
        sql_context: "Optional[pl.SQLContext]" = None,
    ):
        super().__init__(chunk_size=chunk_size, engine_name="polars")
        import polars as pl

        self.len_func = "length"
        self.sql_context = sql_context or pl.SQLContext()

    @property
    def dialect(self):
        return polars_dialect

    @property
    def supports_view_creation(self) -> bool:
        return True

    def create_view(self, name: str, sql: str):
        self.sql_context.register(name, self.sql_context.execute(sql))

    def register_arrow(
        self,
        name: str,
        ds: Union[pyarrow.dataset.Dataset, pyarrow.Table],
    ):
        import polars as pl

        ds = (
            pl.scan_pyarrow_dataset(ds)
            if isinstance(ds, pyarrow.dataset.Dataset)
            else pl.from_arrow(ds)
        )
        self.sql_context.register(name, ds)

    def close(self):
        pass

    def json_function(self, term: ex.Expression, assure_string=False):
        raise NotImplementedError()

    def jsonify_complex(
        self, query: ex.Query, complex_cols: list[str], columns: list[str]
    ):
        import polars as pl

        old_query = query.select(*[ex.column(c, quoted=True) for c in columns])
        if len(complex_cols) == 0:
            return old_query

        df = self.sql_context.execute(old_query.sql(polars_dialect))

        def to_json(x):
            if isinstance(x, pl.Series):
                return json.dumps(x.to_list())
            return json.dumps(x)

        map_cols = [
            pl.col(c).map_elements(to_json, return_dtype=pl.Utf8).alias(c)
            for c in complex_cols
        ]
        df = df.with_columns(map_cols)
        nt_id = "tmp_" + str(uuid4())
        self.sql_context.register(nt_id, df)
        return from_(ex.to_identifier(nt_id)).select(
            *[ex.column(c, quoted=True) for c in columns]
        )

    def register_datasource(
        self,
        target_name: str,
        source_table_name: Optional[str],
        uri: SourceUri,
        file_type: FileTypes,
        partitions: Optional[List[Tuple[str, OperatorType, Any]]],
    ):
        import polars as pl

        ab_uri, uri_opts = uri.get_uri_options(flavor="object_store")

        self.modified_dates[target_name] = self.get_modified_date(uri, file_type)
        match file_type:
            case "delta":
                try:
                    db_uri, db_opts = uri.get_uri_options(flavor="deltalake2db")
                    from deltalake2db import polars_scan_delta

                    partition_filter = (
                        {p[0]: p[2] for p in partitions if p[1] == "="}
                        if partitions
                        else None
                    )
                    df = polars_scan_delta(
                        db_uri,
                        storage_options=db_opts,
                        conditions=partition_filter if partition_filter else None,
                    )
                except DeltaProtocolError as de:
                    raise FileTypeNotSupportedError(
                        f"Delta table version {ab_uri} not supported"
                    ) from de
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

                df = pl.read_database_uri(
                    query=query, uri="sqlite://" + ab_uri, engine="adbc"
                )
            case "duckdb" if uri_opts is None:
                query = "SELECT * FROM " + (source_table_name or target_name)
                import duckdb

                t = duckdb.connect(ab_uri).execute(query).fetch_arrow_table()

                df = cast(pl.LazyFrame, pl.from_arrow(t))
            # case "odbc": to be tested, attention on security!
            #             #    query = "SELECT * FROM " + (source_table_name or target_name)
            #    df = pl.read_database(query=query, connection=uri.uri)
            case _:
                raise FileTypeNotSupportedError(f"Not supported file type {file_type}")
        self.sql_context.register(target_name, df)

    def execute_sql(
        self,
        sql: Union[
            ex.Query,
            str,
        ],
    ) -> PolarsResultData:
        return PolarsResultData(sql, self.sql_context, self.chunk_size)

    def list_tables(self) -> ResultData:
        return self.execute_sql("SHOW TABLES")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass
