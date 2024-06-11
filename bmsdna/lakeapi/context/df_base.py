from abc import abstractmethod, ABC
from datetime import datetime, timezone

from sqlglot import Dialect
from bmsdna.lakeapi.core.types import FileTypes
from typing import Literal, Optional, List, Tuple, Any, TYPE_CHECKING, Union
import pyarrow as pa
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError
import sqlglot.expressions as ex
import polars as pl


from .source_uri import SourceUri

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow.dataset as pas
    from bmsdna.lakeapi.core.config import SearchConfig

FLAVORS = Literal["ansi", "tsql"]


def is_complex_type(
    schema: pa.Schema,
    col_name: str,
):
    import pyarrow.types as pat

    f = schema.field(col_name)
    return pat.is_nested(f.type)


def get_sql(
    sql_or_pypika: str | ex.Query,
    limit: int | None = None,
    *,
    dialect: str | Dialect,
) -> str:
    if not isinstance(sql_or_pypika, str) and dialect == "tsql":
        from_ = sql_or_pypika.args.get(
            "from"
        )  # for sql server always add an alias for sub queries
        if (
            from_
            and not from_.alias
            and isinstance(from_.args.get("this"), ex.Subquery)
            and not from_.args.get("this").alias
        ):
            sub_query = from_.args.get("this")
            sub_query.as_("s", copy=False)
    if limit is not None:
        sql_or_pypika = (
            sql_or_pypika.limit(limit)
            if not isinstance(sql_or_pypika, str)
            # why not just support limit/offset like everyone else, microsoft?
            else (
                f"SELECT * FROM ({sql_or_pypika}) s LIMIT {limit} "
                if dialect != "tsql"
                else f"SELECT top {limit} * FROM ({sql_or_pypika}) s "
            )
        )
    if isinstance(sql_or_pypika, str):
        return sql_or_pypika
    if len(sql_or_pypika.expressions) == 0:
        sql_or_pypika = sql_or_pypika.select("*")
    assert not isinstance(sql_or_pypika, str)
    return sql_or_pypika.sql(dialect=dialect)


class ResultData(ABC):
    def __init__(self, chunk_size: int) -> None:
        super().__init__()
        self.chunk_size = chunk_size

    @abstractmethod
    def columns(self) -> List[str]: ...

    @abstractmethod
    def arrow_schema(self) -> "pa.Schema": ...

    @abstractmethod
    def to_arrow_table(self) -> "pa.Table": ...

    @abstractmethod
    def to_arrow_recordbatch(
        self, chunk_size: int = 10000
    ) -> "pa.RecordBatchReader": ...

    @abstractmethod
    def query_builder(self) -> ex.Select: ...

    def write_json(self, file_name: str):
        import decimal

        def default(obj):
            if isinstance(obj, decimal.Decimal):
                return str(obj)
            raise TypeError

        arrow_table = self.to_arrow_table()
        with open(file_name, mode="wb") as f:
            t = pl.from_arrow(arrow_table)
            assert isinstance(t, pl.DataFrame)
            t.write_json(f, row_oriented=True)

    def to_json(self):
        arrow_table = self.to_arrow_table()
        import pydantic

        return pydantic.TypeAdapter(list[dict]).dump_json(arrow_table.to_pylist())
        # t = pl.from_arrow(arrow_table)
        # assert isinstance(t, pl.DataFrame)
        # return t.write_json(row_oriented=True)

    def to_ndjson(self):
        arrow_table = self.to_arrow_table()
        t = pl.from_arrow(arrow_table)
        assert isinstance(t, pl.DataFrame)
        return t.write_ndjson()

    def write_nd_json(self, file_name: str):
        import polars as pl

        batches = self.to_arrow_recordbatch(self.chunk_size)
        with open(file_name, mode="wb") as f:
            for batch in batches:
                t = pl.from_arrow(batch)
                assert isinstance(t, pl.DataFrame)
                t.write_ndjson(f)

    @abstractmethod
    def to_pandas(self) -> "pd.DataFrame": ...

    def write_parquet(self, file_name: str):
        import pyarrow.parquet as paparquet

        batches = self.to_arrow_recordbatch(self.chunk_size)

        with paparquet.ParquetWriter(
            file_name,
            batches.schema,
        ) as writer:
            for batch in batches:
                writer.write_batch(batch)

    def write_csv(self, file_name: str, *, separator: str):
        import pyarrow.csv as pacsv

        batches = self.to_arrow_recordbatch(self.chunk_size)
        with pacsv.CSVWriter(
            file_name,
            batches.schema,
            write_options=pacsv.WriteOptions(delimiter=separator),
        ) as writer:
            for batch in batches:
                writer.write_batch(batch)


class FileTypeNotSupportedError(Exception):
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)


class ExecutionContext(ABC):
    def __init__(self, chunk_size: int, engine_name: str) -> None:
        super().__init__()
        self.modified_dates: dict[str, datetime | None] = {}
        self.chunk_size = chunk_size
        self.len_func = "LEN"
        self.engine_name = engine_name

        self.array_contains_func = "array_contains"

    def term_like(
        self,
        a: ex.Expression,
        value: str,
        wildcard_loc: Literal["start", "end", "both"],
        *,
        negate=False,
    ):
        if wildcard_loc == "start":
            return a.like("%" + value) if not negate else ~a.like("%" + value)
        elif wildcard_loc == "end":
            return a.like(value + "%") if not negate else ~a.like(value + "%")
        else:
            return (
                a.like("%" + value + "%") if not negate else ~a.like("%" + value + "%")
            )

    @property
    @abstractmethod
    def dialect(self) -> str | Dialect: ...

    @property
    @abstractmethod
    def supports_view_creation(self) -> bool: ...

    def create_view(self, name: str, sql: str):
        self.execute_sql(f"CREATE VIEW {name} as sql")

    @abstractmethod
    def __enter__(self) -> "ExecutionContext": ...

    @abstractmethod
    def __exit__(self, *args, **kwargs): ...

    def get_pyarrow_dataset(
        self,
        uri: SourceUri,
        file_type: FileTypes,
        partitions: Optional[List[Tuple[str, str, Any]]],
    ) -> "Optional[pas.Dataset | pa.Table]":
        spec_fs, spec_uri = uri.get_fs_spec()
        match file_type:
            case "parquet":
                import pyarrow.dataset as ds

                return ds.dataset(
                    spec_uri,
                    filesystem=spec_fs,
                    format=ds.ParquetFileFormat(
                        read_options={"coerce_int96_timestamp_unit": "us"},
                    ),
                )
            case "ipc" | "arrow" | "feather" | "csv" | "orc":
                import pyarrow.dataset as ds

                return ds.dataset(
                    spec_uri,
                    filesystem=spec_fs,
                    format=file_type,
                )
            case "ndjson" | "json":
                import pandas
                import pyarrow

                ab_uri, ab_opts = uri.get_uri_options(flavor="fsspec")
                pd = pandas.read_json(
                    ab_uri,
                    storage_options=ab_opts,
                    orient="records",
                    lines=file_type == "ndjson",
                )

                return pyarrow.Table.from_pandas(pd)
            case "avro":
                import polars as pl

                with spec_fs.open(spec_uri, "rb") as f:
                    pd = pl.read_avro(f).to_arrow()  # type: ignore
                return pd
            case "delta":
                ab_uri, ab_opts = uri.get_uri_options(flavor="object_store")
                dt = DeltaTable(ab_uri, storage_options=ab_opts)
                if dt.protocol().min_reader_version > 1:
                    raise ValueError(
                        "Delta table protocol version not supported, use DuckDB or Polars"
                    )
                return dt.to_pyarrow_dataset(
                    partitions=partitions,
                    parquet_read_options={"coerce_int96_timestamp_unit": "us"},
                )
            case _:
                raise FileTypeNotSupportedError(
                    f"Not supported file type {file_type}",
                )

    @abstractmethod
    def register_arrow(
        self,
        name: str,
        ds: "Union[pas.Dataset, pa.Table]",
    ): ...

    @abstractmethod
    def close(self): ...

    def init_search(
        self,
        source_view: str,
        search_configs: "list[SearchConfig]",
    ):
        pass

    def init_spatial(self):
        pass

    @abstractmethod
    def json_function(
        self,
        term: ex.Expression,
        assure_string=False,
    ) -> ex.Expression: ...

    def jsonify_complex(
        self, query: ex.Query, complex_cols: list[str], columns: list[str]
    ) -> ex.Query:
        return query.select(
            *[
                ex.column(c, quoted=True)
                if c not in complex_cols
                else self.json_function(ex.column(c, quoted=True)).as_(c)
                for c in columns
            ]
        )

    def distance_m_function(
        self,
        lat1: ex.Expression,
        lon1: ex.Expression,
        lat2: ex.Expression,
        lon2: ex.Expression,
    ) -> ex.Expression:
        # haversine which works for duckdb and polars and probably most sql systems
        acos = lambda t: ex.func("acos", t)
        cos = lambda t: ex.func("cos", t)
        radians = lambda t: ex.func("radians", t)
        sin = lambda t: ex.func("sin", t)
        return ex.convert(6371000) * acos(
            cos(radians(lat1)) * cos(radians(lat2)) * cos(radians(lon2) - radians(lon1))
            + sin(radians(lat1)) * sin(radians(lat2))
        )

    def search_score_function(
        self,
        source_view: str,
        search_text: str,
        search_config: "SearchConfig",
        alias: Optional[str],
    ) -> ex.Expression:
        assert len(search_text) > 2
        parts = search_text.split(" ")

        cases = []
        summ = None
        for part in parts:
            case = ex.case()
            cond = ex.Concat(
                expressions=[ex.column(c) for c in search_config.columns]
            ).like("%" + part + "%")
            case.when(cond, ex.convert(1), copy=False)
            case.else_(ex.convert(0), copy=False)
            cases.append(case)
            summ = case if summ is None else summ + case
        assert summ is not None
        ni = ex.Nullif(this=summ, expression=ex.convert(0))
        return ni.as_(alias) if alias else ni

    def get_modified_date(
        self,
        uri: SourceUri,
        file_type: FileTypes,
    ) -> datetime | None:
        if file_type == "odbc":
            return None
        fs, fs_uri = uri.get_fs_spec()
        if not fs.exists(fs_uri):
            return None
        if file_type == "delta":
            try:
                ab_uri, ab_opts = uri.get_uri_options(flavor="object_store")
                dt = DeltaTable(ab_uri, storage_options=ab_opts)
                return datetime.fromtimestamp(
                    dt.history(1)[-1]["timestamp"] / 1000.0,
                    tz=timezone.utc,
                )
            except (TableNotFoundError, FileNotFoundError):
                return None

        fs, fs_uri = uri.get_fs_spec()
        return fs.modified(fs_uri)

    def register_datasource(
        self,
        target_name: str,
        source_table_name: Optional[str],
        uri: SourceUri,
        file_type: FileTypes,
        partitions: Optional[List[Tuple[str, str, Any]]],
    ):
        ds = self.get_pyarrow_dataset(uri, file_type, partitions)
        self.modified_dates[target_name] = self.get_modified_date(uri, file_type)
        self.register_arrow(target_name, ds)

    @abstractmethod
    def execute_sql(self, sql: Union[ex.Query, str]) -> ResultData: ...

    @abstractmethod
    def list_tables(self) -> ResultData: ...
