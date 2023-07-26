from abc import abstractmethod, ABC
from datetime import datetime, timezone
from bmsdna.lakeapi.core.types import FileTypes
from typing import Literal, Optional, List, Tuple, Any, TYPE_CHECKING, Union
import pyarrow as pa
from deltalake import DeltaTable
import pyarrow.dataset
import pypika.queries
import polars as pl
from bmsdna.lakeapi.core.config import SearchConfig
import pypika.terms
import os

if TYPE_CHECKING:
    import pandas as pd

FLAVORS = Literal["ansi", "mssql"]


def get_sql(
    sql_or_pypika: str | pypika.queries.QueryBuilder, limit: int | None = None, flavor: FLAVORS = "ansi"
) -> str:
    if limit is not None:
        sql_or_pypika = (
            sql_or_pypika.limit(limit)
            if not isinstance(sql_or_pypika, str)
            else (  # why not just support limit/offset like everyone else, microsoft?
                f"SELECT * FROM ({sql_or_pypika}) s LIMIT {limit} "
                if flavor == "ansi"
                else f"SELECT top {limit} * FROM ({sql_or_pypika}) s "
            )
        )
    if isinstance(sql_or_pypika, str):
        return sql_or_pypika
    if len(sql_or_pypika._selects) == 0:
        sql_or_pypika = sql_or_pypika.select("*")
    assert not isinstance(sql_or_pypika, str)
    if flavor == "mssql" and (sql_or_pypika._limit is not None or sql_or_pypika._offset is not None):
        old_limit = sql_or_pypika._limit  # why not just support limit/offset like everyone else, microsoft?
        old_offset = sql_or_pypika._offset
        no_limit = sql_or_pypika.limit(None).offset(None)
        if old_offset is None or old_offset == 0:
            sql_no_limit = no_limit.get_sql()
            if sql_no_limit.upper().startswith("SELECT"):
                return "SELECT TOP " + str(old_limit) + sql_no_limit[len("SELECT") :]
            return f" SELECT TOP {old_limit} * from ({sql_no_limit}) s1"
        else:
            if len(no_limit._orderbys) == 0:
                no_limit = no_limit.orderby(1)
            sql_no_limit = no_limit.get_sql()
            assert sql_no_limit.upper().startswith("SELECT")
            return (
                sql_no_limit
                + " OFFSET "
                + str(old_offset)
                + " ROWS FETCH NEXT "
                + str(old_limit or 100000)
                + " ROWS ONLY"
            )

    return sql_or_pypika.get_sql()


class ResultData(ABC):
    def __init__(self, chunk_size: int) -> None:
        super().__init__()
        self.chunk_size = chunk_size

    @abstractmethod
    def columns(self) -> List[str]:
        ...

    @abstractmethod
    def arrow_schema(self) -> pa.Schema:
        ...

    @abstractmethod
    def to_arrow_table(self) -> pa.Table:
        ...

    @abstractmethod
    def to_arrow_recordbatch(self, chunk_size: int = 10000) -> pa.RecordBatchReader:
        ...

    @abstractmethod
    def query_builder(self) -> pypika.queries.QueryBuilder:
        ...

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
        t = pl.from_arrow(arrow_table)
        assert isinstance(t, pl.DataFrame)
        return t.write_json(row_oriented=True)

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
    def to_pandas(self) -> "pd.DataFrame":
        ...

    def write_parquet(self, file_name: str):
        import pyarrow.parquet as paparquet

        batches = self.to_arrow_recordbatch(self.chunk_size)

        with paparquet.ParquetWriter(file_name, batches.schema) as writer:
            for batch in batches:
                writer.write_batch(batch)

    def write_csv(self, file_name: str, *, separator: str):
        import pyarrow.csv as pacsv
        import decimal

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
    def __init__(self, chunk_size: int) -> None:
        super().__init__()
        self.modified_dates: dict[str, datetime] = {}
        self.chunk_size = chunk_size
        self.len_func = "LEN"
        self.array_contains_func = "array_contains"

    @abstractmethod
    def __enter__(self) -> "ExecutionContext":
        ...

    @abstractmethod
    def __exit__(self, *args, **kwargs):
        ...

    def get_pyarrow_dataset(
        self,
        uri: str,
        file_type: FileTypes,
        partitions: Optional[List[Tuple[str, str, Any]]],
    ) -> Optional[pa.dataset.Dataset | pa.Table]:
        match file_type:
            case "parquet":
                import pyarrow.dataset as ds

                return pa.dataset.dataset(
                    uri, format=ds.ParquetFileFormat(read_options={"coerce_int96_timestamp_unit": "us"})
                )
            case "ipc" | "arrow" | "feather" | "csv" | "orc":
                return pa.dataset.dataset(uri, format=file_type)
            case "ndjson" | "json":
                import pandas

                pd = pandas.read_json(uri, orient="records", lines=file_type == "ndjson")

                return pyarrow.Table.from_pandas(pd)
            case "avro":
                import polars as pl

                pd = pl.read_avro(uri).to_arrow()
                return pd
            case "delta":
                dt = DeltaTable(
                    uri,
                )
                return dt.to_pyarrow_dataset(
                    partitions=partitions, parquet_read_options={"coerce_int96_timestamp_unit": "us"}
                )
            case _:
                raise FileTypeNotSupportedError(f"Not supported file type {file_type}")

    @abstractmethod
    def register_arrow(self, name: str, ds: Union[pyarrow.dataset.Dataset, pyarrow.Table]):
        ...

    @abstractmethod
    def close(self):
        ...

    def init_search(
        self,
        source_view: str,
        search_configs: list[SearchConfig],
    ):
        pass

    @abstractmethod
    def json_function(self, term: pypika.terms.Term, assure_string=False) -> pypika.terms.Term:
        ...

    def search_score_function(
        self,
        source_view: str,
        search_text: str,
        search_config: SearchConfig,
        alias: Optional[str],
    ) -> pypika.terms.Term:
        import pypika.terms
        import pypika.functions

        assert len(search_text) > 2
        parts = search_text.split(" ")

        cases = []
        summ = None
        for part in parts:
            case = pypika.Case()
            cond = pypika.functions.Concat(*[pypika.Field(c) for c in search_config.columns]).like("%" + part + "%")
            case.when(cond, pypika.terms.Term.wrap_constant(1))
            case.else_(pypika.terms.Term.wrap_constant(0))
            cases.append(case)
            summ = case if summ is None else summ + case
        assert summ is not None

        return pypika.functions.NullIf(summ, pypika.terms.Term.wrap_constant(0)).as_(alias)

    def get_modified_date(self, uri: str, file_type: FileTypes) -> datetime:
        if file_type == "delta":
            dt = DeltaTable(
                uri,
            )
            return datetime.fromtimestamp(dt.history(1)[-1]["timestamp"] / 1000.0, tz=timezone.utc)
        import os

        return datetime.fromtimestamp(os.path.getmtime(uri), tz=timezone.utc)

    def register_datasource(
        self,
        name: str,
        uri: str,
        file_type: FileTypes,
        partitions: Optional[List[Tuple[str, str, Any]]],
    ):
        ds = self.get_pyarrow_dataset(uri, file_type, partitions)
        if os.path.exists(uri):
            self.modified_dates[name] = self.get_modified_date(uri, file_type)
        self.register_arrow(name, ds)

    @abstractmethod
    def execute_sql(self, sql: Union[pypika.queries.QueryBuilder, str]) -> ResultData:
        ...

    @abstractmethod
    def list_tables(self) -> ResultData:
        ...
