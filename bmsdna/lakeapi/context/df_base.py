from abc import abstractmethod, ABC, abstractproperty
from datetime import datetime, timezone
from bmsdna.lakeapi.core.types import FileTypes
from typing import Optional, List, Tuple, Any, TYPE_CHECKING, Union
import pyarrow as pa
from deltalake import DeltaTable
import pyarrow.dataset
import pypika.queries
import polars as pl
from bmsdna.lakeapi.core.config import SearchConfig
import pypika.terms

if TYPE_CHECKING:
    import pandas as pd


def get_sql(sql_or_pypika: str | pypika.queries.QueryBuilder, limit_zero=False) -> str:
    if limit_zero:
        sql_or_pypika = (
            sql_or_pypika.limit(0)
            if not isinstance(sql_or_pypika, str)
            else "SELECT * FROM (" + sql_or_pypika + ") s LIMIT 0 "
        )
    if isinstance(sql_or_pypika, str):
        return sql_or_pypika
    if len(sql_or_pypika._selects) == 0:
        return sql_or_pypika.select("*").get_sql()
    return sql_or_pypika.get_sql()


class ResultData(ABC):
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
    def to_arrow_recordbatch(self) -> pa.RecordBatchReader:
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

    def write_nd_json(self, file_name: str):
        import polars as pl

        batches = self.to_arrow_recordbatch()
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

        batches = self.to_arrow_recordbatch()

        with paparquet.ParquetWriter(file_name, batches.schema) as writer:
            for batch in batches:
                writer.write_batch(batch)

    def write_csv(self, file_name: str, *, separator: str):
        import pyarrow.csv as pacsv
        import decimal

        batches = self.to_arrow_recordbatch()
        with pacsv.CSVWriter(
            file_name,
            batches.schema,
            write_options=pacsv.WriteOptions(delimiter=separator),
        ) as writer:
            for batch in batches:
                writer.write_batch(batch)


class ExecutionContext(ABC):
    def __init__(self) -> None:
        super().__init__()
        self.modified_dates: dict[str, datetime] = {}

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
        if file_type in ["parquet", "ipc", "arrow", "feather", "csv", "orc"]:
            ds = pa.dataset.dataset(uri, format=file_type)
        elif file_type in ["ndjson", "json"]:
            import pandas

            pd = pandas.read_json(uri, orient="records", lines=file_type == "ndjson")

            return pyarrow.Table.from_pandas(pd)
        elif file_type == "avro":
            import polars as pl

            pd = pl.read_avro(uri).to_arrow()
            return pd
        elif file_type == "delta":
            dt = DeltaTable(
                uri,
            )

            ds = dt.to_pyarrow_dataset(partitions=partitions)

        else:
            raise Exception(f"Not supported file type {file_type}")
        return ds

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

    def register_dataframe(
        self,
        name: str,
        uri: str,
        file_type: FileTypes,
        partitions: Optional[List[Tuple[str, str, Any]]],
    ):
        ds = self.get_pyarrow_dataset(uri, file_type, partitions)
        self.modified_dates[name] = self.get_modified_date(uri, file_type)
        self.register_arrow(name, ds)

    @abstractmethod
    def execute_sql(self, sql: Union[pypika.queries.QueryBuilder, str]) -> ResultData:
        ...

    @abstractmethod
    def list_tables(self) -> ResultData:
        ...
