from datetime import datetime

from fastapi.concurrency import run_in_threadpool
import pyarrow as pa
from typing import List, Optional, Tuple, Any, Union, cast
from bmsdna.lakeapi.core.types import FileTypes, OperatorType
from bmsdna.lakeapi.context.df_base import (
    FLAVORS,
    ExecutionContext,
    ResultData,
    get_sql,
)
import arrow_odbc
import pyarrow.dataset
import sqlglot.expressions as ex
import os
from bmsdna.lakeapi.core.config import SearchConfig
from sqlglot import from_, parse_one
from uuid import uuid4
from .source_uri import SourceUri

ENABLE_COPY_TO = os.environ.get("ENABLE_COPY_TO", "0") == "1"


def _get_temp_table_name():
    return "temp_" + str(uuid4()).replace("-", "")


arrow_odbc.enable_odbc_connection_pooling()


class BatchReaderWrap:
    def __init__(self, rdr: arrow_odbc.BatchReader):
        self.rdr = rdr

    def __enter__(self, *args, **kwargs):
        return self

    @property
    def schema(self):
        return self.rdr.schema

    def __iter__(self):
        return self.rdr.__iter__()

    def __exit__(self, *args, **kwargs):
        pass


class ODBCResultData(ResultData):
    def __init__(
        self,
        original_sql: Union[ex.Query, str],
        connection_string: str,
        chunk_size: int,
    ) -> None:
        super().__init__(chunk_size=chunk_size)
        self.original_sql = original_sql
        self.connection_string = connection_string
        self._arrow_schema = None
        self._df = None
        self.flavor: FLAVORS = (
            "tsql" if " for SQL Server".lower() in connection_string.lower() else "ansi"
        )
        self.dialect = (
            "tsql"
            if " for SQL Server".lower() in connection_string.lower()
            else "duckdb"
        )

    def query_builder(self) -> ex.Select:
        if not isinstance(self.original_sql, str):
            return from_(self.original_sql.subquery().as_("t"))
        else:
            return from_(
                cast(ex.Select, parse_one(self.original_sql, dialect=self.dialect))
                .subquery()
                .as_("t")
            )

    async def arrow_schema(self) -> pa.Schema:
        if self._arrow_schema is not None:
            return self._arrow_schema
        query = get_sql(self.original_sql, limit=0, dialect=self.dialect)
        batches = arrow_odbc.read_arrow_batches_from_odbc(
            query, connection_string=self.connection_string, batch_size=self.chunk_size
        )
        assert batches is not None
        self._arrow_schema = batches.schema
        return self._arrow_schema

    async def get_df(self):
        if self._df is None:
            query = get_sql(self.original_sql, dialect=self.dialect)
            batch_reader = await run_in_threadpool(
                arrow_odbc.read_arrow_batches_from_odbc,
                query,
                connection_string=self.connection_string,
                batch_size=self.chunk_size,
            )
            assert batch_reader is not None
            self._df = pa.Table.from_batches(batch_reader, batch_reader.schema)
        return self._df

    async def to_pandas(self):
        return (await self.get_df()).to_pandas()

    async def to_arrow_table(self):
        return await self.get_df()

    async def to_arrow_recordbatch(self, chunk_size: int = 10000):  # type: ignore
        query = get_sql(self.original_sql, dialect=self.dialect)
        res = await run_in_threadpool(
            arrow_odbc.read_arrow_batches_from_odbc,
            query,
            connection_string=self.connection_string,
            batch_size=self.chunk_size,
        )
        assert res is not None
        return BatchReaderWrap(res)


class ODBCExecutionContext(ExecutionContext):
    def __init__(self, chunk_size: int):
        super().__init__(chunk_size=chunk_size, engine_name="odbc")
        self.res_con = None
        self.datasources = dict()
        self.persistance_file_name = None

    def register_arrow(
        self, name: str, ds: Union[pyarrow.dataset.Dataset, pyarrow.Table]
    ):
        raise NotImplementedError("Cannot read arrow in remote sql")

    def close(self):
        pass

    @property
    def dialect(self):
        if len(self.datasources) > 0:
            return (
                "tsql"
                if " for SQL Server".lower()
                in self.datasources[[list(self.datasources.keys())[0]]]
                else "postgres"
            )
        return "tsql"

    @property
    def supports_view_creation(self) -> bool:
        return False

    async def execute_sql(
        self,
        sql: Union[
            ex.Query,
            str,
        ],
    ) -> ODBCResultData:
        # todo: get correct connection string somehow
        assert len(self.datasources) == 1
        return ODBCResultData(
            sql,
            chunk_size=self.chunk_size,
            connection_string=self.datasources[list(self.datasources.keys())[0]],
        )

    def json_function(self, term: ex.Expression, assure_string=False):
        raise NotImplementedError(
            "Cannot convert to JSON in remote sql"
        )  # we could but sql does not support structured types anyway, so...

    def init_search(
        self,
        source_view: str,
        search_configs: list[SearchConfig],
    ):
        raise NotImplementedError("Not supported")

    def register_datasource(
        self,
        target_name: str,
        source_table_name: Optional[str],
        uri: SourceUri,
        file_type: FileTypes,
        partitions: List[Tuple[str, OperatorType, Any]] | None,
    ):
        assert file_type == "odbc"
        assert uri.account is None
        self.datasources[target_name] = uri.uri

    async def list_tables(self) -> ResultData:
        return await self.execute_sql(
            "SELECT table_schema, table_name as name, table_type from information_schema.tables"
        )

    def get_modified_date(
        self,
        uri: SourceUri,
        file_type: FileTypes,
    ) -> datetime | None:
        return None

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        pass
