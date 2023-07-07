from datetime import datetime
from deltalake import DeltaTable

import pyarrow as pa
from typing import List, Optional, Tuple, Any, Union
from bmsdna.lakeapi.core.types import FileTypes
from bmsdna.lakeapi.context.df_base import ExecutionContext, ResultData, get_sql
import arrow_odbc
import pyarrow.dataset
import pypika.queries
import pypika.terms
import pypika.functions
import pypika.enums
import pypika
import os
from datetime import datetime, timezone
from bmsdna.lakeapi.core.config import SearchConfig
from uuid import uuid4
from ibis.backends.mssql import Backend


ENABLE_COPY_TO = os.environ.get("ENABLE_COPY_TO", "0") == "1"


def _get_temp_table_name():
    return "temp_" + str(uuid4()).replace("-", "")


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
        original_sql: Union[QueryBuilder, str],
        connection_string: str,
        chunk_size: int,
    ) -> None:
        super().__init__(chunk_size=chunk_size)
        self.original_sql = original_sql
        # todo: expand environment variables in connection string
        self.connection_string = connection_string
        self._arrow_schema = None
        self._df = None

    def columns(self):
        return self.arrow_schema().names

    def query_builder(self) -> QueryBuilder:
        return pypika.Query.from_(self.original_sql)

    def arrow_schema(self) -> pa.Schema:
        if self._arrow_schema is not None:
            return self._arrow_schema
        query = get_sql(self.original_sql, limit_zero=True)
        self._arrow_schema = arrow_odbc.read_arrow_batches_from_odbc(
            query, connection_string=self.connection_string, batch_size=self.chunk_size
        ).schema
        return self._arrow_schema

    @property
    def df(self):
        if self._df is None:
            query = get_sql(self.original_sql)
            batch_reader = arrow_odbc.read_arrow_batches_from_odbc(
                query, connection_string=self.connection_string, batch_size=self.chunk_size
            )
            self._df = pa.Table.from_batches(batch_reader, batch_reader.schema)
        return self._df

    def to_pandas(self):
        return self.df.to_pandas()

    def to_arrow_table(self):
        return self.df

    def to_arrow_recordbatch(self, chunk_size: int = 10000):
        query = get_sql(self.original_sql)
        return BatchReaderWrap(
            arrow_odbc.read_arrow_batches_from_odbc(
                query, connection_string=self.connection_string, batch_size=self.chunk_size
            )
        )


class ODBCExecutionContext(ExecutionContext):
    def __init__(self, chunk_size: int):
        super().__init__(chunk_size=chunk_size, backend=)
        self.res_con = None
        self.datasources = dict()
        self.persistance_file_name = None

    def register_arrow(self, name: str, ds: Union[pyarrow.dataset.Dataset, pyarrow.Table]):
        raise NotImplementedError("Cannot read arrow in remote sql")

    def close(self):
        pass

    def execute_sql(
        self,
        sql: Union[
            QueryBuilder,
            str,
        ],
    ) -> ODBCResultData:
        # todo: get correct connection string somehow
        assert len(self.datasources) == 1
        return ODBCResultData(
            sql, chunk_size=self.chunk_size, connection_string=self.datasources[next(self.datasources.keys())]
        )

    def json_function(self, term: pypika.terms.Term, assure_string=False):
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
        self, name: str, uri: str, file_type: FileTypes, partitions: List[Tuple[str, str, Any]] | None
    ):
        assert file_type == "odbc"
        self.datasources[name] = uri

    def list_tables(self) -> ResultData:
        return self.execute_sql("SELECT table_schema, table_name as name, table_type from information_schema.tables")

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        pass
