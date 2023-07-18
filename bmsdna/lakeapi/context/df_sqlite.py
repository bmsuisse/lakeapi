from datetime import datetime
from deltalake import DeltaTable

import pyarrow as pa
from typing import List, Optional, Tuple, Any, Union
from bmsdna.lakeapi.core.types import FileTypes
from bmsdna.lakeapi.context.df_base import FLAVORS, ExecutionContext, ResultData, get_sql
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
from adbc_driver_sqlite.dbapi import Connection, Cursor, connect


class SqliteResultData(ResultData):
    def __init__(
        self,
        original_sql: Union[pypika.queries.QueryBuilder, str],
        connection: Connection,
        chunk_size: int,
    ) -> None:
        super().__init__(chunk_size=chunk_size)
        self.original_sql = original_sql
        self.connection = connection
        self._arrow_schema = None
        self._df = None

    def columns(self):
        return self.arrow_schema().names

    def query_builder(self) -> pypika.queries.QueryBuilder:
        return pypika.Query.from_(self.original_sql)

    def arrow_schema(self) -> pa.Schema:
        if self._arrow_schema is not None:
            return self._arrow_schema
        query = get_sql(self.original_sql, limit=10)  # limit of at least 1 is needed for sqlite!
        with self.connection.cursor() as cur:
            cur.execute(query)
            self._arrow_schema = cur.fetch_arrow_table().schema
        return self._arrow_schema

    @property
    def df(self):
        if self._df is None:
            query = get_sql(self.original_sql)
            with self.connection.cursor() as cur:
                cur.execute(query)
                self._df = cur.fetch_arrow_table()
        return self._df

    def to_pandas(self):
        return self.df.to_pandas()

    def to_arrow_table(self):
        return self.df

    def to_arrow_recordbatch(self, chunk_size: int = 10000):
        query = get_sql(self.original_sql)
        return self.df.to_batches(chunk_size)


class SqliteExecutionContext(ExecutionContext):
    def __init__(self, chunk_size: int):
        super().__init__(chunk_size=chunk_size)
        self.res_con = None
        self.connections: dict[str, Connection] = dict()
        self.persistance_file_name = None
        self.len_func = "length"

    def register_arrow(self, name: str, ds: Union[pyarrow.dataset.Dataset, pyarrow.Table]):
        raise NotImplementedError("Cannot read arrow in remote sql")

    def close(self):
        pass

    def execute_sql(
        self,
        sql: Union[
            pypika.queries.QueryBuilder,
            str,
        ],
    ) -> SqliteResultData:
        # todo: get correct connection string somehow
        assert len(self.connections) == 1
        return SqliteResultData(
            sql, chunk_size=self.chunk_size, connection=self.connections[list(self.connections.keys())[0]]
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
        assert file_type == "sqlite"
        self.connections[name] = connect(uri)

    def list_tables(self) -> ResultData:
        return self.execute_sql("SELECT type as table_type, name from sqlite_schema where type='table'")

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        for _, v in self.connections.items():
            v.close()
        self.connections = dict()
