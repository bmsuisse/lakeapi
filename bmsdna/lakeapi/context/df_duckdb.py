from datetime import datetime
from deltalake import DeltaTable

import pyarrow as pa
from typing import List, Optional, Tuple, Any, Union
from bmsdna.lakeapi.core.types import FileTypes
from bmsdna.lakeapi.context.df_base import ExecutionContext, ResultData, get_sql
import duckdb
import pyarrow.dataset
import pypika.queries
import pypika.terms
import pypika.functions
import pypika.enums
import pypika
import os
from datetime import datetime, timezone
from bmsdna.lakeapi.core.config import SearchConfig

ENABLE_COPY_TO = os.environ.get("ENABLE_COPY_TO", "0") == "1"


class DuckDBResultData(ResultData):
    def __init__(
        self,
        original_sql: Union[pypika.queries.QueryBuilder, str],
        con: duckdb.DuckDBPyConnection,
    ) -> None:
        super().__init__()
        self.original_sql = original_sql
        self.con = con
        self._arrow_schema = None
        self._df = None

    def columns(self):
        return self.arrow_schema().names

    def query_builder(self) -> pypika.queries.QueryBuilder:
        return pypika.Query.from_(self.original_sql)

    def arrow_schema(self) -> pa.Schema:
        if self._arrow_schema is not None:
            return self._arrow_schema
        query = get_sql(self.original_sql, limit_zero=True)
        self._arrow_schema = self.con.execute(query).arrow().schema
        return self._arrow_schema

    @property
    def df(self):
        if self._df is None:
            query = get_sql(self.original_sql)
            self._df = self.con.execute(query)
        return self._df

    def to_pandas(self):
        return self.df.df()

    def to_arrow_table(self):
        return self.df.arrow()

    def to_arrow_recordbatch(self, chunk_size: int = 10000):
        return self.df.fetch_record_batch(chunk_size)

    def write_parquet(self, file_name: str):
        if not ENABLE_COPY_TO:
            return super().write_parquet(file_name)
        query = get_sql(self.original_sql)
        full_query = f"COPY ({query}) TO '{file_name}' (FORMAT PARQUET,use_tmp_file False, ROW_GROUP_SIZE 10000)"
        self.con.execute(full_query)

    def write_nd_json(self, file_name: str):
        if not ENABLE_COPY_TO:
            return super().write_nd_json(file_name)
        query = get_sql(self.original_sql)
        full_query = f"COPY ({query}) TO '{file_name}' (FORMAT JSON)"
        self.con.execute(full_query)

    def write_csv(self, file_name: str, *, separator: str):
        if not ENABLE_COPY_TO:
            return super().write_csv(file_name, separator=separator)
        query = get_sql(self.original_sql)
        full_query = f"COPY ({query}) TO '{file_name}' (FORMAT CSV, delim '{separator}', header True)"
        self.con.execute(full_query)

    def write_json(self, file_name: str):
        if not ENABLE_COPY_TO:
            return super().write_json(file_name)
        query = get_sql(self.original_sql)
        full_query = f"COPY ({query}) TO '{file_name}' (FORMAT JSON, Array True)"
        self.con.execute(full_query)


class Match25Term(pypika.terms.Term):
    def __init__(
        self,
        source_view: str,
        field: pypika.terms.Term,
        search_text: str,
        fields: Optional[str],
        alias: Optional[str] = None,
    ):
        super().__init__()
        self.source_view = source_view
        self.field = field
        self.search_text = search_text
        self.fields = fields
        self.alias = alias

    def get_sql(self, **kwargs):
        search_text_const = pypika.terms.Term.wrap_constant(self.search_text)
        assert isinstance(search_text_const, pypika.terms.Term)
        search_txt = search_text_const.get_sql()
        fields_const = pypika.terms.Term.wrap_constant(self.fields or "")
        assert isinstance(fields_const, pypika.terms.Term)
        field_or_not = ", fields := " + fields_const.get_sql() if self.fields is not None else ""
        sql = f"fts_main_{self.source_view}.match_bm25({self.field.get_sql()}, {search_txt}{field_or_not})"
        if self.alias is not None:
            sql += " AS " + self.alias
        return sql


class DuckDbExecutionContextBase(ExecutionContext):
    def __init__(self, con: duckdb.DuckDBPyConnection):
        super().__init__()
        self.con = con
        self.res_con = None
        self.persistance_file_name = None

    def register_arrow(self, name: str, ds: Union[pyarrow.dataset.Dataset, pyarrow.Table]):
        # self.con.from_arrow(ds).create_view(name, replace=True)
        self.con.register(name, ds)

    def close(self):
        pass

    def execute_sql(
        self,
        sql: Union[
            pypika.queries.QueryBuilder,
            str,
        ],
    ) -> DuckDBResultData:
        if self.persistance_file_name is not None:
            self.res_con = duckdb.connect(self.persistance_file_name, read_only=True)
            return DuckDBResultData(sql, self.res_con)
        return DuckDBResultData(sql, con=self.con)

    def search_score_function(
        self,
        source_view: str,
        search_text: str,
        search_config: SearchConfig,
        alias: Optional[str],
    ):
        fields = ",".join(search_config.columns)
        return Match25Term(source_view, pypika.queries.Field("__search_id"), search_text, fields)

    def json_function(self, term: pypika.terms.Term, assure_string=False):
        fn = pypika.terms.Function("to_json", term)
        if assure_string:
            return pypika.functions.Cast(fn, pypika.enums.SqlTypes.VARCHAR)
        return fn

    def init_search(
        self,
        source_view: str,
        search_configs: list[SearchConfig],
    ):
        modified_date = self.modified_dates[source_view]
        persistence_name = source_view
        search_columns = []
        for cfg in search_configs:
            search_columns = search_columns + cfg.columns
        scc = ", ".join([f"{sc}" for sc in search_columns])
        pk_name = "__search_id"
        real_query = f"CREATE TABLE search_con.{persistence_name} AS SELECT ROW_NUMBER() OVER () AS __search_id, * FROM {source_view} s"
        persitance_path = os.getenv("TEMP", "/tmp/")
        persistance_file_name = os.path.join(persitance_path, persistence_name + ".duckdb")

        if (
            not os.path.exists(persistance_file_name)
            or datetime.fromtimestamp(os.path.getmtime(persistance_file_name), tz=timezone.utc)
            < modified_date.astimezone(timezone.utc)
            or datetime.fromtimestamp(os.path.getmtime(persistance_file_name), tz=timezone.utc)
            < datetime.fromisoformat("2023-05-19T00:00Z")  # before duckdb upgrade
        ):
            persistance_file_name_temp = persistance_file_name + "_temp"
            if os.path.exists(persistance_file_name_temp):
                os.remove(persistance_file_name_temp)
            search_con = duckdb.connect(persistance_file_name_temp, read_only=False)
            search_con.commit()  # create empty duck file
            search_con.close()
            self.con.execute(f"ATTACH '{persistance_file_name_temp}' AS search_con")
            self.con.execute(real_query)
            self.con.execute("DETACH search_con")
            search_con = duckdb.connect(persistance_file_name_temp, read_only=False)
            scc = ", ".join([f"'{sc}'" for sc in search_columns])
            search_con.execute(f"PRAGMA create_fts_index('{persistence_name}', '{pk_name}', {scc})")
            search_con.close()
            if os.path.exists(persistance_file_name):
                os.remove(persistance_file_name)
            os.rename(persistance_file_name_temp, persistance_file_name)
        self.persistance_file_name = persistance_file_name

    def register_dataframe(
        self, name: str, uri: str, file_type: FileTypes, partitions: List[Tuple[str, str, Any]] | None
    ):
        if file_type == "json":
            self.con.execute(f"CREATE VIEW {name} as SELECT *FROM read_json_auto('{uri}', format='array')")
            return
        if file_type == "ndjson":
            self.con.execute(f"CREATE VIEW {name} as SELECT *FROM read_json_auto('{uri}', format='newline_delimited')")
            return
        return super().register_dataframe(name, uri, file_type, partitions)

    def list_tables(self) -> ResultData:
        return self.execute_sql(
            "SELECT table_name as name, table_type from information_schema.tables where table_schema='main'"
        )

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        if self.res_con:
            self.res_con.__exit__(*args, **kwargs)


class DuckDbExecutionContext(DuckDbExecutionContextBase):
    def __init__(self):
        super().__init__(duckdb.connect())

    def __enter__(self):
        super().__enter__()
        self.con.__enter__()
        self.con.execute("SET memory_limit='200MB'")
        self.con.execute("SET threads =2")
        self.con.execute("SET enable_object_cache=true")
        return self

    def __exit__(self, *args, **kwargs):
        super().__exit__(*args, **kwargs)
        self.con.__exit__(*args, **kwargs)

    def close(self):
        self.con.close()
