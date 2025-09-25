from datetime import datetime

from fastapi.concurrency import run_in_threadpool
import pyarrow as pa
from typing import List, Optional, Tuple, Any, Union, cast
from bmsdna.lakeapi.core.types import FileTypes, OperatorType
from bmsdna.lakeapi.context.df_base import ExecutionContext, ResultData, get_sql
from deltalake2db import (
    duckdb_create_view_for_delta,
    duckdb_apply_storage_options,
    FilterType,
)
import duckdb
import pyarrow.dataset
import sqlglot.expressions as ex
from sqlglot import from_, parse_one
import os
from datetime import timezone
from bmsdna.lakeapi.core.config import SearchConfig
from uuid import uuid4

from bmsdna.lakeapi.core.log import get_logger
import multiprocessing
from .source_uri import SourceUri


logger = get_logger(__name__)

ENABLE_COPY_TO = os.environ.get("ENABLE_COPY_TO", "0") == "1"

DUCK_CONFIG = {}
DUCK_INIT_SCRIPTS: list[str] = []
AZURE_LOADED_SCRIPTS: list[str] = []


def _get_temp_table_name():
    return "temp_" + str(uuid4()).replace("-", "")


class DuckDBResultData(ResultData):
    def __init__(
        self,
        original_sql: Union[ex.Query, str],
        con: duckdb.DuckDBPyConnection,
        chunk_size: int,
    ) -> None:
        super().__init__(chunk_size=chunk_size)
        self.original_sql = original_sql
        self.con = con
        self._arrow_schema = None
        self._df = None

    def columns(self):
        return (self.arrow_schema()).names

    def query_builder(self) -> ex.Select:
        if not isinstance(self.original_sql, str):
            return from_(self.original_sql.subquery())
        else:
            return from_(
                cast(
                    ex.Select, parse_one(self.original_sql, dialect="duckdb")
                ).subquery()
            )

    def arrow_schema(self) -> pa.Schema:
        if self._arrow_schema is not None:
            return self._arrow_schema
        query = get_sql(self.original_sql, limit=0, dialect="duckdb")

        def _get_schema():
            self.con.execute(query)
            return self.con.arrow().schema

        self._arrow_schema = _get_schema()

        return self._arrow_schema

    async def to_pandas(self):
        query = get_sql(self.original_sql, dialect="duckdb")

        def _to_df():
            self.con.execute(query)
            return self.con.df()

        return await run_in_threadpool(_to_df)

    async def to_pylist(self):
        query = get_sql(self.original_sql, dialect="duckdb")

        def _to_list():
            self.con.execute(query)
            assert self.con.description is not None
            col_names = [d[0] for d in self.con.description]
            res = self.con.fetchall()
            return [dict(zip(col_names, r)) for r in res]

        return await run_in_threadpool(_to_list)

    async def to_arrow_recordbatch(self, chunk_size: int = 10000):
        query = get_sql(self.original_sql, dialect="duckdb")

        def _to_df():
            self.con.execute(query)
            return self.con.fetch_record_batch(chunk_size)

        return await run_in_threadpool(_to_df)

    async def write_parquet(self, file_name: str):
        if not ENABLE_COPY_TO:
            return await super().write_parquet(file_name)
        query = get_sql(self.original_sql, dialect="duckdb")
        uuidstr = _get_temp_table_name()
        # temp table required because of https://github.com/duckdb/duckdb/issues/7616
        full_query = f"""CREATE TEMP VIEW {uuidstr} AS {query};
                         COPY (SELECT *FROM {uuidstr})
                         TO '{file_name}' (FORMAT PARQUET,use_tmp_file False, ROW_GROUP_SIZE 10000);
                         DROP VIEW {uuidstr}"""

        await run_in_threadpool(self.con.execute, full_query)

    async def write_nd_json(self, file_name: str):
        if not ENABLE_COPY_TO:
            return await super().write_nd_json(file_name)
        query = get_sql(self.original_sql, dialect="duckdb")
        uuidstr = _get_temp_table_name()
        full_query = f"""CREATE TEMP VIEW {uuidstr} AS {query};
                         COPY (SELECT *FROM {uuidstr})
                         TO '{file_name}' (FORMAT JSON);
                         DROP VIEW {uuidstr}"""

        await run_in_threadpool(self.con.execute, full_query)

    async def write_csv(self, file_name: str, *, separator: str):
        if not ENABLE_COPY_TO:
            return await super().write_csv(file_name, separator=separator)
        query = get_sql(self.original_sql, dialect="duckdb")
        uuidstr = _get_temp_table_name()
        full_query = f"""CREATE TEMP VIEW {uuidstr} AS {query};
                         COPY (SELECT *FROM {uuidstr}) 
                         TO '{file_name}' (FORMAT CSV, delim '{separator}', header True);
                         DROP VIEW {uuidstr};"""
        await run_in_threadpool(self.con.execute, full_query)

    async def write_json(self, file_name: str):
        if not ENABLE_COPY_TO:
            return await super().write_json(file_name)
        query = get_sql(self.original_sql, dialect="duckdb")
        uuidstr = _get_temp_table_name()
        full_query = f"""CREATE TEMP VIEW {uuidstr} AS {query};
                         COPY (SELECT *FROM {uuidstr})  
                         TO '{file_name}' (FORMAT JSON, Array True); 
                         DROP VIEW {uuidstr};"""
        await run_in_threadpool(self.con.execute, full_query)


def match_25(
    table: str,
    field: str,
    search_text: str,
    fields: Optional[str] = None,
    alias: Optional[str] = None,
):
    f_args = [ex.column(field), ex.convert(search_text)]
    if fields is not None:
        f_args.append(
            ex.PropertyEQ(
                this=ex.Column(this=ex.Identifier(this="fields", quoted=False)),
                expression=ex.convert(fields),
            )
        )

    fn = ex.Dot(
        this=ex.to_identifier("fts_main_" + table),
        expression=ex.func("match_bm25", *f_args, dialect="duckdb"),
    )
    if alias:
        return fn.as_(alias)
    return fn


class DuckDbExecutionContextBase(ExecutionContext):
    def __init__(
        self,
        con: duckdb.DuckDBPyConnection,
        chunk_size: int,
    ):
        super().__init__(chunk_size=chunk_size, engine_name="duckdb")
        self.con = con.cursor()
        for ins in DUCK_INIT_SCRIPTS:
            self.con.execute(ins)
        self.res_con = None
        self.persistance_file_name = None
        self.array_contains_func = "array_contains"

    @property
    def dialect(self):
        return "duckdb"

    @property
    def supports_view_creation(self) -> bool:
        return True

    def register_arrow(
        self,
        name: str,
        ds: Union[pyarrow.dataset.Dataset, pyarrow.Table],
    ):
        self.con.register(name, ds)

    def close(self):
        self.con.close()

    def execute_sql(
        self,
        sql: Union[
            ex.Query,
            str,
        ],
    ) -> DuckDBResultData:
        if self.persistance_file_name is not None:
            self.res_con = duckdb.connect(
                self.persistance_file_name, read_only=True, config=DUCK_CONFIG
            )

            return DuckDBResultData(
                sql,
                self.res_con,
                self.chunk_size,
            )
        return DuckDBResultData(
            sql,
            con=self.con,
            chunk_size=self.chunk_size,
        )

    def search_score_function(
        self,
        source_view: str,
        search_text: str,
        search_config: SearchConfig,
        alias: Optional[str],
    ):
        fields = ",".join(search_config.columns)
        return match_25(
            source_view,
            "__search_id",
            search_text,
            fields=fields,
            alias=alias,
        )

    def distance_m_function(
        self,
        lat1: ex.Expression,
        lon1: ex.Expression,
        lat2: ex.Expression,
        lon2: ex.Expression,
    ):
        return ex.func("haversine", lat1, lon1, lat2, lon2)

    def json_function(self, term: ex.Expression, assure_string=False):
        fn = ex.func("to_json", term)
        if assure_string:
            return ex.cast(fn, ex.DataType.Type.VARCHAR)
        return fn

    def init_search(
        self,
        source_view: str,
        search_configs: list[SearchConfig],
    ):
        modified_date = self.modified_dates[source_view]
        if modified_date is None:
            return
        persistence_name = source_view
        search_columns = []
        for cfg in search_configs:
            search_columns = search_columns + cfg.columns
        scc = ", ".join([f"{sc}" for sc in search_columns])
        pk_name = "__search_id"
        real_query = f"CREATE TABLE search_con.{persistence_name} AS SELECT ROW_NUMBER() OVER () AS __search_id, * FROM {source_view} s"
        persitance_path = os.getenv("TEMP", "/tmp/")
        persistance_file_name = os.path.join(
            persitance_path, persistence_name + ".duckdb"
        )

        if (
            not os.path.exists(persistance_file_name)
            or datetime.fromtimestamp(
                os.path.getmtime(persistance_file_name), tz=timezone.utc
            )
            < modified_date.astimezone(timezone.utc)
            or datetime.fromtimestamp(
                os.path.getmtime(persistance_file_name), tz=timezone.utc
            )
            < datetime.fromisoformat("2023-05-19").astimezone(
                timezone.utc
            )  # before duckdb upgrade
        ):
            persistance_file_name_temp = persistance_file_name + "_temp"
            if os.path.exists(persistance_file_name_temp):
                os.remove(persistance_file_name_temp)
            search_con = duckdb.connect(
                persistance_file_name_temp, read_only=False, config=DUCK_CONFIG
            )

            search_con.commit()  # create empty duck file
            search_con.close()
            self.con.execute(f"ATTACH '{persistance_file_name_temp}' AS search_con")
            self.con.execute(real_query)
            self.con.execute("DETACH search_con")
            search_con = duckdb.connect(
                persistance_file_name_temp, read_only=False, config=DUCK_CONFIG
            )
            scc = ", ".join([f"'{sc}'" for sc in search_columns])
            search_con.execute(
                f"PRAGMA create_fts_index('{persistence_name}', '{pk_name}', {scc})"
            )
            search_con.close()
            if os.path.exists(persistance_file_name):
                os.remove(persistance_file_name)
            os.rename(persistance_file_name_temp, persistance_file_name)
        if not self.persistance_file_name:
            self.con.load_extension("fts")
        self.persistance_file_name = persistance_file_name

    def init_spatial(self):
        # we don't need spatial extension since haversine is good enough
        self.con.execute(
            """CREATE FUNCTION haversine(lat1, lng1, lat2, lng2) 
    AS ( 6371000 * acos( cos( radians(lat1) ) *
       cos( radians(lat2) ) * cos( radians(lng2) - radians(lng1) ) +
       sin( radians(lat1) ) * sin( radians(lat2) ) ) 
    )"""
        )

    def register_datasource(
        self,
        target_name: str,
        source_table_name: Optional[str],
        uri: SourceUri,
        file_type: FileTypes,
        filters: Optional[FilterType],
        meta_only: bool = False,
        limit: int | None = None,
    ):
        self.modified_dates[target_name] = self.get_modified_date(uri, file_type)

        remote_uri, remote_opts = uri.get_uri_options(flavor="original")
        duckdb_apply_storage_options(
            self.con,
            remote_uri,
            remote_opts,
            use_fsspec=os.getenv("DUCKDB_DELTA_USE_FSSPEC", "0") == "1",
        )

        if file_type == "json":
            self.con.execute(
                f"""CREATE VIEW {target_name} as 
                    SELECT *FROM read_json_auto('{remote_uri}', format='array')
                 """
            )
            return
        if file_type == "ndjson":
            self.con.execute(
                f"""CREATE VIEW {target_name} as 
                    SELECT *FROM read_json_auto('{remote_uri}', format='newline_delimited')
                """
            )
            return
        if file_type == "parquet":
            self.con.execute(
                f"""CREATE VIEW  {target_name} as 
                    SELECT *FROM read_parquet('{remote_uri}')
                """
            )
            return
        if file_type == "csv":
            self.con.execute(
                f"""CREATE VIEW  {target_name} as 
                            SELECT *FROM read_csv_auto('{remote_uri}', delim=',', header=True)"""
            )
            return
        if file_type == "delta" and uri.exists():
            ab_uri, uri_opts = uri.get_uri_options(flavor="original")
            duckdb_create_view_for_delta(
                self.con,
                ab_uri,
                target_name,
                storage_options=uri_opts,
                conditions=filters,
                use_fsspec=os.getenv("DUCKDB_DELTA_USE_FSSPEC", "0") == "1",
                limit=limit,
            )
            return
        if file_type == "duckdb":
            self.con.execute(
                f"ATTACH '{remote_uri}' AS  {target_name}_duckdb (READ_ONLY);"
            )
            self.con.execute(
                f"CREATE VIEW  {target_name} as SELECT *FROM  {target_name}_duckdb.{source_table_name or target_name}"
            )
            return
        if file_type == "sqlite":
            self.con.execute(
                f"ATTACH '{remote_uri}' AS  {target_name}_sqlite (TYPE sqlite, READ_ONLY);"
            )
            self.con.execute(
                f"CREATE VIEW  {target_name} as SELECT *FROM  {target_name}_sqlite.{source_table_name or target_name}"
            )
            return

        return super().register_datasource(
            target_name, source_table_name, uri, file_type, filters
        )

    def list_tables(self) -> ResultData:
        return self.execute_sql(
            """SELECT table_name as name, table_type 
               from information_schema.tables where table_schema='main'
            """
        )

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        if self.res_con:
            self.res_con.__exit__(*args, **kwargs)


class DuckDbExecutionContext(DuckDbExecutionContextBase):
    def __init__(self, chunk_size: int):
        super().__init__(duckdb.connect(config=DUCK_CONFIG), chunk_size=chunk_size)

    def __enter__(self):
        super().__enter__()
        self.con.__enter__()
        self.con.execute("SET memory_limit='500MB'")
        self.con.execute(f"SET threads={int(multiprocessing.cpu_count() / 2)}")
        self.con.execute("SET default_null_order='nulls_first'")  # align with polars
        return self

    def __exit__(self, *args, **kwargs):
        super().__exit__(*args, **kwargs)
        self.con.close()

    def close(self):
        self.con.close()
