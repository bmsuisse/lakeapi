from deltalake import DeltaTable

import pyarrow as pa
from typing import List, Tuple, Any, Union, TYPE_CHECKING
from bmsdna.lakeapi.core.types import FileTypes
from bmsdna.lakeapi.context.df_base import ExecutionContext, ResultData

if TYPE_CHECKING:
    import datafusion  # lazy import it on runtime
import pyarrow.dataset
import pypika.queries
import pypika.terms
import pypika


class DatafusionDBResultData(ResultData):
    def __init__(
        self,
        original_sql: Union[pypika.queries.QueryBuilder, str],
        session: "datafusion.SessionContext",
    ) -> None:
        super().__init__()
        self.original_sql = original_sql
        self.session = session
        self._arrow_schema = None
        self._df = None

    def columns(self):
        return self.arrow_schema().column_names

    def query_builder(self) -> pypika.queries.QueryBuilder:
        return pypika.Query.from_(self.original_sql)

    def arrow_schema(self) -> pa.Schema:
        if self._arrow_schema is not None:
            return self._arrow_schema
        query = (
            self.original_sql.limit(0).get_sql()
            if not isinstance(self.original_sql, str)
            else "SELECT * FROM (" + self.original_sql + ") s LIMIT 0 "
        )
        self._arrow_schema = self.session.sql(query).to_arrow_table()
        return self._arrow_schema

    @property
    def df(self):
        if self._df is None:
            self._df = self.session.sql(
                self.original_sql if isinstance(self.original_sql, str) else self.original_sql.get_sql()
            )
        return self._df

    def to_pandas(self):
        return self.df.to_pandas()

    def to_arrow_table(self):
        return self.df.to_arrow_table()

    def to_arrow_recordbatch(self, chunk_size: int = 10000):
        return self.df.to_arrow_table().to_reader(max_chunksize=chunk_size)


from datafusion import udf


def to_json_impl(array: pa.Array) -> pa.Array:
    import json

    return pa.array([json.dumps(s) for s in array.to_pylist()], type=pa.string)


class DatafusionDbExecutionContextBase(ExecutionContext):
    def __init__(self, session: "datafusion.SessionContext"):
        super().__init__()
        self.session = session
        self._registred_udf = False

    def json_function(self, term: pypika.terms.Term):
        if not self._registred_udf:  # does not seem to work
            to_json = udf(to_json_impl, [pa.struct], pyarrow.string(), "stable")
            self.session.register_udf(to_json)
            self._registred_udf = True
        return pypika.terms.Function("to_json", term)

    def register_arrow(
        self,
        name: str,
        ds: Union[pyarrow.dataset.Dataset, pyarrow.Table, pyarrow.dataset.FileSystemDataset],
    ):
        if isinstance(ds, pyarrow.dataset.Dataset):
            self.session.deregister_table(name)
            self.session.register_dataset(name, ds)
        elif isinstance(ds, pyarrow.dataset.FileSystemDataset):
            self.session.deregister_table(name)
            self.session.register_dataset(name, ds)
        elif isinstance(ds, pyarrow.Table):
            self.session.deregister_table(name)
            self.session.register_table(name, ds)
        else:
            raise ValueError("not supported")

    def close(self):
        pass

    def execute_sql(
        self,
        sql: Union[
            pypika.queries.QueryBuilder,
            str,
        ],
    ) -> DatafusionDBResultData:
        return DatafusionDBResultData(sql, session=self.session)


class DatafusionDbExecutionContext(DatafusionDbExecutionContextBase):
    def __init__(self):
        import datafusion

        runtime = datafusion.RuntimeConfig().with_disk_manager_os()
        config = (
            datafusion.SessionConfig()
            .set("datafusion.execution.parquet.pushdown_filters", "true")
            .set("datafusion.execution.collect_statistics", "true")
            .set("datafusion.execution.parquet.enable_page_index", "true")
            .set("datafusion.sql_parser.enable_ident_normalization", "false")
        )
        ctx = datafusion.SessionContext(config, runtime)
        super().__init__(ctx)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass
