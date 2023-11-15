import asyncio
import hashlib
import os
from datetime import datetime
from typing import Any, List, Literal, Optional, Tuple, Union, cast, get_args
from bmsdna.lakeapi.context.source_uri import SourceUri
import pyarrow as pa
import pyarrow.parquet
import pypika
import pypika.terms
import pypika.queries as fn
from pypika.queries import QueryBuilder
from deltalake.exceptions import TableNotFoundError

from bmsdna.lakeapi.context.df_base import ExecutionContext, ResultData
from bmsdna.lakeapi.core.config import BasicConfig, DatasourceConfig, Param
from bmsdna.lakeapi.core.log import get_logger
from bmsdna.lakeapi.core.model import get_param_def
from bmsdna.lakeapi.core.types import DeltaOperatorTypes
from aiocache import cached
from aiocache.serializers import PickleSerializer

logger = get_logger(__name__)

endpoints = Literal["query", "meta", "request", "sql"]


class Datasource:
    def __init__(
        self,
        version: str,
        tag: str,
        name: str,
        *,
        accounts: dict,
        config: DatasourceConfig,
        sql_context: ExecutionContext,
        basic_config: BasicConfig,
        df: Optional[ResultData] = None,
    ) -> None:
        self.version = version
        self.config = config
        self.tag = tag
        self.name = name
        self.df = df
        self.sql_context = sql_context
        self.basic_config = basic_config
        self.uri = SourceUri(
            config.uri,
            config.account,
            accounts,
            basic_config.data_path if not config.file_type in ["odbc"] else None,
        )

    def file_exists(self):
        if self.config.file_type in ["odbc", "sqlite"]:
            return True  # the uri is not really a file here
        if not self.uri.exists():
            return False
        return True

    def get_delta_table(self):
        if self.config.file_type == "delta":
            if self.uri.exists():
                try:
                    from deltalake import DeltaTable

                    df_uri, df_opts = self.uri.get_uri_options(flavor="object_store")
                    return DeltaTable(df_uri, storage_options=df_opts)
                except TableNotFoundError:
                    return None
        return None

    def select_df(self, df: QueryBuilder) -> QueryBuilder:
        if self.config.select:
            select = [
                pypika.Field(c.name).as_(c.alias or c.name)
                for c in self.config.select
                if c not in (self.config.exclude or [])
            ]
            df = df.select(*select)
        else:
            from pypika.terms import Star

            df = df.select(
                Star(
                    table=pypika.Table(self.tablename),
                )
            )
        return df

    def _prep_df(self, df: QueryBuilder, endpoint: endpoints) -> QueryBuilder:
        if endpoint == "query":
            pass
        else:
            df = self.select_df(df)
        return df

    @property
    def tablename(self):
        if self.config.table_name:
            return self.config.table_name
        if self.version in ["1", "v1"]:
            return self.tag + "_" + self.name
        return self.tag + "_" + self.name + "_" + self.version

    @property
    def unique_table_name(self):
        if self.version in ["1", "v1"]:
            return self.tag + "_" + self.name
        return self.tag + "_" + self.name + "_" + self.version

    def get_table_name(self, force_unique_name: bool) -> pypika.Table:
        tname = self.tablename if not force_unique_name else self.unique_table_name

        if "." in tname:
            parts = tname.split(".")
            if len(parts) == 3:
                return pypika.Table(
                    parts[2],
                    schema=pypika.Schema(parts[1], parent=pypika.Database(parts[0])),
                )
            assert len(parts) == 2
            return pypika.Table(parts[1], schema=pypika.Schema(parts[0]))
        return pypika.Table(tname)

    def get_schema(self) -> pa.Schema:
        schema: pa.Schema | None = None
        if self.config.file_type == "delta" and self.file_exists():
            dt = self.get_delta_table()
            schema = dt.schema().to_pyarrow() if dt else None
        if self.config.file_type == "parquet" and self.file_exists():
            fs, fs_uri = self.uri.get_fs_spec()
            schema = pyarrow.parquet.read_schema(fs_uri, filesystem=fs)
        if schema is not None:
            if self.config.select:
                fields = [schema.field(item.name).with_name(item.alias) for item in self.config.select]
                return pyarrow.schema(fields)
            return schema
        return self.get_df(endpoint="meta").arrow_schema()

    def get_df(
        self,
        partitions: Optional[List[Tuple[str, str, Any]]] = None,
        endpoint: endpoints = "request",
    ) -> ResultData:
        if self.df is None:
            unique_table_name = endpoint == "meta" and self.sql_context.supports_view_creation
            query = pypika.Query.from_(self.get_table_name(unique_table_name))
            self.query = self._prep_df(query, endpoint=endpoint)

            if self.df is None:
                self.sql_context.register_datasource(
                    self.unique_table_name if unique_table_name else self.tablename,
                    self.tablename,
                    self.uri,
                    self.config.file_type,
                    partitions=partitions,
                )
                self.df = self.sql_context.execute_sql(self.query)

        return self.df  # type: ignore


async def get_partition_filter(
    param,
    deltaMeta,
    param_def,
):
    operators = ("<=", ">=", "=", "==", "in", "not in")
    key, value = param
    if not key or not value or key in ("limit", "offset"):
        return None

    prmdef_and_op = await get_param_def(key, param_def)
    if prmdef_and_op is None:
        raise ValueError(f"thats not parameter: {key}")
    prmdef, op = prmdef_and_op
    if op not in get_args(DeltaOperatorTypes):
        return None
    colname = prmdef.colname or prmdef.name

    value_for_partitioning = value
    col_for_partitioning: Optional[str] = None
    for partcol in deltaMeta.partition_columns:
        if partcol == colname:
            col_for_partitioning = partcol
            value_for_partitioning = value

        elif partcol.startswith(colname + "_md5_prefix_"):
            col_for_partitioning = partcol
            prefix_len = int(partcol[len(colname + "_md5_prefix_") :])
            if isinstance(value, (List, Tuple)):
                hashvl = [hashlib.md5(str(v).encode("utf8")).hexdigest() for v in value]
                value_for_partitioning = [hvl[:prefix_len] for hvl in hashvl]
            else:
                hashvl = hashlib.md5(str(value).encode("utf8")).hexdigest()
                value_for_partitioning = hashvl[:prefix_len]
            if op not in operators:
                col_for_partitioning = None
                continue
        elif partcol.startswith(colname + "_md5_mod_"):
            col_for_partitioning = partcol
            modulo_len = int(partcol[len(colname + "_md5_mod_") :])
            if isinstance(value, (List, Tuple)):
                hashvl = [int(hashlib.md5(str(v).encode("utf8")).hexdigest(), 16) for v in value]
                value_for_partitioning = [hvl % modulo_len for hvl in hashvl]
            else:
                hashvl = int(hashlib.md5(str(value).encode("utf8")).hexdigest(), 16)
                value_for_partitioning = hashvl % modulo_len
            if op not in operators:
                col_for_partitioning = None
                continue
        elif partcol.startswith(colname + "_prefix_"):
            col_for_partitioning = partcol
            prefix_len = int(partcol[len(colname + "_prefix_") :])
            if isinstance(value, (List, Tuple)):
                value_for_partitioning = [v[:prefix_len] for v in value]
            else:
                value_for_partitioning = value[:prefix_len]
            if op not in operators:
                col_for_partitioning = None
                continue

    if not col_for_partitioning:
        return None
    # partition value must be string
    return (
        col_for_partitioning,
        op,
        [str(vp) for vp in value_for_partitioning]
        if isinstance(value_for_partitioning, (Tuple, List))
        else str(value_for_partitioning),
    )


async def filter_partitions_based_on_params(
    deltaMeta,
    params,
    param_def,
):
    if len(deltaMeta.partition_columns) == 0:
        return None

    partition_filters = []
    tasks = [get_partition_filter(param, deltaMeta, param_def) for param in params.items()]
    results = await asyncio.gather(*tasks)
    partition_filters = [result for result in results if result is not None]

    return partition_filters if len(partition_filters) > 0 else None


ExpType = Union[list[pypika.Criterion], list[pa.compute.Expression]]


async def concat_expr(
    exprs: ExpType,
) -> Union[pypika.Criterion, pa.compute.Expression]:
    expr: Optional[pypika.Criterion] = None
    for e in exprs:
        if expr is None:
            expr = e
        else:
            expr = expr.__and__(e)
    return cast(pypika.Criterion, expr)


@cached(ttl=2 ^ 10, key="key", serializer=PickleSerializer())
async def _create_inner_expr(
    columns: Optional[tuple[str]],
    prmdef,
    e,
):
    inner_expr: Optional[pypika.Criterion] = None
    for ck, cv in e.items():
        logger.debug(f"key = {ck}, value = {cv}, columns = {columns}")
        if (columns and not ck in columns) and not ck in prmdef.combi:
            pass
        else:
            if inner_expr is None:
                inner_expr = pypika.Field(ck) == cv if cv or cv == 0 else pypika.Field(ck).isnull()
            else:
                inner_expr = inner_expr & (pypika.Field(ck) == cv if cv or cv == 0 else pypika.Field(ck).isnull())
    return inner_expr


@cached(ttl=2 ^ 10, key="key", serializer=PickleSerializer())
async def _process_param(columns, context, key, value, param_def):
    expr: Optional[pypika.Criterion] = None
    prmdef_and_op = await get_param_def(key, param_def)
    if prmdef_and_op is None:
        raise ValueError(f"thats not parameter: {key}")
    prmdef, op = prmdef_and_op
    colname = prmdef.colname or prmdef.name

    if prmdef.combi:
        outer_expr: Optional[pypika.Criterion] = None
        tasks = []
        for e in value:
            task = asyncio.create_task(_create_inner_expr(columns, prmdef, e))
            tasks.append(task)
        results = await asyncio.gather(*tasks)
        for inner_expr in results:
            if inner_expr is not None:
                if outer_expr is None:
                    outer_expr = inner_expr
                else:
                    outer_expr = outer_expr | (inner_expr)
        if outer_expr is not None:
            expr = outer_expr

    elif columns and not colname in columns:
        pass

    else:
        match op:
            case "<":
                expr = fn.Field(colname) < value
            case ">":
                expr = fn.Field(colname) > value
            case ">=":
                expr = fn.Field(colname) >= value
            case "<=":
                expr = fn.Field(colname) <= value
            case "<>":
                expr = fn.Field(colname) != value if value is not None else fn.Field(colname).isnotnull()
            case "==":
                expr = fn.Field(colname) == value if value is not None else fn.Field(colname).isnull()
            case "=":
                expr = fn.Field(colname) == value if value is not None else fn.Field(colname).isnull()
            case "not contains":
                expr = context.term_like(fn.Field(colname), value, "both", negate=True)
            case "contains":
                expr = context.term_like(fn.Field(colname), value, "both")
            case "startswith":
                expr = context.term_like(fn.Field(colname), value, "end")
            case "has":
                expr = fn.Function(
                    context.array_contains_func, fn.Field(colname), pypika.terms.Term.wrap_constant(value)
                )

            case "in":
                lsv = cast(list[str], value)
                if len(lsv) > 0:
                    expr = fn.Field(colname).isin(lsv)
            case "not in":
                lsv = cast(list[str], value)
                if len(lsv) > 0:
                    expr = ~fn.Field(colname).isin(lsv)
            case "between":
                lsv = cast(list[str], value)
                if len(lsv) == 2:
                    expr = fn.Field(colname).between(lsv[0], lsv[1])
                else:
                    from fastapi import HTTPException

                    raise HTTPException(400, "Must have an array with 2 elements for between")
            case "not between":
                lsv = cast(list[str], value)
                if len(lsv) == 2:
                    expr = ~fn.Field(colname).between(lsv[0], lsv[1])
                else:
                    from fastapi import HTTPException

                    raise HTTPException(400, "Must have an array with 2 elements for between")

            case operator:
                logger.error(f"wrong parameter for filter {operator}")
    return expr


async def filter_df_based_on_params(
    context: ExecutionContext,
    params: dict[str, Any],
    param_def: list[Union[Param, str]],
    columns: Optional[list[str]],
) -> Optional[pypika.Criterion]:
    expr: Optional[pypika.Criterion] = None
    exprs: list[Optional[pypika.Criterion]] = []

    tasks = [
        _process_param(tuple(columns) if columns else None, context, key, value, tuple(param_def))
        for key, value in params.items()
        if key and value and key not in ("limit", "offset")
    ]
    results = await asyncio.gather(*tasks)

    exprs.extend(filter(None, results))  # get rid of none values

    expr = await concat_expr(exprs)
    return expr
