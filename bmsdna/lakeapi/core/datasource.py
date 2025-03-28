import asyncio
import hashlib
import os
from datetime import datetime, date
from typing import (
    Any,
    Iterable,
    List,
    Literal,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
    get_args,
    TYPE_CHECKING,
    TypeAlias,
)
from bmsdna.lakeapi.context.source_uri import SourceUri
import pyarrow as pa
import pyarrow.compute as pac
import pyarrow.parquet
import sqlglot.expressions as ex
from sqlglot import select
from deltalake.exceptions import TableNotFoundError
from deltalake import Metadata as DeltaMetadata, Schema as DeltaSchema
from deltalake.schema import PrimitiveType
from bmsdna.lakeapi.context.df_base import ExecutionContext, ResultData
from bmsdna.lakeapi.core.config import (
    BasicConfig,
    DatasourceConfig,
    Param,
    SelectColumn,
)
from bmsdna.lakeapi.core.log import get_logger
from bmsdna.lakeapi.core.model import get_param_def
from bmsdna.lakeapi.core.types import DeltaOperatorTypes, OperatorType

from bmsdna.lakeapi.utils.async_utils import _async

logger = get_logger(__name__)

endpoints = Literal["query", "meta", "request", "sql"]


def select_df(
    df: ex.Query, select_cols: Optional[List[SelectColumn]], exclude_cols: list[str]
) -> ex.Query:
    if select_cols:
        select = [
            ex.column(c.name, quoted=True).as_(c.alias or c.name, quoted=True)
            for c in select_cols
            if c not in (exclude_cols or [])
        ]
        return df.select(*select, append=False)
    else:
        return df.select(ex.Star(), append=False)


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
        base_source_uri = SourceUri(
            config.uri,
            config.account,
            accounts,
            basic_config.data_path if config.file_type not in ["odbc"] else None,
            token_retrieval_func=basic_config.token_retrieval_func,
        )
        self.copy_local = config.copy_local
        self._execution_uri = None
        self.source_uri = base_source_uri

    def __str__(self) -> str:
        return str(self.source_uri)

    @property
    def execution_uri(self):
        if not self.copy_local:
            return self.source_uri
        assert self.config.file_type == "delta", (
            "only delta is supported for copy_local"
        )
        if self._execution_uri is None:
            self._execution_uri = self.source_uri.copy_to_local(
                os.path.join(
                    self.basic_config.local_data_cache_path,
                    hashlib.md5(self.source_uri.uri.encode("utf-8")).hexdigest(),
                )
            )
        return self._execution_uri

    def file_exists(self):
        if self.config.file_type in ["odbc", "sqlite"]:
            return True  # the uri is not really a file here
        if not self.source_uri.exists():
            return False
        return True

    def get_delta_table(self, schema_only: bool):
        if self.config.file_type == "delta":
            if self.source_uri.exists():
                try:
                    from deltalake import DeltaTable

                    df_uri, df_opts = (
                        self.source_uri if schema_only else self.execution_uri
                    ).get_uri_options(flavor="object_store")
                    return DeltaTable(df_uri, storage_options=df_opts)
                except TableNotFoundError:
                    return None
        return None

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

    def get_table_name(self, force_unique_name: bool) -> ex.Table:
        tname = self.tablename if not force_unique_name else self.unique_table_name

        if "." in tname:
            parts = tname.split(".")
            if len(parts) == 3:
                return ex.Table(
                    this=ex.to_identifier(parts[2]),
                    db=ex.to_identifier(parts[1]),
                    catalog=ex.to_identifier(parts[0]),
                )
            assert len(parts) == 2
            return ex.Table(
                this=ex.to_identifier(parts[1]), db=ex.to_identifier(parts[0])
            )
        return ex.table_(tname)

    async def get_schema(self) -> pa.Schema:
        schema: pa.Schema | None = None
        if self.config.file_type == "delta" and self.file_exists():
            dt = self.get_delta_table(schema_only=True)
            schema = dt.schema().to_pyarrow() if dt else None
        if self.config.file_type == "parquet" and self.file_exists():
            fs, fs_uri = self.source_uri.get_fs_spec()
            schema = pyarrow.parquet.read_schema(fs_uri, filesystem=fs)
        if schema is not None:
            if self.config.select:
                fields = [
                    schema.field(item.name).with_name(item.alias or item.name)
                    for item in self.config.select
                ]
                return pyarrow.schema(fields)
            return schema
        return await (await self.get_df(endpoint="meta")).arrow_schema()

    async def get_df(
        self,
        partitions: Optional[List[Tuple[str, OperatorType, Any]]] = None,
        endpoint: endpoints = "request",
    ) -> ResultData:
        if self.df is None:
            unique_table_name = (
                endpoint == "meta" and self.sql_context.supports_view_creation
            )
            query = select("*").from_(self.get_table_name(unique_table_name))

            self.query = (
                select_df(query, self.config.select, self.config.exclude or [])
                if endpoint != "query"
                else query
            )

            if self.df is None:
                self.sql_context.register_datasource(
                    self.unique_table_name if unique_table_name else self.tablename,
                    self.tablename,
                    self.source_uri if endpoint == "meta" else self.execution_uri,
                    self.config.file_type,
                    partitions=partitions,
                )
                self.df = await _async(self.sql_context.execute_sql(self.query))

        return self.df  # type: ignore


def get_partition_filter(
    param: tuple[str, str],
    deltaMeta: DeltaMetadata,
    deltaSchema: DeltaSchema,
    param_def: Iterable[Param | str],
):
    operators = ("<=", ">=", "=", "==", "in", "not in")
    key, value = param
    if not key or not value or key in ("limit", "offset"):
        return None

    prmdef_and_op = get_param_def(key, param_def)
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
            d_type = next((f.type for f in deltaSchema.fields if f.name == partcol))
            d_type_str = str(d_type.type).lower()

            col_for_partitioning = partcol
            value_for_partitioning = (
                int(value) if d_type_str in ["int", "long", "integer"] else str(value)
            )

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
                hashvl = [
                    int(hashlib.md5(str(v).encode("utf8")).hexdigest(), 16)
                    for v in value
                ]
                value_for_partitioning = [str(hvl % modulo_len) for hvl in hashvl]
            else:
                hashvl = int(hashlib.md5(str(value).encode("utf8")).hexdigest(), 16)
                value_for_partitioning = str(hashvl % modulo_len)
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
        value_for_partitioning,
    )


def filter_partitions_based_on_params(
    deltaMeta: DeltaMetadata,
    deltaSchema: DeltaSchema,
    params: dict,
    param_def: Iterable[Param | str],
):
    if len(deltaMeta.partition_columns) == 0:
        return None

    partition_filters = []
    results = [
        get_partition_filter(param, deltaMeta, deltaSchema, param_def)
        for param in params.items()
    ]
    partition_filters = [result for result in results if result is not None]

    return partition_filters if len(partition_filters) > 0 else None


ExpType: TypeAlias = "Sequence[Union[ex.Binary, ex.Condition]]"


def concat_expr(
    exprs: ExpType,
) -> "Union[ex.Binary,  ex.Condition]":
    expr: Optional[ex.Binary | ex.Condition] = None
    for e in exprs:
        if expr is None:
            expr = e
        else:
            expr = expr.__and__(e)
    assert expr is not None
    return expr


def _create_inner_expr(
    columns: Optional[List[str]],
    prmdef,
    e,
):
    inner_expr: Optional[ex.Binary] = None
    for ck, cv in e.items():
        logger.debug(f"key = {ck}, value = {cv}, columns = {columns}")
        if (columns and ck not in columns) and ck not in prmdef.combi:
            pass
        else:
            if inner_expr is None:
                inner_expr = (
                    ex.column(ck, quoted=True).eq(cv)
                    if (cv or cv == 0)
                    else ex.column(ck, quoted=True).is_(ex.Null())
                )
            else:
                inner_expr = inner_expr & (
                    ex.column(ck, quoted=True).eq(cv)
                    if cv or cv == 0
                    else ex.column(ck, quoted=True).is_(ex.Null())
                )
    return inner_expr


def _sql_value(value: str | datetime | date | None, engine: str):
    if engine != "polars":
        return value  # all other engines convert automatically

    if isinstance(value, datetime):
        return ex.cast(ex.convert(value), "datetime")
    if isinstance(value, date):
        return ex.cast(ex.convert(value), "date")
    return value


def filter_df_based_on_params(
    context: ExecutionContext,
    params: dict[str, Any],
    param_def: list[Union[Param, str]],
    columns: Optional[list[str]],
) -> Optional[ex.Condition | ex.Binary]:
    expr: Optional[ex.Condition] = None
    exprs: list[ex.Condition] = []

    for key, value in params.items():
        if not key or not value or key in ("limit", "offset"):
            continue  # can that happen? I don't know
        prmdef_and_op = get_param_def(key, param_def)
        if prmdef_and_op is None:
            raise ValueError(f"thats not parameter: {key}")
        prmdef, op = prmdef_and_op
        colname = prmdef.colname or prmdef.name

        if prmdef.combi:
            outer_expr: Optional[ex.Condition] = None
            results = [_create_inner_expr(columns, prmdef, e) for e in value]
            for inner_expr in results:
                if inner_expr is not None:
                    if outer_expr is None:
                        outer_expr = inner_expr
                    else:
                        outer_expr = outer_expr | (inner_expr)
            if outer_expr is not None:
                exprs.append(outer_expr)

        elif columns and colname not in columns:
            pass

        else:
            match op:
                case "<":
                    exprs.append(
                        ex.column(colname, quoted=True)
                        < _sql_value(value, engine=context.engine_name)
                    )
                case ">":
                    exprs.append(
                        ex.column(colname, quoted=True)
                        > _sql_value(value, engine=context.engine_name)
                    )
                case ">=":
                    exprs.append(
                        ex.column(colname, quoted=True)
                        >= _sql_value(value, engine=context.engine_name)
                    )
                case "<=":
                    exprs.append(
                        ex.column(colname, quoted=True)
                        <= _sql_value(value, engine=context.engine_name)
                    )
                case "<>":
                    exprs.append(
                        ex.column(colname, quoted=True).neq(
                            _sql_value(value, engine=context.engine_name)
                        )
                        if value is not None
                        else ~ex.column(colname, quoted=True).is_(ex.convert(None))
                    )
                case "==":
                    exprs.append(
                        ex.column(colname, quoted=True).eq(
                            _sql_value(value, engine=context.engine_name)
                        )
                        if value is not None
                        else ex.column(colname, quoted=True).is_(ex.convert(None))
                    )
                case "=":
                    exprs.append(
                        ex.column(colname, quoted=True).eq(
                            _sql_value(value, engine=context.engine_name)
                        )
                        if value is not None
                        else ex.column(colname, quoted=True).is_(ex.convert(None))
                    )
                case "not contains":
                    exprs.append(
                        context.term_like(
                            ex.column(colname, quoted=True), value, "both", negate=True
                        )
                    )
                case "contains":
                    exprs.append(
                        context.term_like(
                            ex.column(colname, quoted=True), value, "both"
                        )
                    )
                case "startswith":
                    exprs.append(
                        context.term_like(ex.column(colname, quoted=True), value, "end")
                    )
                case "has":
                    exprs.append(
                        ex.func(
                            context.array_contains_func,
                            ex.column(colname, quoted=True),
                            ex.convert(value),
                        )
                    )
                case "in":
                    lsv = cast(list[str], value)
                    if len(lsv) > 0:
                        exprs.append(ex.column(colname, quoted=True).isin(*lsv))
                case "not in":
                    lsv = cast(list[str], value)
                    if len(lsv) > 0:
                        exprs.append(~ex.column(colname, quoted=True).isin(*lsv))
                case "between":
                    lsv = cast(list[str], value)
                    if len(lsv) == 2:
                        exprs.append(
                            ex.column(colname, quoted=True).between(lsv[0], lsv[1])
                        )
                    else:
                        from fastapi import HTTPException

                        raise HTTPException(
                            400, "Must have an array with 2 elements for between"
                        )
                case "not between":
                    lsv = cast(list[str], value)
                    if len(lsv) == 2:
                        exprs.append(
                            ~ex.column(colname, quoted=True).between(lsv[0], lsv[1])
                        )
                    else:
                        from fastapi import HTTPException

                        raise HTTPException(
                            400, "Must have an array with 2 elements for between"
                        )

                case operator:
                    logger.error(f"wrong parameter for filter {operator}")
    if len(exprs) == 0:
        return None
    return concat_expr(exprs)
