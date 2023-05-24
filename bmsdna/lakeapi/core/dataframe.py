import asyncio
import hashlib
import os
from dataclasses import field
from typing import (
    Any,
    Callable,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
    cast,
    get_args,
    overload,
)

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.parquet
from aiocache import Cache, cached
from aiocache.serializers import PickleSerializer

from bmsdna.lakeapi.core.config import BasicConfig, DatasourceConfig, GroupByConfig, GroupByExpConfig, Param
from bmsdna.lakeapi.core.env import CACHE_EXPIRATION_TIME_SECONDS
from bmsdna.lakeapi.core.log import get_logger
from bmsdna.lakeapi.core.model import get_param_def, should_hide_colname
from bmsdna.lakeapi.core.types import DeltaOperatorTypes, FileTypes
from bmsdna.lakeapi.context.df_base import ResultData, ExecutionContext
import pypika
from pypika.queries import QueryBuilder
import pypika.queries as fn
import duckdb

logger = get_logger(__name__)

endpoints = Literal["query", "meta", "request", "sql"]


cache = cached(ttl=CACHE_EXPIRATION_TIME_SECONDS, cache=Cache.MEMORY, serializer=PickleSerializer())


def get_table_name_from_uri(uri: str):
    parts = uri.split("/")
    try:
        return parts[-2].replace(".", "_") + "_" + parts[-1].replace(".", "_")
    except IndexError:
        return uri


class Dataframe:
    def __init__(
        self,
        version: str,
        tag: str,
        name: str,
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

    @property
    def uri(self):
        return os.path.join(
            self.basic_config.data_path,
            self.config.uri,
        )

    def file_exists(self):
        if not os.path.exists(self.uri):
            return False
        if self.config.file_type == "delta" and not os.path.exists(os.path.join(self.uri, "_delta_log")):
            return False
        return True

    def filter_df(self):
        raise NotImplementedError()

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
                    table=pypika.Table(get_table_name_from_uri(self.config.uri)),
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
        return self.version + "_" + self.tag + "_" + self.name + "_" + get_table_name_from_uri(self.config.uri)

    def get_df(
        self,
        partitions: Optional[List[Tuple[str, str, Any]]],
        endpoint: endpoints = "request",
    ) -> ResultData:
        if self.df is None:
            path = self.uri

            tname = self.tablename
            df = self.sql_context.register_dataframe(
                tname,
                path,
                self.config.file_type,
                partitions=partitions,
            )
            query = pypika.Query.from_(tname)
            self.query = self._prep_df(query, endpoint=endpoint)
            self.df = self.sql_context.execute_sql(self.query)

        return self.df  # type: ignore


@cache
async def get_partition_filter(param, deltaMeta, param_def):
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
                hashvl = [hashlib.md5(v.encode("utf8")).hexdigest() for v in value]
                value_for_partitioning = [hvl[:prefix_len] for hvl in hashvl]
            else:
                hashvl = hashlib.md5(value.encode("utf8")).hexdigest()
                value_for_partitioning = hashvl[:prefix_len]
            if op not in operators:
                col_for_partitioning = None
                continue
        elif partcol.startswith(colname + "_md5_mod_"):
            col_for_partitioning = partcol
            modulo_len = int(partcol[len(colname + "_md5_mod_") :])
            if isinstance(value, (List, Tuple)):
                hashvl = [int(hashlib.md5(v.encode("utf8")).hexdigest(), 16) for v in value]
                value_for_partitioning = [hvl % modulo_len for hvl in hashvl]
            else:
                hashvl = int(hashlib.md5(value.encode("utf8")).hexdigest(), 16)
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


@cache
async def filter_partitions_based_on_params(deltaMeta, params, param_def):
    if len(deltaMeta.partition_columns) == 0:
        return None

    partition_filters = []
    tasks = [get_partition_filter(param, deltaMeta, param_def) for param in params.items()]
    results = await asyncio.gather(*tasks)
    partition_filters = [result for result in results if result is not None]

    return partition_filters if len(partition_filters) > 0 else None


@cache
async def concat_expr(
    exprs: Union[list[pypika.Criterion], list[pa.compute.Expression]],
) -> Union[pypika.Criterion, pa.compute.Expression]:
    expr: Optional[pypika.Criterion] = None
    for e in exprs:
        if expr is None:
            expr = e
        else:
            expr = expr.__and__(e)
    return cast(pypika.Criterion, expr)


@cache
async def _create_inner_expr(columns: Optional[List[str]], prmdef, e):
    inner_expr: Optional[pypika.Criterion] = None
    for ck, cv in e.items():
        if (columns and not ck in columns) and not ck in prmdef.combi:
            pass
        else:
            if inner_expr is None:
                inner_expr = pypika.Field(ck) == cv if cv else pypika.Field(ck).isnull()
            else:
                inner_expr = inner_expr & (pypika.Field(ck) == cv if cv else pypika.Field(ck).isnull())
    return inner_expr


@cache
async def filter_df_based_on_params(
    params: dict[str, Any],
    param_def: list[Union[Param, str]],
    columns: Optional[list[str]],
) -> Optional[pypika.Criterion]:
    expr: Optional[pypika.Criterion] = None
    exprs: list[pypika.Criterion] = []

    for key, value in params.items():
        if not key or not value or key in ("limit", "offset"):
            continue  # can that happen? I don't know
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
                exprs.append(outer_expr)

        elif columns and not colname in columns:
            pass

        else:
            match op:
                case "<":
                    exprs.append(fn.Field(colname) < value)
                case ">":
                    exprs.append(fn.Field(colname) > value)
                case ">=":
                    exprs.append(fn.Field(colname) >= value)
                case "<=":
                    exprs.append(fn.Field(colname) <= value)
                case "<>":
                    exprs.append(fn.Field(colname) != value if value is not None else fn.Field(colname).isnotnull())
                case "==":
                    exprs.append(fn.Field(colname) == value if value is not None else fn.Field(colname).isnull())
                case "=":
                    exprs.append(fn.Field(colname) == value if value is not None else fn.Field(colname).isnull())
                case "not contains":
                    exprs.append(fn.Field(colname).not_like.contains("%" + value + "%"))
                case "contains":
                    exprs.append(fn.Field(colname).like("%" + value + "%"))
                case "in":
                    lsv = cast(list[str], value)
                    if len(lsv) > 0:
                        exprs.append(fn.Field(colname).isin(lsv))
                case "not in":
                    lsv = cast(list[str], value)
                    if len(lsv) > 0:
                        exprs.append(~fn.Field(colname).isin(lsv))
                case "between":
                    lsv = cast(list[str], value)
                    if len(lsv) == 2:
                        exprs.append(fn.Field(colname).between(lsv[0], lsv[1]))
                case "not between":
                    lsv = cast(list[str], value)
                    if len(lsv) == 2:
                        exprs.append(~fn.Field(colname).between(lsv[0], lsv[1]))

                case operator:
                    logger.error(f"wrong parameter for filter {operator}")

    expr = await concat_expr(exprs)
    return expr
