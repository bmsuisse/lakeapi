import datetime
from typing import Any, Dict, List, Literal, Union, cast, Optional, Iterable
from bmsdna.lakeapi.context.df_base import ResultData
from aiocache import Cache, cached
from aiocache.serializers import PickleSerializer
from fastapi import Query
from pydantic import BaseModel, create_model
from bmsdna.lakeapi.context.df_base import ResultData
from bmsdna.lakeapi.core.config import Param, SearchConfig
from bmsdna.lakeapi.core.types import OperatorType
from bmsdna.lakeapi.core.env import CACHE_EXPIRATION_TIME_SECONDS
import pyarrow as pa

cache = cached(ttl=CACHE_EXPIRATION_TIME_SECONDS, cache=Cache.MEMORY, serializer=PickleSerializer())


def _make_model(v, name):
    if type(v) is dict:
        return create_model(name, **{k: _make_model(v, k) for k, v in v.items()}), ...
    return type(v), None


def make_model(v: Dict, name: str):
    return _make_model(v, name)[0]


empty_model = create_model("NoQuery")

_operator_postfix_map: dict[OperatorType, str] = {
    "<": "_lt",
    "<=": "_lte",
    "<>": "_ne",
    "=": "",
    ">": "_gt",
    ">=": "_gte",
    "contains": "_contains",
    "not contains": "_not_contains",
    "in": "_in",
    "not in": "_not_in",
    "not null": "_not_null",
    "null": "_null",
    "between": "_between",
    "not between": "_not_between",
}


@cache
async def get_param_def(queryname: str, paramdef: list[Union[Param, str]]) -> Optional[tuple[Param, OperatorType]]:
    casefoldqueryname = queryname.casefold()
    for param in paramdef:
        param = Param(name=param) if isinstance(param, str) else param
        operators = param.operators or ["="]
        for operator in operators:
            postfix = _operator_postfix_map[operator]
            if (param.name + postfix).casefold() == casefoldqueryname:
                return (param, operator)
    return None


def get_schema_for(model_ns: str, field: pa.Field) -> tuple[Union[type, Any], Any]:
    if pa.types.is_timestamp(field.type):
        return (Union[datetime.datetime, None], None)
    if pa.types.is_duration(field.type):
        return (Union[datetime.time, None], None)
    if pa.types.is_date(field.type):
        return (Union[datetime.date, None], None)
    if pa.types.is_time(field.type):
        return (Union[datetime.time, None], None)
    if pa.types.is_map(field.type):
        return (Union[dict, None], None)
    if pa.types.is_struct(field.type):
        st = field.type
        assert isinstance(st, pa.StructType)
        res = {st.field(ind).name: get_schema_for(model_ns, st.field(ind)) for ind in range(0, st.num_fields)}
        return (
            Union[
                create_model(model_ns + ("_" + field.name if field.name else ""), **res, __base__=TypeBaseModel),
                None,
            ],
            None,
        )
    if pa.types.is_list(field.type) or pa.types.is_large_list(field.type):
        if field.type.value_type is None:
            return (Union[List[Any], None], [])

        itemtype = cast(Any, get_schema_for(model_ns, field.type.value_type)[0])
        return (Union[List[itemtype], None], [])
    if pa.types.is_integer(field.type):
        return (Union[int, None], None)
    if pa.types.is_boolean(field.type):
        return (Union[bool, None], None)
    if pa.types.is_decimal(field.type) or pa.types.is_floating(field.type):
        return (Union[float, None], None)
    if pa.types.is_string(field.type) or pa.types.is_large_string(field.type):
        return (Union[str, None], None)
    if pa.types.is_union(field.type):
        types = [get_schema_for(model_ns, field.type.field(ind)) for ind in range(0, field.type.num_fields)]
        types.append(None)  # type: ignore
        return Union.__call__(*types)
    raise ValueError("Not supported")


def _get_datatype(schema: Optional[pa.Schema], name: str):
    if schema is None:
        return str
    try:
        field = schema.field(name)
        return get_schema_for("prm_" + name, field)[0]
    except KeyError as err:
        return str


def create_parameter_model(
    df: Optional[ResultData],
    name: str,
    params: Optional[Iterable[Union[Param, str]]],
    search: Optional[Iterable[SearchConfig]],
    apimethod: Literal["get", "post"],
):
    query_params = {}
    if params or search:
        for param in params or []:
            param = Param(name=param) if isinstance(param, str) else param
            operators = param.operators or ["="]
            schema = df.arrow_schema() if df else None
            if param.combi:
                if apimethod == "post":  # Only supported in POST Requets for now
                    query_params[param.name] = (
                        Union[List[dict[str, Any]], None],  # type: ignore
                        Query(default=param.real_default if not param.required else ...),
                    )
            else:
                realtype = (
                    _get_datatype(schema, param.name) if df is not None else str
                )  # well. no model, no real thing. just string
                for operator in operators:
                    postfix = _operator_postfix_map[operator]

                    if operator in ["in", "not in", "between", "not between"]:
                        if apimethod == "post":  # Only supported in POST Requets for now
                            query_params[param.name + postfix] = (
                                Union[List[realtype], List[dict[str, realtype]], None],  # type: ignore
                                Query(default=param.real_default if not param.required else ...),
                            )

                    else:
                        query_params[param.name + postfix] = (
                            realtype,
                            param.real_default if not param.required else ...,
                        )
        for sc in search or []:
            query_params[sc.name] = (
                Optional[str],
                None,
            )
        query_model = create_model(name + "Parameter", **query_params)
        return query_model
    return empty_model


class TypeBaseModel(BaseModel):
    class Config:
        orm_mode = True


def should_hide_colname(name: str):
    return name.startswith("_") or "_md5_prefix_" in name or "_xxhash64_prefix_" in name


def create_response_model(name: str, frame: ResultData) -> type[BaseModel]:
    schema = frame.arrow_schema()
    props = {k: get_schema_for(name, schema.field(k)) for k in schema.names if not should_hide_colname(k)}

    return create_model(name, **props, __base__=TypeBaseModel)
