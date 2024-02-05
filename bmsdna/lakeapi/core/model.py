# pyright: reportUndefinedVariable=false, reportGeneralTypeIssues=false

import datetime
from typing import Any, Dict, List, Literal, Union, cast, Optional, Iterable
from bmsdna.lakeapi.context.df_base import ResultData
from fastapi import Query
from pydantic import ConfigDict, BaseModel, create_model
from pydantic.fields import FieldInfo
from bmsdna.lakeapi.context.df_base import ResultData
from bmsdna.lakeapi.core.config import Param, SearchConfig, NearbyConfig
from bmsdna.lakeapi.core.partition_utils import should_hide_colname
from bmsdna.lakeapi.core.types import OperatorType
import pyarrow as pa
import logging

logger = logging.getLogger(__name__)


def _make_model(v, name):
    if type(v) is dict:
        return (
            create_model(  # type: ignore
                name,
                **{k: _make_model(v, k) for k, v in v.items()},  # type: ignore
            ),
            ...,
        )  # type: ignore
    return type(v), None


def make_model(v: Dict, name: str):
    return _make_model(v, name)[0]


empty_model = create_model("NoQuery")


class GeoModel(BaseModel):
    lat: float
    lon: float
    distance_m: float


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
    "has": "_has",
    "startswith": "_startswith",
}


async def get_param_def(queryname: str, paramdef: list[Union[Param, str]]) -> Optional[tuple[Param, OperatorType]]:
    casefoldqueryname = queryname.casefold().replace(" ", "_")
    for param in paramdef:
        param = Param(name=param) if isinstance(param, str) else param
        operators = param.operators or ["="]
        for operator in operators:
            postfix = _operator_postfix_map[operator]
            if (param.name + postfix).casefold().replace(" ", "_") == casefoldqueryname:
                return (param, operator)
    return None


def get_schema_for(model_ns: str, field_name: Optional[str], field_type: pa.DataType) -> tuple[Union[type, Any], Any]:
    if pa.types.is_timestamp(field_type):
        return (Union[datetime.datetime, None], None)
    if pa.types.is_duration(field_type):
        return (Union[datetime.time, None], None)
    if pa.types.is_date(field_type):
        return (Union[datetime.date, None], None)
    if pa.types.is_time(field_type):
        return (Union[datetime.time, None], None)
    if pa.types.is_map(field_type):
        return (Union[dict, None], None)
    if pa.types.is_struct(field_type):
        st = field_type
        assert isinstance(st, pa.StructType)
        res = {
            st.field(ind).name: get_schema_for(model_ns, st.field(ind).name, st.field(ind).type)
            for ind in range(0, st.num_fields)
        }
        return (
            Union[
                create_model(model_ns + ("_" + field_name if field_name else ""), **res, __base__=TypeBaseModel),  # type: ignore
                None,
            ],
            None,
        )
    if pa.types.is_list(field_type) or pa.types.is_large_list(field_type):
        if field_type.value_type is None:
            return (Union[List[Any], None], [])

        itemtype = cast(Any, get_schema_for(model_ns, field_name, field_type.value_type)[0])
        return (Union[List[itemtype], None], [])
    if pa.types.is_integer(field_type):
        return (Union[int, None], None)
    if pa.types.is_boolean(field_type):
        return (Union[bool, None], None)
    if pa.types.is_decimal(field_type) or pa.types.is_floating(field_type):
        return (Union[float, None], None)
    if pa.types.is_string(field_type) or pa.types.is_large_string(field_type):
        return (Union[str, None], None)
    if pa.types.is_union(field_type):
        types = [
            get_schema_for(model_ns, field_type.field(ind).name, field_type.field(ind).type)
            for ind in range(0, field_type.num_fields)
        ]
        types.append(None)  # type: ignore
        return Union.__call__(*types)
    raise ValueError("Not supported")


def _get_datatype(
    schema: Optional[pa.Schema],
    name: str,
    *,
    inner: bool = False,
):
    if schema is None:
        return str | None
    try:
        field = schema.field(name)
        if inner and pa.types.is_list(field.type):
            return get_schema_for("prm_" + name, field.name, field.type.value_type)[0]
        return get_schema_for("prm_" + name, field.name, field.type)[0]
    except KeyError as err:
        return str | None


def _fix_space_names(
    query_params: dict[str, tuple[str, Any]],
):
    res_dict = {}
    for key, value in query_params.items():
        if " " in key or key.startswith("_"):
            new_key = key.replace(" ", "_")
            if new_key.startswith("_"):
                new_key = "p" + new_key
            res_dict[new_key] = (value[0], FieldInfo(default=value[1], title=key))
        else:
            res_dict[key] = (value[0], FieldInfo(default=value[1], title=key))
    return res_dict


def create_parameter_model(
    schema: Optional[pa.Schema],
    name: str,
    params: Optional[Iterable[Union[Param, str]]],
    search: Optional[Iterable[SearchConfig]],
    nearby: Optional[Iterable[NearbyConfig]],
    apimethod: Literal["get", "post"],
):
    query_params: dict[str, tuple[str, Any]] = {}
    if not params and not search and not nearby:
        return empty_model
    for param in params or []:
        param = Param(name=param) if isinstance(param, str) else param
        operators = param.operators or ["="]
        if param.combi:
            if apimethod == "post":  # Only supported in POST Requets for now
                query_params[param.name] = (  # type: ignore
                    Union[List[dict[str, Any]], None],  # type: ignore
                    Query(default=param.real_default if not param.required else ...),
                )
        else:
            realtype = (
                _get_datatype(schema, param.name) if schema is not None else str | None
            )  # well. no model, no real thing. just string
            for operator in operators:
                postfix = _operator_postfix_map[operator]

                if operator in ["in", "not in", "between", "not between"]:
                    if apimethod == "post":  # Only supported in POST Requets for now
                        query_params[param.name + postfix] = (  # type: ignore
                            Union[List[realtype], List[dict[str, realtype]], None],  # type: ignore
                            Query(default=param.real_default if not param.required else ...),
                        )
                elif operator in ["has"] and schema is not None:
                    query_params[param.name + postfix] = (  # type: ignore
                        _get_datatype(schema, param.name, inner=True),
                        param.real_default if not param.required else ...,
                    )
                else:
                    query_params[param.name + postfix] = (  # type: ignore
                        realtype,
                        param.real_default if not param.required else ...,
                    )
    for sc in search or []:
        query_params[sc.name] = (  # type: ignore
            Optional[str],
            None,
        )
    if apimethod == "post":
        for nb in nearby or []:
            query_params[nb.name] = (  # type: ignore
                Optional[GeoModel],
                None,
            )
    query_params = _fix_space_names(query_params)
    try:
        query_model = create_model(name + "Parameter", **query_params)  # type: ignore
    except Exception as err:
        # we do not want to throw here as this fails startup which is bad
        logger.error(f"Could not create parameter model for {name}. Use empty instead")
        query_model = create_model(name + "Parameter")
    return query_model


class TypeBaseModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)


def create_response_model(
    name: str,
    schema: pa.Schema,
) -> type[BaseModel]:
    props = {
        k: get_schema_for(name, schema.field(k).name, schema.field(k).type)
        for k in schema.names
        if not should_hide_colname(k)
    }

    return create_model(name, **props, __base__=TypeBaseModel)  # type: ignore
