import os
import re
from dataclasses import dataclass
from datetime import datetime
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Literal,
    Optional,
    Sequence,
    Tuple,
    TypedDict,
    Union,
    cast,
    TYPE_CHECKING,
)
from typing_extensions import TypedDict, NotRequired, Required
from __future__ import annotations

import copy
import yaml
from bmsdna.lakeapi.context.source_uri import SourceUri

from bmsdna.lakeapi.core.log import get_logger
from bmsdna.lakeapi.core.partition_utils import _with_implicit_parameters
from bmsdna.lakeapi.core.types import (
    FileTypes,
    OperatorType,
    PolaryTypeFunction,
    Engines,
    SearchConfig,
    NearbyConfig,
)
import expandvars
from pathlib import Path
from hashlib import md5

if TYPE_CHECKING:
    from bmsdna.lakeapi.context.df_base import ExecutionContext
logger = get_logger(__name__)


from typing import List, Optional, Union

from pydantic import BaseModel


class App(BaseModel):
    title: str
    description: str
    version: str
    static_file_path: str
    logo_path: str


class TestAccount(BaseModel):
    use_emulator: str


class Accounts(BaseModel):
    test_account: TestAccount


class Param(BaseModel):
    name: str
    operators: Optional[List[str]] = None
    combi: Optional[List[str]] = None


class SortbyItem(BaseModel):
    by: str
    direction: Optional[str] = None


class SelectItem(BaseModel):
    name: str
    alias: Optional[str] = None


class Datasource(BaseModel):
    uri: str
    file_type: Optional[str] = None
    sortby: Optional[List[SortbyItem]] = None
    table_name: Optional[str] = None
    select: Optional[List[SelectItem]] = None
    account: Optional[str] = None


class NearbyItem(BaseModel):
    name: str
    lat_col: str
    lon_col: str


class SearchItem(BaseModel):
    name: str
    columns: List[str]


class Table(BaseModel):
    name: str
    tag: str
    version: Optional[int] = None
    api_method: Union[str, List[str]]
    params: Optional[List[Union[str, Param]]] = None
    datasource: Optional[Datasource] = None
    allow_get_all_pages: Optional[bool] = None
    nearby: Optional[List[NearbyItem]] = None
    engine: Optional[str] = None
    search: Optional[List[SearchItem]] = None
    config_from_delta: Optional[bool] = None


class User(BaseModel):
    name: str
    passwordhash: str


class Model(BaseModel):
    app: App
    accounts: Accounts
    tables: List[Table]
    users: List[User]


def get_model_from_yaml():
    with open("model.yaml", "r") as f:
        model = yaml.safe_load(f)
    return Model(**model)
