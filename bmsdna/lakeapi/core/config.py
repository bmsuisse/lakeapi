import ast
import os
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    List,
    Literal,
    Optional,
    Sequence,
    Tuple,
    TypedDict,
    Union,
    cast,
)
from typing_extensions import TypedDict, NotRequired, Required
from fastapi import APIRouter, Request

import yaml
from polars.type_aliases import JoinStrategy
from bmsdna.lakeapi.core.env import CACHE_EXPIRATION_TIME_SECONDS

from bmsdna.lakeapi.core.log import get_logger
from bmsdna.lakeapi.core.types import FileTypes, OperatorType, Param, PolaryTypeFunction, Engines, SearchConfig

logger = get_logger(__name__)


@dataclass(frozen=True)
class BasicConfig:
    username_retriever: Callable[[Request, "BasicConfig", "Sequence[UserConfig]"], str | Awaitable[str]]
    enable_sql_endpoint: bool
    temp_folder_path: str
    data_path: str
    token_jwt_secret: str | None  # None disables the token feature
    min_search_length: int


def get_default_config():
    from bmsdna.lakeapi.core.uservalidation import get_username

    return BasicConfig(
        username_retriever=get_username,
        enable_sql_endpoint=os.getenv("ENABLE_SQL_ENDPOINT", "0") == "1",
        temp_folder_path=os.getenv("TEMP", "/tmp"),
        data_path=os.environ.get("DATA_PATH", "data"),
        token_jwt_secret=os.getenv("JWT_SECRET", None),
        min_search_length=3,
    )


@dataclass
class Filter:
    key: str
    operator: Literal[
        "=",
        ">",
        "<",
        ">=",
        "<=",
        "<>",
        "!=",
        "==",
        "between",
        "not between",
        "in",
        "not in",
    ]
    value: Union[Any, Tuple[Any, Any]]


@dataclass
class SortBy:
    by: str
    direction: Optional[Literal["asc", "desc"]] = "asc"


@dataclass
class Column:
    name: str
    alias: Optional[str] = None

    def __post_init__(self):
        self.alias = self.alias if self.alias else self.name


@dataclass
class GroupByExpConfig:
    # expression: str
    col: str = ""
    func: Optional[PolaryTypeFunction] = None
    alias: Optional[str] = None

    @classmethod
    def from_dict(cls, config: Dict):
        return cls(col=config["col"], func=config.get("func"), alias=config.get("alias"))


@dataclass
class GroupByConfig:
    by: List[str]
    expressions: List[GroupByExpConfig]


@dataclass
class Join:
    uri: str
    left_on: str
    right_on: str
    file_type: FileTypes = "delta"
    how: JoinStrategy = "inner"
    suffix: str = "_right"


@dataclass
class DatasourceConfig:
    uri: str
    file_type: FileTypes = "delta"
    select: Optional[List[Column]] = None
    exclude: Optional[List[str]] = None
    groupby: Optional[GroupByConfig] = None
    sortby: Optional[List[SortBy]] = None
    joins: Optional[List[Join]] = None
    filters: Optional[List[Filter]] = None
    cache_expiration_time_seconds: Optional[int] = CACHE_EXPIRATION_TIME_SECONDS


@dataclass
class Option:
    ...


@dataclass
class Config:
    name: str
    tag: str
    datasource: DatasourceConfig
    version: Optional[int] = 1
    api_method: Union[Literal["get", "post"], List[Literal["get", "post"]]] = "get"
    params: Optional[List[Union[Param, str]]] = None
    engine: Engines = "duckdb"
    timestamp: Optional[datetime] = None
    cache_expiration_time_seconds: Optional[int] = CACHE_EXPIRATION_TIME_SECONDS
    options: Optional[Union[Option, None]] = None
    allow_get_all_pages: Optional[bool] = False
    search: Optional[List[SearchConfig]] = None

    def __post_init__(self):
        self.version_str = (
            str(self.version) if str(self.version or 1).startswith("v") else "v" + str(self.version or 1)
        )
        self.route = "/api/" + self.version_str + "/" + self.tag + "/" + self.name

    def __repr__(self) -> str:
        kws = [f"{key}={value!r}" for key, value in self.__dict__.items()]
        return "{}({})".format(type(self).__name__, ", ".join(kws))

    def __str__(self) -> str:
        kws = [f"{key}={value!r}" for key, value in self.__dict__.items()]
        return "{}({})".format(type(self).__name__, ", ".join(kws))

    @classmethod
    def from_dict(
        cls, config: Dict, basic_config: BasicConfig, table_names: List[tuple[int, str, str]]
    ) -> List["Config"]:
        name = config["name"]
        tag = config["tag"]
        version = config.get("version", 1)
        engine = config.get("engine", "duckdb")
        api_method = cast(Literal["post", "get"], config.get("api_method", "get"))
        params = config.get("params")
        datasource = config["datasource"]
        uri = datasource["uri"]
        assert isinstance(uri, str)
        file_type = cast(FileTypes, datasource.get("file_type", "delta") if datasource else "delta")
        columns = datasource.get("columns") if datasource else None
        select = datasource.get("select") if datasource else None
        exclude = datasource.get("exclude") if datasource else None
        groupby = datasource.get("groupby") if datasource else None
        sortby = datasource.get("sortby") if datasource else None
        cache_expiration_time_seconds = (
            datasource.get("cache_expiration_time_seconds", CACHE_EXPIRATION_TIME_SECONDS) if datasource else None
        )
        orderby = datasource.get("orderby") if datasource else None
        joins = datasource.get("joins") if datasource else None
        search_config = [SearchConfig(**c) for c in config["search"]] if "search" in config else None
        logger.debug(config)

        _params: list[Param] = []
        if params:
            logger.debug(f"Parameter: {name} -> {params}")

            _params += [Param(name=param) if isinstance(param, str) else Param(**param) for param in params]

        if joins:
            logger.debug(f"Joins: {name} -> {joins}")
            joins = [Join(**join) for join in joins]

        if groupby:
            logger.debug(f"Groupby: {name} -> {groupby}")
            expressions = [GroupByExpConfig.from_dict(e) for e in groupby.get("expressions")]
            groupby = GroupByConfig(by=groupby.get("by"), expressions=expressions)

        sortby = orderby if orderby else sortby
        if sortby:
            logger.debug(f"SortBy: {name} -> {sortby}")
            sortby = [SortBy(by=s.get("by"), direction=s.get("direction")) for s in sortby]

        select = columns if columns else select

        if select:
            select = [Column(name=c.get("name"), alias=c.get("alias")) for c in select]

        if not engine:
            engine = "duckdb"

        if name == "*":
            assert uri.endswith("/*")
            folder = uri.rstrip("/*")
            root_folder = os.path.join(basic_config.data_path, folder)
            if not os.path.exists(root_folder):
                logger.warning("Path not existing: " + root_folder)
                return []
            else:
                ls = []
                for it in os.scandir(root_folder):
                    res_name = (version, tag, it.name)
                    if res_name not in table_names and (
                        (it.is_dir() and file_type == "delta") or (it.is_file() and file_type != "delta")
                    ):
                        datasource = DatasourceConfig(
                            uri=folder + "/" + it.name,
                            file_type=file_type,
                            select=select,
                            exclude=exclude,
                            groupby=groupby,
                            sortby=sortby,
                            joins=joins,
                            filters=None,
                            cache_expiration_time_seconds=cache_expiration_time_seconds,
                        )

                        ls.append(
                            cls(
                                name=it.name,
                                tag=tag,
                                engine=engine,  # type: ignore
                                version=version,
                                api_method=api_method,
                                search=search_config,
                                params=_params,  # type: ignore
                                allow_get_all_pages=config.get("allow_get_all_pages", False),
                                datasource=datasource,
                                cache_expiration_time_seconds=cache_expiration_time_seconds,
                            )
                        )
            return ls
        else:
            datasource = DatasourceConfig(
                uri=uri,
                file_type=file_type,
                select=select,
                exclude=exclude,
                groupby=groupby,
                sortby=sortby,
                joins=joins,
                filters=None,
                cache_expiration_time_seconds=cache_expiration_time_seconds,
            )

            return [
                cls(
                    name=name,
                    tag=tag,
                    version=version,
                    search=search_config,
                    engine=engine,  # type: ignore
                    api_method=api_method,
                    params=_params,  # type: ignore
                    allow_get_all_pages=config.get("allow_get_all_pages", False),
                    datasource=datasource,
                    cache_expiration_time_seconds=cache_expiration_time_seconds,
                )
            ]

    async def to_dict(self) -> dict:
        return self.__dict__


class HttpConfig(TypedDict):
    bind: NotRequired[str]
    max_requests: NotRequired[int]
    max_requests_jitter: NotRequired[int]
    timeout: NotRequired[int]
    graceful_timeout: NotRequired[int]


class UserConfig(TypedDict):
    name: Required[str]
    passwordhash: Required[str]


class AppConfig(TypedDict):
    tile: NotRequired[str]
    description: NotRequired[str]
    version: NotRequired[str]
    static_file_path: NotRequired[str]
    logo_path: NotRequired[str]


class YamlData(TypedDict):
    tables: list[Config]
    users: list[UserConfig]
    app: AppConfig


@dataclass(frozen=True)
class Configs:
    configs: Sequence[Config]
    users: Sequence[UserConfig]

    def get_config_by_route(self, route: str):
        for config in self.configs:
            if config.route == route:
                return config

    def __iter__(self):
        for i in self.configs:
            yield i

    @classmethod
    def from_yamls(
        cls,
        basic_config: BasicConfig,
        root: str = "config",
        exclude_internal: bool = True,
        internal_pattern: str = "_",
    ):
        if re.search(r"\.ya?ml$", root):
            files = [root]
        else:
            files = [
                os.path.join(root, file)
                for file in os.listdir(root)
                if not file.startswith(internal_pattern) or not exclude_internal
            ]

        tables = []
        users = []
        for file in files:
            if file.endswith(".yml"):
                with open(file, encoding="utf-8") as f:
                    y = yaml.safe_load(f)
                    yaml_data_tables = y.get("tables")
                    if yaml_data_tables:
                        tables += [y for y in yaml_data_tables if y.get("name")]
                    yaml_data_users = y.get("users")
                    if yaml_data_users:
                        users += yaml_data_users

        flat_map = lambda f, xs: [y for ys in xs for y in f(ys)]
        table_names = [(t.get("version", 1), t["tag"], t["name"]) for t in tables if t["name"] != "*"]
        configs = flat_map(lambda x: Config.from_dict(x, basic_config=basic_config, table_names=table_names), tables)
        return cls(configs, users)
