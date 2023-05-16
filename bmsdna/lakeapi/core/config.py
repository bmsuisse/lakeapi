import ast
import os
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import (
    Any,
    Dict,
    List,
    Literal,
    NotRequired,
    Optional,
    Required,
    Tuple,
    TypedDict,
    Union,
    cast,
)

import yaml
from polars.type_aliases import JoinStrategy

from bmsdna.lakeapi.core.log import get_logger
from bmsdna.lakeapi.core.types import FileTypes, OperatorType, PolaryTypeFunction, Engines
from bmsdna.lakeapi.core.env import CACHE_EXPIRATION_TIME_SECONDS, DATA_PATH

logger = get_logger(__name__)


@dataclass
class Param:
    name: str
    combi: Optional[List[str] | None] = None
    default: Optional[str] = None
    required: Optional[bool] = False
    operators: Optional[list[OperatorType]] = None
    colname: Optional[str] = None

    @property
    def real_default(self) -> str:
        self._real_default = ast.literal_eval(self.default) if self.default else None
        return cast(str, self._real_default)


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
    func: PolaryTypeFunction | None = None
    alias: str | None = None

    @classmethod
    def from_dict(cls, config: Dict):
        return cls(
            col=config["col"], func=config.get("func"), alias=config.get("alias")
        )


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
class DataframeConfig:
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
class SearchConfig:
    name: str
    columns: List[str]


@dataclass
class Option:
    ...


@dataclass
class Config:
    name: str
    tag: str
    dataframe: DataframeConfig
    route: Optional[str] = None
    version: Optional[int] = 1
    api_method: Literal["get", "post"] | List[Literal["get", "post"]] = "get"
    params: Optional[List[Param | str]] = None
    engine: Engines = "duckdb"
    timestamp: Optional[datetime] = None
    cache_expiration_time_seconds: Optional[int] = CACHE_EXPIRATION_TIME_SECONDS
    options: Optional[Union[Option, None]] = None
    allow_get_all_pages: Optional[bool] = False
    search: Optional[List[SearchConfig]] = None

    def __post_init__(self):
        self.version_str = (
            self.version
            if str(self.version or 1).startswith("v")
            else "v" + str(self.version or 1)
        )

    @property
    def real_route(self) -> str:
        self.route = (
            self.route
            if self.route
            else self.create_route(self.tag, self.name, self.version_str)
        )
        return self.route

    def __repr__(self) -> str:
        kws = [f"{key}={value!r}" for key, value in self.__dict__.items()]
        return "{}({})".format(type(self).__name__, ", ".join(kws))

    def __str__(self) -> str:
        kws = [f"{key}={value!r}" for key, value in self.__dict__.items()]
        return "{}({})".format(type(self).__name__, ", ".join(kws))

    def create_route(self, tag, name, version):
        return "/api/" + version + "/" + tag + "/" + name

    @classmethod
    def from_dict(cls, config: Dict) -> List["Config"]:
        name = config["name"]
        tag = config["tag"]
        version = config.get("version", 1)
        engine = config.get("engine", "duckdb")
        api_method = cast(Literal["post", "get"], config.get("api_method", "get"))
        params = config.get("params")
        route = config.get("route")
        dataframe = config["dataframe"]
        uri = dataframe["uri"]
        assert isinstance(uri, str)
        file_type = cast(
            FileTypes, dataframe.get("file_type", "delta") if dataframe else "delta"
        )
        columns = dataframe.get("columns") if dataframe else None
        select = dataframe.get("select") if dataframe else None
        exclude = dataframe.get("exclude") if dataframe else None
        groupby = dataframe.get("groupby") if dataframe else None
        sortby = dataframe.get("sortby") if dataframe else None
        cache_expiration_time_seconds = (
            dataframe.get(
                "cache_expiration_time_seconds", CACHE_EXPIRATION_TIME_SECONDS
            )
            if dataframe
            else None
        )
        orderby = dataframe.get("orderby") if dataframe else None
        joins = dataframe.get("joins") if dataframe else None
        search_config = [SearchConfig(**c) for c in config["search"]] if "search" in config else None
        logger.debug(config)

        _params: list[Param] = []
        if params:
            logger.debug(f"Parameter: {name} -> {params}")

            _params += [
                Param(name=param) if isinstance(param, str) else Param(**param)
                for param in params
            ]

        if joins:
            logger.debug(f"Joins: {name} -> {joins}")
            joins = [Join(**join) for join in joins]

        if groupby:
            logger.debug(f"Groupby: {name} -> {groupby}")
            expressions = [
                GroupByExpConfig.from_dict(e) for e in groupby.get("expressions")
            ]
            groupby = GroupByConfig(by=groupby.get("by"), expressions=expressions)

        sortby = orderby if orderby else sortby
        if sortby:
            logger.debug(f"SortBy: {name} -> {sortby}")
            sortby = [
                SortBy(by=s.get("by"), direction=s.get("direction")) for s in sortby
            ]

        select = columns if columns else select

        if select:
            select = [Column(name=c.get("name"), alias=c.get("alias")) for c in select]

        if not engine:
            engine = "duckdb"

        if name == "*":
            assert uri.endswith("/*")
            folder = uri.rstrip("/*")
            root_folder = os.path.join(DATA_PATH, folder)
            if not os.path.exists(root_folder):
                logger.warning("Path not existing: " + root_folder)
                return []
            else:
                ls = []
                for it in os.scandir(root_folder):
                    if it.is_dir():
                        dataframe = DataframeConfig(
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
                                route=route,
                                engine=engine,  # type: ignore
                                version=version,
                                api_method=api_method,
                                search=search_config,
                                params=_params,  # type: ignore
                                allow_get_all_pages=config.get(
                                    "allow_get_all_pages", False
                                ),
                                dataframe=dataframe,
                                cache_expiration_time_seconds=cache_expiration_time_seconds,
                            )
                        )
            return ls
        else:
            dataframe = DataframeConfig(
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
                    route=route,
                    version=version,
                    search=search_config,
                    engine=engine,  # type: ignore
                    api_method=api_method,
                    params=_params,  # type: ignore
                    allow_get_all_pages=config.get("allow_get_all_pages", False),
                    dataframe=dataframe,
                    cache_expiration_time_seconds=cache_expiration_time_seconds,
                )
            ]

    async def to_dict(self) -> dict:
        return self.__dict__


@dataclass
class Configs:
    configs: List[Config] = field(default_factory=List)

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
        root: str = "config",
        exclude_internal: bool = True,
        internal_pattern: str = "_",
    ):
        yamls = []
        if re.search(r"\.ya?ml$", root):
            files = [root]
        else:
            files = [
                os.path.join(root, file)
                for file in os.listdir(root)
                if not file.startswith(internal_pattern) or not exclude_internal
            ]

        for file in files:
            if file.endswith(".yml"):
                with open(file, encoding="utf-8") as f:
                    yaml_data = yaml.safe_load(f).get("tables")
                    yamls += [y for y in yaml_data if y.get("name")]
        flat_map = lambda f, xs: [y for ys in xs for y in f(ys)]
        configs = flat_map(Config.from_dict, yamls)
        return cls(configs)


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
    http: HttpConfig
    users: list[UserConfig]
    app: AppConfig
