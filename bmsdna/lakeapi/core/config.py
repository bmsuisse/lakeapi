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
import copy
import yaml
from bmsdna.lakeapi.core.env import CACHE_EXPIRATION_TIME_SECONDS

from bmsdna.lakeapi.core.log import get_logger
from bmsdna.lakeapi.core.partition_utils import _with_implicit_parameters
from bmsdna.lakeapi.core.types import FileTypes, OperatorType, Param, PolaryTypeFunction, Engines, SearchConfig

if TYPE_CHECKING:
    from bmsdna.lakeapi.context.df_base import ExecutionContext
logger = get_logger(__name__)


@dataclass(frozen=True)
class BasicConfig:
    enable_sql_endpoint: bool
    temp_folder_path: str
    data_path: str
    min_search_length: int
    default_engine: Engines
    default_chunk_size: int
    prepare_sql_db_hook: "Callable[[ExecutionContext], Any] | None"


def get_default_config():
    return BasicConfig(
        enable_sql_endpoint=os.getenv("ENABLE_SQL_ENDPOINT", "0") == "1",
        temp_folder_path=os.getenv("TEMP", "/tmp"),
        data_path=os.environ.get("DATA_PATH", "data"),
        min_search_length=3,
        default_engine="duckdb",
        default_chunk_size=10000,
        prepare_sql_db_hook=None,
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
class DatasourceConfig:
    uri: str
    file_type: FileTypes = "delta"
    select: Optional[List[Column]] = None
    exclude: Optional[List[str]] = None
    sortby: Optional[List[SortBy]] = None
    filters: Optional[List[Filter]] = None
    table_name: Optional[str] = None
    in_memory: bool = False
    cache_expiration_time_seconds: Optional[int] = CACHE_EXPIRATION_TIME_SECONDS


@dataclass
class Config:
    name: str
    tag: str
    datasource: Optional[DatasourceConfig] = None
    config_from_delta: Optional[bool] = False
    version: Optional[int] = 1
    api_method: Union[Literal["get", "post"], List[Literal["get", "post"]]] = "get"
    params: Optional[List[Union[Param, str]]] = None
    timestamp: Optional[datetime] = None
    cache_expiration_time_seconds: Optional[int] = CACHE_EXPIRATION_TIME_SECONDS
    allow_get_all_pages: Optional[bool] = False
    search: Optional[List[SearchConfig]] = None
    engine: Optional[Engines] = None
    chunk_size: Optional[int] = None

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
    def _from_dict(cls, config: Dict, basic_config: BasicConfig):
        name = config["name"]
        tag = config["tag"]
        datasource: dict[str, Any] = config.get("datasource", {})
        file_type: FileTypes = datasource.get("file_type", "delta")
        uri = datasource.get("uri", tag + "/" + name)
        if config.get("config_from_delta"):
            assert file_type == "delta"
            real_path = os.path.join(basic_config.data_path, uri)
            if not os.path.exists(os.path.join(real_path, "_delta_log")):
                logger.warning(f"Not a real delta path: {real_path}")
            else:
                import deltalake
                import json

                try:
                    dt = deltalake.DeltaTable(real_path)
                    cfg = json.loads(dt.metadata().configuration.get("lakeapi.config", "{}"))
                    config = config | cfg  # simple merge. in that case we expect config to be in delta mainly
                    datasource = config.get(
                        "datasource", {"uri": uri, "file_type": file_type}
                    )  # get data source again, could have select, columns etc
                except json.JSONDecodeError as err:
                    logger.warning(f"Not correct json: {real_path}\n{err}")

        version = config.get("version", 1)
        api_method = cast(Literal["post", "get"], config.get("api_method", "get"))
        params = config.get("params")
        assert isinstance(uri, str)
        columns = datasource.get("columns") if datasource else None
        select = datasource.get("select") if datasource else None
        exclude = datasource.get("exclude") if datasource else None
        sortby = datasource.get("sortby") if datasource else None
        cache_expiration_time_seconds = (
            datasource.get("cache_expiration_time_seconds", CACHE_EXPIRATION_TIME_SECONDS) if datasource else None
        )
        orderby = datasource.get("orderby") if datasource else None
        search_config = [SearchConfig(**c) for c in config["search"]] if "search" in config else None
        logger.debug(config)

        _params: list[Param] = []
        if params:
            logger.debug(f"Parameter: {name} -> {params}")

            _params += [Param(name=param) if isinstance(param, str) else Param(**param) for param in params]

        sortby = orderby if orderby else sortby
        if sortby:
            logger.debug(f"SortBy: {name} -> {sortby}")
            sortby = [SortBy(by=s.get("by"), direction=s.get("direction")) for s in sortby]

        select = columns if columns else select

        if select:
            select = [Column(name=c.get("name"), alias=c.get("alias")) for c in select]

        datasource_obj = DatasourceConfig(
            uri=uri,
            file_type=file_type,
            select=select,
            exclude=exclude,
            in_memory=datasource.get("in_memory", False),
            sortby=sortby,
            table_name=datasource.get("table_name", None),
            filters=None,
            cache_expiration_time_seconds=cache_expiration_time_seconds,
        )
        new_params = _with_implicit_parameters(_params, file_type, basic_config, datasource_obj.uri)

        return cls(
            name=name,
            tag=tag,
            version=version,
            search=search_config,
            api_method=api_method,
            engine=config.get("engine", None),
            chunk_size=config.get("chunk_size", None),
            params=new_params,  # type: ignore
            allow_get_all_pages=config.get("allow_get_all_pages", False),
            datasource=datasource_obj,
            cache_expiration_time_seconds=cache_expiration_time_seconds,
        )

    @classmethod
    def from_dict(
        cls, config: Dict, basic_config: BasicConfig, table_names: List[tuple[int, str, str]]
    ) -> List["Config"]:
        name = config["name"]
        tag = config["tag"]

        if name == "*":
            uri = config.get("datasource", {}).get("uri", tag + "/*")
            assert uri.endswith("/*")
            folder = uri.rstrip("/*")
            root_folder = os.path.join(basic_config.data_path, folder)
            if not os.path.exists(root_folder):
                logger.warning("Path not existing: " + root_folder)
                return []
            else:
                ls = []
                for it in os.scandir(root_folder):
                    config_sub = copy.deepcopy(config)  # important! deepcopy required for subobjects like datasource
                    config_sub["name"] = it.name
                    config_sub["datasource"]["uri"] = config["datasource"]["uri"].replace("/*", "/" + it.name)
                    tbl_name = (config_sub.get("version", 1), config_sub["tag"], config_sub["name"])
                    file_type = config_sub["datasource"].get("file_type", "delta")
                    res_name = config_sub
                    if tbl_name not in table_names and (
                        (it.is_dir() and file_type == "delta") or (it.is_file() and file_type != "delta")
                    ):
                        ls.append(cls._from_dict(config_sub, basic_config))
            return ls
        else:
            return [cls._from_dict(config, basic_config)]

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
