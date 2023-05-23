from dataclasses import dataclass
from typing import Awaitable, Callable, Final
from fastapi import FastAPI, Request
from bmsdna.lakeapi.core.config import BasicConfig, Configs, get_default_config
from bmsdna.lakeapi.core.route import init_routes
import os


@dataclass(frozen=True)
class LakeApiStartInfo:
    start_config: BasicConfig
    config: Configs
    get_username: Callable[[Request], Awaitable]


def init_lakeapi(
    app: FastAPI, start_config: BasicConfig | None = None, config: Configs | str | None = None
) -> LakeApiStartInfo:
    start_config = start_config or get_default_config()
    real_config: Configs
    if config is None:
        real_config = Configs.from_yamls(start_config, os.getenv("CONFIG_PATH", "config.yml"))
    elif isinstance(config, str):
        real_config = Configs.from_yamls(start_config, config)
    else:
        real_config = config
    router, get_username = init_routes(real_config, start_config)
    app.include_router(router)
    return LakeApiStartInfo(start_config, real_config, get_username)
