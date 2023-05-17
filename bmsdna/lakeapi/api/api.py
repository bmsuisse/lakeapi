from dataclasses import dataclass
from typing import Callable, Final
from fastapi import FastAPI
from bmsdna.lakeapi.core.config import BasicConfig, Configs, get_default_config
from bmsdna.lakeapi.core.route import init_routes
from bmsdna.lakeapi.core.uservalidation import get_username
import os


def init_lakeapi(app: FastAPI, start_config: BasicConfig | None = None, config: Configs | str | None = None):
    start_config = start_config or get_default_config()
    if config is None:
        config = Configs.from_yamls(start_config, os.getenv("CONFIG_PATH", "config.yml"))
    elif isinstance(config, str):
        config = Configs.from_yamls(start_config, config)
    router = init_routes(config, start_config)
    app.include_router(router)
