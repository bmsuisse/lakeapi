from dataclasses import dataclass
from fastapi import FastAPI, Request
from bmsdna.lakeapi.core.config import BasicConfig, Configs, get_default_config
from bmsdna.lakeapi.core.route import init_routes
from bmsdna.lakeapi.core.env import CACHE_EXPIRATION_TIME_SECONDS
import os


@dataclass(frozen=True)
class LakeApiStartInfo:
    start_config: BasicConfig
    config: Configs


def setup_cashews():
    from cashews import cache

    KB = 1024
    MB = KB * 1024
    GB = MB * 1024

    cache.setup(f"disk://", max_size=GB * 50)


def init_lakeapi(
    app: FastAPI, use_basic_auth: bool, start_config: BasicConfig | None = None, config: Configs | str | None = None
) -> LakeApiStartInfo:
    start_config = start_config or get_default_config()
    real_config: Configs
    if config is None:
        real_config = Configs.from_yamls(start_config, os.getenv("CONFIG_PATH", "config.yml"))
    elif isinstance(config, str):
        real_config = Configs.from_yamls(start_config, config)
    else:
        real_config = config
    router = init_routes(real_config, start_config)
    if use_basic_auth:
        from bmsdna.lakeapi.core.uservalidation import add_user_middlware

        add_user_middlware(app, start_config, real_config.users)
    if CACHE_EXPIRATION_TIME_SECONDS > 0:
        from bmsdna.lakeapi.core.cache import add_cache_middleware

        setup_cashews()
        add_cache_middleware(app)

    app.include_router(router)
    return LakeApiStartInfo(start_config, real_config)
