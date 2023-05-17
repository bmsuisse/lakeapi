from fastapi import FastAPI
from bmsdna.lakeapi.core.config import Configs
from bmsdna.lakeapi.core.route import init_routes
import os


def init_lakeapi(app: FastAPI):
    configs: Configs = Configs.from_yamls(os.getenv("CONFIG_PATH", "config.yml"))
    router = init_routes(configs)
    app.include_router(router)
