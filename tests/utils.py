from fastapi import Depends, FastAPI
import dataclasses
import os


def get_app():
    import bmsdna.lakeapi

    app = FastAPI()
    def_cfg = bmsdna.lakeapi.get_default_config()
    cfg = dataclasses.replace(def_cfg, enable_sql_endpoint=True, data_path="tests/data")
    bmsdna.lakeapi.init_lakeapi(app, cfg, "config_test.yml")

    @app.get("/")
    async def root(username: str = Depends(def_cfg.get_username)):
        return {"User": username}

    return app


def get_auth():
    user = "test"
    pw = "B~C:BB*_9-1u"
    return (user, pw)
