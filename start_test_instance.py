import dataclasses
import bmsdna.lakeapi
import fastapi
import test_server
import os
import asyncio

no_sql = os.getenv("NO_SQL_SERVER", "0") == "1"
sql_server = test_server.start_mssql_server() if not no_sql else None
print("after start mssql")
app = fastapi.FastAPI()


def init():
    def_cfg = bmsdna.lakeapi.get_default_config()  # Get default startup config
    cfg = dataclasses.replace(
        def_cfg, enable_sql_endpoint=True, data_path="tests/data"
    )  # Use dataclasses.replace to set the properties you want
    sti = bmsdna.lakeapi.init_lakeapi(
        app, True, cfg, "config_test.yml"
    )  # Enable it. The first parameter is the FastAPI instance, the 2nd one is the basic config and the third one the config of the tables


init()
