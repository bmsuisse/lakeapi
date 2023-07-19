import dataclasses
import bmsdna.lakeapi
import fastapi
import test_server

sql_server = test_server.start_mssql_server()
app = fastapi.FastAPI()

def_cfg = bmsdna.lakeapi.get_default_config()  # Get default startup config
cfg = dataclasses.replace(
    def_cfg, enable_sql_endpoint=True, data_path="tests/data"
)  # Use dataclasses.replace to set the properties you want
sti = bmsdna.lakeapi.init_lakeapi(
    app, True, cfg, "config_test.yml"
)  # Enable it. The first parameter is the FastAPI instance, the 2nd one is the basic config and the third one the config of the tables


@app.on_event("shutdown")
def shutdown_event():
    sql_server.stop()
