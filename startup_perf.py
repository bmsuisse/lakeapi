from bmsdna.lakeapi.core.config import BasicConfig, Configs, get_default_config
import dataclasses
from bmsdna.lakeapi.core.route import init_routes
from bmsdna.lakeapi.api.api import setup_cashews
import os

setup_cashews()
def_cfg = get_default_config()  # Get default startup config
start_config = dataclasses.replace(def_cfg, enable_sql_endpoint=True, data_path="tests/data")
real_config = Configs.from_yamls(start_config, os.getenv("CONFIG_PATH", "config_test.yml"))
router = init_routes(real_config, start_config)
print("done")
