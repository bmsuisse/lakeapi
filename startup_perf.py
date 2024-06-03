from bmsdna.lakeapi.core.config import Configs, get_default_config
import dataclasses
from bmsdna.lakeapi.core.route import init_routes
import os

def_cfg = get_default_config()  # Get default startup config
start_config = dataclasses.replace(
    def_cfg, enable_sql_endpoint=True, data_path="tests/data"
)
real_config = Configs.from_yamls(
    start_config, os.getenv("CONFIG_PATH", "config_test.yml")
)
router = init_routes(real_config, start_config)
print("done")
