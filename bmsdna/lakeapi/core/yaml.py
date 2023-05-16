import yaml
from bmsdna.lakeapi.core.config import YamlData


def get_yaml(file_path) -> YamlData:
    with open(file_path, encoding="utf-8") as f:
        yaml_config = yaml.safe_load(f)
        return yaml_config
