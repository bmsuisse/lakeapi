import json
import yaml
import pydantic
from bmsdna.lakeapi.core.config import Config, YamlData


def validate_schema(schema_file: str, yaml_file: str):
    td = pydantic.TypeAdapter(YamlData)
    with open(schema_file, "w") as str:
        json.dump(td.json_schema(), str, indent=4)

    with open(schema_file, "r") as str2:
        schema = json.load(str2)

    with open(yaml_file, "r", encoding="utf-8") as r:
        data = yaml.safe_load(r)
        json_str = json.dumps(data, indent=4)
        td.validate_json(json_str)
        print("ok")


def validate_schema_cli():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--yaml-file", default="config.yml")
    parser.add_argument("--schema-file", default="config_schema.json")
    args = parser.parse_args()
    validate_schema(args.schema_file, args.yaml_file)


if __name__ == "__main__":
    validate_schema_cli()
