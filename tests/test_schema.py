import sys

sys.path.append(".")


def test_schema():
    from bmsdna.lakeapi.tools.validateschema import validate_schema

    validate_schema("config_schema.json", "config_test.yml")
