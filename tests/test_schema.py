import sys

sys.path.append(".")


def test_schema():
    from bmsdna.lakeapi.tools.validateschema import validate_schema

    validate_schema()
