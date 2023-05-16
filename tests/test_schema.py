import sys

sys.path.append(".")


def test_schema():
    from createschema import create_schema
    from validateschema import validate_schema

    create_schema()
    validate_schema()
