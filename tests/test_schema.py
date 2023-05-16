import sys

sys.path.append(".")


def test_schema():
    from validateschema import validate_schema

    validate_schema()
