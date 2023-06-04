import sys

sys.path.append(".")

from bmsdna.lakeapi.core.yaml import get_yaml


def test_load_yaml():
    y = get_yaml("config_test.yml")
    assert y.get("app").get("title") == "LakeAPI"
