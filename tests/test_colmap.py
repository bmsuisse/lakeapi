from fastapi.testclient import TestClient
from .utils import get_app, get_auth
import sys

sys.path.append(".")
client = TestClient(get_app())
auth = get_auth()

engines = ("duckdb", "polars")


def test_no_filter():
    for e in engines:
        response = client.get(
            f"/api/v1/deltatest/table_w_col_map?limit=50&format=json&%24engine={e}",
            auth=auth,
        )
        assert response.status_code == 200
        jsd = response.json()
        assert len(jsd) == 50


def test_col_map_filter():
    for e in engines:
        response = client.get(
            f"/api/v1/deltatest/table_w_col_map?limit=50&format=json&Super_Name_=John Duncan&%24engine={e}",
            auth=auth,
        )
        assert response.status_code == 200
        jsd = response.json()
        assert len(jsd) == 1
        for item in jsd:
            assert item["Super Name_"] == "John Duncan"
