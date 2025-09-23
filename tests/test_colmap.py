from fastapi.testclient import TestClient
import sys
import pytest

sys.path.append(".")


engines = ("duckdb", "polars")


@pytest.mark.parametrize("e", engines)
def test_no_filter(client: TestClient, e):
    response = client.get(
        f"/api/v1/deltatest/table_w_col_map?limit=50&format=json&%24engine={e}"
    )
    assert response.status_code == 200
    jsd = response.json()
    assert len(jsd) == 50


@pytest.mark.parametrize("e", engines)
def test_col_map_filter(client: TestClient, e):
    response = client.get(
        f"/api/v1/deltatest/table_w_col_map?limit=50&format=json&Super_Name_=John Duncan&%24engine={e}"
    )
    assert response.status_code == 200
    jsd = response.json()
    assert len(jsd) == 1
    for item in jsd:
        assert item["Super Name_"] == "John Duncan"
