import pytest
from fastapi.testclient import TestClient

engines = ["duckdb", "polars"]


@pytest.mark.parametrize("engine", engines)
def test_parquet(engine, client: TestClient):
    response = client.get(
        f"/api/v1/blobb/blob_test?format=json&limit=50&$engine={engine}",
    )
    assert response.status_code == 200
    fakedt = response.json()
    assert len(fakedt) == 50


@pytest.mark.parametrize("engine", engines)
def test_delta(engine, client: TestClient):
    response = client.get(
        f"/api/v1/blobb/fake?format=json&limit=50&$engine={engine}",
    )
    assert response.status_code == 200
    fakedt = response.json()
    assert len(fakedt) == 50
