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
def test_parquet_copied(engine, client: TestClient):
    response = client.get(
        f"/api/v1/blobb/blob_test_copied?format=json&limit=50&$engine={engine}",
    )
    assert response.status_code == 200
    fakedt = response.json()
    assert len(fakedt) == 50

    response2 = client.get(
        f"/api/v1/blobb/blob_test_copied?format=json&limit=50&$engine={engine}",
    )
    assert response2.status_code == 200
    fakedt2 = response2.json()
    assert len(fakedt2) == 50


@pytest.mark.parametrize("engine", engines)
def test_delta(engine, client: TestClient):
    response = client.get(
        f"/api/v1/blobb/fake?format=json&limit=50&$engine={engine}",
    )
    assert response.status_code == 200
    fakedt = response.json()
    assert len(fakedt) == 50
