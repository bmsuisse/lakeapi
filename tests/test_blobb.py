

import pytest

engines = ["duckdb", "polars"]


@pytest.mark.parametrize("engine", engines)
def test_parquet(engine):
    response = client.get(
        f"/api/v1/blobb/blob_test?format=json&limit=50&$engine={engine}",
        auth=auth,
    )
    assert response.status_code == 200
    fakedt = response.json()
    assert len(fakedt) == 50


@pytest.mark.parametrize("engine", engines)
def test_delta(engine):
    response = client.get(
        f"/api/v1/blobb/fake?format=json&limit=50&$engine={engine}",
        auth=auth,
    )
    assert response.status_code == 200
    fakedt = response.json()
    assert len(fakedt) == 50
