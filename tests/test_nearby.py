import sys
import json
from fastapi.testclient import TestClient
import pytest

sys.path.append(".")

engines = ["duckdb", "polars"]


@pytest.mark.parametrize("engine", engines)
def test_nearby(client: TestClient, engine):
    response = client.post(
        f"/api/v1/test/fake_delta?limit=50&format=ndjson&%24engine={engine}",
        json={"nearby": {"lat": 46.7, "lon": 8.6, "distance_m": 10000}},
    )
    assert response.status_code == 200
    lines = [json.loads(l) for l in response.text.split("\n") if len(l) > 0]
    assert (
        len(lines) >= 15 and len(lines) <= 40
    )  # it's a bit fuzzy since distance calc is never 100% accurate

    assert lines[0]["nearby"] <= lines[1]["nearby"]
    assert lines[1]["nearby"] <= lines[2]["nearby"]
    assert lines[2]["nearby"] <= lines[3]["nearby"]
    for item in lines:
        assert item["nearby"] <= 10000


def test_no_nearby(client: TestClient):
    for e in engines:
        response = client.post(
            f"/api/v1/test/fake_delta?limit=50&format=ndjson&%24engine={e}",
            json={},
        )
        assert response.status_code == 200
        assert response.status_code == 200
        lines = [json.loads(l) for l in response.text.split("\n") if len(l) > 0]
        assert len(lines) == 50
        assert "nearby" not in lines[0]
