from fastapi.testclient import TestClient
from .utils import get_app, get_auth
import sys
import pyarrow as pa
import polars as pl
import pytest
import pandas as pd
from urllib.parse import quote

sys.path.append(".")
client = TestClient(get_app())
auth = get_auth()
engines = ["duckdb"]


def test_nearby():
    for e in engines:
        response = client.post(
            f"/api/v1/test/fake_delta?limit=50&format=ndjson&%24engine={e}",
            auth=auth,
            json={"nearby": {"lat": 46.7, "lon": 8.6, "distance_m": 4200}},
        )
        assert response.status_code == 200
        lines = response.text().split("\n")
        assert len(lines) >= 25 and len(lines) <= 40  # it's a bit fuzzy since distance calc is never 100% accurate

        assert jsd[0]["nearby"] <= jsd[1]["nearby"]
        assert jsd[1]["nearby"] <= jsd[2]["nearby"]
        assert jsd[2]["nearby"] <= jsd[3]["nearby"]
        for item in jsd:
            assert item["nearby"] <= 4200


def test_no_nearby():
    for e in engines:
        response = client.post(
            f"/api/v1/test/fake_delta?limit=50&format=ndjson&%24engine={e}",
            auth=auth,
            json={},
        )
        assert response.status_code == 200
        assert response.status_code == 200
        lines = response.text().split("\n")
        assert len(lines) == 50
        assert "nearby" not in lines[0]
