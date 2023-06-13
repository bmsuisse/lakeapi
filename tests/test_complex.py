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


def test_returns_complex():
    for e in engines:
        response = client.post(
            f"/api/v1/complexer/complex_fruits?limit=10&format=json&%24engine={e}",
            auth=auth,
        )
        assert response.status_code == 200
        jsd = response.json()
        assert len(jsd) == 6
        assert isinstance(jsd[0]["vitamines"], list)
        assert isinstance(jsd[0]["person"], dict)
