from fastapi.testclient import TestClient
from .utils import get_app, get_auth
import sys
import pyarrow as pa
import polars as pl
import pytest
import pandas as pd
from urllib.parse import quote
from httpx._types import RequestData
import time

sys.path.append(".")
client = TestClient(get_app())
auth = get_auth()

engines = ("duckdb", "polars")
# engines = ("duckdb")


def test_caching_speed():
    endpoint = f"/api/v1/test/fake_delta_cache?limit=100&format=json"
    start1 = time.time()
    response = client.get(endpoint, auth=auth)
    end1 = time.time()
    assert response.status_code == 200
    duration1 = end1 - start1

    start2 = time.time()
    response = client.get(endpoint, auth=auth)
    end2 = time.time()
    assert response.status_code == 200

    print(f"Duration 1: {duration1}, Duration 2: {end2 - start2}")

    duration2 = end2 - start2

    assert duration2 < duration1
