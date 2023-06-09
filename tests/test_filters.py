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


def test_not_in():
    for e in engines:
        response = client.post(
            f"/api/v1/complexer/complex_fruits?limit=5&format=json&%24engine={e}",
            auth=auth,
            json={"cars_not_in": ["audi", "fiat"]},
        )
        assert response.status_code == 200
        jsd = response.json()
        assert len(jsd) > 0

        for item in jsd:
            assert item["cars"] != "audi"
            assert item["cars"] != "fiat"


def test_in():
    for e in engines:
        response = client.post(
            f"/api/v1/complexer/complex_fruits?limit=5&format=json&%24engine={e}",
            auth=auth,
            json={"fruits_in": ["banana", "kiwi"]},
        )
        assert response.status_code == 200
        jsd = response.json()
        assert len(jsd) > 0

        for item in jsd:
            assert item["fruits"] == "banana"


def test_contains():
    for e in engines:
        response = client.post(
            f"/api/v1/complexer/complex_fruits?limit=5&format=json&%24engine={e}",
            auth=auth,
            json={"fruits_contains": "anan"},
        )
        assert response.status_code == 200
        jsd = response.json()
        assert len(jsd) > 0

        for item in jsd:
            assert item["fruits"] in ["banana", "ananas"]


def test_not_contains():
    for e in engines:
        response = client.post(
            f"/api/v1/complexer/complex_fruits?limit=5&format=json&%24engine={e}",
            auth=auth,
            json={"fruits_not_contains": "anan"},
        )
        assert response.status_code == 200
        jsd = response.json()
        assert len(jsd) > 0

        for item in jsd:
            assert item["fruits"] not in ["banana", "ananas"]


def test_not_equals():
    for e in engines:
        response = client.post(
            f"/api/v1/complexer/complex_fruits?limit=5&format=json&%24engine={e}",
            auth=auth,
            json={"fruits_ne": "banana"},
        )
        assert response.status_code == 200
        jsd = response.json()
        assert len(jsd) > 0

        for item in jsd:
            assert item["fruits"] != "banana"


def test_gt():
    for e in engines:
        response = client.post(
            f"/api/v1/complexer/complex_fruits?limit=5&format=json&%24engine={e}",
            auth=auth,
            json={"B_gt": "2"},
        )
        assert response.status_code == 200
        jsd = response.json()
        assert len(jsd) > 0

        for item in jsd:
            assert item["B"] > 2


def test_gte():
    for e in engines:
        response = client.post(
            f"/api/v1/complexer/complex_fruits?limit=5&format=json&%24engine={e}",
            auth=auth,
            json={"B_gte": "2"},
        )
        assert response.status_code == 200
        jsd = response.json()
        assert len(jsd) > 0

        for item in jsd:
            assert item["B"] >= 2


def test_lt():
    for e in engines:
        response = client.post(
            f"/api/v1/complexer/complex_fruits?limit=5&format=json&%24engine={e}",
            auth=auth,
            json={"B_lt": 7},
        )
        assert response.status_code == 200
        jsd = response.json()
        assert len(jsd) > 0

        for item in jsd:
            assert item["B"] < 7


def test_lte():
    for e in engines:
        response = client.post(
            f"/api/v1/complexer/complex_fruits?limit=5&format=json&%24engine={e}",
            auth=auth,
            json={"B_lte": 7},
        )
        assert response.status_code == 200
        jsd = response.json()
        assert len(jsd) > 0

        for item in jsd:
            assert item["B"] <= 7


def test_between():
    for e in engines:
        response = client.post(
            f"/api/v1/complexer/complex_fruits?limit=5&format=json&%24engine={e}",
            auth=auth,
            json={"B_between": [5, 7]},
        )
        assert response.status_code == 200
        jsd = response.json()
        assert len(jsd) > 0

        for item in jsd:
            assert item["B"] >= 5 and item["B"] <= 7

        response = client.post(
            f"/api/v1/complexer/complex_fruits?limit=5&format=json&%24engine={e}",
            auth=auth,
            json={"B_between": [5, 7, 9]},
        )
        assert response.status_code == 400


def test_not_between():
    for e in engines:
        response = client.post(
            f"/api/v1/complexer/complex_fruits?limit=5&format=json&%24engine={e}",
            auth=auth,
            json={"B_not_between": [5, 7]},
        )
        assert response.status_code == 200
        jsd = response.json()
        assert len(jsd) > 0

        for item in jsd:
            assert not (item["B"] >= 5 and item["B"] <= 7)

        response = client.post(
            f"/api/v1/complexer/complex_fruits?limit=5&format=json&%24engine={e}",
            auth=auth,
            json={"B_not_between": [5, 7, 9]},
        )
        assert response.status_code == 400
