from fastapi.testclient import TestClient
from .utils import get_app, get_auth
import sys
import polars as pl
import pandas as pd
import pytest

sys.path.append(".")
client = TestClient(get_app())
auth = get_auth()

engines = ("duckdb", "polars")


@pytest.mark.parametrize("engine", engines)
def test_data_partition(engine):
    for _ in range(2):
        response = client.get(
            f"/api/v1/test/fruits_partition?limit=1&format=json&cars=audi&%24engine={engine}",
            auth=auth,
        )
        assert response.status_code == 200
        assert response.json() == [
            {
                "A": 2,
                "fruits": "banana",
                "B": 4,
                "cars": "audi",
                "my_empty_col": None,
            }
        ]
        response = client.get(
            f"/api/v1/test/fruits_partition?limit=1&format=json&fruits=ananas&%24engine={engine}",
            auth=auth,
        )
        assert response.status_code == 200
        assert response.json() == [
            {
                "A": 9,
                "fruits": "ananas",
                "B": 9,
                "cars": "fiat",
                "my_empty_col": None,
            }
        ]


@pytest.mark.parametrize("engine", engines)
def test_data_partition_mod(engine):
    for _ in range(2):
        response = client.get(
            f"/api/v1/test/fruits_partition_mod?limit=1&format=json&cars=audi&%24engine={engine}",
            auth=auth,  # works because of implicit parameters
        )
        assert response.status_code == 200
        assert response.json() == [
            {
                "A": 2,
                "fruits": "banana",
                "B": 4,
                "cars": "audi",
            }
        ]
        response = client.post(
            f"/api/v1/test/fruits_partition_mod?limit=1&format=json&%24engine={engine}",
            auth=auth,
            json={"cars_in": ["audi"]},
        )
        assert response.status_code == 200
        assert response.json() == [
            {
                "A": 2,
                "fruits": "banana",
                "B": 4,
                "cars": "audi",
            }
        ]


@pytest.mark.parametrize("engine", engines)
def test_data_partition_int(engine):
    for _ in range(2):
        response = client.get(
            f"/api/v1/test/fruits_partition_int?limit=1&format=json&A=2&%24engine={engine}",
            auth=auth,  # works because of implicit parameters
        )
        assert response.status_code == 200
        assert response.json() == [
            {
                "A": 2,
                "fruits": "banana",
                "B": 4,
                "cars": "audi",
            }
        ]
