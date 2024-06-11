from fastapi.testclient import TestClient
from .utils import get_app, get_auth
import sys
import csv

sys.path.append(".")
client = TestClient(get_app())
auth = get_auth()
engines = ["duckdb"]


def test_duckdb_file_type():
    for _ in range(2):
        response = client.get("/api/v1/test/fake_duck?limit=1&format=json", auth=auth)
        assert response.status_code == 200


def test_duckdb_file_type_limit_100():
    for _ in range(2):
        response = client.get("/api/v1/test/fake_duck?limit=100&format=json", auth=auth)
        assert response.status_code == 200
        assert len(response.json()) == 100


def test_duckdb_fruits_fruit_param():
    for _ in range(2):
        response = client.get(
            "/api/v1/test/fruits_duck?limit=2&format=json&fruits=banana", auth=auth
        )
        assert response.status_code == 200
        assert response.json() == [
            {"A": 1, "fruits": "banana", "B": 5, "cars": "beetle"},
            {"A": 2, "fruits": "banana", "B": 4, "cars": "audi"},
        ]


def test_duckdb_fruits_car_param():
    for _ in range(2):
        response = client.get(
            "/api/v1/test/fruits_duck?limit=2&format=json&cars=lamborghini", auth=auth
        )
        assert response.status_code == 200
        assert response.json() == [
            {"A": 0, "fruits": "apple", "B": 5, "cars": "lamborghini"}
        ]


def test_data_csv():
    for _ in range(2):
        response = client.get(
            "/api/v1/test/fruits_duck?limit=1&format=csv&cars=audi", auth=auth
        )
        assert response.status_code == 200
        txt = response.text

        reader = csv.DictReader(txt.splitlines())
        line1 = reader.__next__()
        assert line1 == {"A": "2", "fruits": "banana", "B": "4", "cars": "audi"}
