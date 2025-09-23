import sys
import csv

from fastapi.testclient import TestClient

sys.path.append(".")

engines = ["duckdb"]


def test_duckdb_file_type(client: TestClient):
    for _ in range(2):
        response = client.get("/api/v1/test/fake_duck?limit=1&format=json")
        assert response.status_code == 200


def test_duckdb_file_type_limit_100(client: TestClient):
    for _ in range(2):
        response = client.get("/api/v1/test/fake_duck?limit=100&format=json")
        assert response.status_code == 200
        assert len(response.json()) == 100


def test_duckdb_fruits_fruit_param(client: TestClient):
    for _ in range(2):
        response = client.get(
            "/api/v1/test/fruits_duck?limit=2&format=json&fruits=banana"
        )
        assert response.status_code == 200
        assert response.json() == [
            {"A": 1, "fruits": "banana", "B": 5, "cars": "beetle"},
            {"A": 2, "fruits": "banana", "B": 4, "cars": "audi"},
        ]


def test_duckdb_fruits_car_param(client: TestClient):
    for _ in range(2):
        response = client.get(
            "/api/v1/test/fruits_duck?limit=2&format=json&cars=lamborghini"
        )
        assert response.status_code == 200
        assert response.json() == [
            {"A": 0, "fruits": "apple", "B": 5, "cars": "lamborghini"}
        ]


def test_data_csv(client: TestClient):
    for _ in range(2):
        response = client.get("/api/v1/test/fruits_duck?limit=1&format=csv&cars=audi")
        assert response.status_code == 200
        txt = response.text

        reader = csv.DictReader(txt.splitlines())
        line1 = reader.__next__()
        assert line1 == {"A": "2", "fruits": "banana", "B": "4", "cars": "audi"}
