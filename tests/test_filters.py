import sys
from fastapi.testclient import TestClient

sys.path.append(".")

engines = ["duckdb", "polars"]


def test_not_in(client: TestClient):
    for e in engines:
        response = client.post(
            f"/api/v1/complexer/complex_fruits?limit=5&format=json&%24engine={e}",
            json={"cars_not_in": ["audi", "fiat"]},
        )
        assert response.status_code == 200
        jsd = response.json()
        assert len(jsd) > 0

        for item in jsd:
            assert item["cars"] != "audi"
            assert item["cars"] != "fiat"


def test_in(client: TestClient):
    for e in engines:
        response = client.post(
            f"/api/v1/complexer/complex_fruits?limit=5&format=json&%24engine={e}",
            json={"fruits_in": ["banana", "kiwi"]},
        )
        assert response.status_code == 200
        jsd = response.json()
        assert len(jsd) > 0

        for item in jsd:
            assert item["fruits"] == "banana"


def test_contains(client: TestClient):
    for e in engines:
        response = client.post(
            f"/api/v1/complexer/complex_fruits?limit=5&format=json&%24engine={e}",
            json={"fruits_contains": "anan"},
        )
        assert response.status_code == 200
        jsd = response.json()
        assert len(jsd) > 0

        for item in jsd:
            assert item["fruits"] in ["banana", "ananas"]


def test_startswith(client: TestClient):
    for e in engines:
        response = client.post(
            f"/api/v1/complexer/complex_fruits?limit=5&format=json&%24engine={e}",
            json={"fruits_startswith": "anan"},
        )
        assert response.status_code == 200
        jsd = response.json()
        assert len(jsd) > 0

        for item in jsd:
            assert item["fruits"] in ["ananas"]
            assert item["fruits"] not in ["banana"]


def test_not_contains(client: TestClient):
    for e in engines:
        response = client.post(
            f"/api/v1/complexer/complex_fruits?limit=5&format=json&%24engine={e}",
            json={"fruits_not_contains": "anan"},
        )
        assert response.status_code == 200
        jsd = response.json()
        assert len(jsd) > 0

        for item in jsd:
            assert item["fruits"] not in ["banana", "ananas"]


def test_not_equals(client: TestClient):
    for e in engines:
        response = client.post(
            f"/api/v1/complexer/complex_fruits?limit=5&format=json&%24engine={e}",
            json={"fruits_ne": "banana"},
        )
        assert response.status_code == 200
        jsd = response.json()
        assert len(jsd) > 0

        for item in jsd:
            assert item["fruits"] != "banana"


def test_gt(client: TestClient):
    for e in engines:
        response = client.post(
            f"/api/v1/complexer/complex_fruits?limit=5&format=json&%24engine={e}",
            json={"B_gt": "2"},
        )
        assert response.status_code == 200
        jsd = response.json()
        assert len(jsd) > 0

        for item in jsd:
            assert item["B"] > 2


def test_gt_date_get(client: TestClient):
    from datetime import datetime

    for e in ["duckdb"]:  # TODO: Enable polalrs once they support datetime decently
        response = client.get(
            f"/api/v1/test/fruits_date?limit=5&format=json&%24engine={e}&date_field_gt=2023-01-01T00:00",
        )
        assert response.status_code == 200
        jsd = response.json()
        assert len(jsd) > 0

        for item in jsd:
            assert datetime.fromisoformat(item["date_field"]) > datetime(2023, 1, 1)


def test_gte(client: TestClient):
    for e in engines:
        response = client.post(
            f"/api/v1/complexer/complex_fruits?limit=5&format=json&%24engine={e}",
            json={"B_gte": "2"},
        )
        assert response.status_code == 200
        jsd = response.json()
        assert len(jsd) > 0

        for item in jsd:
            assert item["B"] >= 2


def test_lt(client: TestClient):
    for e in engines:
        response = client.post(
            f"/api/v1/complexer/complex_fruits?limit=5&format=json&%24engine={e}",
            json={"B_lt": 7},
        )
        assert response.status_code == 200
        jsd = response.json()
        assert len(jsd) > 0

        for item in jsd:
            assert item["B"] < 7


def test_lte(client: TestClient):
    for e in engines:
        response = client.post(
            f"/api/v1/complexer/complex_fruits?limit=5&format=json&%24engine={e}",
            json={"B_lte": 7},
        )
        assert response.status_code == 200
        jsd = response.json()
        assert len(jsd) > 0

        for item in jsd:
            assert item["B"] <= 7


def test_between(client: TestClient):
    for e in engines:
        response = client.post(
            f"/api/v1/complexer/complex_fruits?limit=5&format=json&%24engine={e}",
            json={"B_between": [5, 7]},
        )
        assert response.status_code == 200
        jsd = response.json()
        assert len(jsd) > 0

        for item in jsd:
            assert item["B"] >= 5 and item["B"] <= 7

        response = client.post(
            f"/api/v1/complexer/complex_fruits?limit=5&format=json&%24engine={e}",
            json={"B_between": [5, 7, 9]},
        )
        assert response.status_code == 400


def test_not_between(client: TestClient):
    for e in engines:
        response = client.post(
            f"/api/v1/complexer/complex_fruits?limit=5&format=json&%24engine={e}",
            json={"B_not_between": [5, 7]},
        )
        assert response.status_code == 200
        jsd = response.json()
        assert len(jsd) > 0

        for item in jsd:
            assert not (item["B"] >= 5 and item["B"] <= 7)

        response = client.post(
            f"/api/v1/complexer/complex_fruits?limit=5&format=json&%24engine={e}",
            json={"B_not_between": [5, 7, 9]},
        )
        assert response.status_code == 400


def test_has(client: TestClient):
    for e in engines:
        response = client.get(
            f"/api/v1/array/weather?limit=100&temperatures_has=E1&format=json&%24engine={e}",
        )
        assert response.status_code == 200
        jsd = response.json()
        assert len(jsd) == 3

        for item in jsd:
            assert "E1" in item["temperatures"]

        response = client.post(
            f"/api/v1/array/weather?limit=100&format=json&%24engine={e}",
            json={"temperatures_has": "E1"},
        )
        assert response.status_code == 200
        jsd = response.json()
        assert len(jsd) == 3

        for item in jsd:
            assert "E1" in item["temperatures"]
