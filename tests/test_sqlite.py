from fastapi.testclient import TestClient


def test_simple_customers(client: TestClient):
    response = client.get("/api/v1/sqlite/sqlite_customers?format=json&limit=50")
    assert response.status_code == 200
    tables = response.json()
    assert len(tables) == 50


def test_filter_country(client: TestClient):
    response = client.get(
        "/api/v1/sqlite/sqlite_customers?format=json&limit=100&Country=Germany"
    )
    assert response.status_code == 200
    tables = response.json()
    assert len(tables) == 4


def test_metadata_detail(client: TestClient):
    response = client.get("/api/v1/sqlite/sqlite_customers/metadata_detail")
    assert response.status_code == 200
