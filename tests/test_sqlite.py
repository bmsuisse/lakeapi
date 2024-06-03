from fastapi.testclient import TestClient
from .utils import get_app, get_auth

client = TestClient(get_app())
auth = get_auth()


def test_simple_customers():
    response = client.get(
        "/api/v1/sqlite/sqlite_customers?format=json&limit=50",
        auth=auth,
    )
    assert response.status_code == 200
    tables = response.json()
    assert len(tables) == 50


def test_filter_country():
    response = client.get(
        "/api/v1/sqlite/sqlite_customers?format=json&limit=100&Country=Germany",
        auth=auth,
    )
    assert response.status_code == 200
    tables = response.json()
    assert len(tables) == 4


def test_metadata_detail():
    response = client.get(
        "/api/v1/sqlite/sqlite_customers/metadata_detail",
        auth=auth,
    )
    assert response.status_code == 200
