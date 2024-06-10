from fastapi.testclient import TestClient
from .utils import get_app, get_auth

client = TestClient(get_app())
auth = get_auth()


def test_simple_department():
    response = client.get(
        "/api/v1/mssql/mssql_department?format=json&limit=50",
        auth=auth,
    )
    assert response.status_code == 200
    departments = response.json()
    assert len(departments) == 16


def test_filter_group_name():
    response = client.get(
        "/api/v1/mssql/mssql_department?format=json&limit=100&GroupName=Research%20and%20Development",
        auth=auth,
    )
    assert response.status_code == 200
    tables = response.json()
    assert len(tables) == 3


def test_filter_offset():
    response = client.get(
        "/api/v1/mssql/mssql_department?format=json&limit=100&offset=10",
        auth=auth,
    )
    assert response.status_code == 200
    tables = response.json()
    assert len(tables) == 6


def test_metadata_detail():
    response = client.get(
        "/api/v1/mssql/mssql_department/metadata_detail",
        auth=auth,
    )
    assert response.status_code == 200
