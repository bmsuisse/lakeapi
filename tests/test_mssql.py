from fastapi.testclient import TestClient
import pytest
import os

no_sql = os.getenv("NO_SQL_SERVER", "0") == "1"


@pytest.mark.skipif(no_sql, reason="No SQL Server available")
def test_simple_department(client: TestClient):
    response = client.get(
        "/api/v1/mssql/mssql_department?format=json&limit=50",
    )
    assert response.status_code == 200
    departments = response.json()
    assert len(departments) == 16


@pytest.mark.skipif(no_sql, reason="No SQL Server available")
def test_filter_group_name(client: TestClient):
    response = client.get(
        "/api/v1/mssql/mssql_department?format=json&limit=100&GroupName=Research%20and%20Development",
    )
    assert response.status_code == 200
    tables = response.json()
    assert len(tables) == 3


@pytest.mark.skipif(no_sql, reason="No SQL Server available")
def test_filter_offset(client: TestClient):
    response = client.get(
        "/api/v1/mssql/mssql_department?format=json&limit=100&offset=10",
    )
    assert response.status_code == 200
    tables = response.json()
    assert len(tables) == 6


@pytest.mark.skipif(no_sql, reason="No SQL Server available")
def test_metadata_detail(client: TestClient):
    response = client.get(
        "/api/v1/mssql/mssql_department/metadata_detail",
    )
    assert response.status_code == 200
