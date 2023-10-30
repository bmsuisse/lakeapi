from fastapi.testclient import TestClient
from .utils import get_app, get_auth

client = TestClient(get_app())
auth = get_auth()


def test_parquet():
    response = client.get(
        f"/api/v1/blobb/blob_test?format=json&limit=50",
        auth=auth,
    )
    assert response.status_code == 200
    fakedt = response.json()
    assert len(fakedt) == 50


def test_delta():
    response = client.get(
        f"/api/v1/blobb/blob_test_delta?format=json&limit=50",
        auth=auth,
    )
    assert response.status_code == 200
    fakedt = response.json()
    assert len(fakedt) == 50
