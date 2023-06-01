from fastapi.testclient import TestClient
from .utils import get_app, get_auth

client = TestClient(get_app())
auth = get_auth()


def test_tables():
    response = client.get(
        f"/api/sql/tables",
        auth=auth,
    )
    assert response.status_code == 200
    tables = response.json()
    assert len(tables) > 5


def test_get():
    response = client.get(
        f"/api/sql?sql=SELECT distinct B FROM complexer_complex_fruits union select distinct A FROM startest_fruits",
        auth=auth,
    )
    assert response.status_code == 200
    tables = response.json()
    assert len(tables) > 5


def test_post():
    response = client.post(
        f"/api/sql",
        auth=auth,
        data="SELECT distinct B FROM complexer_complex_fruits union select distinct A FROM startest_fruits",
    )
    assert response.status_code == 200
    tables = response.json()
    assert len(tables) > 5
