import sys

sys.path.append(".")

from fastapi.testclient import TestClient
from .utils import get_app, get_auth

client = TestClient(get_app())
auth = get_auth()


def test_openid():
    response = client.get("/openapi.json", auth=auth)
    assert response.status_code == 200
    jsd = response.json()
    assert isinstance(jsd, dict)
    schema = jsd["components"]["schemas"]
    ## TODO : Add more tests for different endpoints
    assert "test_fake_delta_partition" in schema
