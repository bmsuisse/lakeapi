import sys

sys.path.append(".")

from fastapi.testclient import TestClient
from .utils import get_app, get_auth

auth = get_auth()

engines = ("duckdb", "polars")


def test_openid():
    for engine in engines:
        client = TestClient(get_app(default_engine=engine))
        response = client.get("/openapi.json", auth=auth)
        assert response.status_code == 200
        jsd = response.json()
        assert isinstance(jsd, dict)
        paths = jsd["paths"]
        schema = jsd["components"]["schemas"]
        ## TODO : Add more tests for different endpoints
        assert "test_fake_delta_partition" in schema.keys()
        assert "test_fake_polars_postParameter" in schema.keys()
        assert "abc" in schema["test_fake_polars_postParameter"]["properties"]
        assert "/api/v1/startest/fruits" in paths
        assert isinstance(paths["/api/v1/startest/fruits"], dict)
        assert len(paths["/api/v1/startest/fruits"].keys()) == 1
        assert "get" in paths["/api/v1/startest/fruits"].keys()

        assert isinstance(paths["/api/v1/startest/fruits_partition"], dict)
        assert len(paths["/api/v1/startest/fruits_partition"].keys()) == 1
        assert "post" in paths["/api/v1/startest/fruits_partition"].keys()
