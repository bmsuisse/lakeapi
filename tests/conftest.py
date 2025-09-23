from fastapi.testclient import TestClient
import pytest
import os
from dotenv import load_dotenv


load_dotenv()


@pytest.fixture(scope="session")
def app():
    from tests.utils import get_app

    app = get_app()
    yield app


@pytest.fixture(scope="session")
def client_no_auth(app):
    client = TestClient(app)
    yield client
    client.close()


@pytest.fixture(scope="session")
def client(app):
    from tests.utils import get_auth

    client = TestClient(app)
    client.auth = get_auth()
    yield client
    client.close()


@pytest.fixture(scope="session", autouse=True)
def spawn_sql():
    import test_server

    if os.getenv("NO_SQL_SERVER", "0") == "1":
        yield None
    else:
        sql_server = test_server.start_mssql_server()
        yield sql_server
        if os.getenv("KEEP_SQL_SERVER", "0") == "0":  # can be handy during development
            sql_server.stop()


@pytest.fixture(scope="session", autouse=True)
def spawn_azurite():
    import test_server

    if os.getenv("NO_AZURITE_DOCKER", "0") == "1":
        yield None
    else:
        azurite = test_server.start_azurite()
        yield azurite
        if (
            os.getenv("KEEP_AZURITE_DOCKER", "0") == "0"
        ):  # can be handy during development
            azurite.stop()
