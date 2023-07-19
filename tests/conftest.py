from time import sleep
import docker
import pytest
import os
from docker.models.containers import Container


@pytest.fixture(scope="session", autouse=True)
def spawn_sql():
    import test_server

    sql_server = test_server.start_mssql_server()
    yield sql_server
    sql_server.stop()
