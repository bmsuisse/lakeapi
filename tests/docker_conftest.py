import docker
import pytest
import os
from docker.models.containers import Container

# @pytest.fixture(scope="session", autouse=True)
# def spawn_sql():
print("Starting sql docker container")
os.environ["SA_PASSWORD"] = "MyPass@word4tests"
os.environ["ACCEPT_EULA"] = "Y"
os.environ["MSSQL_PID"] = "Express"
client = docker.from_env()

sql_server: Container = client.containers.run(
    "chriseaton/adventureworks:light",
    "",
    detach=True,
    ports={"1439": "1433"},
)  # type: ignore
sql_server.start()
print(sql_server.status)
print("Successfully created sql container...")
print("press something to stop")
p = input()
sql_server.stop()
