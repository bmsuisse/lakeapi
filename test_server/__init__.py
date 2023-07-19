import docker
from docker.models.containers import Container
from time import sleep
from typing import cast
import docker.errors


def _getenvs():
    envs = dict()
    with open("test_server/sql_docker.env", "r") as f:
        lines = f.readlines()
        envs = {
            item[0].strip(): item[1].strip()
            for item in [line.split("=") for line in lines if len(line.strip()) > 0 and not line.startswith("#")]
        }
    return envs


def start_mssql_server() -> Container:
    client = docker.from_env()  # code taken from https://github.com/fsspec/adlfs/blob/main/adlfs/tests/conftest.py#L72
    sql_server: Container | None = None
    try:
        m = cast(Container, client.containers.get("test4sql"))
        if m.status == "running":
            return m
        else:
            sql_server = m
    except docker.errors.NotFound as err:
        pass

    envs = _getenvs()

    if sql_server is None:
        # using podman:  podman run  --env-file=TESTS/SQL_DOCKER.ENV --publish=1439:1433 --name=mssql1 chriseaton/adventureworks:light
        #                podman kill mssql1
        sql_server = client.containers.run(
            "chriseaton/adventureworks:light",
            environment=envs,
            detach=True,
            name="test4sql",
            ports={"1433/tcp": "1439"},
        )  # type: ignore
    assert sql_server is not None
    sql_server.start()
    print(sql_server.status)
    sleep(45)  # the install script takes a sleep of 30s to create the db and then restores adventureworks
    import pyodbc

    with pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};SERVER=127.0.0.1,1439;ENCRYPT=yes;TrustServerCertificate=Yes;UID=sa;PWD="
        + envs["MSSQL_SA_PASSWORD"]
        + ";DATABASE=master"
    ) as con:
        with con.cursor() as cur:
            cur.execute("SELECT * FROM sys.databases where name='AdventureWorks'")
            if len(cur.fetchall()) > 0:
                print("db created")
            else:
                print("db not created yet")
                sleep(25)
    print("Successfully created sql container...")
    return sql_server
