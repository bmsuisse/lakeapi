import docker
from docker.models.containers import Container
from time import sleep


def start_mssql_server() -> Container:
    client = docker.from_env()  # code taken from https://github.com/fsspec/adlfs/blob/main/adlfs/tests/conftest.py#L72

    envs = dict()
    with open("test_server/sql_docker.env", "r") as f:
        lines = f.readlines()
        envs = {
            item[0].strip(): item[1].strip()
            for item in [line.split("=") for line in lines if len(line.strip()) > 0 and not line.startswith("#")]
        }
    print(envs)
    # using podman:  podman run  --env-file=TESTS/SQL_DOCKER.ENV --publish=1439:1433 --name=mssql1 chriseaton/adventureworks:light
    #                podman kill mssql1
    sql_server: Container = client.containers.run(
        "chriseaton/adventureworks:light",
        environment=envs,
        detach=True,
        ports={"1433/tcp": "1439"},
    )  # type: ignore
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
