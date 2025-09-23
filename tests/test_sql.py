from fastapi.testclient import TestClient

engines = ["duckdb", "polars"]


def test_tables(client: TestClient):
    for e in engines:
        response = client.get(f"/api/sql/tables?%24engine={e}")
        assert response.status_code == 200
        tables = response.json()
        assert len(tables) > 5


def test_get(client: TestClient):
    for e in engines:
        response = client.get(
            f"/api/sql?%24engine={e}&sql=SELECT distinct B FROM complexer_complex_fruits union select distinct A as B FROM startest_fruits"
        )
        assert response.status_code == 200
        tables = response.json()
        assert len(tables) > 5


def test_post(client: TestClient):
    for e in engines:
        response = client.post(
            f"/api/sql?%24engine={e}&",
            data="SELECT distinct B FROM complexer_complex_fruits union select distinct  A as B FROM startest_fruits",  # type: ignore
        )
        assert response.status_code == 200
        tables = response.json()
        assert len(tables) > 5


def test_sql_where_post(client: TestClient):
    # better naming needed in the future

    query = """select * 
        from test_fruits 
        where cars = 'audi' 
        and fruits = 'banana' 
        and A = 2 and B = 4"""

    response = client.post(
        "/api/sql",
        data=query,  # type: ignore
    )
    assert response.status_code == 200
    print(response.json())
    assert response.json() == [
        {
            "A": 2,
            "fruits": "banana",
            "B": 4,
            "cars": "audi",
        }
    ]


def test_sql_where_get(client: TestClient):
    # better naming needed in the future

    query = """select * 
        from test_fruits 
        where cars = 'audi' 
        and fruits = 'banana' 
        and A = 2 and B = 4"""

    response = client.get(
        "/api/sql",
        params={"sql": query},
    )
    assert response.status_code == 200
    print(response.json())
    assert response.json() == [
        {
            "A": 2,
            "fruits": "banana",
            "B": 4,
            "cars": "audi",
        }
    ]
