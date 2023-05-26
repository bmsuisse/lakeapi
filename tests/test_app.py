from fastapi.testclient import TestClient
from .utils import get_app, get_auth
import time
from datetime import datetime
import sys
import pyarrow as pa
import polars as pl
import pytest
import pandas as pd
from urllib.parse import quote
from httpx._types import RequestData
from typing import cast

sys.path.append(".")
client = TestClient(get_app())
auth = get_auth()

# engines = ("duckdb", "datafusion", "polars")
engines = ("duckdb", "datafusion", "polars")
# engines = ("duckdb")


def test_no_authentication():
    response = client.get("/")
    assert response.status_code == 401
    assert response.json() == {"detail": "Not authenticated"}


def test_authentication():
    response = client.get("/", auth=auth)
    assert response.status_code == 200


def test_fruits_limit_1():
    for e in engines:
        response = client.get(f"/api/v1/test/fruits?limit=1&format=json&%24engine={e}", auth=auth)
        assert response.status_code == 200
        assert response.json() == [
            {
                "A": 1,
                "fruits": "banana",
                "B": 5,
                "cars": "beetle",
            }
        ]


def test_fruits_offset_1():
    for e in engines:  # polars does not support offset
        response = client.get(
            f"/api/v1/test/fruits?limit=1&&offset=1&format=json&%24engine={e}",
            auth=auth,
        )
        assert response.status_code == 200
        assert response.json() == [
            {
                "A": 2,
                "fruits": "banana",
                "B": 4,
                "cars": "audi",
            }
        ]


def test_data_limit():
    for e in engines:
        response = client.get(f"/api/v1/test/fake_delta?limit=1000&&format=json&%24engine={e}", auth=auth)
        assert response.status_code == 200
        assert len(response.json()) == 1000


def test_data_filter():
    for e in engines:
        response = client.get(
            f"/api/v1/test/fruits?limit=1&format=json&cars=audi&%24engine={e}",
            auth=auth,
        )
        assert response.status_code == 200
        assert response.json() == [
            {
                "A": 2,
                "fruits": "banana",
                "B": 4,
                "cars": "audi",
            }
        ]
        response = client.get(
            f"/api/v1/test/fruits?limit=1&format=json&fruits=ananas&%24engine={e}",
            auth=auth,
        )
        assert response.status_code == 200
        assert response.json() == [
            {
                "A": 9,
                "fruits": "ananas",
                "B": 9,
                "cars": "fiat",
            }
        ]


def test_data_csv():
    for e in engines:
        response = client.get(f"/api/v1/test/fruits?limit=1&format=csv&cars=audi&%24engine={e}", auth=auth)
        assert response.status_code == 200
        txt = response.text
        import csv

        reader = csv.DictReader(txt.splitlines())
        line1 = reader.__next__()
        assert line1 == {"A": "2", "fruits": "banana", "B": "4", "cars": "audi"}


def test_data_scsv():
    for e in engines:
        response = client.get(
            f"/api/v1/test/fruits?limit=1&format=scsv&cars=audi&%24engine={e}",
            auth=auth,
        )
        assert response.status_code == 200
        txt = response.text
        import csv

        reader = csv.DictReader(txt.splitlines(), delimiter=";")
        line1 = reader.__next__()
        assert line1 == {"A": "2", "fruits": "banana", "B": "4", "cars": "audi"}


def test_data_minus_1():
    for e in engines:
        response = client.get(f"/api/v1/test/fake_delta?limit=-1&format=json&%24engine={e}", auth=auth)
        assert response.status_code == 200
        assert len(response.json()) == 100_011


def test_data_minus_1_csv():
    from io import StringIO

    for f in ("csv", "scsv"):
        separator = ";" if f == "scsv" else ","
        for e in engines:
            response = client.get(f"/api/v1/test/fake_delta?limit=-1&format={f}&%24engine={e}", auth=auth)
            assert response.status_code == 200
            csv_file = StringIO(response.text)
            assert len(pd.read_csv(csv_file, sep=separator)) == 100_011


def test_data_default_limit():
    for e in engines:
        response = client.get(f"/api/v1/test/fake_delta?format=json&%24engine={e}", auth=auth)
        assert response.status_code == 200
        assert len(response.json()) == 100


def test_get_parquet_data():
    for e in engines:
        response = client.get(
            f"/api/v1/test/fruits?limit=1000&format=parquet&%24engine={e}",
            auth=auth,
        )
        assert response.status_code == 200
        from io import BytesIO

        buf = BytesIO(response.content)
        df = pd.read_parquet(buf)
        df2 = pl.read_delta("tests/data/delta/fruits").to_pandas()
        assert df.equals(df2)


def test_get_arrow_data():
    for e in engines:
        response = client.get(
            f"/api/v1/test/fruits?limit=1000&format=arrow&%24engine={e}",
            auth=auth,
        )
        assert response.status_code == 200
        from io import BytesIO

        buf = BytesIO(response.content)
        df = pl.read_ipc(buf).to_pandas()
        df2 = pl.read_delta("tests/data/delta/fruits").to_pandas()
        assert df.equals(df2)


def test_get_ndjson_data():
    for e in engines:
        response = client.get(
            f"/api/v1/test/fruits?limit=1000&format=ndjson&%24engine={e}",
            auth=auth,
        )
        assert response.status_code == 200
        from io import BytesIO

        buf = BytesIO(response.content)
        df = pl.read_ndjson(buf).to_pandas()
        df2 = pl.read_delta("tests/data/delta/fruits").to_pandas()
        assert df.equals(df2)


def test_get_excel_data():
    for e in engines:
        response = client.get(
            f"/api/v1/test/fruits?limit=1000&format=xlsx&%24engine={e}",
            auth=auth,
        )
        assert response.status_code == 200
        from io import BytesIO

        buf = BytesIO(response.content)
        df = pl.read_excel(buf).to_pandas()
        df2 = pl.read_delta("tests/data/delta/fruits").to_pandas()
        assert df.equals(df2)


def test_fruits_in():
    for e in engines:
        response = client.post(
            f"/api/v1/test/fruits?limit=1000",
            json={"fruits_in": ["banana", "ananas"]},
            auth=auth,
        )
        assert response.status_code == 200
        assert len(response.json()) == 4

        response = client.post(
            f"/api/v1/test/fruits?limit=1000",
            json={"fruits_in": ["apple", "ananas"]},
            auth=auth,
        )
        assert response.status_code == 200
        assert len(response.json()) == 3


def test_fruits_combi():
    for e in engines:
        response = client.post(
            f"/api/v1/test/fruits?limit=1000",
            json={"pk": [{"cars": "audi", "fruits": "banana"}]},
            auth=auth,
        )
        assert response.status_code == 200
        assert response.json() == [
            {
                "A": 2,
                "fruits": "banana",
                "B": 4,
                "cars": "audi",
            }
        ]


def test_fruits_combi_different_name():
    for e in engines:
        response = client.post(
            f"/api/v1/test/fruits?limit=1000",
            json={"combi": [{"cars": "audi", "fruits": "banana"}]},
            auth=auth,
        )
        assert response.status_code == 200
        assert response.json() == [
            {
                "A": 2,
                "fruits": "banana",
                "B": 4,
                "cars": "audi",
            }
        ]


def test_fruits_combi_multi():
    for e in engines:
        response = client.post(
            f"/api/v1/test/fruits?limit=1000",
            json={
                "pk": [
                    {"cars": "audi", "fruits": "banana"},
                    {"cars": "fiat", "fruits": "ananas"},
                ]
            },
            auth=auth,
        )
        assert response.status_code == 200
        assert len(response.json()) == 2
        assert response.json() == [
            {
                "A": 2,
                "fruits": "banana",
                "B": 4,
                "cars": "audi",
            },
            {
                "A": 9,
                "fruits": "ananas",
                "B": 9,
                "cars": "fiat",
            },
        ]


def test_fruits_select():
    for e in engines:
        response = client.get(f"/api/v1/test/fruits_select?limit=1&format=json&%24engine={e}", auth=auth)
        assert response.status_code == 200
        assert response.json() == [{"fruits_new": "banana"}]


def test_fake_parquet():
    for e in engines:
        response = client.get(f"/api/v1/test/fake_parquet?limit=10&format=json&%24engine={e}", auth=auth)
        assert response.status_code == 200
        assert len(response.json()) == 10


def test_fake_csv():
    for e in engines:
        response = client.get(f"/api/v1/test/fruits_csv?limit=3&format=json&%24engine={e}", auth=auth)
        assert response.status_code == 200
        assert len(response.json()) == 3


def test_fake_arrow():
    for e in engines:
        response = client.get(f"/api/v1/test/fake_arrow?limit=10&format=json&%24engine={e}", auth=auth)
        assert response.status_code == 200
        assert len(response.json()) == 10


def test_all_metadata():
    response = client.get(f"/metadata", auth=auth)
    assert response.status_code == 200
    jsd = response.json()
    for item in jsd:
        name = item["name"]
        tag = item["tag"]
        route = item["route"]
        meta_detail_route = route + "/metadata_detail"
        response = client.get(meta_detail_route, auth=auth)
        if name != "not_existing":
            assert name + "_" + str(response.status_code) == name + "_200"
        else:
            assert name + "_" + str(response.status_code) == name + "_404"

    response = client.get(f"/api/v1/test/fake_arrow/metadata_detail", auth=auth)
    assert response.status_code == 200
    jsd = response.json()
    assert len(jsd["parameters"]) == 2


def test_auth_metadata():
    response = client.get(f"/metadata")
    assert response.status_code == 401

    response = client.get(f"/api/v1/test/fake_arrow/metadata_detail")
    assert response.status_code == 401


def test_data_partition():
    for e in engines:
        response = client.get(
            f"/api/v1/test/fruits_partition?limit=1&format=json&cars=audi&%24engine={e}",
            auth=auth,
        )
        assert response.status_code == 200
        assert response.json() == [
            {
                "A": 2,
                "fruits": "banana",
                "B": 4,
                "cars": "audi",
            }
        ]
        response = client.get(
            f"/api/v1/test/fruits_partition?limit=1&format=json&fruits=ananas&%24engine={e}",
            auth=auth,
        )
        assert response.status_code == 200
        assert response.json() == [
            {
                "A": 9,
                "fruits": "ananas",
                "B": 9,
                "cars": "fiat",
            }
        ]


def test_data_partition_mod():
    for e in engines:
        response = client.get(
            f"/api/v1/test/fruits_partition_mod?limit=1&format=json&cars=audi&%24engine={e}",
            auth=auth,  # works because of implicit parameters
        )
        assert response.status_code == 200
        assert response.json() == [
            {
                "A": 2,
                "fruits": "banana",
                "B": 4,
                "cars": "audi",
            }
        ]
        response = client.post(
            f"/api/v1/test/fruits_partition_mod?limit=1&format=json&%24engine={e}",
            auth=auth,
            json={"cars_in": ["audi"]},
        )
        assert response.status_code == 200
        assert response.json() == [
            {
                "A": 2,
                "fruits": "banana",
                "B": 4,
                "cars": "audi",
            }
        ]


def test_sql_endoint_post():
    # better naming needed in the future

    query = """select * 
        from delta_fruits 
        where cars = 'audi' 
        and fruits = 'banana' 
        and A = 2 and B = 4"""

    response = client.post(
        f"/api/sql",
        auth=auth,
        data=cast(RequestData, query),
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


def test_sql_endoint_get():
    # better naming needed in the future

    query = """select * 
        from delta_fruits 
        where cars = 'audi' 
        and fruits = 'banana' 
        and A = 2 and B = 4"""

    response = client.get(
        f"/api/sql",
        auth=auth,
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


def test_fruits_nested():
    for e in engines:
        response = client.get(f"/api/v1/test/fruits_nested?limit=2&format=json&%24engine={e}", auth=auth)
        assert response.status_code == 200
        assert response.json() == [
            {"A": 1, "fruits": "banana", "B": 5, "cars": "beetle", "nested": {"fruits": "banana", "cars": "beetle"}},
            {"A": 2, "fruits": "banana", "B": 4, "cars": "audi", "nested": {"fruits": "banana", "cars": "audi"}},
        ] or response.json() == [
            {"A": 1, "fruits": "banana", "B": 5, "cars": "beetle", "nested": {"cars": "beetle", "fruits": "banana"}},
            {"A": 2, "fruits": "banana", "B": 4, "cars": "audi", "nested": {"cars": "audi", "fruits": "banana"}},
        ]


def test_sortby():
    for e in engines:
        response = client.get(
            f"/api/v1/test/search_sample?limit=5&format=json&%24engine={e}",
            auth=auth,
        )
        assert response.status_code == 200
        jsd = response.json()
        assert len(jsd) == 5

        assert jsd[0]["randomdata"] <= jsd[1]["randomdata"]
        assert jsd[1]["randomdata"] <= jsd[2]["randomdata"]
        assert jsd[2]["randomdata"] <= jsd[3]["randomdata"]
        assert jsd[3]["randomdata"] <= jsd[4]["randomdata"]
