from fastapi.testclient import TestClient
from .utils import get_app, get_auth
import sys
import polars as pl
import pandas as pd
import pytest


sys.path.append(".")
client = TestClient(get_app())
auth = get_auth()

engines = ("duckdb", "polars")
# engines = ("duckdb",)


def test_no_authentication():
    response = client.get("/")
    assert response.status_code == 401
    assert response.json() == {"detail": "Not authenticated"}


def test_authentication():
    response = client.get("/", auth=auth)
    assert response.status_code == 200


@pytest.mark.parametrize("engine", engines)
def test_fruits_limit_1(engine):
    for _ in range(2):
        response = client.get(
            f"/api/v1/test/fruits?limit=1&format=json&%24engine={engine}", auth=auth
        )
        assert (
            response.headers["Content-Type"].replace(" ", "")
            == "application/json;charset=utf-8"
        )
        assert response.status_code == 200
        assert response.json() == [
            {
                "A": 1,
                "fruits": "banana",
                "B": 5,
                "cars": "beetle",
            }
        ]


def test_fruits_sort_asc():
    for _ in range(2):
        for e in engines:
            response = client.get(
                f"/api/v1/test/fruits_sortby_asc?limit=1&format=json&%24engine={e}",
                auth=auth,
            )
            assert response.status_code == 200
            assert response.json() == [
                {
                    "A": 5,
                    "fruits": "banana",
                    "B": 1,
                    "cars": "beetle",
                }
            ]


def test_fruits_sort_desc():
    for _ in range(2):
        for e in engines:
            response = client.get(
                f"/api/v1/test/fruits_sortby_desc?limit=1&format=json&%24engine={e}",
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


def test_fruits_offset_1():
    for _ in range(2):
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
    for _ in range(2):
        for e in engines:
            response = client.get(
                f"/api/v1/test/fake_delta?limit=1000&&format=json&%24engine={e}",
                auth=auth,
            )
            assert response.status_code == 200
            assert len(response.json()) == 1000


def test_data_filter():
    for _ in range(2):
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
    for _ in range(2):
        for e in engines:
            response = client.get(
                f"/api/v1/test/fruits?limit=1&format=csv&cars=audi&%24engine={e}",
                auth=auth,
            )
            assert response.status_code == 200
            txt = response.text
            import csv

            reader = csv.DictReader(txt.splitlines())
            line1 = reader.__next__()
            assert line1 == {"A": "2", "fruits": "banana", "B": "4", "cars": "audi"}


def test_data_scsv():
    for _ in range(2):
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


def test_data_tsv():
    for _ in range(2):
        for e in engines:
            response = client.get(
                f"/api/v1/test/fruits?limit=1&format=csv&$csv_separator=\\t&cars=audi&%24engine={e}",
                auth=auth,
            )
            assert response.status_code == 200
            txt = response.text
            import csv

            reader = csv.DictReader(txt.splitlines(), delimiter="\t")
            line1 = reader.__next__()
            assert line1 == {"A": "2", "fruits": "banana", "B": "4", "cars": "audi"}


def test_data_minus_1():
    for _ in range(2):
        for e in engines:
            response = client.get(
                f"/api/v1/test/fake_delta?limit=-1&format=json&%24engine={e}", auth=auth
            )
            assert response.status_code == 200
            assert len(response.json()) == 100_011


def test_data_minus_1_csv():
    for _ in range(2):
        from io import StringIO

        for f in ("csv", "scsv"):
            separator = ";" if f == "scsv" else ","
            for e in engines:
                response = client.get(
                    f"/api/v1/test/fake_delta?limit=-1&format={f}&%24engine={e}",
                    auth=auth,
                )
                assert response.status_code == 200
                csv_file = StringIO(response.text)
                assert len(pd.read_csv(csv_file, sep=separator)) == 100_011


def test_data_default_limit():
    for _ in range(2):
        for e in engines:
            response = client.get(
                f"/api/v1/test/fake_delta?format=json&%24engine={e}", auth=auth
            )
            assert response.status_code == 200
            assert len(response.json()) == 100


def test_get_parquet_data():
    for _ in range(2):
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
    for _ in range(2):
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
    for _ in range(2):
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
    for _ in range(2):
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
    for _ in range(2):
        for e in engines:
            response = client.post(
                "/api/v1/test/fruits?limit=1000",
                json={"fruits_in": ["banana", "ananas"]},
                auth=auth,
            )
            assert response.status_code == 200
            assert len(response.json()) == 4

            response = client.post(
                "/api/v1/test/fruits?limit=1000",
                json={"fruits_in": ["apple", "ananas"]},
                auth=auth,
            )
            assert response.status_code == 200
            assert len(response.json()) == 4


def test_fruits_in_zero():
    for _ in range(2):
        for e in engines:
            response = client.post(
                "/api/v1/test/fruits?limit=1000",
                json={"A_in": [0, 9]},
                auth=auth,
            )
            assert response.status_code == 200
            assert len(response.json()) == 2


def test_fruits_combi():
    for _ in range(2):
        for e in engines:
            response = client.post(
                "/api/v1/test/fruits?limit=1000",
                json={"pk": [{"cars": "audi"}]},
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


def test_fruits_combi_int():
    for _ in range(2):
        for e in engines:
            response = client.post(
                "/api/v1/test/fruits?limit=1000",
                json={"combiint": [{"A": 0, "cars": "lamborghini", "B": 5}]},
                auth=auth,
            )
            assert response.status_code == 200
            assert response.json() == [
                {
                    "A": 0,
                    "fruits": "apple",
                    "B": 5,
                    "cars": "lamborghini",
                }
            ]

            response = client.post(
                "/api/v1/test/fruits?limit=1000",
                json={"combiint": [{"A": 1, "cars": "beetle", "B": 5}]},
                auth=auth,
            )
            assert response.status_code == 200
            assert response.json() == [
                {
                    "A": 1,
                    "fruits": "banana",
                    "B": 5,
                    "cars": "beetle",
                }
            ]


def test_fruits_combi_different_name():
    for _ in range(2):
        for e in engines:
            response = client.post(
                "/api/v1/test/fruits?limit=1000",
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
    for _ in range(2):
        for e in engines:
            response = client.post(
                "/api/v1/test/fruits?limit=1000",
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
    for _ in range(2):
        for e in engines:
            response = client.get(
                f"/api/v1/test/fruits_select?limit=1&format=json&%24engine={e}",
                auth=auth,
            )
            assert response.status_code == 200
            assert response.json() == [{"fruits_new": "banana"}]


def test_fake_parquet():
    for _ in range(2):
        for e in engines:
            response = client.get(
                f"/api/v1/test/fake_parquet?limit=10&format=json&%24engine={e}",
                auth=auth,
            )
            assert response.status_code == 200
            assert len(response.json()) == 10


def test_fake_parquet_ns():
    for _ in range(2):
        for e in engines:
            response = client.get(
                f"/api/v1/test/fake_parquet_ns?limit=10&format=json&%24engine={e}",
                auth=auth,
            )
            assert response.status_code == 200
            assert len(response.json()) == 10


def test_fake_csv():
    for _ in range(2):
        for e in engines:
            response = client.get(
                f"/api/v1/test/fruits_csv?limit=3&format=json&%24engine={e}", auth=auth
            )
            assert response.status_code == 200
            assert len(response.json()) == 3


def test_json():
    for _ in range(2):
        for e in engines:
            response = client.get(
                f"/api/v1/test/fruits_json?limit=3&format=json&%24engine={e}", auth=auth
            )
            assert response.status_code == 200
            assert len(response.json()) == 3


def test_ndjson():
    for _ in range(2):
        for e in engines:
            response = client.get(
                f"/api/v1/test/fruits_ndjson?limit=3&format=json&%24engine={e}",
                auth=auth,
            )
            assert response.status_code == 200
            assert len(response.json()) == 3


def test_avro():
    for _ in range(2):
        for e in engines:
            response = client.get(
                f"/api/v1/test/fruits_avro?limit=3&format=json&%24engine={e}", auth=auth
            )
            assert response.status_code == 200
            assert len(response.json()) == 3


def test_fake_arrow():
    for _ in range(2):
        for e in engines:
            response = client.get(
                f"/api/v1/test/fake_arrow?limit=10&format=json&%24engine={e}", auth=auth
            )
            assert response.status_code == 200
            assert len(response.json()) == 10


def test_all_metadata():
    response = client.get("/metadata", auth=auth)
    assert response.status_code == 200
    jsd = response.json()
    for item in jsd:
        for e in engines:
            name = item["name"]
            tag = item["tag"]
            route = item["route"]
            meta_detail_route = route + f"/metadata_detail?%24engine={e}"
            print(meta_detail_route)
            response = client.get(meta_detail_route, auth=auth)
            if name not in ["not_existing", "not_existing2"]:
                assert name + "_" + str(response.status_code) == name + "_200"
            else:
                assert name + "_" + str(response.status_code) == name + "_404"

    response = client.get("/api/v1/test/fake_arrow/metadata_detail", auth=auth)
    assert response.status_code == 200
    jsd = response.json()
    assert len(jsd["parameters"]) == 2


def test_metadata_stringify():
    for e in engines:
        response = client.get(
            f"/api/v1/complexer/complex_fruits/metadata_detail?jsonify_complex=True&%24engine={e}",
            auth=auth,
        )
        assert response.status_code == 200
        assert response.json()["modified_date"] is not None


def test_metadata_no_str_length():
    for e in engines:
        response = client.get(
            f"/api/v1/complexer/complex_fruits/metadata_detail?include_str_lengths=False&%24engine={e}",
            auth=auth,
        )
        assert response.status_code == 200
        assert response.json()["modified_date"] is not None


def test_metadata_no_hidden():
    response = client.get("/api/v1/test/fruits_partition/metadata_detail", auth=auth)
    assert response.status_code == 200
    jsd = response.json()
    prm_names = [p["name"] for p in jsd["parameters"]]
    assert "cars_md5_prefix_2" not in prm_names
    field_names = [f["name"] for f in jsd["data_schema"]]
    assert "cars_md5_prefix_2" not in field_names


def test_auth_metadata():
    response = client.get("/metadata")
    assert response.status_code == 401

    response = client.get("/api/v1/test/fake_arrow/metadata_detail")
    assert response.status_code == 401


def test_fruits_nested():
    for _ in range(2):
        for e in engines:
            response = client.get(
                f"/api/v1/test/fruits_nested?limit=2&format=json&%24engine={e}",
                auth=auth,
            )
            assert response.status_code == 200
            assert response.json() == [
                {
                    "A": 1,
                    "fruits": "banana",
                    "B": 5,
                    "cars": "beetle",
                    "nested": {"fruits": "banana", "cars": "beetle"},
                },
                {
                    "A": 2,
                    "fruits": "banana",
                    "B": 4,
                    "cars": "audi",
                    "nested": {"fruits": "banana", "cars": "audi"},
                },
            ] or response.json() == [
                {
                    "A": 1,
                    "fruits": "banana",
                    "B": 5,
                    "cars": "beetle",
                    "nested": {"cars": "beetle", "fruits": "banana"},
                },
                {
                    "A": 2,
                    "fruits": "banana",
                    "B": 4,
                    "cars": "audi",
                    "nested": {"cars": "audi", "fruits": "banana"},
                },
            ]


def test_sortby():
    for _ in range(2):
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
