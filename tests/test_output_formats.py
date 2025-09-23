import sys
import pyarrow as pa
import pytest

sys.path.append(".")

engines = ["duckdb", "polars"]


from fastapi.testclient import TestClient


@pytest.mark.parametrize("engine", engines)
def test_data_csv4excel(client: TestClient, engine):
    # csv 4 excel is a really ... strange... format
    response = client.get(
        f"/api/v1/test/fruits?limit=1&format=csv4excel&cars=audi&%24engine={engine}"
    )
    assert response.status_code == 200

    import csv

    firstline = response.content[0:6]
    assert firstline.decode("ascii") == "sep=,\n"
    rest = response.content[6:].decode("utf-16-le")
    reader = csv.DictReader(rest.splitlines())
    line1 = reader.__next__()
    assert line1 == {"A": "2", "fruits": "banana", "B": "4", "cars": "audi"}


@pytest.mark.parametrize("engine", engines)
def test_data_csv_custom(client: TestClient, engine):
    response = client.get(
        f"/api/v1/test/fruits?limit=1&format=csv&cars=audi&%24engine={engine}&$encoding=utf-16-be&$csv_separator=|"
    )
    assert response.status_code == 200

    rest = response.content.decode("utf-16-be")
    assert "|" in rest

    lines = rest.replace("\r\n", "\n").split("\n")
    header = lines[0].replace('"', "").split("|")
    line1 = lines[1].replace('"', "").split("|")
    line1_dict = dict(zip(header, line1))

    assert line1_dict == {"A": "2", "fruits": "banana", "B": "4", "cars": "audi"}

    response = client.get(
        f"/api/v1/test/fruits?limit=1&format=csv&cars=audi&%24engine={engine}&$encoding=cp850&$csv_separator=\\t"
    )
    assert response.status_code == 200

    rest = response.content.decode("cp850")
    assert "\t" in rest
    lines = rest.replace("\r\n", "\n").split("\n")
    header = lines[0].replace('"', "").split("\t")
    line1 = lines[1].replace('"', "").split("\t")
    line1_dict = dict(zip(header, line1))
    assert line1_dict == {"A": "2", "fruits": "banana", "B": "4", "cars": "audi"}


def test_data_html(client: TestClient):
    for e in engines:
        # csv 4 excel is a really ... strange... format
        response = client.get(
            f"/api/v1/test/fruits?limit=1&format=html&cars=audi&%24engine={e}"
        )
        assert response.status_code == 200
        assert response.text.startswith("<")


def test_data_xml(client: TestClient):
    for e in engines:
        # csv 4 excel is a really ... strange... format
        response = client.get(
            f"/api/v1/test/fruits?limit=1&format=xml&cars=audi&%24engine={e}"
        )
        assert response.status_code == 200
        assert response.text.startswith("<")


def test_data_arrow_stream(client: TestClient):
    for e in engines:
        # csv 4 excel is a really ... strange... format
        response = client.get(
            f"/api/v1/test/fruits?limit=1&format=arrow-stream&cars=audi&%24engine={e}"
        )
        assert response.status_code == 200
        import tempfile

        temp_fn = tempfile.mktemp()
        with open(temp_fn, "wb") as f:
            f.write(response.content)
        with pa.OSFile(temp_fn, "rb") as fl:
            with pa.ipc.open_stream(fl) as reader:
                df = reader.read_pandas()
                assert df["A"][0] == 2
