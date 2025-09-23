import sys
from fastapi.testclient import TestClient

sys.path.append(".")

engines = ["duckdb", "polars"]


def test_returns_complex(client: TestClient):
    for e in engines:
        response = client.post(
            f"/api/v1/complexer/complex_fruits?limit=10&format=json&%24engine={e}",
        )
        assert response.status_code == 200
        jsd = response.json()
        assert len(jsd) == 6
        assert isinstance(jsd[0]["vitamines"], list)
        assert isinstance(jsd[0]["person"], dict)


def test_returns_jsonify(client: TestClient):
    for e in engines:
        response = client.post(
            f"/api/v1/complexer/complex_fruits?limit=10&format=json&jsonify_complex=True&%24engine={e}",
        )
        assert response.status_code == 200
        jsd = response.json()
        assert len(jsd) == 6
        assert isinstance(jsd[0]["vitamines"], str)
        assert isinstance(jsd[0]["person"], str)
        import json

        assert isinstance(json.loads(jsd[0]["vitamines"]), list)
        assert isinstance(json.loads(jsd[0]["person"]), dict)


def test_returns_metadatadeta(client: TestClient):
    response = client.get(
        "/api/v1/complexer/complex_fruits/metadata_detail",
    )
    assert response.status_code == 200

    jsd = response.json()

    vit = [p for p in jsd["data_schema"] if p["name"] == "vitamines"][0]
    assert vit is not None
    assert vit["type"]["type_str"].lower().startswith("list<")

    per = [p for p in jsd["data_schema"] if p["name"] == "person"][0]
    assert per is not None
    assert per["type"]["type_str"].lower().startswith("struct<")

    assert "person" not in jsd["max_string_lengths"]
    assert "vitamines" not in jsd["max_string_lengths"]
    assert "fruits" in jsd["max_string_lengths"]


def test_returns_metadatadeta_partition(client: TestClient):
    response = client.get(
        "/api/v1/test/fruits_partition/metadata_detail",
    )
    assert response.status_code == 200

    jsd = response.json()

    assert jsd["max_string_lengths"]["my_empty_col"] is None


def test_returns_metadatadeta_jsonifiyed(client: TestClient):
    response = client.get(
        "/api/v1/complexer/complex_fruits/metadata_detail?jsonify_complex=True",
    )
    assert response.status_code == 200

    jsd = response.json()

    vit = [p for p in jsd["data_schema"] if p["name"] == "vitamines"][0]
    assert vit is not None
    assert vit["type"]["type_str"].lower() == "string"

    per = [p for p in jsd["data_schema"] if p["name"] == "person"][0]
    assert per is not None
    assert per["type"]["type_str"].lower() == "string"

    assert "person" in jsd["max_string_lengths"]
    assert "vitamines" in jsd["max_string_lengths"]
    assert "fruits" in jsd["max_string_lengths"]


def test_returns_csv(client: TestClient):
    for e in engines:
        response = client.post(
            f"/api/v1/complexer/complex_fruits?limit=10&format=csv&%24engine={e}",
        )
        assert response.status_code == 200
        import csv

        reader = csv.DictReader(response.text.splitlines())
        line1 = reader.__next__()
        assert isinstance(line1["vitamines"], str)
        assert isinstance(line1["person"], str)
        import json

        assert isinstance(json.loads(line1["vitamines"]), list)
        assert isinstance(json.loads(line1["person"]), dict)
