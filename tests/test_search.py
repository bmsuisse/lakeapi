import sys

sys.path.append(".")

engines = ["duckdb"]


from fastapi.testclient import TestClient


def test_search(client: TestClient):
    for e in engines:
        response = client.get(
            f"/api/v1/test/search_sample?limit=5&format=json&%24engine={e}&search=Karen%20example"
        )
        assert response.status_code == 200
        jsd = response.json()
        assert len(jsd) >= 3
        assert len(jsd) <= 5
        assert jsd[0]["search_score"] is not None
        assert jsd[1]["search_score"] is not None
        assert jsd[2]["search_score"] is not None

        assert jsd[0]["search_score"] >= jsd[1]["search_score"]
        assert jsd[1]["search_score"] >= jsd[2]["search_score"]
        for item in jsd:
            assert (
                "karen"
                in (item["email"] + " " + item["name"] + " " + item["address"]).lower()
                or "example"
                in (item["email"] + " " + item["name"] + " " + item["address"]).lower()
            )


def test_no_search(client: TestClient):
    for e in engines:
        response = client.get(
            f"/api/v1/test/search_sample?limit=5&format=json&%24engine={e}"
        )
        assert response.status_code == 200
        jsd = response.json()
        assert len(jsd) >= 3
        assert len(jsd) <= 5
        assert "search_score" not in jsd[0]
