from fastapi.testclient import TestClient
import sys

sys.path.append(".")
import pytest

min_rounds = 1


def benchmark_engine(client: TestClient, engine="duckdb"):
    total_time = 0
    response = client.get(f"/api/v1/test/fake_{engine}?limit=10&abc=a")

    assert response.status_code == 200
    assert len(response.json()) == 10

    client.get(f"/api/v1/test/fake_delta?limit=100&abc=a&%24engine={engine}")
    client.get(f"/api/v1/test/fake_delta?limit=100&abc=b&%24engine={engine}")
    client.get(f"/api/v1/test/fake_delta?limit=100&abc=c&%24engine={engine}")

    return True


@pytest.mark.benchmark(min_rounds=min_rounds, warmup=False)
def test_benchmark_duckdb(benchmark, client: TestClient):
    result = benchmark(benchmark_engine, client, engine="duckdb")
    assert result == True


@pytest.mark.benchmark(min_rounds=min_rounds, warmup=False)
def test_benchmark_polars(benchmark, client: TestClient):
    result = benchmark(benchmark_engine, client, engine="polars")
    assert result == True
