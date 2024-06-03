from fastapi.testclient import TestClient
import sys

sys.path.append(".")
from .utils import get_app

import pytest

client = TestClient(get_app())

user = "test"
pw = "B~C:BB*_9-1u"
auth = (user, pw)

min_rounds = 1


def benchmark_engine(engine="duckdb"):
    total_time = 0
    response = client.get(f"/api/v1/test/fake_{engine}?limit=10&abc=a", auth=auth)

    assert response.status_code == 200
    assert len(response.json()) == 10

    client.get(f"/api/v1/test/fake_delta?limit=100&abc=a&%24engine={engine}", auth=auth)
    client.get(f"/api/v1/test/fake_delta?limit=100&abc=b&%24engine={engine}", auth=auth)
    client.get(f"/api/v1/test/fake_delta?limit=100&abc=c&%24engine={engine}", auth=auth)

    return True


@pytest.mark.benchmark(min_rounds=min_rounds, warmup=False)
def test_benchmark_duckdb(benchmark):
    result = benchmark(benchmark_engine, engine="duckdb")
    assert result == True


@pytest.mark.benchmark(min_rounds=min_rounds, warmup=False)
def test_benchmark_polars(benchmark):
    result = benchmark(benchmark_engine, engine="polars")
    assert result == True
