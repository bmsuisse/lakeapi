import time
import sys
import asyncio
from fastapi.concurrency import run_in_threadpool
from fastapi.testclient import TestClient
import pytest

sys.path.append(".")


engines = ("duckdb", "polars")


@pytest.mark.asyncio
async def test_async_execution(client: TestClient):
    # simulate a lot of async requests and guarantee that the execution is under 1 second

    def call_api_1(engine, format):
        start = time.time()
        response = client.get(
            f"/api/v1/test/fake_delta?limit=10000&format=json&%24engine={engine}&format={format}"
        )
        end = time.time()
        assert response.status_code == 200

        duration = end - start
        print(f"Engine {engine} took {duration} seconds with format {format}")
        assert duration < 1.0

    def call_api_2(engine, format):
        start = time.time()
        response = client.get(
            f"/api/v1/test/fruits_partition?limit=10000&format=json&cars=audi&%24engine={engine}&format={format}"
        )
        end = time.time()
        assert response.status_code == 200

        duration = end - start
        print(f"Engine {engine} took {duration} seconds with format {format}")
        assert duration < 1.0

    def call_api_3(engine, format):
        start = time.time()
        response = client.get(
            f"/api/v1/test/fruits_sortby_desc?limit=10000&format=json&cars=audi&%24engine={engine}&format={format}"
        )
        end = time.time()
        assert response.status_code == 200

        duration = end - start
        print(f"Engine {engine} took {duration} seconds with format {format}")
        assert duration < 1.0

    def call_api_4(engine, format):
        start = time.time()
        response = client.get(
            f"/api/v1/test/fruits_sortby_asc?limit=10000&format=json&cars=audi&%24engine={engine}&format={format}"
        )
        end = time.time()
        assert response.status_code == 200

        duration = end - start
        print(f"Engine {engine} took {duration} seconds with format {format}")
        assert duration < 1.0

    tasks = []
    for _ in range(100):
        for e in engines:
            for f in ("json", "csv", "arrow", "parquet"):
                tasks.append(run_in_threadpool(call_api_1, e, f))
                tasks.append(run_in_threadpool(call_api_2, e, f))
                tasks.append(run_in_threadpool(call_api_3, e, f))
                tasks.append(run_in_threadpool(call_api_4, e, f))
    print(f"Running {len(tasks)} tasks")
    await asyncio.gather(*tasks)
