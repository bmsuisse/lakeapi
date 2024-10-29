from fastapi.testclient import TestClient
from .utils import get_app, get_auth
import time
import sys
import asyncio

sys.path.append(".")
client = TestClient(get_app())
auth = get_auth()

engines = ("duckdb", "polars")


def test_async_execution():
    # simulate a lot of async requests and guarantee that the execution is under 1 second

    async def call_api_1(engine, format):
        start = time.time()
        response = client.get(
            f"/api/v1/test/fake_delta?limit=10000&format=json&%24engine={engine}&format={format}",
            auth=auth,
        )
        end = time.time()
        assert response.status_code == 200

        duration = end - start
        print(f"Engine {engine} took {duration} seconds with format {format}")
        assert duration < 1.0

    async def call_api_2(engine, format):
        start = time.time()
        response = client.get(
            f"/api/v1/test/fruits_partition?limit=10000&format=json&cars=audi&%24engine={engine}&format={format}",
            auth=auth,
        )
        end = time.time()
        assert response.status_code == 200

        duration = end - start
        print(f"Engine {engine} took {duration} seconds with format {format}")
        assert duration < 1.0

    async def call_api_3(engine, format):
        start = time.time()
        response = client.get(
            f"/api/v1/test/fruits_sortby_desc?limit=10000&format=json&cars=audi&%24engine={engine}&format={format}",
            auth=auth,
        )
        end = time.time()
        assert response.status_code == 200

        duration = end - start
        print(f"Engine {engine} took {duration} seconds with format {format}")
        assert duration < 1.0

    async def call_api_4(engine, format):
        start = time.time()
        response = client.get(
            f"/api/v1/test/fruits_sortby_asc?limit=10000&format=json&cars=audi&%24engine={engine}&format={format}",
            auth=auth,
        )
        end = time.time()
        assert response.status_code == 200

        duration = end - start
        print(f"Engine {engine} took {duration} seconds with format {format}")
        assert duration < 1.0

    async def main():
        tasks = []
        for _ in range(100):
            for e in engines:
                for f in ("json", "csv", "arrow", "parquet"):
                    tasks.append(asyncio.create_task(call_api_1(e, f)))
                    tasks.append(asyncio.create_task(call_api_2(e, f)))
                    tasks.append(asyncio.create_task(call_api_3(e, f)))
                    tasks.append(asyncio.create_task(call_api_4(e, f)))
        print(f"Running {len(tasks)} tasks")
        await asyncio.gather(*tasks)

    asyncio.run(main())
