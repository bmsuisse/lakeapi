
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
import asyncio

sys.path.append(".")
client = TestClient(get_app())
auth = get_auth()

engines = ("duckdb", "polars")
# engines = ("duckdb")


def test_async_execution():

    # api should not block

    async def call_api_1(engine, format):
        start = time.time()
        response = client.get(f"/api/v1/test/fake_delta?limit=10000&format=json&%24engine={engine}&format={format}", auth=auth)
        end = time.time()
        assert response.status_code == 200

        duration = end - start
        print(f"Engine {engine} took {duration} seconds with format {format}")
        assert end - start < 0.75


    async def call_api_2(engine, format):
        start = time.time()
        response = client.get(f"/api/v1/test/fruits_partition?limit=10000&format=json&cars=audi&%24engine={engine}&format={format}", auth=auth)
        end = time.time()
        assert response.status_code == 200

        duration = end - start
        print(f"Engine {engine} took {duration} seconds with format {format}")
        assert end - start < 1 # subsecond resuls needs to be acieved

    async def main():
        tasks = []
        for _ in range(100):
            for e in engines:
                for f in ("json", "csv", "scsv", "arrow", "parquet"):
                    tasks.append(asyncio.create_task(call_api_1(e, f)))
                    tasks.append(asyncio.create_task(call_api_2(e, f)))
        print(f"Running {len(tasks)} tasks")
        await asyncio.gather(*tasks)

    asyncio.run(main())