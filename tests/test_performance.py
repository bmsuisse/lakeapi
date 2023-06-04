
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

    async def call_api(engine):
        start = time.time()
        response = client.get(f"/api/v1/test/fake_delta?limit=10000&format=json&%24engine={engine}", auth=auth)
        end = time.time()
        assert response.status_code == 200

        duration = end - start
        print(f"Engine {engine} took {duration} seconds")
        assert end - start < 0.75

    async def main():
        tasks = []
        for _ in range(1000):
            for e in engines:
                task = asyncio.create_task(call_api(e))
                tasks.append(task)
        await asyncio.gather(*tasks)

    asyncio.run(main())