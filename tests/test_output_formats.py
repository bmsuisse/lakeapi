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

sys.path.append(".")
client = TestClient(get_app())
auth = get_auth()
engines = ["duckdb"]


def test_data_csv4excel():
    for e in engines:
        # csv 4 excel is a really ... strange... format
        response = client.get(f"/api/v1/test/fruits?limit=1&format=csv4excel&cars=audi&%24engine={e}", auth=auth)
        assert response.status_code == 200

        import csv

        firstline = response.content[0:6]
        assert firstline.decode("ascii") == "sep=,\n"
        rest = response.content[6:].decode("utf-16-le")
        reader = csv.DictReader(rest.splitlines())
        line1 = reader.__next__()
        assert line1 == {"A": "2", "fruits": "banana", "B": "4", "cars": "audi"}
