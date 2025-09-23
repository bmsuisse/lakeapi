from fastapi.testclient import TestClient
from .utils import get_app, get_auth, create_rows_faker
import sys
import polars as pl
from deltalake import write_deltalake
import os
from hashlib import md5

sys.path.append(".")

auth = get_auth()
engines = ["duckdb", "polars"]


def test_data_overwrite(client: TestClient):
    for e in engines:
        response = client.get(
            f"/api/v1/test/fake_delta?limit=1000&&format=json&%24engine={e}"
        )
        assert response.status_code == 200
        assert len(response.json()) == 1000

    df_faker = pl.DataFrame(create_rows_faker(1001)).to_pandas()

    df_faker["name_md5_prefix_2"] = [
        md5(val.encode("UTF-8")).hexdigest()[:1] for val in df_faker["name"]
    ]

    df_faker["name1"] = df_faker["name"]

    print(df_faker)
    assert os.path.exists("tests/data/delta/fake/_delta_log")
    write_deltalake(
        "tests/data/delta/fake", df_faker, mode="overwrite", schema_mode="overwrite"
    )

    for e in engines:
        response = client.get(
            f"/api/v1/test/fake_delta?limit=1000&&format=json&%24engine={e}"
        )
        assert response.status_code == 200
        assert len(response.json()) == 1000
