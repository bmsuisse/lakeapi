import sys
import os
import pathlib
from typing import Any, Optional
import polars as pl
from deltalake import write_deltalake
import shutil
import pandas as pd
from hashlib import md5
import pyarrow as pa
import json
import pyarrow.dataset as ds

try:
    from .utils import create_rows_faker
except ImportError:
    from utils import create_rows_faker


dir_path = pathlib.Path(os.path.dirname(os.path.realpath(__file__))).parent
sys.path.append(os.path.realpath(f"{dir_path}"))


def delete_folder(path):
    if not os.path.exists(path):
        return  # nothing todo
    if os.path.isfile(path):
        os.remove(path)
    else:
        shutil.rmtree(path)


def store_df_as_delta(
    data: dict[str, list[Any]] | pd.DataFrame | pa.Table,
    data_path: str,
    partition_by: Optional[list[str]] = None,
    *,
    configuration: Optional[dict[str, str | dict | list]] = None,
    compression: str | None = None,
) -> pd.DataFrame:
    dfp: pa.Table | pd.DataFrame = (
        data if isinstance(data, pd.DataFrame) or isinstance(data, pa.Table) else pl.DataFrame(data).to_pandas()
    )
    delta_path = "tests/data/" + data_path
    delete_folder(delta_path)

    def _str_or_json(v: str | dict | list):
        if isinstance(v, str):
            return v
        return json.dumps(v)

    opts = ds.ParquetFileFormat().make_write_options(compression=compression or "snappy")
    write_deltalake(
        delta_path,
        dfp,
        mode="overwrite",
        partition_by=partition_by,
        file_options=opts,
        configuration={k: _str_or_json(v) for k, v in configuration.items()} if configuration is not None else None,
    )
    assert not isinstance(dfp, dict)
    return dfp if isinstance(dfp, pd.DataFrame) else dfp.to_pandas()


if __name__ == "__main__":
    if not os.path.exists("tests/data/parquet/search.parquet"):
        os.makedirs("tests/data/parquet", exist_ok=True)
        pl.DataFrame(create_rows_faker(1000)).write_parquet("tests/data/parquet/search.parquet")

    df_fruits = store_df_as_delta(
        {
            "A": [1, 2, 3, 4, 5, 0, 9],
            "fruits": ["banana", "banana", "apple", "apple", "banana", "apple", "ananas"],
            "B": [5, 4, 3, 2, 1, 5, 9],
            "cars": ["beetle", "audi", "beetle", "beetle", "beetle", "lamborghini", "fiat"],
        },
        "delta/fruits",
    )
    store_df_as_delta(df_fruits, "startest/fruits")

    store_df_as_delta(
        {
            "A": [1, 2, 3, 4, 5, 9],
            "fruits": ["banana", "banana", "apple", "apple", "banana", "ananas"],
            "B": [5, 4, 3, 2, 1, 9],
            "cars": ["beetle", "audi", "beetle", "beetle", "beetle", "fiat"],
            "person": [
                {"name": "tom", "age": 3},
                {"name": "bob", "age": 5},
                {"name": "tim", "age": 7},
                {"name": "john", "age": 1},
                {"name": "marc", "age": 3},
                {"name": "peter", "age": 32},
            ],
            "vitamines": [["A", "B12"], [], ["C", "B12"], ["D", "B12", "C"], ["C"], ["E", "B12"]],
        },
        "delta/struct_fruits",
        configuration={
            "lakeapi.config": {
                "params": [
                    {"name": "fruits", "operators": ["not in", "in", "contains", "not contains", "<>"]},
                    {"name": "cars", "operators": ["not in", "in", "contains", "not contains", "<>"]},
                    {"name": "B", "operators": [">", "<", "<=", ">=", "between", "not between"]},
                ]
            }
        },
    )

    fruits_partition = df_fruits.copy()
    fruits_partition["my_empty_col"] = pd.Series(
        data=[None for _ in range(0, fruits_partition.shape[0])], dtype="string"
    )
    fruits_partition["fruits_partition"] = fruits_partition["fruits"]
    fruits_partition["cars_md5_prefix_2"] = [
        md5(val.encode("UTF-8")).hexdigest()[:2] for val in fruits_partition["cars"]
    ]
    store_df_as_delta(fruits_partition, "delta/fruits_partition", partition_by=["cars_md5_prefix_2", "cars"])
    store_df_as_delta(fruits_partition, "startest/fruits_partition", partition_by=["cars_md5_prefix_2", "cars"])
    fruits_partition_mod = df_fruits.copy()
    fruits_partition_mod["cars_md5_mod_27"] = [
        str(int(md5(val.encode("UTF-8")).hexdigest(), 16) % 27) for val in fruits_partition_mod["cars"]
    ]
    store_df_as_delta(fruits_partition_mod, "delta/fruits_partition_mod", partition_by=["cars_md5_mod_27"])

    df_fruits_nested = pl.from_pandas(df_fruits)
    df_fruits_nested = df_fruits_nested.with_columns(
        pl.struct([df_fruits_nested["fruits"], df_fruits_nested["cars"]]).alias("nested")
    )

    df_fruits_nested = df_fruits_nested.to_pandas()
    store_df_as_delta(df_fruits_nested, "delta/fruits_nested")
    store_df_as_delta(df_fruits_nested, "startest/fruits_nested")
    weather = pl.DataFrame(
        {
            "station": ["Station " + str(x) for x in range(1, 6)],
            "temperatures": [
                "20 5 5 E1 7 13 19 9 6 20",
                "18 8 16 11 23 E2 8 E2 E2 E2 90 70 40",
                "19 24 E9 16 6 12 10 22",
                "E2 E0 15 7 8 10 E1 24 17 13 6",
                "14 8 E0 16 22 24 E1",
            ],
        }
    )
    out = weather.with_columns(pl.col("temperatures").str.split(" "))  # thanks for the sample, polars
    store_df_as_delta(out.to_pandas(), "delta/weather", compression="zstd")

    csv_path = "tests/data/csv/fruits.csv"
    delete_folder(csv_path)
    os.makedirs(pathlib.Path(csv_path).parent, exist_ok=True)
    df_fruits.to_csv(csv_path)
    df_fruits.to_csv("tests/data/startest/fruits_csv.csv")
    os.makedirs("tests/data/avro", exist_ok=True)
    pl.from_pandas(df_fruits).write_avro("tests/data/avro/fruits.avro")
    json_path = "tests/data/json/fruits.json"
    delete_folder(json_path)
    os.makedirs(pathlib.Path(json_path).parent, exist_ok=True)
    df_fruits.to_json(json_path, orient="records")

    json_path = "tests/data/ndjson/fruits.ndjson"
    delete_folder(json_path)
    os.makedirs(pathlib.Path(json_path).parent, exist_ok=True)
    df_fruits.to_json(json_path, orient="records", lines=True)

    df_faker = pl.DataFrame(create_rows_faker(100_011)).to_pandas()

    df_faker["name_md5_prefix_2"] = [md5(val.encode("UTF-8")).hexdigest()[:1] for val in df_faker["name"]]

    df_faker["name1"] = df_faker["name"]

    print(df_faker)
    store_df_as_delta(df_faker, "delta/fake", partition_by=None)
    # store_df_as_delta(df_faker, "delta/fake_partition", partition_by=None)

    parquet_path = "tests/data/parquet/fake.parquet"
    delete_folder(parquet_path)
    os.makedirs(pathlib.Path(parquet_path).parent, exist_ok=True)
    df_faker.to_parquet(parquet_path)
    df_faker.to_parquet("tests/data/startest/faker.parquet")
    arrow_path = "tests/data/arrow/fake.arrow"
    delete_folder(arrow_path)
    os.makedirs(pathlib.Path(arrow_path).parent, exist_ok=True)
    df_faker.to_feather(arrow_path)

    df_ns = df_faker.copy()
    from faker import Faker

    fakeit = Faker()
    df_ns["ts"] = pl.Series("ts", [fakeit.date_time() for _ in range(0, df_ns.shape[0])], pl.Datetime(time_unit="ns"))
    df_ns.to_parquet("tests/data/parquet/fake_ns.parquet")
