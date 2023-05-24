import sys
import os
import pathlib
from typing import Any, Optional
import polars as pl
from deltalake import write_deltalake
import shutil
from faker import Faker
import pandas as pd
import random
from hashlib import md5


dir_path = pathlib.Path(os.path.dirname(os.path.realpath(__file__))).parent
sys.path.append(os.path.realpath(f"{dir_path}"))


def delete_folder(path):
    if not os.path.exists(path):
        return  # nothing todo
    if os.path.isfile(path):
        os.remove(path)
    else:
        shutil.rmtree(path)


def create_rows_faker(num=1):
    fake = Faker()
    output = [
        {
            "name": fake.name(),
            "address": fake.address(),
            "name": fake.name(),
            "email": fake.email(),
            "bs": fake.bs(),
            "city": fake.city(),
            "state": fake.state(),
            "date_time": fake.date_time(),
            "paragraph": fake.paragraph(),
            "Conrad": fake.catch_phrase(),
            "randomdata": random.randint(1000, 2000),
            "abc": random.choice(["a", "b", "c"]),
        }
        for x in range(num)
    ]
    return output


def store_df_as_delta(
    data: dict[str, list[Any]] | pd.DataFrame, data_path: str, partition_by: Optional[list[str]] = None
):
    dfp = data if isinstance(data, pd.DataFrame) else pl.DataFrame(data).to_pandas()
    delta_path = "tests/data/" + data_path
    delete_folder(delta_path)
    write_deltalake(delta_path, dfp, mode="overwrite", partition_by=partition_by)
    return dfp


if __name__ == "__main__":
    if not os.path.exists("tests/data/parquet/search.parquet"):
        os.makedirs("tests/data/parquet", exist_ok=True)
        pl.DataFrame(create_rows_faker(1000)).write_parquet("tests/data/parquet/search.parquet")

    df_fruits = store_df_as_delta(
        {
            "A": [1, 2, 3, 4, 5, 9],
            "fruits": ["banana", "banana", "apple", "apple", "banana", "ananas"],
            "B": [5, 4, 3, 2, 1, 9],
            "cars": ["beetle", "audi", "beetle", "beetle", "beetle", "fiat"],
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
    )

    fruits_partition = df_fruits.copy()

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

    csv_path = "tests/data/csv/fruits.csv"
    delete_folder(csv_path)
    os.makedirs(pathlib.Path(csv_path).parent, exist_ok=True)
    df_fruits.to_csv(csv_path)
    df_fruits.to_csv("tests/data/startest/fruits_csv.csv")

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
