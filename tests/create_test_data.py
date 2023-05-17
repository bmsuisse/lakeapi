import sys
import os
import pathlib
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
    if os.path.exists(path):
        try:
            os.remove(path)
        except:
            print("File not deleted")
    try:
        shutil.rmtree(path)
        print(f"Remove: {path}")
    except:
        print("Folder not deleted")


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


if __name__ == "__main__":
    if not os.path.exists("tests/data/parquet/search.parquet"):
        os.makedirs("tests/data/parquet", exist_ok=True)
        pl.DataFrame(create_rows_faker(1000)).write_parquet("tests/data/parquet/search.parquet")

    df = pl.DataFrame(
        {
            "A": [1, 2, 3, 4, 5, 9],
            "fruits": ["banana", "banana", "apple", "apple", "banana", "ananas"],
            "B": [5, 4, 3, 2, 1, 9],
            "cars": ["beetle", "audi", "beetle", "beetle", "beetle", "fiat"],
        }
    )
    df = df.to_pandas()
    delta_path = "tests/data/delta/fruits"
    delete_folder(delta_path)
    write_deltalake(delta_path, df, mode="overwrite")

    print(df)

    delta_path = "tests/data/delta/fruits_partition"

    df["fruits_partition"] = df["fruits"]
    df["cars_md5_prefix_2"] = [md5(val.encode("UTF-8")).hexdigest()[:2] for val in df["cars"]]

    print(df)

    delete_folder(delta_path)
    write_deltalake(delta_path, df, mode="overwrite", partition_by=["cars_md5_prefix_2", "cars"])

    csv_path = "tests/data/csv/fruits.csv"
    delete_folder(csv_path)
    os.makedirs(pathlib.Path(csv_path).parent, exist_ok=True)
    df.to_csv(csv_path)

    delta_path = "tests/data/delta/fake"
    delete_folder(delta_path)

    df = pl.DataFrame(create_rows_faker(100_011)).to_pandas()

    df["name_md5_prefix_2"] = [md5(val.encode("UTF-8")).hexdigest()[:1] for val in df["name"]]

    df["name1"] = df["name"]

    print(df)

    write_deltalake(delta_path, df, mode="overwrite")  # , partition_by=["name_md5_prefix_2", "abc"]

    write_deltalake(
        delta_path + "_partition",
        df,
        mode="overwrite",  # , partition_by=["name_md5_prefix_2", "abc"]
    )

    parquet_path = "tests/data/parquet/fake.parquet"
    delete_folder(parquet_path)
    os.makedirs(pathlib.Path(parquet_path).parent, exist_ok=True)
    df.to_parquet(parquet_path)

    arrow_path = "tests/data/arrow/fake.arrow"
    delete_folder(arrow_path)
    os.makedirs(pathlib.Path(arrow_path).parent, exist_ok=True)
    df.to_feather(arrow_path)
