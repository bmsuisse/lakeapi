import duckdb
from threading import active_count
from uuid import uuid4
import os
import psutil
import shutil
import deltalake

process = psutil.Process()


def naive_json_1(
    res: duckdb.DuckDBPyConnection, fn: str
):  # This works, no matter how often executed
    import json

    first = True
    with open(fn, "w") as f:
        f.write("[")
        with res.fetch_record_batch(1000) as reader:
            for batch in reader:
                aspy = batch.to_pylist()
                for i in aspy:
                    if first:
                        first = False
                    else:
                        f.write(", ")
                    f.write(json.dumps(i, default=str))
        f.write("]")


shutil.rmtree("out")
os.makedirs("out", exist_ok=True)
i = 0
while i < 100:
    print(f"{i} run")
    i += 1
    with duckdb.connect() as con:
        dt = deltalake.DeltaTable(
            "tests/data/delta/fake",
        )
        ds = dt.to_pyarrow_dataset()

        con.register("fake", ds)
        naive_json_1(con.execute("SELECT * FROM fake"), f"out/{str(uuid4())}.json")
        count = active_count()
        rss = process.memory_info().rss
        print(f"After Naive impl: NR Threads: {count}. RAM: {rss / 1000 / 1000} MB")
        con.execute(
            f"CREATE TEMP VIEW t1 AS SELECT * FROM fake order by 1; COPY (SELECT * FROM t1) TO 'out/{str(uuid4())}.json' (FORMAT JSON, Array True);drop VIEW t1"
        )  # Works only once
        count = active_count()
        rss = process.memory_info().rss
        print(f"NR Threads: {count}. RAM: {rss / 1000 / 1000} MB")
