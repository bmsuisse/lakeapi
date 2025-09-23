from threading import active_count
from uuid import uuid4
import os
import psutil
import shutil
import deltalake
import pyarrow.dataset as ds

process = psutil.Process()


def naive_json_1(res: ds.Dataset, fn: str):  # This works, no matter how often executed
    import json

    first = True
    with open(fn, "w") as f:
        f.write("[")
        for batch in res.to_batches(batch_size=1000):
            aspy = batch.to_pylist()
            for i in aspy:
                if first:
                    first = False
                else:
                    f.write(", ")
                f.write(json.dumps(i, default=str))
        f.write("]")


if os.path.exists("out"):
    shutil.rmtree("out")
os.makedirs("out", exist_ok=True)
i = 0
while i < 100:
    print(f"{i} run")
    i += 1

    dt = deltalake.DeltaTable(
        "tests/data/delta/fake",
    )
    dss = dt.to_pyarrow_dataset()

    count = active_count()
    rss = process.memory_info().rss
    print(f"before Naive impl: NR Threads: {count}. RAM: {rss / 1000 / 1000} MB")
    naive_json_1(dss, f"out/{str(uuid4())}.json")
