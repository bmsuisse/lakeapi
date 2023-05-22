import pyarrow as pa
import duckdb
from threading import active_count
from uuid import uuid4
import os
import psutil
import shutil
import deltalake

process = psutil.Process()


shutil.rmtree("out")
os.makedirs("out", exist_ok=True)
i = 0
while i < 100:
    i += 1
    with duckdb.connect() as con:
        dt = deltalake.DeltaTable(
            "tests/data/delta/fruits",
        )
        ds = dt.to_pyarrow_dataset()
        con.register("fruits", ds)
        con.execute(
            f"COPY (SELECT * FROM fruits WHERE cars='audi' LIMIT 1000) TO 'out/{str(uuid4())}.json' (FORMAT JSON)"
        )

        count = active_count()
        rss = process.memory_info().rss
        print(f"NR Threads: {count}. RAM: {rss/1000/1000} MB")
