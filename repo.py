import polars as pl

sql_cont = pl.SQLContext()
sql_cont.register("test_fake_arrow", pl.scan_ipc("tests/data/arrow/fake.arrow"))
print(sql_cont.execute("SELECT * FROM test_fake_arrow s LIMIT 1").collect())
