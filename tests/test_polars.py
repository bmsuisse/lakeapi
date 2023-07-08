


import polars as pl
from bmsdna.lakeapi.polars_extensions.delta import scan_delta2



df = scan_delta2("tests/data/delta/fruits")


print(df.collect(streaming=True))



