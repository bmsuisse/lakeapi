import duckdb

with duckdb.connect(config={"allow_unsigned_extensions": True}) as con:
    AZURE_EXT_LOC = "azure.duckdb_extension.gz"
    with con.cursor() as cur:
        cur.execute(f"PRAGMA platform;CALL pragma_platform();")
        print(cur.fetchall())
