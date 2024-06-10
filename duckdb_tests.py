import duckdb
import requests

with open("azure.duckdb_extension.gz", "wb") as f:
    f.write(
        requests.get(
            "http://extensions.duckdb.org/v0.10.0/windows_amd64/azure.duckdb_extension.gz"
        ).content
    )

emulator_con_str = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;"

with duckdb.connect() as con:
    con.execute(
        f"""FORCE INSTALL azure;
        INSTALL azure;
LOAD azure;
CREATE SECRET secret2 (
    TYPE AZURE,
    CONNECTION_STRING  '{emulator_con_str}'
);"""
    )
    with con.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM read_parquet('azure://testlake/td/faker.parquet')"
        )
        print(cur.fetchall())
