import duckdb

with duckdb.connect() as con:
    con.execute(
        """FORCE INSTALL azure;
LOAD azure;CREATE SECRET secret2 (
    TYPE AZURE,
    ACCOUNT_NAME 'account_name'
);"""
    )
