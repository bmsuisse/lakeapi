import os
import duckdb
from bmsdna.lakeapi.context.source_uri import SourceUri
from deltalake2db import (
    get_deltalake_meta as _get_deltalake_meta,
    PolarsMetaEngine,
    DuckDBMetaEngine,
    DeltaTableMeta,
    duckdb_apply_storage_options,
)
from deltalake2db.duckdb import apply_storage_options_fsspec
from deltalake2db.azure_helper import get_account_name_from_path
from typing import Optional


_cached_meta: dict[SourceUri, DeltaTableMeta] = {}

_global_duck_con: Optional[duckdb.DuckDBPyConnection] = None


def get_deltalake_meta(use_polars: bool, uri: SourceUri):
    global _global_duck_con
    if use_polars:
        ab_uri, ab_opts = uri.get_uri_options(flavor="object_store")

        meta_engine = PolarsMetaEngine(ab_opts)
    else:
        if _global_duck_con is None:
            _global_duck_con = duckdb.connect(":memory:")
        ab_uri, ab_opts = uri.get_uri_options(flavor="original")

        if not uri.is_local():
            if os.getenv("DUCKDB_DELTA_USE_FSSPEC", "0") == "1" and "://" in ab_uri:
                account_name_path = get_account_name_from_path(ab_uri)
                fake_protocol = apply_storage_options_fsspec(
                    _global_duck_con,
                    ab_uri,
                    ab_opts or {},
                    account_name_path=account_name_path,
                )
                ab_uri = fake_protocol + "://" + ab_uri.split("://")[1]
            else:
                duckdb_apply_storage_options(
                    _global_duck_con,
                    ab_uri,
                    ab_opts,
                    use_fsspec=os.getenv("DUCKDB_DELTA_USE_FSSPEC", "0") == "1",
                )
        meta_engine = DuckDBMetaEngine(_global_duck_con)

    if mt := _cached_meta.get(uri):
        mt.update_incremental(meta_engine)
        return mt
    mt = _get_deltalake_meta(meta_engine, ab_uri)
    _cached_meta[uri] = mt
    return mt
