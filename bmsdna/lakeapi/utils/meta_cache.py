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
_global_duck_meta_engine: Optional[DuckDBMetaEngine] = None


def get_deltalake_meta(use_polars: bool, uri: SourceUri):
    global _global_duck_con
    global _global_duck_meta_engine
    if use_polars:
        ab_uri, ab_opts = uri.get_uri_options(flavor="object_store")

        meta_engine = PolarsMetaEngine()
    else:
        if _global_duck_con is None:
            _global_duck_con = duckdb.connect(":memory:")
            _global_duck_meta_engine = DuckDBMetaEngine(
                _global_duck_con,
                use_fsspec=os.getenv("DUCKDB_DELTA_USE_FSSPEC", "0") == "1",
            )
        ab_uri, ab_opts = uri.get_uri_options(flavor="original")
        assert _global_duck_meta_engine is not None
        meta_engine = _global_duck_meta_engine

    if mt := _cached_meta.get(uri):
        mt.update_incremental(meta_engine)
        return mt
    mt = _get_deltalake_meta(meta_engine, ab_uri, ab_opts)
    _cached_meta[uri] = mt
    return mt
