import os
import duckdb
from bmsdna.lakeapi.context.source_uri import SourceUri
from deltalake2db import (
    get_deltalake_meta as _get_deltalake_meta,
    PolarsMetaEngine,
    DuckDBMetaEngine,
    DeltaTableMeta,
)
from typing import Optional

from bmsdna.lakeapi.core.config import BasicConfig


_cached_meta: dict[SourceUri, DeltaTableMeta] = {}

_global_duck_con: Optional[duckdb.DuckDBPyConnection] = None
_global_duck_meta_engine: Optional[DuckDBMetaEngine] = None

DELTA_META_ENGINE = os.getenv("DELTA_META_ENGINE")


def get_deltalake_meta(use_polars: bool, uri: SourceUri):
    ab_uri, ab_opts = uri.get_uri_options(flavor="original")

    def duck_meta_engine():
        global _global_duck_con
        global _global_duck_meta_engine
        if _global_duck_con is None:
            _global_duck_con = duckdb.connect(":memory:")
            _global_duck_con.execute("set azure_transport_option_type='curl'")
            _global_duck_meta_engine = DuckDBMetaEngine(
                _global_duck_con,
                use_fsspec=os.getenv("DUCKDB_DELTA_USE_FSSPEC", "0") == "1",
            )
        assert _global_duck_meta_engine is not None
        return _global_duck_meta_engine

    if (
        use_polars and not DELTA_META_ENGINE == "duckdb"
    ) or DELTA_META_ENGINE == "polars":
        meta_engine = PolarsMetaEngine()
    else:
        meta_engine = duck_meta_engine()

    if mt := _cached_meta.get(uri):
        try:
            mt.update_incremental(meta_engine)
            return mt
        except Exception:
            if isinstance(meta_engine, DuckDBMetaEngine):
                mt.update_incremental(PolarsMetaEngine())
            else:
                mt.update_incremental(duck_meta_engine())
    else:
        mt = _get_deltalake_meta(meta_engine, ab_uri, ab_opts)
    _cached_meta[uri] = mt
    return mt


def clear_deltalake_meta_cache():
    global _cached_meta
    global _global_duck_meta_engine
    global _global_duck_con
    _cached_meta.clear()
    if _global_duck_meta_engine is not None:
        _global_duck_meta_engine = None
    if _global_duck_con is not None:
        _global_duck_con.close()
        _global_duck_con = None
