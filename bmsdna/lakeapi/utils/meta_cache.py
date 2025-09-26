import duckdb
from bmsdna.lakeapi.context.source_uri import SourceUri
from deltalake2db import (
    get_deltalake_meta as _get_deltalake_meta,
    PolarsMetaEngine,
    DuckDBMetaEngine,
    DeltaTableMeta,
)


_cached_meta: dict[SourceUri, DeltaTableMeta] = {}


def get_deltalake_meta(uri: SourceUri):
    ab_uri, ab_opts = uri.get_uri_options(flavor="object_store")
    meta_engine = (
        PolarsMetaEngine(ab_opts)
        if not uri.is_local()
        else DuckDBMetaEngine(duckdb.default_connection())
    )
    if mt := _cached_meta.get(uri):
        mt.update_incremental(meta_engine)
        return mt
    mt = _get_deltalake_meta(meta_engine, ab_uri)
    _cached_meta[uri] = mt
    return mt
