from bmsdna.lakeapi.context.source_uri import SourceUri
from deltalake2db.delta_meta_retrieval import (
    MetaState,
    get_meta,
    PolarsEngine,
)


_cached_meta: dict[SourceUri, MetaState] = {}


def get_deltalake_meta(uri: SourceUri):
    ab_uri, ab_opts = uri.get_uri_options(flavor="object_store")
    meta_engine = PolarsEngine(ab_opts)
    if mt := _cached_meta.get(uri):
        mt.update_incremental(meta_engine)
        return mt
    mt = get_meta(meta_engine, ab_uri)
    _cached_meta[uri] = mt
    return mt
