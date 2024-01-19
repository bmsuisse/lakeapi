from bmsdna.lakeapi.core.datasource import Datasource
from bmsdna.lakeapi.core.config import BasicConfig


def get_schema_cached(cfg: BasicConfig, datasource: Datasource, key: str):
    if cfg.schema_cache_ttl is not None:
        import os
        import time
        import pyarrow.parquet as pq

        schema_cache_file = os.path.join(
            cfg.temp_folder_path,
            key + ".parquet",
        )
        if (
            not os.path.exists(schema_cache_file)
            or (time.time() - os.path.getmtime(schema_cache_file)) > cfg.schema_cache_ttl
        ):
            if not datasource.file_exists():
                schema = None
            else:
                schema = datasource.get_schema()
                pq.write_metadata(schema, schema_cache_file)
        else:
            schema = pq.read_schema(schema_cache_file)
        return schema
    else:
        if not datasource.file_exists():
            return None
        return datasource.get_schema()
