import polars as pl
from deltalake import DeltaTable
import pyarrow.parquet as pq
import os


def scan_delta_union(delta_table: DeltaTable) -> pl.LazyFrame:
    colmaps: dict[str, str] = dict()
    for field in delta_table.schema().fields:
        colmaps[field.name] = field.metadata.get("delta.columnMapping.physicalName", field.name)
    all_ds = []
    for ac in delta_table.get_add_actions(flatten=True).to_pylist():
        fullpath = os.path.join(delta_table.table_uri, ac["path"])
        base_ds = pl.scan_parquet(fullpath, storage_options=delta_table._storage_options)
        selects = []
        part_cols = []
        for ln, pn in colmaps.items():
            if "partition_values" in ac and pn in ac["partition_values"]:
                part_vl = ac["partition_values"][pn]
                part_cols.append(pl.lit(part_vl).alias(ln))
            elif "partition." + pn in ac:
                part_vl = ac["partition." + pn]
                part_cols.append(pl.lit(part_vl).alias(ln))
            elif "partition_values" in ac and ln in ac["partition_values"]:
                # as of delta 0.14
                part_vl = ac["partition_values"][ln]
                part_cols.append(pl.lit(part_vl).alias(ln))
            elif "partition." + ln in ac:
                # as of delta 0.14
                part_vl = ac["partition." + ln]
                part_cols.append(pl.lit(part_vl).alias(ln))
            else:
                selects.append(pl.col(pn).alias(ln))
        ds = base_ds.select(*selects).with_columns(*part_cols)
        all_ds.append(ds)
    return pl.concat(all_ds)
