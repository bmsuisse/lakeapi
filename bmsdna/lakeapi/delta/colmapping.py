from deltalake import DeltaTable
from deltalake.table import MAX_SUPPORTED_READER_VERSION
from deltalake.fs import DeltaStorageHandler
import pyarrow.parquet as pq
import pyarrow
import pyarrow.fs as pa_fs
import os
import logging

logger = logging.getLogger(__name__)

from pyarrow.dataset import (
    Expression,
    FileSystemDataset,
    ParquetFileFormat,
    ParquetReadOptions,
)
from typing import List, Tuple, Union, Optional, Any


def _apply_col_mapping(dt: DeltaTable, logical_schema: pyarrow.Schema):
    meta = dt.metadata()
    schema = dt.schema()
    physical_fields = []
    for field in schema.fields:
        phys_name = field.metadata.get("delta.columnMapping.physicalName", field.name)
        existing_def = logical_schema.field(field.name)
        physical_fields.append(existing_def.with_name(phys_name))
    return pyarrow.schema(physical_fields)


def only_fixed_supported(dt: DeltaTable):
    return dt.protocol().min_reader_version == 2 and MAX_SUPPORTED_READER_VERSION == 1


def _quote(s: str):
    assert '"' not in s
    return '"' + s + '"'


def _literal(vl: Any):
    if isinstance(vl, str):
        return "'" + vl.replace("'", "''") + "'"
    if isinstance(vl, int) or isinstance(vl, float):
        return str(vl)
    return _literal(str(vl))


def get_sql_for_delta(dt: DeltaTable):
    dt.update_incremental()
    sql = "WITH files as ("
    file_selects = []

    colmaps = []
    for field in dt.schema().fields:
        colmaps.append((field.name, field.metadata.get("delta.columnMapping.physicalName", field.name)))
    phys_names = [cm[1] for cm in colmaps]
    for ac in dt.get_add_actions(flatten=True).to_pylist():
        fullpath = os.path.join(dt.table_uri, ac["path"])
        sc = pq.read_schema(fullpath)

        cols = sc.names
        cols_sqls = []
        for c in phys_names:
            if "partition_values" in ac and c in ac["partition_values"]:
                cols_sqls.append(_literal(ac["partition_values"][c]) + " AS " + _quote(c))
            elif "partition." + c in ac:
                cols_sqls.append(_literal(ac["partition." + c]) + " AS " + _quote(c))
            elif c in cols:
                cols_sqls.append(_quote(c))
            else:
                cols_sqls.append("NULL AS " + _quote(c))

        select = "SELECT " + ", ".join(cols_sqls) + " FROM read_parquet('" + fullpath + "')"
        file_selects.append(select)
    sql += "\r\n UNION ALL ".join(file_selects) + "\r\n)"
    colmapssql = ", ".join(('"' + phys_name + '" AS "' + name + '"' for name, phys_name in colmaps))
    return f"""{sql} 
     SELECT {colmapssql} from files"""


def get_pyarrow_dataset(
    dt: DeltaTable,
    partitions: Optional[List[Tuple[str, str, Any]]] = None,
    filesystem: Optional[Union[str, pa_fs.FileSystem]] = None,
    parquet_read_options: Optional[ParquetReadOptions] = None,
):
    dt.update_incremental()
    if not (dt.protocol().min_reader_version == 2 and MAX_SUPPORTED_READER_VERSION == 1):
        sc = dt.schema().to_pyarrow()
        return dt.to_pyarrow_dataset(), sc

    if not filesystem:
        filesystem = pa_fs.PyFileSystem(
            DeltaStorageHandler(
                dt._table.table_uri(),
                dt._storage_options,
            )
        )

    format = ParquetFileFormat(read_options=parquet_read_options)
    logical_schema = dt.schema().to_pyarrow()
    physical_schema = _apply_col_mapping(dt, logical_schema)
    fragments = [
        format.make_fragment(
            file,
            filesystem=filesystem,
            partition_expression=part_expression,
        )
        for file, part_expression in dt._table.dataset_partitions(physical_schema, partitions)
    ]

    schema = physical_schema

    dictionary_columns = format.read_options.dictionary_columns or set()
    if dictionary_columns:
        for index, field in enumerate(schema):
            if field.name in dictionary_columns:
                dict_field = field.with_type(pyarrow.dictionary(pyarrow.int32(), field.type))
                schema = schema.set(index, dict_field)

    fs = FileSystemDataset(fragments, schema, format, filesystem)
    return fs, logical_schema


def get_pyarrow_table(
    dt: DeltaTable,
    partitions: Optional[List[Tuple[str, str, Any]]] = None,
    filesystem: Optional[Union[str, pa_fs.FileSystem]] = None,
    parquet_read_options: Optional[ParquetReadOptions] = None,
):
    ds, real_schema = get_pyarrow_dataset(
        dt, partitions=partitions, filesystem=filesystem, parquet_read_options=parquet_read_options
    )
    t = ds.to_table().rename_columns(real_schema.names)
    return t


if __name__ == "__main__":
    d = DeltaTable("tests/data/delta/table_w_col_map")

    ds, real_schema = get_pyarrow_dataset(d)
    t = ds.to_table().rename_columns(real_schema.names)

    print(repr(t))

    lns = get_sql_for_delta(d).split("\n")
    for ln in lns:
        print(ln)
