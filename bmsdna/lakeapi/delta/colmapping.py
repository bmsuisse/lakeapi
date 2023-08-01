from deltalake import DeltaTable
from deltalake.table import MAX_SUPPORTED_READER_VERSION
from deltalake.fs import DeltaStorageHandler
import pyarrow
import pyarrow.fs as pa_fs
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


def get_pyarrow_dataset(
    dt: DeltaTable,
    partitions: Optional[List[Tuple[str, str, Any]]] = None,
    filesystem: Optional[Union[str, pa_fs.FileSystem]] = None,
    parquet_read_options: Optional[ParquetReadOptions] = None,
):
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
