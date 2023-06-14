import polars as pl
from deltalake import write_deltalake, DeltaTable
import pyarrow as pa
from polars import LazyFrame, DataFrame
from typing import cast, Any, Optional
from functools import partial
import pickle


def _scan_pyarrow_dataset_impl(
    ds: pa.dataset.Dataset,
    with_columns: Optional[list[str]],
    predicate: Optional[str],
    n_rows: Optional[int],
) -> DataFrame:
    _filter = None
    if predicate:
        # imports are used by inline python evaluated by `eval`
        from polars.datatypes import Date, Datetime, Duration  # noqa: F401
        from polars.utils.convert import (
            _to_python_datetime,  # noqa: F401
            _to_python_time,  # noqa: F401
            _to_python_timedelta,  # noqa: F401
        )

        _filter = eval(predicate)
    ds = ds.scanner(columns=with_columns, filter=_filter)
    if n_rows:
        return cast(DataFrame, pl.from_arrow(ds.head(n_rows)).head(n_rows))

    return cast(DataFrame, pl.from_arrow(ds.to_table()))


def _scan_pyarrow_dataset(ds: pa.dataset.Dataset, allow_pyarrow_filter: bool = True) -> LazyFrame:
    func = partial(_scan_pyarrow_dataset_impl, ds)
    func_serialized = pickle.dumps(func)
    return LazyFrame._scan_python_function(ds.schema, func_serialized, allow_pyarrow_filter)


def scan_delta2(
    table_uri: str,
    version: Optional[int] = None,
    storage_options: Optional[dict[str, Any]] = None,
    delta_table_options: Optional[dict[str, Any]] = None,
    pyarrow_options: Optional[dict[str, Any]] = None,
) -> LazyFrame:
    dt = DeltaTable(
        table_uri,
        version=version,
        storage_options=storage_options,
        **delta_table_options or {},
    )

    pa_ds = dt.to_pyarrow_dataset(**pyarrow_options or {})

    return _scan_pyarrow_dataset(pa_ds)


pl.scan_delta2 = scan_delta2  # type: ignore
