from typing import List, TYPE_CHECKING
import os
from bmsdna.lakeapi.context.source_uri import SourceUri

if TYPE_CHECKING:
    from bmsdna.lakeapi.core.config import BasicConfig
    from bmsdna.lakeapi.core.types import Param


def should_hide_colname(name: str):
    return name.startswith("_") or "_md5_prefix_" in name or "_xxhash64_prefix_" in name or "_md5_mod_" in name


def _with_implicit_parameters(
    paramslist: "List[Param]",
    file_type: str,
    uri: SourceUri,
):
    if file_type == "delta":
        fs, fs_spec = uri.get_fs_spec()
        if not fs.exists(fs_spec + "/_delta_log"):
            return paramslist
        from deltalake import DeltaTable
        from deltalake.exceptions import DeltaError

        try:
            dt_uri, dt_opts = uri.get_uri_options(flavor="object_store")
            part_cols = DeltaTable(dt_uri, storage_options=dt_opts).metadata().partition_columns
            if part_cols and len(part_cols) > 0:
                all_names = [(p.colname or p.name).lower() for p in paramslist]
                new_params = list(paramslist)
                for pc in part_cols:
                    if pc.lower() not in all_names and not should_hide_colname(pc):
                        from bmsdna.lakeapi.core.types import Param

                        new_params.append(Param(pc, operators=["="], colname=pc))
                return new_params
        except FileNotFoundError as err:
            return paramslist  # this is not critical here
        except DeltaError as err:
            return paramslist  # this is not critical here

    return paramslist
