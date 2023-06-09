from typing import List, TYPE_CHECKING
import os

if TYPE_CHECKING:
    from bmsdna.lakeapi.core.config import BasicConfig
    from bmsdna.lakeapi.core.types import Param


def should_hide_colname(name: str):
    return name.startswith("_") or "_md5_prefix_" in name or "_xxhash64_prefix_" in name or "_md5_mod_" in name


def _with_implicit_parameters(paramslist: "List[Param]", file_type: str, basic_config: "BasicConfig", uri: str):
    if file_type == "delta":
        delta_uri = os.path.join(
            basic_config.data_path,
            uri,
        )
        if not os.path.exists(os.path.join(delta_uri, "_delta_log")):
            return paramslist
        from deltalake import DeltaTable
        from deltalake.exceptions import DeltaError

        try:
            part_cols = DeltaTable(delta_uri).metadata().partition_columns
            if part_cols and len(part_cols) > 0:
                all_names = [(p.colname or p.name).lower() for p in paramslist]
                new_params = list(paramslist)
                for pc in part_cols:
                    if pc.lower() not in all_names and not should_hide_colname(pc):
                        from bmsdna.lakeapi.core.types import Param

                        new_params.append(Param(pc, operators=["="], colname=pc))
                return new_params
        except DeltaError as err:
            return paramslist  # this is not critical here

    return paramslist
