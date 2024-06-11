from typing import List, TYPE_CHECKING
from bmsdna.lakeapi.context.source_uri import SourceUri

if TYPE_CHECKING:
    from bmsdna.lakeapi.core.types import Param
    from bmsdna.lakeapi.core.config import BasicConfig


def _with_implicit_parameters(
    paramslist: "List[Param]",
    file_type: str,
    uri: SourceUri,
    basic_config: "BasicConfig",
):
    if file_type == "delta":
        fs, fs_spec = uri.get_fs_spec()
        if not fs.exists(fs_spec + "/_delta_log"):
            return paramslist
        from deltalake import DeltaTable
        from deltalake.exceptions import DeltaError

        try:
            dt_uri, dt_opts = uri.get_uri_options(flavor="object_store")
            part_cols = (
                DeltaTable(dt_uri, storage_options=dt_opts).metadata().partition_columns
            )
            if part_cols and len(part_cols) > 0:
                all_names = [(p.colname or p.name).lower() for p in paramslist]
                new_params = list(paramslist)
                for pc in part_cols:
                    if (
                        pc.lower() not in all_names
                        and not basic_config.should_hide_col_name(pc)
                    ):
                        from bmsdna.lakeapi.core.types import Param

                        new_params.append(Param(pc, operators=["="], colname=pc))
                return new_params
        except FileNotFoundError:
            return paramslist  # this is not critical here
        except DeltaError:
            return paramslist  # this is not critical here

    return paramslist
