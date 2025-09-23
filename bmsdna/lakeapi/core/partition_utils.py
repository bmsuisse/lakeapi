from typing import List, TYPE_CHECKING
from bmsdna.lakeapi.context.source_uri import SourceUri
from deltalake2db.delta_meta_retrieval import (
    get_meta,
    PolarsEngine,
)
import logging

if TYPE_CHECKING:
    from bmsdna.lakeapi.core.types import Param
    from bmsdna.lakeapi.core.config import BasicConfig

logger = logging.getLogger(__name__)


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

        try:
            dt_uri, dt_opts = uri.get_uri_options(flavor="object_store")
            meta = get_meta(PolarsEngine(dt_opts), dt_uri)  # to ensure connectivity
            assert meta.last_metadata is not None
            part_cols = meta.last_metadata.get("partitionColumns", [])
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
        except Exception as e:
            logger.warning(f"Error retrieving delta metadata: {e}")
            return paramslist  # this is not critical here

    return paramslist
