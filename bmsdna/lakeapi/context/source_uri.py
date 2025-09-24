from pathlib import Path
from typing import Callable, Literal, TYPE_CHECKING
import fsspec
import adlfs
import os
import urllib.parse

if TYPE_CHECKING:
    from azure.core.credentials import TokenCredential


def _convert_options(
    uri: str,
    options: dict | None,
    flavor: Literal["fsspec", "object_store", "original"],
    token_retrieval_func: "Callable[[str], TokenCredential] | None" = None,
):
    if options is None:
        return uri, None
    if flavor == "fsspec":
        from deltalake2db.azure_helper import get_storage_options_fsspec

        return uri, get_storage_options_fsspec(options)
    elif flavor == "original":
        return uri, options
    else:
        from deltalake2db.azure_helper import get_storage_options_object_store

        nr, no = get_storage_options_object_store(uri, options, token_retrieval_func)
        assert isinstance(nr, str)
        return nr, no


local_versions = dict()


class SourceUri:
    uri: str
    account: str | None

    def __init__(
        self,
        uri: str,
        account: str | None,
        accounts: dict,
        data_path: str | None,
        token_retrieval_func: "Callable[[SourceUri, str], TokenCredential] | None" = None,
    ):
        self.uri = uri
        self.account = account
        self.accounts = accounts or {}
        self.data_path = data_path
        self.token_retrieval_func = token_retrieval_func
        self.retrieve_token = (
            (lambda v: token_retrieval_func(self, v)) if token_retrieval_func else None
        )
        self.real_uri = (
            uri
            if "://" in uri or account is not None or data_path is None
            else os.path.join(data_path, uri)
        )

    def is_azure(self):
        return (
            self.uri.startswith("azure://")  # duckdb
            or self.uri.startswith("az://")  # duckdb
            or self.uri.startswith("abfs://")  # fsspec
            or self.uri.startswith("abfss://")  # fsspec
        )

    def get_fs_spec(self) -> tuple[fsspec.AbstractFileSystem, str]:
        if self.account is None:
            return fsspec.filesystem("file"), self.real_uri
        real_uri = self.real_uri
        real_uri, opts = _convert_options(
            real_uri,
            self.accounts.get(self.account, {}),
            "fsspec",
            token_retrieval_func=self.retrieve_token,
        )
        assert opts is not None
        if self.is_azure():
            return adlfs.AzureBlobFileSystem(**opts), real_uri  # type: ignore
        else:
            pr = urllib.parse.urlparse(self.uri)
            return fsspec.filesystem(pr, **opts), real_uri

    def get_uri_options(
        self, *, flavor: Literal["fsspec", "object_store", "original"]
    ) -> tuple[str, dict | None]:
        return _convert_options(
            self.real_uri,
            self.accounts.get(self.account) if self.account else None,
            flavor,
            token_retrieval_func=self.retrieve_token,
        )

    def exists(self) -> bool:
        if self.account is None:
            return os.path.exists(self.real_uri)
        fs, fs_path = self.get_fs_spec()
        return fs.exists(fs_path)

    def copy_to_local(self, local_path: str, delta_table: bool):
        if self.account is None:
            raise ValueError("Cannot copy local files")
        if not delta_table:
            fs, fs_path = self.get_fs_spec()
            stat = fs.stat(fs_path)
            lm = stat.get("last_modified", stat.get("Last-Modified"))
            if not lm:
                raise ValueError("Cannot determine last modified date", stat.keys())

            if local_versions.get(self.uri) != lm:
                fs.get_file(fs_path, local_path)
                local_versions[self.uri] = lm
            return SourceUri(
                uri=local_path,
                data_path=None,
                account=None,
                accounts=self.accounts,
                token_retrieval_func=self.token_retrieval_func,
            )
        if delta_table:
            from bmsdna.lakeapi.utils.meta_cache import get_deltalake_meta

            meta = get_deltalake_meta(self)
            vnr = meta.version
            if local_versions.get(self.uri) == vnr:
                return SourceUri(
                    uri=local_path,
                    data_path=None,
                    account=None,
                    accounts=self.accounts,
                    token_retrieval_func=self.token_retrieval_func,
                )

            os.makedirs(local_path + "/_delta_log", exist_ok=True)
            fs, fs_path = self.get_fs_spec()
            if fs.exists(fs_path.removesuffix("/") + "/_delta_log"):
                fs.get(
                    fs_path + "/_delta_log/",
                    local_path + "/_delta_log/",
                    recursive=True,
                )
                for path in meta.add_actions.keys():
                    if not os.path.exists(local_path + "/" + path):
                        Path(local_path + "/" + path).parent.mkdir(
                            parents=True, exist_ok=True
                        )
                        fs.get_file(fs_path + "/" + path, local_path + "/" + path)
            else:
                fs.get(fs_path, local_path, recursive=True)
            local_versions[self.uri] = vnr
            return SourceUri(
                uri=local_path,
                data_path=None,
                account=None,
                accounts=self.accounts,
                token_retrieval_func=self.token_retrieval_func,
            )

    def __str__(self):
        return self.real_uri

    def __hash__(self) -> int:
        return hash(self.real_uri)
