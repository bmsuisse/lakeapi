from typing import Literal
import fsspec
import adlfs
import os
import urllib.parse
import expandvars


def _convert_options(options: dict | None, flavor: Literal["fsspec", "object_store"]):
    if options is None:
        return None
    if flavor == "fsspec" and "connection_string" not in options and options.get("use_emulator", "0") in ["1", "true"]:
        constr = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;"
        return {"connection_string": constr}

    return options


class SourceUri:
    uri: str
    account: str | None

    def __init__(self, uri: str, account: str | None, accounts: dict, data_path: str | None):
        self.uri = uri
        self.account = account
        self.accounts = accounts or {}
        self.data_path = data_path
        self.real_uri = (
            uri if "://" in uri or account is not None or data_path is None else os.path.join(data_path, uri)
        )

    def is_azure(self):
        return self.uri.startswith("abfs://") or self.uri.startswith("abfss://")

    def get_fs_spec(self) -> tuple[fsspec.AbstractFileSystem, str]:
        if self.account is None:
            return fsspec.filesystem("file"), self.real_uri
        opts = _convert_options(self.accounts.get(self.account, {}), "fsspec")
        assert opts is not None
        if self.is_azure():
            return adlfs.AzureBlobFileSystem(**opts), self.real_uri  # type: ignore
        else:
            raise ValueError("Not supported FS")

    def get_uri_options(
        self, *, flavor: Literal["fsspec", "object_store"], azure_protocol="original"
    ) -> tuple[str, dict | None]:
        if self.is_azure() and azure_protocol != "original":
            pr = urllib.parse.urlparse(self.uri)
            return (
                f"{azure_protocol}://{pr.netloc}{pr.path}",
                _convert_options(self.accounts.get(self.account) if self.account else None, flavor),
            )
        return self.real_uri, _convert_options(self.accounts.get(self.account) if self.account else None, flavor)

    def exists(self) -> bool:
        if self.account is None:
            return os.path.exists(self.real_uri)
        fs, fs_path = self.get_fs_spec()
        return fs.exists(fs_path)

    def __str__(self):
        return self.real_uri
