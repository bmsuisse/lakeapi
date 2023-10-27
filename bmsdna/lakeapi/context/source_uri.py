import fsspec
import adlfs
import os
import urllib.parse


class SourceUri:
    uri: str
    account: str | None

    def __init__(self, uri: str, account: str | None, accounts: dict, data_path: str | None):
        self.uri = uri
        self.account = account
        self.accounts = accounts
        self.data_path = data_path
        self.real_uri = (
            uri if "://" in uri or account is not None or data_path is None else os.path.join(data_path, uri)
        )

    def is_azure(self):
        return self.uri.startswith("abfs://") or self.uri.startswith("abfss://")

    def get_fs_spec(self) -> tuple[fsspec.AbstractFileSystem, str]:
        if self.account is None:
            return fsspec.filesystem("file"), self.real_uri
        opts = self.accounts.get(self.account, {})
        if self.is_azure():
            return adlfs.AzureBlobFileSystem(**opts), self.real_uri
        else:
            raise ValueError("Not supported FS")

    def get_uri_options(self, azure_protocol="original") -> tuple[str, dict | None]:
        if self.is_azure() and azure_protocol != "original":
            pr = urllib.parse.urlparse(self.uri)
            return (
                f"{azure_protocol}://{pr.netloc}{pr.path}",
                self.accounts.get(self.account) if self.account else None,
            )
        return self.real_uri, self.accounts.get(self.account) if self.account else None

    def exists(self) -> bool:
        if self.account is None:
            return os.path.exists(self.real_uri)
        fs, fs_path = self.get_fs_spec()
        return fs.exists(fs_path)

    def __str__(self):
        return self.real_uri
