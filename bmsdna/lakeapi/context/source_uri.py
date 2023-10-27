import fsspec
import adlfs


class SourceUri:
    uri: str
    account: str | None

    def __init__(self, uri: str, account: str | None, accounts: dict, data_path: str):
        self.uri = uri
        self.account = account
        self.accounts = accounts
        self.data_path = data_path
        self.real_uri = uri if "://" in uri or account is not None else os.path.join(data_path, uri)

    def get_fs_spec(self) -> tuple[fsspec.AbstractFileSystem, str]:
        if self.account is None:
            return fsspec.implementations.local.LocalFileSystem(), self.real_uri
        opts = accounts.get(self.account)
        return adlfs.AzureBlobFileSystem(**opts), self.real_uri

    def get_uri_options(self) -> tuple[str, dict]:
        return self.real_uri, accounts.get(self.account) if self.account else None
