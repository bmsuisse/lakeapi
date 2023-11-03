from typing import Callable, Literal
import fsspec
import adlfs
import os
import urllib.parse
import expandvars
from datetime import datetime, timezone, timedelta

_token_state = dict()


def _get_default_token() -> str:
    global _token_state
    token_expiry: datetime | None = _token_state.get("token_expiry", None)
    if not token_expiry or (token_expiry - datetime.now(tz=timezone.utc)) < timedelta(minutes=2):
        from azure.identity import DefaultAzureCredential

        tk = DefaultAzureCredential().get_token("https://storage.azure.com/.default")
        _token_state["token_dt"] = tk
        _token_state["token_expiry"] = datetime.fromtimestamp(tk.expires_on, tz=timezone.utc)
        _token_state["token"] = tk.token
    return _token_state["token"]


def _convert_options(
    options: dict | None,
    flavor: Literal["fsspec", "object_store"],
    token_retrieval_func: Callable[[], str] | None = None,
):
    if options is None:
        return None
    use_emulator: bool = options.get("use_emulator", "0").lower() in ["1", "true"]
    if flavor == "fsspec" and "connection_string" not in options and use_emulator:
        constr = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;"
        return {"connection_string": constr}
    elif (
        flavor == "fsspec" and "anon" not in options and "account_name" in options
    ):  # anon is true by default in fsspec which makes no sense mostly
        return {"anon": "False"} | options

    new_opts = options.copy()
    anon_value = "false"
    if flavor == "object_store" and "anon" in options:
        anon_value = new_opts.pop("anon")
    if flavor == "object_store" and not use_emulator and anon_value.lower() in ["false", "0"]:
        new_opts["token"] = (token_retrieval_func or _get_default_token)()
    return new_opts


class SourceUri:
    uri: str
    account: str | None

    def __init__(
        self,
        uri: str,
        account: str | None,
        accounts: dict,
        data_path: str | None,
        token_retrieval_func: Callable[[], str] | None = None,
    ):
        self.uri = uri
        self.account = account
        self.accounts = accounts or {}
        self.data_path = data_path
        self.token_retrieval_func = token_retrieval_func
        self.real_uri = (
            uri if "://" in uri or account is not None or data_path is None else os.path.join(data_path, uri)
        )

    def is_azure(self):
        return self.uri.startswith("abfs://") or self.uri.startswith("abfss://")

    def get_fs_spec(self) -> tuple[fsspec.AbstractFileSystem, str]:
        if self.account is None:
            return fsspec.filesystem("file"), self.real_uri
        opts = _convert_options(
            self.accounts.get(self.account, {}), "fsspec", token_retrieval_func=self.token_retrieval_func
        )
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
                _convert_options(
                    self.accounts.get(self.account) if self.account else None,
                    flavor,
                    token_retrieval_func=self.token_retrieval_func,
                ),
            )
        return self.real_uri, _convert_options(
            self.accounts.get(self.account) if self.account else None,
            flavor,
            token_retrieval_func=self.token_retrieval_func,
        )

    def exists(self) -> bool:
        if self.account is None:
            return os.path.exists(self.real_uri)
        fs, fs_path = self.get_fs_spec()
        return fs.exists(fs_path)

    def __str__(self):
        return self.real_uri
