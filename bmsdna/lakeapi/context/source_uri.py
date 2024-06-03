from typing import Callable, Literal, TYPE_CHECKING
import fsspec
import adlfs
import os
import urllib.parse

if TYPE_CHECKING:
    from azure.identity import DefaultAzureCredential, ManagedIdentityCredential
_credential: "DefaultAzureCredential | None | ManagedIdentityCredential" = None

default_azure_args = [
    "authority",
    "exclude_workload_identity_credential",
    "exclude_developer_cli_credential",
    "exclude_cli_credential",
    "exclude_environment_credential",
    "exclude_managed_identity_credential",
    "exclude_powershell_credential",
    "exclude_visual_studio_code_credential",
    "exclude_shared_token_cache_credential",
    "exclude_interactive_browser_credential",
    "interactive_browser_tenant_id",
    "managed_identity_client_id",
    "workload_identity_client_id",
    "workload_identity_tenant_id",
    "interactive_browser_client_id",
    "shared_cache_username",
    "shared_cache_tenant_id",
    "visual_studio_code_tenant_id",
    "process_timeout",
]


def _get_default_token(**kwargs) -> str:
    global _credential
    if _credential is None:
        if os.getenv("LAKE_MANAGED_IDENTITY_ID") is not None:
            from azure.identity import ManagedIdentityCredential

            mid = os.environ["LAKE_MANAGED_IDENTITY_ID"]
            mid = None if mid == "default" else mid
            _credential = ManagedIdentityCredential(client_id=mid)
        else:
            from azure.identity import DefaultAzureCredential

            _credential = DefaultAzureCredential(**kwargs)
    return _credential.get_token("https://storage.azure.com/.default").token


def _convert_options(
    options: dict | None,
    flavor: Literal["fsspec", "object_store", "deltalake2db"],
    token_retrieval_func: Callable[[], str] | None = None,
):
    if options is None:
        return None
    use_emulator: bool = options.get("use_emulator", "0").lower() in ["1", "true"]
    if flavor == "fsspec" and "connection_string" not in options and use_emulator:
        constr = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;"
        return {"connection_string": constr}
    elif (
        flavor == "fsspec"
        and "account_key" not in options
        and "anon" not in options
        and "sas_token" not in options
        and "account_name" in options
        and "chain" not in options
    ):  # anon is true by default in fsspec which makes no sense mostly
        return {"anon": False} | options

    new_opts = options.copy()
    anon_value = False
    if flavor in ["deltalake2db", "object_store"] and "anon" in options:
        anon_value = new_opts.pop("anon")

    if (
        flavor in ["deltalake2db", "object_store"]
        and "account_key" not in options
        and "sas_token" not in options
        and "token" not in options
        and "chain" not in options
        and not use_emulator
        and not anon_value
    ):
        token_kwargs = {k: new_opts.pop(k) for k in default_azure_args if k in new_opts}
        new_opts["token"] = (token_retrieval_func or _get_default_token)(**token_kwargs)
    if "chain" in options and flavor != "deltalake2db":
        from deltalake2db.azure_helper import apply_azure_chain

        return apply_azure_chain(new_opts)
    return new_opts


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
        token_retrieval_func: "Callable[[SourceUri], str] | None" = None,
    ):
        self.uri = uri
        self.account = account
        self.accounts = accounts or {}
        self.data_path = data_path
        self.token_retrieval_func = token_retrieval_func
        self.retrieve_token = (
            (lambda: token_retrieval_func(self)) if token_retrieval_func else None
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
        opts = _convert_options(
            self.accounts.get(self.account, {}),
            "fsspec",
            token_retrieval_func=self.retrieve_token,
        )
        assert opts is not None
        if self.is_azure():
            return adlfs.AzureBlobFileSystem(**opts), self.real_uri  # type: ignore
        else:
            raise ValueError("Not supported FS")

    def get_uri_options(
        self,
        *,
        flavor: Literal["fsspec", "object_store", "deltalake2db"],
        azure_protocol="original",
    ) -> tuple[str, dict | None]:
        if self.is_azure() and azure_protocol != "original":
            pr = urllib.parse.urlparse(self.uri)
            return (
                f"{azure_protocol}://{pr.netloc}{pr.path}",
                _convert_options(
                    self.accounts.get(self.account) if self.account else None,
                    flavor,
                    token_retrieval_func=self.retrieve_token,
                ),
            )
        return self.real_uri, _convert_options(
            self.accounts.get(self.account) if self.account else None,
            flavor,
            token_retrieval_func=self.retrieve_token,
        )

    def exists(self) -> bool:
        if self.account is None:
            return os.path.exists(self.real_uri)
        fs, fs_path = self.get_fs_spec()
        return fs.exists(fs_path)

    def copy_to_local(self, local_path: str):
        if self.account is None:
            raise ValueError("Cannot copy local files")

        from deltalake import DeltaTable

        df_uri, df_opts = self.get_uri_options(flavor="object_store")
        dt = DeltaTable(df_uri, storage_options=df_opts)
        vnr = dt.version()
        if local_versions.get(self.uri) == vnr:
            return SourceUri(
                uri=local_path,
                data_path=None,
                account=None,
                accounts=self.accounts,
                token_retrieval_func=self.token_retrieval_func,
            )
        os.makedirs(local_path, exist_ok=True)
        fs, fs_path = self.get_fs_spec()
        fs.get(fs_path + "/", local_path, recursive=True)
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
