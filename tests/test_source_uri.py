from bmsdna.lakeapi.context.source_uri import _convert_options
from azure.core.credentials import TokenCredential, AccessToken
from datetime import datetime


def fake_token(**kwargs):
    if len(kwargs) > 0:
        kwargs_str = ",".join([f"{k}={v}" for k, v in kwargs.items()])
        return "fake_token_" + kwargs_str
    return "fake_token"


class FakeCredential(TokenCredential):
    def __init__(self, _type):
        self._type = _type

    def get_token(self, *args, **kwargs):
        exp = int(datetime.now().timestamp()) + 72000
        if len(kwargs) > 0:
            kwargs_str = ",".join([f"{k}={v}" for k, v in kwargs.items()])
            return AccessToken(token="fake_token_" + kwargs_str, expires_on=exp)
        return AccessToken(token="fake_token", expires_on=exp)


def test_fsspec():
    o_non_anon = {"account_name": "blubb"}
    full_url = "https://blubb.blob.core.windows.net/xyz/abc"
    _, o = _convert_options(
        full_url,
        o_non_anon,
        flavor="fsspec",
        token_retrieval_func=lambda x: FakeCredential(x),
    )
    assert o == {"account_name": "blubb", "anon": False}
    assert "anon" not in o_non_anon

    o_anon = {"account_name": "blubb", "anon": True}
    _, o = _convert_options(
        full_url, o_anon, flavor="fsspec", token_retrieval_func=FakeCredential
    )
    assert o == {"account_name": "blubb", "anon": True}


def test_account_key():
    o_non_anon = {"account_name": "blubb", "account_key": "nix"}
    full_url = "https://blubb.blob.core.windows.net/xyz/abc"
    _, o = _convert_options(
        full_url, o_non_anon, flavor="object_store", token_retrieval_func=FakeCredential
    )
    assert o == {"account_name": "blubb", "account_key": "nix"}

    _, o2 = _convert_options(
        full_url, o_non_anon, flavor="fsspec", token_retrieval_func=FakeCredential
    )
    assert o2 == {"account_name": "blubb", "account_key": "nix"}


if __name__ == "__main__":
    test_fsspec()
    test_account_key()
