from bmsdna.lakeapi.context.source_uri import (
    _convert_options,
)


def fake_token(**kwargs):
    if len(kwargs) > 0:
        kwargs_str = ",".join([f"{k}={v}" for k, v in kwargs.items()])
        return "fake_token_" + kwargs_str
    return "fake_token"


def test_fsspec():
    o_non_anon = {"account_name": "blubb"}
    o = _convert_options(o_non_anon, flavor="fsspec", token_retrieval_func=fake_token)
    assert o == {"account_name": "blubb", "anon": False}
    assert "anon" not in o_non_anon

    o_anon = {"account_name": "blubb", "anon": True}
    o = _convert_options(o_anon, flavor="fsspec", token_retrieval_func=fake_token)
    assert o == {"account_name": "blubb", "anon": True}


def test_object_store():
    o_non_anon = {"account_name": "blubb"}
    o = _convert_options(
        o_non_anon, flavor="object_store", token_retrieval_func=fake_token
    )
    assert o == {"account_name": "blubb", "token": "fake_token"}
    assert "anon" not in o_non_anon

    o_anon = {"account_name": "blubb", "anon": True}
    o = _convert_options(o_anon, flavor="object_store", token_retrieval_func=fake_token)
    assert o == {"account_name": "blubb"}

    o_non_anon = {
        "account_name": "blubb",
        "exclude_interactive_browser_credential": False,
    }
    o = _convert_options(
        o_non_anon, flavor="object_store", token_retrieval_func=fake_token
    )
    assert o == {
        "account_name": "blubb",
        "token": "fake_token_exclude_interactive_browser_credential=False",
    }


def test_account_key():
    o_non_anon = {"account_name": "blubb", "account_key": "nix"}
    o = _convert_options(
        o_non_anon, flavor="object_store", token_retrieval_func=fake_token
    )
    assert o == {"account_name": "blubb", "account_key": "nix"}

    o2 = _convert_options(o_non_anon, flavor="fsspec", token_retrieval_func=fake_token)
    assert o2 == {"account_name": "blubb", "account_key": "nix"}


if __name__ == "__main__":
    test_fsspec()
    test_object_store()
    test_account_key()
