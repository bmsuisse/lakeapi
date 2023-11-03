from bmsdna.lakeapi.context.source_uri import SourceUri, _convert_options, _get_default_token


def fake_token():
    return "fake_token"


def test_fsspec():
    o_non_anon = {"account_name": "blubb"}
    o = _convert_options(o_non_anon, flavor="fsspec", token_retrieval_func=fake_token)
    assert o == {"account_name": "blubb", "anon": "False"}
    assert "anon" not in o_non_anon

    o_anon = {"account_name": "blubb", "anon": "True"}
    o = _convert_options(o_anon, flavor="fsspec", token_retrieval_func=fake_token)
    assert o == {"account_name": "blubb", "anon": "True"}


def test_object_store():
    o_non_anon = {"account_name": "blubb"}
    o = _convert_options(o_non_anon, flavor="object_store", token_retrieval_func=fake_token)
    assert o == {"account_name": "blubb", "token": "fake_token"}
    assert "anon" not in o_non_anon

    o_anon = {"account_name": "blubb", "anon": "True"}
    o = _convert_options(o_anon, flavor="object_store", token_retrieval_func=fake_token)
    assert o == {"account_name": "blubb"}


if __name__ == "__main__":
    test_fsspec()
    test_object_store()
