def test_date32():
    from bmsdna.lakeapi.core.model import create_parameter_model
    import pyarrow as pa

    sc = pa.schema([pa.field("date_field", pa.date32())])
    t = create_parameter_model(sc, "test_date32", ["date_field"], None, None, apimethod="get")
    assert t is not None
    print(t.model_json_schema())
    assert t.model_json_schema()["properties"]["date_field"]["format"] == "date"


if __name__ == "__main__":
    test_date32()
