import sqlglot

print(
    repr(
        sqlglot.parse_one(
            "select table.match_bm25(input_id, query_string, fields := 'text_field_2, text_field_N', k := 1.2, b:= 0.75, conjunctive := 0)",
            dialect="duckdb",
        )
    )
)
