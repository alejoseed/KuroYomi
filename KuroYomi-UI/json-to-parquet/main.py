import polars as pl

term_bank_schema = {
    "column_0": pl.String,   # term
    "column_1": pl.String,   # reading
    "column_2": pl.String,   # definition tags
    "column_3": pl.String,   # rule ids
    "column_4": pl.Float64,  # score
    "column_5": pl.List(pl.Object),
    "column_6": pl.Int64,    # sequence
    "column_7": pl.String,   # term tags
}


def main():
    test = pl.read_json(
        "/home/alejoseed/Projects/KuroYomi/KuroYomi-UI/json-to-parquet/example.json",
        schema=term_bank_schema,
        infer_schema_length=None
    )
    print(test)


if __name__ == "__main__":
    main()
