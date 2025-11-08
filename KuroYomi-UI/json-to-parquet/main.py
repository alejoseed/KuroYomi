import polars as pl
import json
import time
from pathlib import Path

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

def convert_json_to_dict(json_path : Path) -> list[list[str]]:
    def get_json_as_str(json_path: Path):
        with open(json_path) as f:
            json_str = f.readline()
            return json_str

    json_str = get_json_as_str(json_path)

    json_obj : list[list[str]] = json.loads(json_str)
    
    return json_obj

def normalize_yomitan_json() -> str:
    test = """
        [
            [
                "マンシングウエア",
                "マンシングウエア",
                "n product",
                "",
                -200,
                [
                    {
                        "content": {
                            "content": {
                                "content": "Munsingwear",
                                "tag": "li"
                            },
                            "data": {
                                "content": "glossary"
                            },
                            "lang": "en",
                            "style": {
                                "listStyleType": "circle"
                            },
                            "tag": "ul"
                        },
                        "type": "structured-content"
                    }
                ],
                5741143,
                ""
            ]
        ]
    """

    att = json.loads(test)

    print(att)
    return test
def main():
    test_json_path = Path(f"/home/alejoseed/Projects/KuroYomi/KuroYomi-UI/json-to-parquet/JMDict/term_bank_52.json")
    term_bank_52 = convert_json_to_dict(test_json_path)
    print(term_bank_52)

if __name__ == "__main__":
    main()
