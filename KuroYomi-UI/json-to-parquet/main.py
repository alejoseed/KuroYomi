import polars as pl
import json
import sqlite3
from pathlib import Path

term_bank_schema = {
    "term": pl.String,                        # term
    "reading": pl.String,                     # reading
    "tags": pl.String,                        # definition tags
    "deflection": pl.String,                  # rule ids
    "score": pl.Float64,                      # score
    "definition_list": pl.List(pl.String),    # normalized definitions
    "sequence": pl.Int64,                     # Sequence
    "str_tags": pl.String,                    # term tags
}

"""
Consider this example yomitan dict 
test = 
    [
        [
            "マンシングウエア",
            "マンシングウエア",
            "n product",
            "",
            -200,
            [ Notice how the only definition that was valid was hidden being a li 
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

def convert_json_to_list(json_path : Path) -> list[list[str | list[str] | int]]:
    def get_json_as_str(json_path: Path):
        with open(json_path) as f:
            json_str = f.readline()
            return json_str

    json_str = get_json_as_str(json_path)
    
    json_obj : list[list[str | list[str] | int]] = json.loads(json_str)
    
    def extract_definitions(structured_content) -> list[str]:
        definitions = []
        
        stack = [structured_content]
        
        while stack:
            node = stack.pop()
            
            if isinstance(node, dict):
                if node.get('data', {}).get('content') == 'glossary':
                    content = node.get('content', [])
                    if isinstance(content, list):
                        for item in content:
                            if isinstance(item, dict) and item.get('tag') == 'li':
                                text = item.get('content')
                                if isinstance(text, str):
                                    definitions.append(text)
                    elif isinstance(content, dict) and content.get('tag') == 'li':
                        text = content.get('content')
                        if isinstance(text, str):
                            definitions.append(text)
                    continue
                
                for value in node.values():
                    stack.append(value)
                    
            elif isinstance(node, list):
                stack.extend(node)
        
        return definitions

    
    definition_index = 5

    for i, item in enumerate(json_obj):
        definition = extract_definitions(item[definition_index])        
        json_obj[i][definition_index] = definition

    return json_obj

def djb2(s : str) -> int:
    hash = 5381

    for c in s:
        hash  = ((hash << 5) + hash) + ord(c)
    return hash & 0xFFFFFFFFFFFFFFFF # u64


def main():
    test_json_path = Path(f"/home/alejoseed/Projects/KuroYomi/KuroYomi-UI/json-to-parquet/new_term_bank_212.jsonl")

    jintendex = pl.read_parquet("/home/alejoseed/Projects/KuroYomi/KuroYomi-UI/json-to-parquet/Jintendex_parquets/term_bank_*.parquet")
    
    df = jintendex.with_columns(
        col_to_hash = (
            pl.concat_str(
                pl.col("term"),
                pl.col("reading"),
                pl.col("tags"),
                pl.col("deflection"),
                pl.col("score").cast(pl.String),
                pl.col("definition_list").list.join(""),
                pl.col("sequence").cast(pl.String),
                pl.col("str_tags")
            )
        )
    )

    df = df.with_columns(
        pl.col("col_to_hash").map_elements(djb2, return_dtype=pl.UInt64).alias("hash")
    )

    print(df)
    # print(term_bank_52)

if __name__ == "__main__":
    main()
