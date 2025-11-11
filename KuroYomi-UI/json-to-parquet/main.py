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
    "column_5": pl.List,
    "column_6": pl.Int64,    # sequence
    "column_7": pl.String,   # term tags
}

class DictEntry:
    term : str = ""
    reading: str = ""
    tags : str = ""
    deflection : str = ""
    score : int = 0
    definition : object = None

    def __init__(self) -> None:
        pass
def convert_json_to_dict(json_path : Path) -> list[list[str]]:
    def get_json_as_str(json_path: Path):
        with open(json_path) as f:
            json_str = f.readline()
            return json_str

    json_str = get_json_as_str(json_path)
    
    json_obj : list[list[str]] = json.loads(json_str)


    def dfs(node) -> list[str] | str:
        if not node:
            return ""

        if isinstance(node, str):
            return node
        
        if isinstance(node, list):
            definitions = []

            for item in node:
                ret = dfs(item)
                definitions.append(ret)
            return definitions
        
        if isinstance(node, dict):
            content = node.get("content")
            if content:
                return dfs(content)
            return ""
        
        return ""
    

    def flatten(item):
        if isinstance(item, str):
            return [item]
        if isinstance(item, list):
            result = []
            for i in item:
                result.extend(flatten(i))
            return result
        return []

    for i, item in enumerate(json_obj.copy()):
        if not item[1]:
            continue
        for definition in item[5]:
            if isinstance(definition, str): # This will not happen in the Jintendex case.
                print(definition)
            if isinstance(definition, dict):
                dtype = definition.get("type")
                
                if dtype == "structured-content":
                    nodes = definition.get("content") or []

                    for node in nodes:
                        definition_list = dfs(node)
                        flattened_dl = flatten(definition_list)
                        json_obj[i].append(flattened_dl)
                    continue

            if isinstance(definition, list):
                print("THIS IS A LIST")
            else:
                print("CASE NOT MADE")
    # all_possible_tags = set()

    # dfs_obj = {}

    # for i, item in enumerate(json_obj):
    #     all_tags = item[2].split(" ")
    #     all_possible_tags.update(all_tags)

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
    # test_json_path = Path(f"/home/alejoseed/Projects/KuroYomi/KuroYomi-UI/json-to-parquet/new_term_bank_212.jsonl")
    for i in range(1, 212):
        test_json_path = Path(f"/home/alejoseed/Projects/KuroYomi/KuroYomi-UI/json-to-parquet/Jintendex/term_bank_{i}.json")
        term_bank_52 = convert_json_to_dict(test_json_path)
        break
    # print(term_bank_52)

if __name__ == "__main__":
    main()
