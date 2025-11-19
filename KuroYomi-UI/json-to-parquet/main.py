import polars as pl
import json
import sqlite3
import argparse
import os

from pathlib import Path

from pathlib import Path

SQLITE_PATH = "/home/alejoseed/Projects/KuroYomi/KuroYomi-UI/json-to-parquet/KuroYomi.sqlite"
parser = argparse.ArgumentParser()
parser.add_argument(
    "--dict_folder",
    nargs="+",
    type=Path,
    required=True
)

args = parser.parse_args()
dict_paths : list[Path] = args.dict_folder

term_bank_schema = {
    "term": pl.String,                        # term
    "reading": pl.String,                     # reading
    "tags": pl.String,                        # definition tags
    "deflection": pl.String,                  # rule ids
    "score": pl.Float64,                      # score
    "definition_list": pl.List(pl.String),    # normalized definitions
    "sequence": pl.Int64,                     # Sequence
    "string_tags": pl.String,                 # term tags
}

class DictionaryMetadata:
    title : str
    author : str
    revision : str
    dict_id : int
    local_path : Path
    def __init__(self, title: str, author: str, revision: str, path : Path) -> None:
        if not title or not author or not revision:
            print("A dictionary entry requires at least a title, author, and revision")
            return
        self.title = title
        self.author = author
        self.revision = revision
        self.local_path = path
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

def convert_json_to_list(json_path : Path) -> list[list[str | list[str] | int]]:
    with open(json_path, "r") as f:
        json_obj : list[list[str | list[str] | int]] = json.load(f)
        
        definition_index = 5

        for i, item in enumerate(json_obj):
            definition = extract_definitions(item[definition_index])        
            json_obj[i][definition_index] = definition

        return json_obj

def insert_to_db():
    return 0

def djb2(s : str) -> str:
    hash = 5381

    for c in s:
        hash  = ((hash << 5) + hash) + ord(c)
    return str(hash & 0xFFFFFFFFFFFFFFFF) # u64 but i will convert to text bc otherwise there is overflow in db


def get_dictionary_metadata(path : Path) -> DictionaryMetadata | None:
    if not path:
        print("Path needed to get dict name")
        return None
    
    files = os.listdir(path)
    
    if "index.json" not in files:
        print("Not index.json found in path. Exiting..")
        return None
    
    index_file = path / "index.json"

    with open(index_file, "r") as f:
        json_obj = json.load(f)
        title = json_obj.get("title", "")
        author = json_obj.get("author", "")
        revision = json_obj.get("revision", "")

        return DictionaryMetadata(title, author, revision, path)
    
    return None

"""
    returns the dict id of the create dictionary
"""
def create_dictionary(metadata : DictionaryMetadata, sqlite_path : str) -> int:
    if not metadata:
        print("No metadata received.")
        return -1
    if not sqlite_path:
        print("No path to write dictionaries to.")
        return -1
    try:
        with sqlite3.connect(sqlite_path) as conn:
            cur = conn.cursor()
            
            create_dict_query = """
                insert into Dictionaries (title, author, revision) values (?, ?, ?)
                on conflict (title, author, revision) do nothing
            """

            cur.execute(create_dict_query, (metadata.title, metadata.author, metadata.revision))
            conn.commit()

            if cur.lastrowid:
                return cur.lastrowid

            return get_dict_id(metadata=metadata, sqlite_path=sqlite_path)
        
    except Exception as e:
        print(f"Error while writing dicts. {e}")
        return False

def get_dict_id(metadata : DictionaryMetadata, sqlite_path : str) -> int:
    if not metadata:
        print("No metadata received.")
        return -1
    if not sqlite_path:
        print("No path to read dictionaries from.")
        return -1
    
    try:
        with sqlite3.connect(sqlite_path) as conn:
            cur = conn.cursor()
            
            create_dict_query = """
                select id from Dictionaries where title = ? and author = ? and revision = ?
            """

            cur.execute(create_dict_query, (metadata.title, metadata.author, metadata.revision))

            dict_id = cur.fetchone()

            return dict_id[0] if dict_id else -1
        return True
    except Exception as e:
        print(f"Error while writing dicts. {e}")
        return -1

def json_files_to_df(dict_path : Path) -> pl.DataFrame:
    term_files = [x for x in os.listdir(dict_path) if "term_bank" in x]
    if not term_files:
        print("No term_bank files found. Exiting...")
        return None
    
    # for file in term_files:
    #     flatenned_definitions = 
    #     _ = convert_json_to_list(json_path=dict_path / file)
    # json_obj = convert_json_to_list(json_path=json_dict)
    return pl.DataFrame()

def process_term_banks(term_bank_list : list[Path], dict_id : int) -> bool:
    if not term_bank_list:
        print("No term_bank provided.")

    for term_bank in term_bank_list:
        with open(term_bank, "r") as f:
            json_file = json.load(f)
            
            try:
                _ = insert_single_term_bank(json_file=json_file, dict_id=dict_id)
            except Exception as e:
                print(f"There was an error insertion batch {term_bank}")
                print(e)
    return False

def insert_single_term_bank(json_file: list[dict], dict_id: int):
    conn = sqlite3.connect(SQLITE_PATH)
    cur = conn.cursor()
    try:
        cur.execute("BEGIN")
        terms_data = []
        definitions_data = []
        hash_to_defs = {} 

        for term in json_file:
            flat_term = (
                term[0], term[1], dict_id, term[4], 
                term[6], term[2], term[3], term[7]
            )

            term_unique_str = "".join(str(x) for x in term) 
            term_hash = djb2(term_unique_str)

            terms_data.append(flat_term + (term_hash,))

            hash_to_defs[term_hash] = extract_definitions(term[5])

        cur.executemany("""
            insert into Terms (term, reading, dictionary_id, score, 
                               sequence, tags, deflection, term_tags, hash)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, terms_data)

        for term_hash, defs in hash_to_defs.items():
            for definition in defs:
                definitions_data.append((term_hash, definition))
                
        cur.executemany("""
            insert into Definitions (term_hash, definition)
            values (?, ?)
        """, definitions_data)
        
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        print(f"Error: {e}")
        return False
    finally:
        conn.close()

def main():
    try:
        list_dict_metadata: list[DictionaryMetadata] = [
            metadata
            for dict_path in dict_paths
            if (metadata := get_dictionary_metadata(dict_path)) is not None
        ]

        for i, metadata in enumerate(list_dict_metadata):
            dict_id = create_dictionary(metadata=metadata, sqlite_path="/home/alejoseed/Projects/KuroYomi/KuroYomi-UI/json-to-parquet/KuroYomi.sqlite")
            if not dict_id:
                print(f"Dictionary not created for {metadata.title}")
            list_dict_metadata[i].dict_id = dict_id
        
        for metadata in list_dict_metadata:
            term_bank_files = [metadata.local_path / x for x in os.listdir(metadata.local_path) if "term_bank" in x]
            _ = process_term_banks(term_bank_files, metadata.dict_id)
            _ = json_files_to_df(metadata.local_path)
            
        return 0
    except Exception as e:
        print(e)
        return

    # jintendex = pl.read_parquet("/home/alejoseed/Projects/KuroYomi/KuroYomi-UI/json-to-parquet/Jintendex_parquets/term_bank_*.parquet")
    
    # df = jintendex.with_columns(
    #     col_to_hash = (
    #         pl.concat_str(
    #             pl.col("term"),
    #             pl.col("reading"),
    #             pl.col("tags"),
    #             pl.col("deflection"),
    #             pl.col("score").cast(pl.String),
    #             pl.col("definition_list").list.join(""),
    #             pl.col("sequence").cast(pl.String),
    #             pl.col("str_tags")
    #         )
    #     )
    # )

    # df = df.with_columns(
    #     pl.col("col_to_hash").map_elements(djb2, return_dtype=pl.UInt64).alias("hash").cast(pl.String)
    # ).select(
    #     pl.exclude("col_to_hash", "definition_list")
    # ).rename(
    #     {"str_tags":"string_tags"}
    # )

    # df.write_database("Jintendex", f"sqlite:////home/alejoseed/Projects/KuroYomi/KuroYomi-UI/json-to-parquet/KuroYomi.sqlite", if_table_exists="append")

    # print(df)
    # # print(term_bank_52)``

if __name__ == "__main__":
    main()
