use std::fs;
use std::io;
use std::path::Path;
use rusqlite::Connection;
use serde::Deserialize;

const SQLITE_PATH: &str = "/home/alejoseed/Projects/KuroYomi/KuroYomi-UI/json-to-parquet/KuroYomi.sqlite";

#[derive(Debug)]
struct DictionaryFolder {
    index_file: String,
    term_bank_files: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct DictionaryIndexFile {
    pub title: String,
    pub author: String,
    pub sequenced: bool,
    pub format: u32,
    pub url: String,
    pub description: String,
    pub attribution: String,
    pub revision: String,

    #[serde(rename = "isUpdatable")]
    pub is_updatable: bool,

    #[serde(rename = "indexUrl")]
    pub index_url: String,

    #[serde(rename = "downloadUrl")]
    pub download_url: String,
    
    #[serde(rename = "sourceLanguage")]
    pub source_language: String,

    #[serde(rename = "targetLanguage")]
    pub target_language: String,
}


pub fn init_dict(path: &Path) -> io::Result<()> {
    let mut dictionary: DictionaryFolder = DictionaryFolder {
        index_file: String::new(),
        term_bank_files: Vec::new(),
    };

    if !path.is_dir() {
        println!("This function should only receive folders.");
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Path is not folder"
        ));
    }

    let entries = fs::read_dir(path)?;

    for entry in entries {
        let entry = entry?;
        let entry_path = entry.path();
        if entry_path.is_file() {
            get_folder_files(&mut dictionary, &entry_path)?;
        }
    }

    if dictionary.index_file.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "Dictionary index file not found."
        ));
    }

    let dictionary_metadata = get_dictionary_metadata(&dictionary.index_file)?;

    let dict_entry_result: Result<i64, rusqlite::Error> = insert_dictionary_db(&dictionary_metadata);
        
    println!("Dictionary id: {dict_entry_result:?}");
    Ok(())
}

fn get_folder_files(dictionary: &mut DictionaryFolder, path: &Path) -> io::Result<()> {
    if !path
        .extension()
        .map_or(false, |ext| ext.eq_ignore_ascii_case("json"))
    {
        return Ok(());
    }

    let path_string = path.to_string_lossy().into_owned();
    let file_name = path.file_name().unwrap_or_default().to_string_lossy();

    if path.file_stem()
        .map_or(false, |stem| stem.eq_ignore_ascii_case("index"))
    {
        dictionary.index_file = path_string;
    } else if file_name.starts_with("term_bank_") {
        dictionary.term_bank_files.push(path_string);
    } else {
        println!("Ignoring file {path_string}")
    };

    Ok(())
}

fn get_dictionary_metadata(dictionary_index: &String) -> io::Result<DictionaryIndexFile> {
    
    let file = fs::File::open(dictionary_index).inspect_err(|e: &io::Error| println!("There was an error {e}"))?;
    let reader = io::BufReader::new(file);
    
    println!("Reading index file {dictionary_index}");
    let dictionary_index: DictionaryIndexFile = serde_json::from_reader(reader)
        .map_err(|e: serde_json::Error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("JSON parsing failed from reader {}", e)
            )
    }).inspect_err(|e| println!("There was an error while mapping errors {e}"))?;

    return Ok(dictionary_index);
}

fn insert_dictionary_db(dictionary_metadata: &DictionaryIndexFile) -> Result<i64, rusqlite::Error> {
    let conn = Connection::open(
        SQLITE_PATH
    )?;
    
    let query= "insert into Dictionaries (title, revision, author) values (?1, ?2, ?3) 
    on conflict (title, revision, author) do nothing";

    conn.execute(query, (&dictionary_metadata.title, &dictionary_metadata.revision, &dictionary_metadata.author))?;
    
    let mut dictionary_id = conn.last_insert_rowid();

    if dictionary_id == 0 {
        dictionary_id = get_dictionary_id(&dictionary_metadata)?;
    }
    
    return Ok(dictionary_id);
}

fn get_dictionary_id(dictionary_metadata: &DictionaryIndexFile) -> Result<i64, rusqlite::Error> {
    let conn = Connection::open(
        SQLITE_PATH
    )?;
    
    let query= "select id from Dictionaries where title = ?1 and author = ?2 and revision = ?3";
    let dict_id = conn.query_row
        (
        query, 
        (&dictionary_metadata.title, &dictionary_metadata.author, &dictionary_metadata.revision), 
        |row| row.get(0))?;
    
    return Ok(dict_id);
}

fn process_term_bank_list(term_bank_list: &Vec<String>, dict_id: &i64) -> io::Result<()> {
    for term_bank in term_bank_list {
        let term_bank_file = fs::File::open(term_bank)?;
        let reader = io::BufReader::new(term_bank_file);

        // let 
    }
    return Ok(())
}

// fn insert_term_bank() {

// }

