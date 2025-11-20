// use polars::prelude::*;
use std::io::{self, BufRead};
use rusqlite::{Connection, Result};

#[derive(Debug)]
struct Definitions {
    id: i64,
    definition: String,
    term_hash: String,
}

#[derive(Debug)]
struct Term {
    id: i64,
    term: String,
    reading: String,
    dictionary_id: i32,
    score: i64,
    sequence: i64,
    tags: String,
    deflection: String,
    term_tags: String,
    hash: String,
}

#[derive(Debug)]
struct TermAndDefinition {
    term: String,
    reading: String,
    score: i64,
    tags: String,
    definition: String,
}

fn find_in_dict(word: &str) -> Result<()> {
    let conn = Connection::open(
        "/home/alejoseed/Projects/KuroYomi/KuroYomi-UI/json-to-parquet/KuroYomi.sqlite"
    )?;
    
    let query = "select * from Terms t left join Definitions d on d.term_hash = t.hash where term = ?1";

    let mut stmt = conn.prepare(query)?;
    let rows = stmt.query_map([word], |row| {
        Ok(TermAndDefinition {
            term: row.get("term")?,
            reading: row.get("reading")?,
            score: row.get("score")?,
            tags: row.get("tags")?,
            definition: row.get("definition")?
        })
    })?;

    for term in rows {
        let term = term?;
        println!("{:?}", term);
    }

    println!("This is the string printed as a reference: {word}");

    Ok(())
}

fn main() {
    let stdin = io::stdin();
    let mut handle = stdin.lock();
    
    loop {
        let mut line = String::new();

        match handle.read_line(&mut line) {
            Ok(0) => break,
            Ok(_) => find_in_dict(&line.trim_end()).expect("Something crazy happened"),
            Err(e) => {
                eprintln!("error {}", e);
                break;
            }
        }
    }
}
