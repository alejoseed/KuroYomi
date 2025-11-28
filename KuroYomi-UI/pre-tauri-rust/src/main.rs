// use polars::prelude::*;
use rusqlite::{Connection, Result};
use std::io::{self, BufRead};
use std::{fs, path::PathBuf};
use clap::{Parser, Subcommand};
use pre_tauri_rust::json_files;

#[derive(Parser)]
#[command(name = "KuroYomi CLI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Import { paths: Vec<PathBuf> },
    Search { term: String },
}

fn find_in_dict(_word: &str) -> Result<()> {
    // let conn = Connection::open(
    //     "/home/alejoseed/Projects/KuroYomi/KuroYomi-UI/json-to-parquet/KuroYomi.sqlite",
    // )?;


    // let query = "select * from Terms t left join Definitions d on d.term_hash = t.hash where term = ?1 or reading = ?1";

    // let mut stmt = conn.prepare(query)?;
    // let rows = stmt.query_map([word], |row| {
    //     Ok(DictSearchResult {
    //         term: row.get("term")?,
    //         reading: row.get("reading")?,
    //         score: row.get("score")?,
    //         tags: row.get("tags")?,
    //         definition: row.get("definition")?,
    //     })
    // })?;

    // for term in rows {
    //     let term = term?;
    //     println!("{:?}", term);
    // }

    // println!("This is the string printed as a reference: {word}");

    Ok(())
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Import { paths } => {
            for path in paths {
                if path.is_dir() {
                    let _ = json_files::init_dict(&path);
                }
            }
        },
        _ => {
            panic!("Command not implemented yet...")
        }
    }

    // let stdin = io::stdin();
    // let mut handle = stdin.lock();

    // loop {
    //     let mut line = String::new();

    //     match handle.read_line(&mut line) {
    //         Ok(0) => break,
    //         Ok(_) => find_in_dict(&line.trim_end()).expect("Something crazy happened"),
    //         Err(e) => {
    //             eprintln!("error {}", e);
    //             break;
    //         }
    //     }
    // }
}
