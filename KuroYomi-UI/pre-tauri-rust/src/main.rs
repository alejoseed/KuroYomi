// use polars::prelude::*;
use std::io::{self, BufRead};

fn main() {
    let stdin = io::stdin();
    let mut handle = stdin.lock();
    
    loop {
        let mut line = String::new();

        match handle.read_line(&mut line) {
            Ok(0) => break,
            Ok(_) => print!("{}", line),
            Err(e) => {
                eprintln!("error {}", e);
                break;
            }
        }
    }
}
