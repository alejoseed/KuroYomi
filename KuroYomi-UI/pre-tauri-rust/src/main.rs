// use polars::prelude::*;
use gpui::{
    App, Application, Bounds, Context, SharedString, Window, WindowBounds, WindowOptions, div,
    prelude::*, px, rgb, rgba, size, white,
};
use rusqlite::{Connection, Result};
use std::io::{self, BufRead};

struct HelloWorld {
    text: SharedString,
}

struct OverlayApp {
    selected_text: SharedString,
}

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
        "/home/alejoseed/Projects/KuroYomi/KuroYomi-UI/json-to-parquet/KuroYomi.sqlite",
    )?;

    let query = "select * from Terms t left join Definitions d on d.term_hash = t.hash where term = ?1 or reading = ?1";

    let mut stmt = conn.prepare(query)?;
    let rows = stmt.query_map([word], |row| {
        Ok(TermAndDefinition {
            term: row.get("term")?,
            reading: row.get("reading")?,
            score: row.get("score")?,
            tags: row.get("tags")?,
            definition: row.get("definition")?,
        })
    })?;

    for term in rows {
        let term = term?;
        println!("{:?}", term);
    }

    println!("This is the string printed as a reference: {word}");

    Ok(())
}

impl Render for OverlayApp {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .size_full()
            .bg(white())
            .child(div().child(format!("Selected {}", self.selected_text)))
    }
}

impl Render for HelloWorld {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .flex()
            .flex_col()
            .gap_3()
            .bg(rgb(0x505050))
            .size(px(500.0))
            .justify_center()
            .items_center()
            .shadow_lg()
            .border_1()
            .border_color(rgb(0x0000ff))
            .text_xl()
            .text_color(rgb(0xffffff))
            .child(format!("Hello, {}!", &self.text))
            .child(
                div()
                    .flex()
                    .gap_2()
                    .child(div().size_8().bg(gpui::red()))
                    .child(div().size_8().bg(gpui::green()))
                    .child(div().size_8().bg(gpui::blue()))
                    .child(div().size_8().bg(gpui::yellow()))
                    .child(div().size_8().bg(gpui::black()))
                    .child(div().size_8().bg(gpui::white())),
            )
    }
}

fn main() {
    Application::new().run(|cx: &mut App| {
        let bounds = Bounds::centered(None, size(px(500.), px(500.0)), cx);

        cx.open_window(
            WindowOptions {
                titlebar: None,
                window_bounds: Some(WindowBounds::Windowed(bounds)),
                ..Default::default()
            },
            |_, cx| {
                cx.new(|_| OverlayApp {
                    selected_text: "No text selected".into(),
                })
            },
        )
        .unwrap();
    });
}
// fn main() {
//     Application::new().run(|cx: &mut App| {
//         let bounds = Bounds::centered(None, size(px(500.), px(500.0)), cx);
//         cx.open_window(
//             WindowOptions {
//                 window_bounds: Some(WindowBounds::Windowed(bounds)),
//                 ..Default::default()
//             },
//             |_, cx| {
//                 cx.new(|_| HelloWorld {
//                     text: "World".into(),
//                 })
//             },
//         )
//         .unwrap();
//     });
// }

// let stdin = io::stdin();
// let mut handle = stdin.lock();

// loop {
// let mut line = String::new();

//     match handle.read_line(&mut line) {
//         Ok(0) => break,
//         Ok(_) => find_in_dict(&line.trim_end()).expect("Something crazy happened"),
//         Err(e) => {
//             eprintln!("error {}", e);
//             break;
//         }
//     }
// }
