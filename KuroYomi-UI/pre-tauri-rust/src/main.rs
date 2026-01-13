use std::path::{Path, PathBuf};

use iced::widget::{button, column, row, scrollable, text, text_input};
use iced::{Alignment, Application, Command, Element, Length, Settings, Theme};
use pre_tauri_rust::models::DictSearchResult;
use rusqlite::{params, Connection};

const QUERY: &str = "select t.term as term, t.reading as reading, t.score as score, t.tags as tags, d.definition as definition from Terms t left join Definitions d on d.term_hash = t.hash where t.term like ?1 or t.reading like ?2 order by t.score desc, t.sequence asc limit 50";
const DEFAULT_DB_RELATIVE_PATH: &str = "../json-to-parquet/KuroYomi.sqlite";
const DB_ENV_VAR: &str = "KUROYOMI_DB_PATH";

fn main() -> iced::Result {
    DictionaryApp::run(Settings::default())
}

struct DictionaryApp {
    db_path: PathBuf,
    connection: Option<Connection>,
    query: String,
    results: Vec<DictSearchResult>,
    status: Option<String>,
}

#[derive(Debug, Clone)]
enum Message {
    QueryChanged(String),
    Search,
}

impl Application for DictionaryApp {
    type Executor = iced::executor::Default;
    type Message = Message;
    type Theme = Theme;
    type Flags = ();

    fn new(_flags: Self::Flags) -> (Self, Command<Self::Message>) {
        let db_path = resolve_db_path();
        let (connection, status) = match Connection::open(&db_path) {
            Ok(connection) => (Some(connection), None),
            Err(err) => (
                None,
                Some(format!(
                    "Could not open database at {}: {err}",
                    db_path.display()
                )),
            ),
        };

        (
            Self {
                db_path,
                connection,
                query: String::new(),
                results: Vec::new(),
                status,
            },
            Command::none(),
        )
    }

    fn title(&self) -> String {
        "KuroYomi Dictionary".to_string()
    }

    fn update(&mut self, message: Self::Message) -> Command<Self::Message> {
        match message {
            Message::QueryChanged(value) => {
                self.query = value;
            }
            Message::Search => {
                self.search();
            }
        }

        Command::none()
    }

    fn view(&self) -> Element<'_, Self::Message> {
        let search_input = text_input("Search term or reading...", &self.query)
            .on_input(Message::QueryChanged)
            .on_submit(Message::Search)
            .padding(8)
            .width(Length::Fill);

        let search_row = row![]
            .spacing(12)
            .align_items(Alignment::Center)
            .push(search_input)
            .push(button(text("Search")).on_press(Message::Search));

        let mut content = column![]
            .spacing(16)
            .padding(20)
            .push(text("KuroYomi Dictionary").size(28))
            .push(text(format!("Database: {}", self.db_path.display())).size(12))
            .push(search_row);

        if let Some(status) = &self.status {
            content = content.push(text(status));
        }

        let results_view = scrollable(self.view_results()).height(Length::Fill);

        content.push(results_view).into()
    }
}

impl DictionaryApp {
    fn search(&mut self) {
        self.status = None;
        self.results.clear();

        let term = self.query.trim();
        if term.is_empty() {
            self.status = Some("Enter a term to search.".to_string());
            return;
        }

        let Some(connection) = self.connection.as_mut() else {
            self.status = Some("Database connection is not available.".to_string());
            return;
        };

        let like_term = format!("{term}%");
        let mut statement = match connection.prepare(QUERY) {
            Ok(statement) => statement,
            Err(err) => {
                self.status = Some(format!("Query preparation failed: {err}"));
                return;
            }
        };

        let rows = match statement.query_map(params![like_term, like_term], |row| {
            Ok(DictSearchResult {
                term: row.get("term")?,
                reading: row.get("reading")?,
                score: row
                    .get::<_, Option<i64>>("score")?
                    .unwrap_or_default(),
                tags: row
                    .get::<_, Option<String>>("tags")?
                    .unwrap_or_default(),
                definition: row
                    .get::<_, Option<String>>("definition")?
                    .unwrap_or_else(|| "No definition available.".to_string()),
            })
        }) {
            Ok(rows) => rows,
            Err(err) => {
                self.status = Some(format!("Query failed: {err}"));
                return;
            }
        };

        for row in rows {
            match row {
                Ok(result) => self.results.push(result),
                Err(err) => {
                    self.status = Some(format!("Row parse failed: {err}"));
                    return;
                }
            }
        }

        if self.results.is_empty() {
            self.status = Some("No results found.".to_string());
        }
    }

    fn view_results(&self) -> Element<'_, Message> {
        if self.results.is_empty() {
            let hint = if self.query.trim().is_empty() {
                "Type a term to search."
            } else {
                "No results yet."
            };

            return column![].push(text(hint)).into();
        }

        let mut entries = column![].spacing(12);

        for result in &self.results {
            let mut entry = column![].spacing(4).push(
                row![]
                    .spacing(8)
                    .align_items(Alignment::Center)
                    .push(text(&result.term).size(20))
                    .push(text(format!("({})", result.reading))),
            );

            if !result.tags.is_empty() {
                entry = entry.push(text(format!("Tags: {}", result.tags)).size(12));
            }

            if result.score != 0 {
                entry = entry.push(text(format!("Score: {}", result.score)).size(12));
            }

            entry = entry.push(text(&result.definition));

            entries = entries.push(entry);
        }

        entries.into()
    }
}

fn resolve_db_path() -> PathBuf {
    if let Ok(db_path) = std::env::var(DB_ENV_VAR) {
        return PathBuf::from(db_path);
    }

    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest_dir.join(DEFAULT_DB_RELATIVE_PATH)
}
