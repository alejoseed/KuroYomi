use serde::{Deserialize, Serialize,};

#[derive(Debug, Clone)]
pub struct DictionaryMetadata {
    pub id: Option<i64>,
    pub title: String,
    pub author: String,
    pub revision: String,
}

#[derive(Debug, Clone)]
pub struct Term {
    pub term: String,
    pub reading: String,
    pub dictionary_id: i64,
    pub score: i64,
    pub sequence: i64,
    pub hash: String,
}

#[derive(Debug, Clone)]
pub struct Definition {
    pub term_hash: String,
    pub definition: String,
}


#[derive(Debug, Clone)]
pub struct DictSearchResult {
    pub term: String,
    pub reading: String,
    pub score: i64,
    pub tags: String,
    pub definition: String,
}
