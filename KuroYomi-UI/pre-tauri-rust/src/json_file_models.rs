use serde::{Deserialize, Serialize,};

#[derive(Debug, Deserialize)]
pub struct DictionaryIndexFile {
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

#[derive(Debug, Deserialize)]
pub struct TermEntry {
    pub text: String,
    pub reading: String,
    pub definition_tags: Option<String>,
    pub rule_ids: String,
    pub score: f32,
    pub definitions: Vec<DefinitionItemPlaceholder>,
    pub sequence: u32,
    pub term_tags: String
}

pub type DefinitionItemPlaceholder = serde_json::Value;
pub type TermBankFile = Vec<TermEntry>;

