use serde::Serialize;

#[derive(Debug, PartialEq, Clone, Serialize)]
pub struct OutputLine {
    #[serde(rename = "line")]
    pub content: String,
    pub line_number: usize,
}
