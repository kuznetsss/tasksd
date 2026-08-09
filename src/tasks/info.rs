use std::path::PathBuf;

use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct TaskInfo {
    pub executable: String,
    pub args: Vec<String>,
    pub working_dir: PathBuf, // TODO: serialization may panic on non UTF-8 string
}
