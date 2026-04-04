use std::path::PathBuf;

#[derive(Debug)]
pub struct TaskInfo {
    pub executable: String,
    pub args: Vec<String>,
    pub working_dir: PathBuf,
}
