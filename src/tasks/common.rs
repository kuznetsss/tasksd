use std::path::PathBuf;

pub const CHANNEL_CAPACITY: usize = 16;

#[derive(Debug)]
pub struct TaskInfo {
    pub executable: String,
    pub args: Vec<String>,
    pub working_dir: PathBuf,
}
