use std::path::Path;

use crate::types::ProcessID;

#[derive(Debug)]
pub struct ProcessManager {}

impl ProcessManager {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn start_process(
        executable: String,
        args: Vec<String>,
        working_directory: String,
    ) -> Result<ProcessID, usize> {
        todo!()
    }

    pub async fn send_signal(pid: ProcessID, signal: u32) -> Result<(), usize> {
        todo!()
    }
}
