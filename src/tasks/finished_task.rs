use std::{process::ExitStatus, sync::Arc};

use crate::tasks::{info::TaskInfo, output_buffer::OutputBuffer};

#[derive(Debug)]
pub struct FinishedTask {
    pub info: Arc<TaskInfo>,
    pub output_buffer: Arc<OutputBuffer>,
    pub exit_status: ExitStatus,
}
