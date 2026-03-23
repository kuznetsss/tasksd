use std::{process::ExitStatus, sync::Arc};

use crate::tasks::common::TaskInfo;

#[derive(Debug)]
pub struct FinishedTask {
    pub info: Arc<TaskInfo>,
    pub exit_status: ExitStatus,
}
