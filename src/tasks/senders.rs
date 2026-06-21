use std::{process::ExitStatus, sync::Arc};

use tokio::sync::broadcast;

pub const CHANNEL_CAPACITY: usize = 16;

#[derive(Debug, Clone)]
pub(in crate::tasks) enum TaskEvent {
    Output(Arc<String>),
    Exit(ExitStatus),
}

impl From<String> for TaskEvent {
    fn from(value: String) -> Self {
        TaskEvent::Output(Arc::new(value))
    }
}

impl From<ExitStatus> for TaskEvent {
    fn from(value: ExitStatus) -> Self {
        TaskEvent::Exit(value)
    }
}

#[derive(Debug)]
pub(in crate::tasks) struct TaskSender(pub broadcast::Sender<TaskEvent>);

impl TaskSender {
    pub(in crate::tasks) fn new() -> Self {
        let (output_tx, _) = broadcast::channel(CHANNEL_CAPACITY);
        Self(output_tx)
    }
}
