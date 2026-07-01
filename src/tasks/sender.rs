use std::{process::ExitStatus, sync::Arc};

use tokio::sync::{broadcast, watch};

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
pub(in crate::tasks) struct TaskSender {
    pub events_tx: broadcast::Sender<TaskEvent>,
    pub exit_tx: watch::Sender<Option<ExitStatus>>,
}

impl TaskSender {
    pub(in crate::tasks) fn new() -> Self {
        let (events_tx, _) = broadcast::channel(CHANNEL_CAPACITY);
        let (exit_tx, _) = watch::channel(None);
        Self { events_tx, exit_tx }
    }
}

pub type TaskEventsStream = broadcast::Receiver<TaskEvent>;
