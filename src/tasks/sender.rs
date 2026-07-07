use std::{process::ExitStatus, sync::Arc};

use serde::Serialize;
use tokio::sync::{broadcast, watch};

pub const CHANNEL_CAPACITY: usize = 16;

#[derive(Debug, PartialEq, Clone, Serialize)]
pub struct OutputLine {
    #[serde(rename = "line")]
    pub content: String,
    pub line_number: usize,
}

#[derive(Debug, Clone)]
pub enum TaskEvent {
    Output(Arc<OutputLine>),
    Exit(ExitStatus),
}

pub type TaskEventsStream = broadcast::Receiver<TaskEvent>;

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
