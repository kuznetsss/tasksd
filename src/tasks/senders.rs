use std::{process::ExitStatus, sync::Arc};

use tokio::sync::{broadcast, watch};

pub const CHANNEL_CAPACITY: usize = 16;

#[derive(Debug)]
pub(in crate::tasks) struct TaskSenders {
    pub output_tx: broadcast::Sender<Arc<String>>,
    pub on_exit_tx: watch::Sender<Option<ExitStatus>>,
}

impl TaskSenders {
    pub(in crate::tasks) fn new() -> Self {
        let (output_tx, _) = broadcast::channel(CHANNEL_CAPACITY);
        let (on_exit_tx, _) = watch::channel(None);
        Self {
            output_tx,
            on_exit_tx,
        }
    }
}
