use std::{process::ExitStatus, sync::Arc};

use tokio::sync::{broadcast, watch};

pub const CHANNEL_CAPACITY: usize = 16;
// NOTE: broadcast is lossy which means either:
// - slow subscriber losses data
// - too fast producer will cause loosing data for all subscribers
// Maybe in the future this should be changed to broadcast into output_buffer
// with slowing down producer when needed by backpressure.

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
