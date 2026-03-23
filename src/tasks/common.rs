use std::{
    path::PathBuf,
    process::ExitStatus,
    sync::{Arc, Mutex},
};

use tokio::{
    sync::{broadcast, watch},
    task::{AbortHandle, JoinSet},
};
use tokio_util::sync::CancellationToken;
use tracing::warn;

pub const CHANNEL_CAPACITY: usize = 16;

#[derive(Debug)]
pub struct TaskInfo {
    pub executable: String,
    pub args: Vec<String>,
    pub working_dir: PathBuf,
}

#[derive(Debug)]
pub(in crate::tasks) struct TaskSenders {
    pub stdout_tx: broadcast::Sender<Arc<String>>,
    pub on_exit_tx: watch::Sender<Option<ExitStatus>>,
}

impl TaskSenders {
    pub(in crate::tasks) fn new() -> Self {
        let (stdout_tx, _) = broadcast::channel(CHANNEL_CAPACITY);
        let (on_exit_tx, _) = watch::channel(None);
        Self {
            stdout_tx,
            on_exit_tx,
        }
    }
}

#[derive(Debug)]
pub(in crate::tasks) struct TaskCallbacks {
    pub cancel: CancellationToken,
    pub stdout_rx: broadcast::Receiver<Arc<String>>,
    pub on_exit_rx: watch::Receiver<Option<ExitStatus>>,
    pub related_tasks: Mutex<JoinSet<()>>,
}

impl TaskCallbacks {
    pub(in crate::tasks) fn new(senders: &TaskSenders) -> Self {
        Self {
            cancel: CancellationToken::new(),
            stdout_rx: senders.stdout_tx.subscribe(),
            on_exit_rx: senders.on_exit_tx.subscribe(),
            related_tasks: Mutex::new(JoinSet::new()),
        }
    }
}

pub trait TaskOutputCallback: FnMut(Arc<String>) + 'static + Send {}
impl<F> TaskOutputCallback for F where F: FnMut(Arc<String>) + 'static + Send {}

pub trait TaskExitCallback: FnOnce(ExitStatus) + 'static + Send {}
impl<F> TaskExitCallback for F where F: FnOnce(ExitStatus) + 'static + Send {}

impl TaskCallbacks {
    pub(in crate::tasks) fn on_output<F>(&self, mut f: F) -> AbortHandle
    where
        F: TaskOutputCallback,
    {
        let mut stdout_rx = self.stdout_rx.resubscribe();
        let cancel = self.cancel.child_token();
        self.related_tasks.lock().unwrap().spawn(async move {
            while let Some(input_line) = cancel.run_until_cancelled(stdout_rx.recv()).await {
                match input_line {
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!("Stdout receiver is too slow. Have to skip {n} lines");
                    }
                    Err(_) => break,
                    Ok(line) => f(line),
                };
            }
        })
    }

    pub(in crate::tasks) fn on_exit<F>(&self, f: F) -> AbortHandle
    where
        F: TaskExitCallback,
    {
        let mut on_exit_rx = self.on_exit_rx.clone();
        self.related_tasks.lock().unwrap().spawn(async move {
            on_exit_rx.changed().await.expect("on_exit_rx.await");
            f(on_exit_rx.borrow_and_update().unwrap());
        })
    }

    pub(in crate::tasks) fn cancel(&self) {
        self.cancel.cancel();
    }

    pub(in crate::tasks) async fn join_all(self) {
        let related_tasks = self.related_tasks.into_inner().unwrap();
        related_tasks.join_all().await;
    }
}
