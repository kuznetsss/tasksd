use std::{
    path::PathBuf,
    process::ExitStatus,
    sync::{Arc, Mutex, atomic::AtomicBool},
};

use tokio::{
    sync::{broadcast, watch},
    task::{AbortHandle, JoinSet},
};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::tasks::{task::Task, task_error::TaskError};

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

pub trait TaskOutputCallback: FnMut(Arc<String>) + 'static + Send {}
impl<F> TaskOutputCallback for F where F: FnMut(Arc<String>) + 'static + Send {}

pub trait TaskExitCallback: FnOnce(ExitStatus) + 'static + Send {}
impl<F> TaskExitCallback for F where F: FnOnce(ExitStatus) + 'static + Send {}

#[derive(Debug)]
pub(in crate::tasks) struct TaskEvents {
    pub cancel: CancellationToken,
    stdout_rx: broadcast::Receiver<Arc<String>>,
    on_exit_rx: watch::Receiver<Option<ExitStatus>>,
    related_tasks: Mutex<Option<JoinSet<()>>>,
}

impl TaskEvents {
    pub(in crate::tasks) fn new(senders: &TaskSenders) -> Self {
        Self {
            cancel: CancellationToken::new(),
            stdout_rx: senders.stdout_tx.subscribe(),
            on_exit_rx: senders.on_exit_tx.subscribe(),
            related_tasks: Mutex::new(Some(JoinSet::new())),
        }
    }

    pub(in crate::tasks) fn on_output<F>(&self, mut f: F) -> Result<AbortHandle, TaskError>
    where
        F: TaskOutputCallback,
    {
        let mut stdout_rx = self.stdout_rx.resubscribe();
        let cancel = self.cancel.child_token();
        let mut related_tasks = self.related_tasks.lock().unwrap();
        if self.has_exited() {
            return Err(TaskError::AlreadyExited);
        }
        let abort_handle = related_tasks.as_mut().unwrap().spawn(async move {
            while let Some(input_line) = cancel.run_until_cancelled(stdout_rx.recv()).await {
                match input_line {
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!("Stdout receiver is too slow. Have to skip {n} lines");
                    }
                    Err(_) => break,
                    Ok(line) => f(line),
                };
            }
        });
        Ok(abort_handle)
    }

    pub(in crate::tasks) fn on_exit<F>(&self, f: F) -> Result<AbortHandle, TaskError>
    where
        F: TaskExitCallback,
    {
        let mut on_exit_rx = self.on_exit_rx.clone();
        let mut related_tasks = self.related_tasks.lock().unwrap();
        if self.has_exited() {
            return Err(TaskError::AlreadyExited);
        }
        let abort_handle = related_tasks.as_mut().unwrap().spawn(async move {
            on_exit_rx.changed().await.expect("on_exit_rx.await");
            f(on_exit_rx.borrow_and_update().unwrap());
        });
        Ok(abort_handle)
    }

    pub(in crate::tasks) fn cancel(&self) {
        self.cancel.cancel();
    }

    fn has_exited(&self) -> bool {
        self.on_exit_rx.has_changed().unwrap_or(true)
    }

    pub(in crate::tasks) async fn exit_status(&self) -> ExitStatus {
        if (self.has_exited()) {
            return self.on_exit_rx.borrow().unwrap();
        }
        let mut on_exit_rx = self.on_exit_rx.clone();
        on_exit_rx.changed().await.unwrap();
        on_exit_rx.borrow().unwrap()
    }

    #[allow(clippy::await_holding_lock)] // lock is dropped before the await point
    pub(in crate::tasks) async fn join_all(&self) {
        let mut rt_lock = self.related_tasks.lock().unwrap();
        if rt_lock.is_none() {
            return;
        }
        let rt = rt_lock.take().unwrap();
        drop(rt_lock);
        rt.join_all().await;
    }
}
