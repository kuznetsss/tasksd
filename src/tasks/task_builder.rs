use std::{path::PathBuf, process::ExitStatus, sync::Arc};

use tokio::{
    sync::{broadcast, watch},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::tasks::common::CHANNEL_CAPACITY;

pub struct TaskBuilder {
    executable: String,
    args: Option<Vec<String>>,
    working_dir: Option<PathBuf>,

    cancel: CancellationToken,
    stdout_tx: broadcast::Sender<Arc<String>>,
    on_exit_tx: watch::Sender<Option<ExitStatus>>,
    input: Option<String>,
    related_tasks: JoinSet<()>,
}

impl TaskBuilder {
    pub fn new(executable: String) -> Self {
        let (stdout_tx, _) = broadcast::channel(CHANNEL_CAPACITY);
        let (on_exit_tx, _) = watch::channel(None);
        Self {
            executable,
            args: None,
            working_dir: None,
            cancel: CancellationToken::new(),
            stdout_tx,
            on_exit_tx,
            input: None,
            related_tasks: JoinSet::new(),
        }
    }

    pub fn arg(&mut self, arg: impl Into<String>) -> &mut Self {
        self.args.get_or_insert(Vec::new()).push(arg.into());
        self
    }

    pub fn args(&mut self, args: Vec<impl Into<String>>) -> &mut Self {
        match &mut self.args {
            Some(v) => v.extend(args.into_iter().map(Into::into)),
            None => self.args = Some(args.into_iter().map(Into::into).collect()),
        };
        self
    }

    pub fn working_dir(&mut self, path: impl Into<PathBuf>) -> &mut Self {
        self.working_dir = Some(path.into());
        self
    }

    pub fn on_output<F>(&mut self, mut f: F) -> &mut Self
    where
        F: FnMut(Arc<String>) + 'static + Send,
    {
        let mut stdout_rx = self.stdout_tx.subscribe();
        let cancel = self.cancel.child_token();
        self.related_tasks.spawn(async move {
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
        self
    }

    pub fn on_exit<F>(&mut self, f: F) -> &mut Self
    where
        F: FnOnce(ExitStatus) + 'static + Send,
    {
        let mut on_exit_rx = self.on_exit_tx.subscribe();
        self.related_tasks.spawn(async move {
            on_exit_rx.changed().await.expect("on_exit_rx.await");
            f(on_exit_rx.borrow_and_update().unwrap());
        });
        self
    }
}
