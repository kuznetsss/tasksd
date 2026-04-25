use std::{
    process::ExitStatus,
    sync::{Arc, Mutex},
};

use tokio::{
    sync::{broadcast, watch},
    task::{AbortHandle, JoinSet},
};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::tasks::{senders::TaskSenders, task_error::TaskError};

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
        if self.has_exited() {
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

#[cfg(test)]
mod tests {
    use super::*;

    // Tests moved from task_builder
    /*
    fn make_on_output_test_data() -> (TaskBuilder, Arc<Mutex<Vec<Arc<String>>>>) {
        let mut builder = TaskBuilder::new("some_executable");
        let captured_output = Arc::new(Mutex::new(Vec::new()));
        builder.on_output({
            let captured_output = captured_output.clone();
            move |line| {
                captured_output.lock().unwrap().push(line);
            }
        });
        (builder, captured_output)
    }

    #[tokio::test]
    async fn on_output_subscribes_to_stdout() {
        let (builder, captured_output) = make_on_output_test_data();
        let lines = ["some", "output", "lines"];
        for l in lines {
            assert_eq!(
                builder
                    .senders
                    .stdout_tx
                    .send(Arc::new(l.to_string()))
                    .unwrap(),
                2
            );
        }
        drop(builder.senders.stdout_tx);
        builder.events.join_all().await;
        let captured_output = captured_output.lock().unwrap();
        assert_eq!(captured_output.len(), lines.len());
        for (i, line) in lines.iter().enumerate() {
            assert_eq!(*line, *captured_output[i]);
        }
    }

    #[tokio::test]
    async fn on_output_sender_dropped() {
        let (builder, captured_output) = make_on_output_test_data();
        drop(builder.senders.stdout_tx);
        builder.events.join_all().await;
        assert!(captured_output.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn on_output_cancellation() {
        let mut builder = TaskBuilder::new("some_executable");
        let got_message = Arc::new(Notify::new());
        let captured_output = Arc::new(Mutex::new(Vec::new()));
        builder.on_output({
            let captured_output = captured_output.clone();
            let got_message = got_message.clone();
            move |line| {
                captured_output.lock().unwrap().push(line);
                got_message.notify_one();
            }
        });
        let lines = ["some", "output", "lines"];
        assert_eq!(
            builder
                .senders
                .stdout_tx
                .send(Arc::new(lines[0].to_string()))
                .unwrap(),
            2
        );
        got_message.notified().await;
        builder.events.cancel();
        builder.events.join_all().await;

        for l in lines.iter().skip(1) {
            assert_eq!(
                builder
                    .senders
                    .stdout_tx
                    .send(Arc::new(l.to_string()))
                    .unwrap(),
                1
            );
        }
        let captured_output = captured_output.lock().unwrap();
        assert_eq!(captured_output.len(), 1);
        assert_eq!(lines[0], *captured_output[0]);
    }

    #[tokio::test]
    async fn on_output_slow_subscriber() {
        // tokio::test is single threaded by default so this test is deterministic
        let (builder, captured_output) = make_on_output_test_data();
        let range = 0..(CHANNEL_CAPACITY + 2);
        for i in range.clone() {
            assert_eq!(
                builder
                    .senders
                    .stdout_tx
                    .send(Arc::new(i.to_string()))
                    .unwrap(),
                2
            );
        }
        drop(builder.senders.stdout_tx);
        builder.events.join_all().await;
        let captured_output = captured_output.lock().unwrap();
        assert_eq!(captured_output.len(), CHANNEL_CAPACITY);
        for (i, message) in range.skip(2).enumerate() {
            assert_eq!(message.to_string(), *captured_output[i]);
        }
    }

    #[tokio::test]
    async fn on_exit_subscribes_to_exit_tx() {
        let mut builder = TaskBuilder::new("some_executable");
        let captured_exit_code = Arc::new(Mutex::new(Option::<ExitStatus>::None));
        builder.on_exit({
            let captured_exit_code = captured_exit_code.clone();
            move |e| {
                *captured_exit_code.lock().unwrap() = Some(e);
            }
        });
        let exit_code = 123;
        builder
            .senders
            .on_exit_tx
            .send(Some(ExitStatus::from_raw(exit_code)))
            .unwrap();
        builder.events.join_all().await;
        assert_eq!(
            captured_exit_code.lock().unwrap().unwrap().into_raw(),
            exit_code
        );
    }

    #[tokio::test]
    async fn on_exit_tx_never_called() {
        let mut builder = TaskBuilder::new("some_executable");
        let captured_exit_code = Arc::new(Mutex::new(Option::<ExitStatus>::None));
        builder.on_exit({
            let captured_exit_code = captured_exit_code.clone();
            move |e| {
                *captured_exit_code.lock().unwrap() = Some(e);
            }
        });
        drop(builder);
        assert!(captured_exit_code.lock().unwrap().is_none());
    }
    */
}
