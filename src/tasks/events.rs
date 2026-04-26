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
        // TODO: run_until_cancelled() is running until Poll::Pending appear.
        // So maybe JoinSet::about_all() will be better here.
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
        let mut rt = rt_lock.take().unwrap();
        drop(rt_lock);
        while let Some(join_result) = rt.join_next().await {
            if let Err(e) = join_result
                && e.is_panic()
            {
                std::panic::resume_unwind(e.into_panic());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::process::ExitStatusExt;

    use tokio::{sync::Notify, task::yield_now};

    use crate::tasks::senders;

    use super::*;

    fn make_test_data() -> (TaskSenders, TaskEvents) {
        let senders = TaskSenders::new();
        let events = TaskEvents::new(&senders);
        (senders, events)
    }

    #[test]
    fn on_output_returns_error_after_exit() {
        let (senders, events) = make_test_data();
        senders
            .on_exit_tx
            .send(Some(ExitStatus::from_raw(123)))
            .unwrap();
        events.on_output(|_| {}).unwrap_err();
    }

    #[tokio::test]
    async fn on_output_abort_handle_cancels_subscription() {
        let (senders, events) = make_test_data();
        let captured_output = Arc::new(Mutex::new(Vec::new()));
        let got_output = Arc::new(Notify::new());
        let abort_handle = events
            .on_output({
                let captured_output = captured_output.clone();
                let got_output = got_output.clone();
                move |o| {
                    captured_output.lock().unwrap().push(o);
                    got_output.notify_waiters();
                }
            })
            .unwrap();
        let output = ["some output", "another output"];
        assert_eq!(
            senders
                .stdout_tx
                .send(Arc::new(output[0].to_string()))
                .unwrap(),
            2
        );
        got_output.notified().await;
        assert!(!abort_handle.is_finished());
        abort_handle.abort();
        senders
            .stdout_tx
            .send(Arc::new(output[1].to_string()))
            .unwrap();
        events.join_all().await;
        let captured_output = captured_output.lock().unwrap();
        assert_eq!(captured_output.len(), 1);
        assert_eq!(*captured_output[0], output[0]);
    }

    #[tokio::test]
    async fn on_output_cancelation_token_cancels_task() {
        let (senders, events) = make_test_data();
        let captured_output = Arc::new(Mutex::new(Vec::new()));
        let got_output = Arc::new(Notify::new());
        events
            .on_output({
                let captured_output = captured_output.clone();
                let got_output = got_output.clone();
                move |o| {
                    captured_output.lock().unwrap().push(o);
                    got_output.notify_waiters();
                }
            })
            .unwrap();
        let output = ["some output", "another output"];
        assert_eq!(
            senders
                .stdout_tx
                .send(Arc::new(output[0].to_string()))
                .unwrap(),
            2
        );
        got_output.notified().await;
        events.cancel();
        yield_now().await;
        senders
            .stdout_tx
            .send(Arc::new(output[1].to_string()))
            .unwrap();
        events.join_all().await;
        let captured_output = captured_output.lock().unwrap();
        assert_eq!(captured_output.len(), 1);
        assert_eq!(*captured_output[0], output[0]);
    }
}
