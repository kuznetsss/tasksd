use std::{
    process::ExitStatus,
    sync::{Arc, Mutex},
};

use tokio::{
    sync::{broadcast, watch},
    task::{AbortHandle, JoinSet},
};
use tracing::warn;

use crate::tasks::{senders::TaskSenders, task_error::TaskError};

pub trait TaskOutputCallback: FnMut(Arc<String>) + 'static + Send {}
impl<F> TaskOutputCallback for F where F: FnMut(Arc<String>) + 'static + Send {}

pub trait TaskExitCallback: FnOnce(ExitStatus) + 'static + Send {}
impl<F> TaskExitCallback for F where F: FnOnce(ExitStatus) + 'static + Send {}

#[derive(Debug)]
pub(in crate::tasks) struct TaskEvents {
    stdout_rx: broadcast::Receiver<Arc<String>>,
    on_exit_rx: watch::Receiver<Option<ExitStatus>>,
    related_tasks: Mutex<Option<JoinSet<()>>>,
}

impl TaskEvents {
    pub(in crate::tasks) fn new(senders: &TaskSenders) -> Self {
        Self {
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
        let mut related_tasks = self.related_tasks.lock().unwrap();
        if self.has_exited() {
            return Err(TaskError::AlreadyExited);
        }
        let abort_handle = related_tasks.as_mut().unwrap().spawn(async move {
            loop {
                match stdout_rx.recv().await {
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

    pub(in crate::tasks) fn abort(&self) {
        // CancellationToken doesn't immediately abort tasks but waits for pending future, so using JoinSet's abort here
        self.related_tasks
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .abort_all();
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

    use tokio::sync::Notify;

    use crate::tasks::senders::CHANNEL_CAPACITY;

    use super::*;

    struct OnOutputTestsData {
        senders: TaskSenders,
        events: TaskEvents,
        captured_output: Arc<Mutex<Vec<Arc<String>>>>,
        abort_handle: AbortHandle,
        got_output: Arc<Notify>,
    }

    impl OnOutputTestsData {
        fn new() -> Self {
            let senders = TaskSenders::new();
            let events = TaskEvents::new(&senders);
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
            Self {
                senders,
                events,
                captured_output,
                abort_handle,
                got_output,
            }
        }
    }

    #[tokio::test]
    async fn on_output_returns_error_after_exit() {
        let test_data = OnOutputTestsData::new();
        test_data
            .senders
            .on_exit_tx
            .send(Some(ExitStatus::from_raw(123)))
            .unwrap();
        test_data.events.on_output(|_| {}).unwrap_err();
    }

    #[tokio::test]
    async fn on_output_abort_handle_cancels_subscription() {
        let test_data = OnOutputTestsData::new();
        let output = ["some output", "another output"];
        assert_eq!(
            test_data
                .senders
                .stdout_tx
                .send(Arc::new(output[0].to_string()))
                .unwrap(),
            2
        );
        test_data.got_output.notified().await;
        assert!(!test_data.abort_handle.is_finished());
        test_data.abort_handle.abort();
        test_data
            .senders
            .stdout_tx
            .send(Arc::new(output[1].to_string()))
            .unwrap();
        test_data.events.join_all().await;
        let captured_output = test_data.captured_output.lock().unwrap();
        assert_eq!(captured_output.len(), 1);
        assert_eq!(*captured_output[0], output[0]);
    }

    #[tokio::test]
    async fn on_output_abort_cancels_task() {
        let test_data = OnOutputTestsData::new();
        let output = ["some output", "another output"];
        assert_eq!(
            test_data
                .senders
                .stdout_tx
                .send(Arc::new(output[0].to_string()))
                .unwrap(),
            2
        );
        test_data.got_output.notified().await;
        test_data.events.abort();
        tokio::task::yield_now().await;
        test_data
            .senders
            .stdout_tx
            .send(Arc::new(output[1].to_string()))
            .unwrap();
        test_data.events.join_all().await;
        let captured_output = test_data.captured_output.lock().unwrap();
        assert_eq!(captured_output.len(), 1);
        assert_eq!(*captured_output[0], output[0]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn on_output_slow_receiver() {
        let test_data = OnOutputTestsData::new();
        for i in 0..CHANNEL_CAPACITY * 2 {
            test_data
                .senders
                .stdout_tx
                .send(Arc::new(i.to_string()))
                .unwrap();
            assert!(
                test_data.captured_output.lock().unwrap().is_empty(),
                "receiver should not have run during the send burst"
            );
        }

        drop(test_data.senders);
        test_data.events.join_all().await;
        let captured_output = test_data.captured_output.lock().unwrap();
        assert_eq!(captured_output.len(), CHANNEL_CAPACITY);
        for i in 0..CHANNEL_CAPACITY {
            assert_eq!(*captured_output[i], (CHANNEL_CAPACITY + i).to_string());
        }
    }

    #[tokio::test]
    async fn on_output_subscribes_to_stdout_tx() {
        let test_data = OnOutputTestsData::new();
        let output = ["some output", "another output"];
        for o in output {
            assert_eq!(
                test_data
                    .senders
                    .stdout_tx
                    .send(Arc::new(o.to_string()))
                    .unwrap(),
                2
            );
        }
        drop(test_data.senders);
        test_data.events.join_all().await;

        let captured_output = test_data.captured_output.lock().unwrap();
        assert_eq!(captured_output.len(), output.len());
        for (i, &o) in output.iter().enumerate() {
            assert_eq!(*captured_output[i], o);
        }
    }

    // TODO:
    // - multiple concurrent subscribers each receive every message (fan-out)
    // - late subscriber misses messages sent before on_output() was called (resubscribe semantics)
    // - panic in the callback propagates through join_all()
    // - abort_handle.is_finished() becomes true after the task exits naturally (senders dropped)
}
