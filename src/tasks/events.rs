use std::{process::ExitStatus, sync::Arc};

use tokio::{
    sync::{broadcast, watch},
    task::AbortHandle,
};
use tracing::warn;

use crate::tasks::{
    senders::TaskSenders,
    task_error::TaskError,
    tracker::{PanicHandler, WrappedTaskTracker},
};

pub trait TaskOutputCallback: FnMut(Arc<String>) + 'static + Send {}
impl<F> TaskOutputCallback for F where F: FnMut(Arc<String>) + 'static + Send {}

pub trait TaskExitCallback: FnOnce(ExitStatus) + 'static + Send {}
impl<F> TaskExitCallback for F where F: FnOnce(ExitStatus) + 'static + Send {}

#[derive(Debug)]
pub(in crate::tasks) struct TaskEvents {
    stdout_rx: broadcast::Receiver<Arc<String>>,
    on_exit_rx: watch::Receiver<Option<ExitStatus>>,
    related_tasks: WrappedTaskTracker,
}

impl TaskEvents {
    pub(in crate::tasks) fn new(senders: &TaskSenders) -> Self {
        Self {
            stdout_rx: senders.stdout_tx.subscribe(),
            on_exit_rx: senders.on_exit_tx.subscribe(),
            related_tasks: WrappedTaskTracker::new(PanicHandler::new_aborting()),
        }
    }

    pub(in crate::tasks) fn on_output<F>(&self, mut f: F) -> Result<AbortHandle, TaskError>
    where
        F: TaskOutputCallback,
    {
        if self.has_exited() {
            return Err(TaskError::AlreadyExited);
        }
        let mut stdout_rx = self.stdout_rx.resubscribe();
        self.related_tasks.spawn(async move {
            loop {
                match stdout_rx.recv().await {
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!("Stdout receiver is too slow. Have to skip {n} lines");
                    }
                    Err(_) => break,
                    Ok(line) => f(line),
                };
            }
        })
    }

    pub(in crate::tasks) fn on_exit<F>(&self, f: F) -> Result<AbortHandle, TaskError>
    where
        F: TaskExitCallback,
    {
        if self.has_exited() {
            return Err(TaskError::AlreadyExited);
        }
        let mut on_exit_rx = self.on_exit_rx.clone();
        self.related_tasks.spawn(async move {
            on_exit_rx.changed().await.expect("on_exit_rx.await");
            f(on_exit_rx.borrow_and_update().unwrap());
        })
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

    pub(in crate::tasks) async fn join_all(&self) {
        self.related_tasks.join().await;
    }
}

#[cfg(test)]
mod tests {
    use std::{
        os::unix::process::ExitStatusExt,
        pin::pin,
        sync::Mutex,
        task::{Context, Poll, Waker},
        time::Duration,
    };

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
        test_data
            .events
            .on_output(|_| panic!("The callback should never be called"))
            .unwrap_err();
        drop(test_data.senders);
        test_data.events.join_all().await;
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
        assert!(test_data.abort_handle.is_finished());

        let captured_output = test_data.captured_output.lock().unwrap();
        assert_eq!(captured_output.len(), output.len());
        for (i, &o) in output.iter().enumerate() {
            assert_eq!(*captured_output[i], o);
        }
    }

    #[tokio::test]
    async fn on_output_multiple_receivers() {
        let test_data = OnOutputTestsData::new();
        let captured_output2 = Arc::new(Mutex::new(Vec::new()));
        let got_output2 = Arc::new(Notify::new());
        let abort_handle2 = test_data
            .events
            .on_output({
                let captured_output2 = captured_output2.clone();
                let got_output2 = got_output2.clone();
                move |o| {
                    captured_output2.lock().unwrap().push(o);
                    got_output2.notify_waiters();
                }
            })
            .unwrap();
        let output = ["first output", "second output"];
        for o in &output {
            assert_eq!(
                test_data
                    .senders
                    .stdout_tx
                    .send(Arc::new(o.to_string()))
                    .unwrap(),
                3
            );
        }
        got_output2.notified().await;
        assert!(!abort_handle2.is_finished());
        abort_handle2.abort();
        let third_output = "third output";
        test_data
            .senders
            .stdout_tx
            .send(Arc::new(third_output.to_string()))
            .unwrap();
        drop(test_data.senders);
        test_data.events.join_all().await;
        let captured_output = test_data.captured_output.lock().unwrap();
        let captured_output2 = captured_output2.lock().unwrap();
        assert_eq!(captured_output.len(), 3);
        assert_eq!(captured_output2.len(), 2);
        for (i, &o) in output.iter().enumerate() {
            assert_eq!(*captured_output[i], o);
            assert_eq!(*captured_output2[i], o);
        }
        assert_eq!(*captured_output[2], third_output);
    }

    #[tokio::test]
    async fn on_output_subscriber_doesnt_receive_old_messages() {
        let senders = TaskSenders::new();
        let events = TaskEvents::new(&senders);
        let output = ["first", "second"];
        assert_eq!(
            senders
                .stdout_tx
                .send(Arc::new(output[0].to_string()))
                .unwrap(),
            1
        );
        let captured_output = Arc::new(Mutex::new(Vec::new()));
        events
            .on_output({
                let captured_output = captured_output.clone();
                move |o| {
                    captured_output.lock().unwrap().push(o);
                }
            })
            .unwrap();
        assert_eq!(
            senders
                .stdout_tx
                .send(Arc::new(output[1].to_string()))
                .unwrap(),
            2
        );
        drop(senders);
        events.join_all().await;
        let captured_output = captured_output.lock().unwrap();
        assert_eq!(captured_output.len(), 1);
        assert_eq!(*captured_output[0], output[1]);
    }

    struct OnExitTestData {
        senders: TaskSenders,
        events: TaskEvents,
        abort_handle: AbortHandle,
        captured_exit_codes: Arc<Mutex<Vec<ExitStatus>>>,
        exit_code: i32,
    }

    impl OnExitTestData {
        fn new() -> Self {
            let senders = TaskSenders::new();
            let events = TaskEvents::new(&senders);
            let captured_exit_codes = Arc::new(Mutex::new(Vec::new()));
            let abort_handle = events
                .on_exit({
                    let captured_exit_codes = captured_exit_codes.clone();
                    move |e| {
                        captured_exit_codes.lock().unwrap().push(e);
                    }
                })
                .unwrap();
            Self {
                senders,
                events,
                abort_handle,
                captured_exit_codes,
                exit_code: 123,
            }
        }

        fn send_code(&self) {
            self.senders
                .on_exit_tx
                .send(Some(ExitStatus::from_raw(self.exit_code)))
                .unwrap();
        }
    }

    #[tokio::test]
    async fn on_exit_returns_error_after_exit() {
        let senders = TaskSenders::new();
        let events = TaskEvents::new(&senders);
        senders
            .on_exit_tx
            .send(Some(ExitStatus::from_raw(123)))
            .unwrap();
        events
            .on_exit(|_| panic!("The callback should never be called"))
            .unwrap_err();
        drop(senders);
        events.join_all().await;
    }

    #[tokio::test]
    async fn on_exit_abort_handle_cancels_subscription() {
        let test_data = OnExitTestData::new();
        assert!(!test_data.abort_handle.is_finished());
        test_data.abort_handle.abort();
        test_data.send_code();
        drop(test_data.senders);
        test_data.events.join_all().await;
        assert!(test_data.captured_exit_codes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn on_exit_subscribes_to_exit_code() {
        let test_data = OnExitTestData::new();
        test_data.send_code();
        drop(test_data.senders);
        test_data.events.join_all().await;
        assert!(test_data.abort_handle.is_finished());
        let captured_exit_codes = test_data.captured_exit_codes.lock().unwrap();
        assert_eq!(captured_exit_codes.len(), 1);
        assert_eq!(captured_exit_codes[0].into_raw(), test_data.exit_code);
    }

    #[tokio::test]
    async fn on_exit_multiple_subscribers() {
        let test_data = OnExitTestData::new();
        let captured_exit_codes2 = Arc::new(Mutex::new(Vec::new()));
        test_data
            .events
            .on_exit({
                let captured_exit_codes2 = captured_exit_codes2.clone();
                move |e| {
                    captured_exit_codes2.lock().unwrap().push(e);
                }
            })
            .unwrap();
        test_data.send_code();
        drop(test_data.senders);
        test_data.events.join_all().await;
        for c in [&test_data.captured_exit_codes, &captured_exit_codes2] {
            let c = c.lock().unwrap();
            assert_eq!(c.len(), 1);
            assert_eq!(c[0].into_raw(), test_data.exit_code);
        }
    }

    #[tokio::test]
    async fn join_all_can_be_called_multiple_times() {
        let test_data = OnOutputTestsData::new();
        drop(test_data.senders);
        test_data.events.join_all().await;
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            test_data.events.join_all(),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn exit_status_returns_immediately_after_exit() {
        let senders = TaskSenders::new();
        let events = TaskEvents::new(&senders);
        let exit_code = 123;
        senders
            .on_exit_tx
            .send(Some(ExitStatus::from_raw(exit_code)))
            .unwrap();
        tokio::task::yield_now().await;
        let future = events.exit_status();
        let mut context = Context::from_waker(Waker::noop());
        let poll_result = pin!(future).poll(&mut context);
        let Poll::Ready(exit_status) = poll_result else {
            panic!("poll_result is expected Ready");
        };
        assert_eq!(exit_status.into_raw(), exit_code);
        events.join_all().await;
    }

    #[tokio::test]
    async fn exit_status_waits_for_exit_code() {
        let senders = TaskSenders::new();
        let events = Arc::new(TaskEvents::new(&senders));
        let exit_code = 123;
        let handle = tokio::spawn({
            let events = events.clone();
            async move {
                let exit_status = events.exit_status().await;
                assert_eq!(exit_status.into_raw(), exit_code);
            }
        });
        tokio::task::yield_now().await;
        assert!(!handle.is_finished());
        senders
            .on_exit_tx
            .send(Some(ExitStatus::from_raw(exit_code)))
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("handle didn't complete")
            .unwrap();
        events.join_all().await;
    }
}
