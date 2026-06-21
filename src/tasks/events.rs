use std::{process::ExitStatus, sync::Arc};

use tokio::{
    sync::{broadcast, watch},
    task::AbortHandle,
};
use tracing::warn;

use crate::tasks::{
    senders::{TaskEvent, TaskSender},
    task_error::TaskError,
    tracker::{PanicHandler, WrappedTaskTracker},
};

pub trait TaskOutputCallback: (FnMut(Arc<String>) -> Self::Future) + 'static + Send {
    type Future: Future<Output = Result<(), TaskCallbackError>> + Send;
}
impl<T, F> TaskOutputCallback for T
where
    T: (FnMut(Arc<String>) -> F) + 'static + Send,
    F: Future<Output = Result<(), TaskCallbackError>> + Send,
{
    type Future = F;
}

pub trait TaskExitCallback: (FnOnce(ExitStatus) -> Self::Future) + 'static + Send {
    type Future: Future<Output = ()> + Send;
}
impl<T, F> TaskExitCallback for T
where
    T: (FnOnce(ExitStatus) -> F) + 'static + Send,
    F: Future<Output = ()> + Send,
{
    type Future = F;
}

pub enum TaskCallbackError {
    ShouldExit,
}

#[derive(Debug)]
pub(in crate::tasks) struct TaskEvents {
    events_rx: broadcast::Receiver<TaskEvent>,
    related_tasks: WrappedTaskTracker,
    exit_rx: watch::Receiver<Option<ExitStatus>>,
}

impl TaskEvents {
    pub(in crate::tasks) fn new(sender: &TaskSender) -> Self {
        let (exit_tx, exit_rx) = watch::channel(None);
        let events = Self {
            events_rx: sender.0.subscribe(),
            related_tasks: WrappedTaskTracker::new(PanicHandler::new_aborting()),
            exit_rx,
        };
        events
            .on_exit(move |e| {
                exit_tx
                    .send(Some(e))
                    .expect("Receiver should still be alive");
                async {}
            })
            .expect("related_tasks is not exited yet");
        events
    }

    pub(in crate::tasks) fn on_output<F>(&self, mut f: F) -> Result<AbortHandle, TaskError>
    where
        F: TaskOutputCallback,
    {
        if self.has_exited() {
            return Err(TaskError::AlreadyExited);
        }
        let mut events_rx = self.events_rx.resubscribe();
        self.related_tasks.spawn(async move {
            loop {
                let output = match events_rx.recv().await {
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!("Output receiver is too slow. Have to skip {n} lines");
                        continue;
                    }
                    Err(_) => break,
                    Ok(TaskEvent::Output(o)) => o,
                    Ok(TaskEvent::Exit(_)) => break,
                };
                if f(output).await.is_err() {
                    break;
                }
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
        let mut events_rx = self.events_rx.resubscribe();
        self.related_tasks.spawn(async move {
            loop {
                let exit_status = match events_rx.recv().await {
                    Ok(TaskEvent::Exit(e)) => e,
                    Ok(_) => continue,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(_) => break,
                };
                f(exit_status).await;
                break;
            }
        })
    }

    pub(in crate::tasks) fn has_exited(&self) -> bool {
        self.exit_rx.has_changed().unwrap_or(true)
    }

    pub(in crate::tasks) async fn exit_status(&self) -> ExitStatus {
        if self.has_exited() {
            return self.exit_rx.borrow().unwrap();
        }
        let mut exit_rx = self.exit_rx.clone();
        exit_rx.changed().await.unwrap();
        exit_rx.borrow().unwrap()
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
        sync::{Mutex, atomic::AtomicUsize},
        task::{Context, Poll, Waker},
        time::Duration,
    };

    use tokio::sync::Notify;

    use crate::tasks::senders::CHANNEL_CAPACITY;

    use super::*;

    struct OnOutputTestsData {
        sender: TaskSender,
        events: TaskEvents,
        captured_output: Arc<Mutex<Vec<Arc<String>>>>,
        abort_handle: AbortHandle,
        got_output: Arc<Notify>,
    }

    impl OnOutputTestsData {
        fn new() -> Self {
            let sender = TaskSender::new();
            let events = TaskEvents::new(&sender);
            let captured_output = Arc::new(Mutex::new(Vec::new()));
            let got_output = Arc::new(Notify::new());
            let abort_handle = events
                .on_output({
                    let captured_output = captured_output.clone();
                    let got_output = got_output.clone();
                    move |o| {
                        captured_output.lock().unwrap().push(o);
                        got_output.notify_waiters();
                        async { Ok(()) }
                    }
                })
                .unwrap();
            Self {
                sender,
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
            .sender
            .0
            .send(ExitStatus::from_raw(123).into())
            .unwrap();
        tokio::task::yield_now().await;
        test_data
            .events
            .on_output(|_| async { panic!("The callback should never be called") })
            .unwrap_err();
        drop(test_data.sender);
        test_data.events.join_all().await;
    }

    #[tokio::test]
    async fn on_output_abort_handle_cancels_subscription() {
        let test_data = OnOutputTestsData::new();
        let output = ["some output", "another output"];
        assert_eq!(
            test_data
                .sender
                .0
                .send(output[0].to_string().into())
                .unwrap(),
            3
        );
        test_data.got_output.notified().await;
        assert!(!test_data.abort_handle.is_finished());
        test_data.abort_handle.abort();
        test_data
            .sender
            .0
            .send(output[1].to_string().into())
            .unwrap();
        drop(test_data.sender);
        test_data.events.join_all().await;
        let captured_output = test_data.captured_output.lock().unwrap();
        assert_eq!(captured_output.len(), 1);
        assert_eq!(*captured_output[0], output[0]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn on_output_slow_receiver() {
        let test_data = OnOutputTestsData::new();
        for i in 0..CHANNEL_CAPACITY * 2 {
            test_data.sender.0.send(i.to_string().into()).unwrap();
            assert!(
                test_data.captured_output.lock().unwrap().is_empty(),
                "receiver should not have run during the send burst"
            );
        }

        drop(test_data.sender);
        test_data.events.join_all().await;
        let captured_output = test_data.captured_output.lock().unwrap();
        assert_eq!(captured_output.len(), CHANNEL_CAPACITY);
        for i in 0..CHANNEL_CAPACITY {
            assert_eq!(*captured_output[i], (CHANNEL_CAPACITY + i).to_string());
        }
    }

    #[tokio::test]
    async fn on_output_subscribes_to_output_tx() {
        let test_data = OnOutputTestsData::new();
        let output = ["some output", "another output"];
        for o in output {
            assert_eq!(test_data.sender.0.send(o.to_string().into()).unwrap(), 3);
        }
        drop(test_data.sender);
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
                    async { Ok(()) }
                }
            })
            .unwrap();
        let output = ["first output", "second output"];
        for o in &output {
            assert_eq!(test_data.sender.0.send(o.to_string().into()).unwrap(), 4);
        }
        got_output2.notified().await;
        assert!(!abort_handle2.is_finished());
        abort_handle2.abort();
        let third_output = "third output";
        test_data
            .sender
            .0
            .send(third_output.to_string().into())
            .unwrap();
        drop(test_data.sender);
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
        let sender = TaskSender::new();
        let events = TaskEvents::new(&sender);
        let output = ["first", "second"];
        assert_eq!(sender.0.send(output[0].to_string().into()).unwrap(), 2);
        let captured_output = Arc::new(Mutex::new(Vec::new()));
        events
            .on_output({
                let captured_output = captured_output.clone();
                move |o| {
                    captured_output.lock().unwrap().push(o);
                    async { Ok(()) }
                }
            })
            .unwrap();
        assert_eq!(sender.0.send(output[1].to_string().into()).unwrap(), 3);
        drop(sender);
        events.join_all().await;
        let captured_output = captured_output.lock().unwrap();
        assert_eq!(captured_output.len(), 1);
        assert_eq!(*captured_output[0], output[1]);
    }

    #[tokio::test]
    async fn on_output_subscriber_is_unsubscribed_after_returning_error() {
        let sender = TaskSender::new();
        let events = TaskEvents::new(&sender);
        let call_count = Arc::new(AtomicUsize::new(0));
        events
            .on_output({
                let call_count = call_count.clone();
                move |_| {
                    call_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    async { Err(TaskCallbackError::ShouldExit) }
                }
            })
            .unwrap();
        for _ in 0..10 {
            sender.0.send("some line".to_string().into()).unwrap();
        }
        drop(sender);
        tokio::time::timeout(std::time::Duration::from_secs(1), events.join_all())
            .await
            .unwrap();
        assert_eq!(call_count.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    struct OnExitTestData {
        sender: TaskSender,
        events: TaskEvents,
        abort_handle: AbortHandle,
        captured_exit_codes: Arc<Mutex<Vec<ExitStatus>>>,
        exit_code: i32,
    }

    impl OnExitTestData {
        fn new() -> Self {
            let sender = TaskSender::new();
            let events = TaskEvents::new(&sender);
            let captured_exit_codes = Arc::new(Mutex::new(Vec::new()));
            let abort_handle = events
                .on_exit({
                    let captured_exit_codes = captured_exit_codes.clone();
                    move |e| {
                        captured_exit_codes.lock().unwrap().push(e);
                        async {}
                    }
                })
                .unwrap();
            Self {
                sender,
                events,
                abort_handle,
                captured_exit_codes,
                exit_code: 123,
            }
        }

        fn send_code(&self) {
            self.sender
                .0
                .send(ExitStatus::from_raw(self.exit_code).into())
                .unwrap();
        }
    }

    #[tokio::test]
    async fn on_exit_returns_error_after_exit() {
        let sender = TaskSender::new();
        let events = TaskEvents::new(&sender);
        sender.0.send(ExitStatus::from_raw(123).into()).unwrap();
        tokio::task::yield_now().await;
        events
            .on_exit(|_| async { panic!("The callback should never be called") })
            .unwrap_err();
        drop(sender);
        events.join_all().await;
    }

    #[tokio::test]
    async fn on_exit_abort_handle_cancels_subscription() {
        let test_data = OnExitTestData::new();
        assert!(!test_data.abort_handle.is_finished());
        test_data.abort_handle.abort();
        test_data.send_code();
        drop(test_data.sender);
        test_data.events.join_all().await;
        assert!(test_data.captured_exit_codes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn on_exit_subscribes_to_exit_code() {
        let test_data = OnExitTestData::new();
        test_data.send_code();
        drop(test_data.sender);
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
                    async {}
                }
            })
            .unwrap();
        test_data.send_code();
        drop(test_data.sender);
        test_data.events.join_all().await;
        for c in [&test_data.captured_exit_codes, &captured_exit_codes2] {
            let c = c.lock().unwrap();
            assert_eq!(c.len(), 1);
            assert_eq!(c[0].into_raw(), test_data.exit_code);
        }
    }

    #[tokio::test]
    async fn on_exit_sender_dropped() {
        let test_data = OnExitTestData::new();
        drop(test_data.sender);
        assert!(test_data.captured_exit_codes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn join_all_can_be_called_multiple_times() {
        let test_data = OnOutputTestsData::new();
        drop(test_data.sender);
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
        let sender = TaskSender::new();
        let events = TaskEvents::new(&sender);
        let exit_code = 123;
        sender
            .0
            .send(ExitStatus::from_raw(exit_code).into())
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
        let sender = TaskSender::new();
        let events = Arc::new(TaskEvents::new(&sender));
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
        sender
            .0
            .send(ExitStatus::from_raw(exit_code).into())
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("handle didn't complete")
            .unwrap();
        events.join_all().await;
    }

    #[tokio::test]
    async fn has_exited_returns_true_after_exit() {
        let sender = TaskSender::new();
        let events = TaskEvents::new(&sender);
        assert!(!events.has_exited());
        sender.0.send(ExitStatus::default().into()).unwrap();
        tokio::task::yield_now().await;
        assert!(events.has_exited());
    }
}
