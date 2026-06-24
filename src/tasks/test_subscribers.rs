use std::{
    process::ExitStatus,
    sync::{Arc, Mutex},
};

use tokio::sync::Notify;

use crate::tasks::{TaskEventsSubscriber, TaskSubscriberError};

#[derive(Debug)]
pub struct NoopSubscriber {}

impl TaskEventsSubscriber for NoopSubscriber {
    #[allow(clippy::manual_async_fn)]
    fn on_output(
        &mut self,
        _: Arc<String>,
    ) -> impl Future<Output = std::result::Result<(), crate::tasks::events::TaskSubscriberError>> + Send
    {
        async { Ok(()) }
    }

    #[allow(clippy::manual_async_fn)]
    fn on_exit(&mut self, _: ExitStatus) -> impl Future<Output = ()> + Send {
        async {}
    }
}

#[derive(Default)]
pub struct CapturingSubscriber {
    pub captured_output: Arc<Mutex<Vec<Arc<String>>>>,
    pub captured_exit_codes: Arc<Mutex<Vec<ExitStatus>>>,
    pub got_output: Arc<Notify>,
}

impl TaskEventsSubscriber for CapturingSubscriber {
    fn on_output(
        &mut self,
        line: Arc<String>,
    ) -> impl Future<Output = Result<(), TaskSubscriberError>> + Send {
        self.captured_output.lock().unwrap().push(line);
        self.got_output.notify_waiters();
        async { Ok(()) }
    }

    fn on_exit(&mut self, status: ExitStatus) -> impl Future<Output = ()> + Send {
        self.captured_exit_codes.lock().unwrap().push(status);
        async {}
    }
}
