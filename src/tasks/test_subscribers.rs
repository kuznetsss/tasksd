#![cfg_attr(coverage_nightly, coverage(off))]
use std::{
    process::ExitStatus,
    sync::{Arc, Mutex},
};

use tokio::{sync::Notify, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::tasks::{TaskEventsStream, sender::TaskEvent};

#[derive(Default)]
pub struct CapturingSubscriber {
    pub captured_output: Arc<Mutex<Vec<Arc<String>>>>,
    pub captured_exit_codes: Arc<Mutex<Vec<ExitStatus>>>,
    pub got_output: Arc<Notify>,
    pub cancel: CancellationToken,
    pub join_handle: JoinHandle<()>,
}

impl CapturingSubscriber {
    fn spawn(stream: TaskEventsStream) -> Self {
        let mut s = CapturingSubscriber::default();
        s.join_handle = tokio::spawn({
            let captured_output = s.captured_output.clone();
            let captured_exit_codes = s.captured_exit_codes.clone();
            let got_output = s.got_output.clone();
            let cancel = s.cancel.clone();
            async move {
                while let Some(Ok(event)) = cancel.run_until_cancelled(stream.recv()).await {
                    match event {
                        TaskEvent::Output(o) => {
                            captured_output.lock().unwrap().push(o);
                            got_output.notify_one();
                        }
                        TaskEvent::Exit(e) => {
                            captured_exit_codes.lock().unwrap().push(e);
                        }
                    };
                }
            }
        });
        s
    }
}

/*
#[derive(Debug, PartialEq, Clone)]
pub enum Event {
    Output,
    Exit,
}

pub struct EventsCapturingSubscriber {
    pub captured_events: Arc<Mutex<Vec<Event>>>,
}

impl TaskEventsSubscriber for EventsCapturingSubscriber {
    fn on_output(
        &mut self,
        _: Arc<String>,
    ) -> impl Future<Output = std::result::Result<(), TaskSubscriberError>> + Send {
        self.captured_events.lock().unwrap().push(Event::Output);
        async { Ok(()) }
    }

    fn on_exit(&mut self, _: ExitStatus) -> impl Future<Output = ()> + Send {
        self.captured_events.lock().unwrap().push(Event::Exit);
        async {}
    }
}
*/
