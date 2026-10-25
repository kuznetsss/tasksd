use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use tokio::sync::{broadcast, watch};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::{
    api::{Notification, NotificationBody, SubscribeEventType, SubscribeEvents},
    tasks::{OutputLine, TaskEvent, TaskEventsStream, TaskId},
    transport::{ConnectionWriter, TransportError},
};

#[derive(Debug, PartialEq, Eq)]
pub(in crate::application) enum CreatingEvent {
    Start,
    Subscribe,
}

pub(in crate::application) struct Subscriber {
    connection_writer: ConnectionWriter,
    task_id: TaskId,
    subscribe_to_output: Arc<AtomicBool>,
    events: TaskEventsStream,
    last_seen_line: Option<usize>,
    creating_event: CreatingEvent,
    cancel: CancellationToken,
    completion_sender: watch::Sender<bool>,
    completion_receiver: watch::Receiver<bool>,
}

impl Subscriber {
    pub fn new(
        connection_writer: ConnectionWriter,
        task_id: TaskId,
        subscribe_to_events: &SubscribeEvents,
        events: TaskEventsStream,
        creating_event: CreatingEvent,
    ) -> Self {
        assert!(
            subscribe_to_events.0.contains(&SubscribeEventType::Exit),
            "Subscriber always subscribes to exit events"
        );
        let subscribe_to_output = subscribe_to_events.0.contains(&SubscribeEventType::Output);
        let (completion_sender, completion_receiver) = watch::channel(false);
        Self {
            connection_writer,
            task_id,
            subscribe_to_output: Arc::new(subscribe_to_output.into()),
            events,
            last_seen_line: None,
            creating_event,
            cancel: CancellationToken::new(),
            completion_sender,
            completion_receiver,
        }
    }

    pub fn handle(&self) -> SubscriberHandle {
        SubscriberHandle {
            subscribe_to_output: self.subscribe_to_output.clone(),
            cancel: self.cancel.clone(),
            completion_receiver: self.completion_receiver.clone(),
        }
    }

    pub async fn run(mut self) {
        loop {
            let event = match self.cancel.run_until_cancelled(self.events.recv()).await {
                Some(e) => e,
                None => break,
            };
            match event {
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    if let Err(e) = self.on_lag(n as usize).await {
                        warn!("Error sending missed_output: {e}");
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Closed) => {
                    panic!(
                        "Task {} was dropped without sending Exit event",
                        self.task_id
                    );
                }
                Ok(TaskEvent::Output(line)) => {
                    if let Err(e) = self.on_output(line).await {
                        warn!("Error sending output: {e}");
                        break;
                    }
                }
                Ok(TaskEvent::Exit(e)) => {
                    self.on_exit(e).await;
                    break;
                }
            }
        }
    }

    async fn on_output(&mut self, line: std::sync::Arc<OutputLine>) -> Result<(), TransportError> {
        if let Some(l) = self.last_seen_line {
            assert_eq!(
                line.line_number,
                l + 1,
                "output line numbers must be contiguous with lag accounting"
            );
        }

        self.last_seen_line = Some(line.line_number);
        if !self.subscribe_to_output.load(Ordering::Relaxed) {
            return Ok(());
        }
        let notification: Notification = NotificationBody::task_output(self.task_id, line).into();
        self.connection_writer
            .write(&notification.to_json_string())
            .await
    }

    async fn on_lag(&mut self, number_of_missed_lines: usize) -> Result<(), TransportError> {
        let prev_last_seen_line = self.last_seen_line;
        if prev_last_seen_line.is_none() && self.creating_event != CreatingEvent::Start {
            // Can't send missed output notification without knowing the last seen line
            return Ok(());
        }

        assert_ne!(
            number_of_missed_lines, 0,
            "tokio should never provide Lagged(0)"
        );
        self.last_seen_line = self
            .last_seen_line
            .map(|l| l + number_of_missed_lines)
            .or_else(|| Some(number_of_missed_lines - 1));

        if !self.subscribe_to_output.load(Ordering::Relaxed) {
            return Ok(());
        }
        let notification: Notification = NotificationBody::task_missed_output(
            self.task_id,
            prev_last_seen_line.map(|l| l + 1).unwrap_or(0),
            number_of_missed_lines,
        )
        .into();

        self.connection_writer
            .write(&notification.to_json_string())
            .await
    }

    async fn on_exit(&mut self, status: std::process::ExitStatus) {
        let notification: Notification = NotificationBody::task_exit(self.task_id, status).into();
        if let Err(e) = self
            .connection_writer
            .write(&notification.to_json_string())
            .await
        {
            warn!("Error writing on_exit notification: {e}");
        }
    }
}

#[derive(Clone)]
pub(in crate::application) struct SubscriberHandle {
    subscribe_to_output: Arc<AtomicBool>,
    cancel: CancellationToken,
    completion_receiver: watch::Receiver<bool>,
}

impl SubscriberHandle {
    pub(in crate::application) fn set_subscribe_to_output(&self, v: bool) {
        self.subscribe_to_output.store(v, Ordering::Relaxed);
    }

    pub(in crate::application) async fn stop(&mut self) {
        self.cancel.cancel();
        // Error means already exited so ignoring it
        let _ = self.completion_receiver.changed().await;
    }
}
