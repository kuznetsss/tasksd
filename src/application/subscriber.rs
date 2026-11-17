use std::sync::Arc;

use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::{
    api::{Notification, NotificationBody, SubscribeEventType, SubscribeEvents},
    tasks::{OutputLine, TaskEvent, TaskEventsStream, TaskExitNotificationGate, TaskId},
    transport::{ConnectionWriter, TransportError},
};

#[derive(Debug, PartialEq, Eq)]
pub(in crate::application) enum CreatingEvent {
    Start,
    Subscribe,
}

pub(in crate::application) struct Subscriber {
    writer_fields: WriterFieldsMutex,
    task_id: TaskId,
    events: TaskEventsStream,
    last_seen_line: Option<usize>,
    creating_event: CreatingEvent,
    cancel: CancellationToken,
    exit_notification_gate: TaskExitNotificationGate,
}

struct WriterFields {
    connection_writer: ConnectionWriter,
    subscribe_to_output: bool,
}
type WriterFieldsMutex = Arc<tokio::sync::Mutex<WriterFields>>;

pub struct SubscriberSendingGateGuard(tokio::sync::OwnedMutexGuard<WriterFields>);

impl Subscriber {
    pub fn new(
        connection_writer: ConnectionWriter,
        task_id: TaskId,
        subscribe_to_events: &SubscribeEvents,
        events: TaskEventsStream,
        creating_event: CreatingEvent,
        exit_notification_gate: TaskExitNotificationGate,
    ) -> Self {
        assert!(
            subscribe_to_events.contains(&SubscribeEventType::Exit),
            "Subscriber always subscribes to exit events"
        );
        let subscribe_to_output = subscribe_to_events.contains(&SubscribeEventType::Output);
        let writer_fields = tokio::sync::Mutex::new(WriterFields {
            connection_writer,
            subscribe_to_output,
        })
        .into();
        Self {
            writer_fields,
            task_id,
            events,
            last_seen_line: None,
            creating_event,
            cancel: CancellationToken::new(),
            exit_notification_gate,
        }
    }

    pub async fn sending_guard(&self) -> SubscriberSendingGateGuard {
        SubscriberSendingGateGuard(self.writer_fields.clone().lock_owned().await)
    }

    pub fn handle(&self) -> SubscriberHandle {
        SubscriberHandle {
            writer_fields: self.writer_fields.clone(),
            cancel: self.cancel.clone(),
        }
    }

    #[must_use]
    pub async fn run(mut self) -> Option<ExitEvent> {
        let exit_status = loop {
            let event = match self.cancel.run_until_cancelled(self.events.recv()).await {
                Some(e) => e,
                None => break None,
            };
            match event {
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    if let Err(e) = self.on_lag(n as usize).await {
                        warn!("Error sending missed_output: {e}");
                        break None;
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
                        break None;
                    }
                }
                Ok(TaskEvent::Exit(e)) => {
                    break Some(e);
                }
            }
        };
        exit_status.map(|s| ExitEvent {
            task_id: self.task_id,
            status: s,
            exit_notification_gate: self.exit_notification_gate,
            writer_fields: self.writer_fields,
            cancel: self.cancel,
        })
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
        let writer_fields = self.writer_fields.lock().await;
        if !writer_fields.subscribe_to_output {
            return Ok(());
        }
        let notification: Notification = NotificationBody::task_output(self.task_id, line).into();
        writer_fields
            .connection_writer
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

        let writer_fields = self.writer_fields.lock().await;
        if !writer_fields.subscribe_to_output {
            return Ok(());
        }
        let notification: Notification = NotificationBody::task_missed_output(
            self.task_id,
            prev_last_seen_line.map(|l| l + 1).unwrap_or(0),
            number_of_missed_lines,
        )
        .into();

        writer_fields
            .connection_writer
            .write(&notification.to_json_string())
            .await
    }
}

#[derive(Clone)]
pub(in crate::application) struct SubscriberHandle {
    writer_fields: Arc<tokio::sync::Mutex<WriterFields>>,
    cancel: CancellationToken,
}

impl SubscriberHandle {
    pub(in crate::application) fn stop(&self) {
        self.cancel.cancel();
    }

    pub(in crate::application) async fn lock(&self) -> SubscriberHandleLock {
        SubscriberHandleLock {
            writer_fields: self.writer_fields.clone().lock_owned().await,
        }
    }

    pub(in crate::application) async fn gate_guard(&self) -> SubscriberSendingGateGuard {
        SubscriberSendingGateGuard(self.writer_fields.clone().lock_owned().await)
    }
}

pub(in crate::application) struct SubscriberHandleLock {
    writer_fields: tokio::sync::OwnedMutexGuard<WriterFields>,
}

impl SubscriberHandleLock {
    pub(in crate::application) fn set_subscribe_to_output(&mut self, v: bool) {
        self.writer_fields.subscribe_to_output = v;
    }

    pub(in crate::application) fn into_gate_guard(self) -> SubscriberSendingGateGuard {
        SubscriberSendingGateGuard(self.writer_fields)
    }
}

#[must_use]
pub(in crate::application) struct ExitEvent {
    task_id: TaskId,
    status: std::process::ExitStatus,
    exit_notification_gate: TaskExitNotificationGate,
    writer_fields: WriterFieldsMutex,
    cancel: CancellationToken,
}

impl ExitEvent {
    pub(in crate::application) async fn send(mut self) {
        self.cancel
            .run_until_cancelled(async {
                self.exit_notification_gate.wait().await;
                let notification: Notification =
                    NotificationBody::task_exit(self.task_id, self.status).into();

                if let Err(e) = self
                    .writer_fields
                    .lock()
                    .await
                    .connection_writer
                    .write(&notification.to_json_string())
                    .await
                {
                    warn!("Error writing on_exit notification: {e}");
                }
            })
            .await;
    }
}
