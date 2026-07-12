use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use tokio::sync::broadcast;
use tracing::warn;

use crate::{
    api::{Notification, NotificationBody},
    tasks::{OutputLine, TaskEvent, TaskEventsStream, TaskId},
    transport::{ConnectionWriter, TransportError},
};

pub(in crate::application) struct Subscriber {
    connection_writer: ConnectionWriter,
    task_id: TaskId,
    subscribe_to_output: Arc<AtomicBool>,
    events: TaskEventsStream,
    last_seen_line: Option<usize>,
}

impl Subscriber {
    pub(in crate::application) fn new(
        connection_writer: ConnectionWriter,
        task_id: TaskId,
        subscribe_to_output: bool,
        events: TaskEventsStream,
    ) -> Self {
        Self {
            connection_writer,
            task_id,
            subscribe_to_output: Arc::new(AtomicBool::new(subscribe_to_output)),
            events,
            last_seen_line: None,
        }
    }

    pub fn handle(&self) -> SubscriberHandle {
        SubscriberHandle {
            subscribe_to_output: self.subscribe_to_output.clone(),
        }
    }

    pub async fn run(mut self) {
        loop {
            match self.events.recv().await {
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    if let Err(e) = self.on_lag(n as usize).await {
                        warn!("Error sending missed_output: {e}");
                        return;
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
                        return;
                    }
                }
                Ok(TaskEvent::Exit(e)) => {
                    self.on_exit(e).await;
                    return;
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
        if prev_last_seen_line.is_none() {
            // Can't send missed output notification without knowing the last seen line
            return Ok(());
        }

        assert_ne!(
            number_of_missed_lines, 0,
            "tokio should never provide Lagged(0)"
        );
        self.last_seen_line = self.last_seen_line.map(|l| l + number_of_missed_lines);

        if !self.subscribe_to_output.load(Ordering::Relaxed) {
            return Ok(());
        }
        let notification: Notification = NotificationBody::task_missed_output(
            self.task_id,
            prev_last_seen_line
                .map(|l| l + 1)
                .expect("Need to know prev_last_seen_line to send missed_output notification"),
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

pub(in crate::application) struct SubscriberHandle {
    subscribe_to_output: Arc<AtomicBool>,
}

impl SubscriberHandle {
    pub(in crate::application) fn set_subscribe_to_output(&self, v: bool) {
        self.subscribe_to_output.store(v, Ordering::Relaxed);
    }
}
