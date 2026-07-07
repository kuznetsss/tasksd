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
    subscribe_to_output: bool,
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
            subscribe_to_output,
            events,
            last_seen_line: None,
        }
    }

    pub async fn run(mut self) {
        loop {
            match self.events.recv().await {
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    if let Err(e) = self.on_lag(n.try_into().unwrap()).await {
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
        self.last_seen_line = Some(line.line_number);
        if !self.subscribe_to_output {
            return Ok(());
        }
        let notification: Notification = NotificationBody::task_output(self.task_id, line).into();
        self.connection_writer
            .write(&notification.to_json_string())
            .await
    }

    async fn on_lag(&mut self, number_of_missed_lines: usize) -> Result<(), TransportError> {
        if !self.subscribe_to_output {
            return Ok(());
        }
        let notification: Notification = NotificationBody::task_missed_output(
            self.task_id,
            self.last_seen_line.map(|l| l + 1).unwrap_or(0),
            number_of_missed_lines,
        )
        .into();

        self.last_seen_line = self
            .last_seen_line
            .map(|l| l + number_of_missed_lines)
            .or(Some(number_of_missed_lines - 1));
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
