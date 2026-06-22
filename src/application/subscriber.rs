use tracing::warn;

use crate::{
    api::{Notification, NotificationBody},
    tasks::{TaskEventsSubscriber, TaskId, TaskSubscriberError},
    transport::ConnectionWriter,
};

pub(in crate::application) struct Subscriber {
    connection_writer: ConnectionWriter,
    task_id: TaskId,
    subscribe_to_output: bool,
}

impl Subscriber {
    pub(in crate::application) fn new(
        connection_writer: ConnectionWriter,
        task_id: TaskId,
        subscribe_to_output: bool,
    ) -> Self {
        Self {
            connection_writer,
            task_id,
            subscribe_to_output,
        }
    }
}

impl TaskEventsSubscriber for Subscriber {
    fn on_output(
        &mut self,
        line: std::sync::Arc<String>,
    ) -> impl Future<Output = Result<(), TaskSubscriberError>> + Send {
        async move {
            if self.subscribe_to_output {
                let notification: Notification =
                    NotificationBody::task_output(self.task_id, line).into();
                self.connection_writer
                    .write(&notification.to_json_string())
                    .await
                    .map_err(|_| TaskSubscriberError::ShouldExit)
            } else {
                Ok(())
            }
        }
    }

    fn on_exit(&mut self, status: std::process::ExitStatus) -> impl Future<Output = ()> + Send {
        async move {
            let notification: Notification =
                NotificationBody::task_exit(self.task_id, status).into();
            if let Err(e) = self
                .connection_writer
                .write(&notification.to_json_string())
                .await
            {
                warn!("Error writing on_exit notification: {e}");
            }
        }
    }
}

