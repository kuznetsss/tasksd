use tracing::warn;

use crate::{
    api::{
        Notification, NotificationBody, Request, RequestBody, Response, ResponseResult,
        TaskSendSignalParams, TaskStartParams,
    },
    tasks::{TaskCallbackError, TaskError, TaskManager},
    transport::ConnectionWriter,
};

use std::sync::Arc;

pub(in crate::application) struct Handler {
    connection_writer: ConnectionWriter,
    task_manager: Arc<TaskManager>,
}

impl Handler {
    pub(in crate::application) fn new(
        connection_writer: ConnectionWriter,
        task_manager: Arc<TaskManager>,
    ) -> Self {
        Self {
            connection_writer,
            task_manager,
        }
    }

    pub(in crate::application) async fn handle_request(&self, request: Request) {
        let response_body = match request.body {
            RequestBody::TaskStart(params) => self.start_task(params),
            RequestBody::TaskSendSignal(params) => self.send_signal(params),
        }
        .map_or_else(Into::into, Into::into);
        let response = Response::new(Some(request.id), response_body).to_json_string();
        if let Err(e) = self.connection_writer.write(&response).await {
            warn!("Error writing to connection: {e}")
        }
    }

    fn start_task(&self, params: TaskStartParams) -> Result<ResponseResult, TaskError> {
        let mut task_builder = self.task_manager.create_task(params.executable);
        if let Some(args) = params.args {
            task_builder.args(args);
        }
        if let Some(working_dir) = params.working_dir {
            task_builder.working_dir(working_dir);
        }
        let task_id = task_builder.task_id();
        if params.subscribe_to_output {
            task_builder.on_output({
                let connection_writer = self.connection_writer.clone();
                move |s| {
                    let connection_writer = connection_writer.clone();
                    let notification: Notification =
                        NotificationBody::task_output(task_id, s).into();
                    async move {
                        connection_writer
                            .write(&notification.to_json_string())
                            .await
                            .map_err(|_| TaskCallbackError::ShouldExit)
                    }
                }
            });
        }
        task_builder.on_exit({
            let connection_writer = self.connection_writer.clone();
            move |s| {
                let notification: Notification = NotificationBody::task_exit(task_id, s).into();
                async move {
                    if let Err(e) = connection_writer
                        .write(&notification.to_json_string())
                        .await
                    {
                        warn!("Error writing on_exit notification: {e}");
                    }
                }
            }
        });
        task_builder
            .submit()
            .map(|_| ResponseResult::StartTaskResult { task_id })
    }

    fn send_signal(&self, params: TaskSendSignalParams) -> Result<ResponseResult, TaskError> {
        let task = match self.task_manager.get_task(params.task_id) {
            Some(t) => t,
            // TODO: return not found here
            None => return Err(TaskError::AlreadyExited),
        };
        task.send_signal(params.signal)
            .map(|_| ResponseResult::SendSignalResult {})
    }
}
