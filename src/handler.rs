use tracing::info;

use crate::{
    api::{
        JsonRpcVersion, Notification, NotificationBody, Request, RequestBody, Response,
        ResponseBody, ResponseResult, TaskExitParams, TaskOutputParams,
    },
    tasks::{TaskCallbackError, TaskError, TaskId, TaskManager},
    transport::ConnectionWriter,
};

use std::{os::unix::process::ExitStatusExt, sync::Arc};

pub struct Handler {
    connection_writer: ConnectionWriter,
    task_manager: Arc<TaskManager>,
}

impl Handler {
    pub fn new(connection_writer: ConnectionWriter, task_manager: Arc<TaskManager>) -> Self {
        Self {
            connection_writer,
            task_manager,
        }
    }

    pub async fn handle_request(&self, request: Request) {
        let response_body = match self.process_request(request.body) {
            Ok(r) => ResponseBody::Result(r),
            Err(e) => e.into(),
        };
        let response = Response {
            jsonrpc: JsonRpcVersion {},
            id: Some(request.id),
            body: response_body,
        };
        let response_str = serde_json::to_string(&response)
            .unwrap_or_else(|_| panic!("Error serializing response: {response:?}"));
        if let Err(e) = self.connection_writer.write(&response_str).await {
            info!("Error writing to connection: {e}")
        }
    }

    fn process_request(&self, request_body: RequestBody) -> Result<ResponseResult, TaskError> {
        match request_body {
            RequestBody::TaskStart(task_start_params) => {
                let mut task_builder = self.task_manager.create_task(task_start_params.executable);
                if let Some(args) = task_start_params.args {
                    task_builder.args(args);
                }
                if let Some(working_dir) = task_start_params.working_dir {
                    task_builder.working_dir(working_dir);
                }
                let task_id = task_builder.task_id();
                if task_start_params.subscribe_to_output {
                    task_builder.on_output({
                        let connection_writer = self.connection_writer.clone();
                        move |s| {
                            let connection_writer = connection_writer.clone();
                            let body =
                                NotificationBody::TaskOutput(TaskOutputParams { task_id, line: s });
                            let notification = Notification {
                                jsonrpc: JsonRpcVersion {},
                                body,
                            };
                            let notification_str = serde_json::to_string(&notification)
                                .expect("Serialization shouldn't fail");
                            async move {
                                connection_writer
                                    .write(&notification_str)
                                    .await
                                    .map_err(|_| TaskCallbackError::ShouldExit)
                            }
                        }
                    });
                }
                task_builder.on_exit({
                    let connection_writer = self.connection_writer.clone();
                    move |s| {
                        let body = NotificationBody::TaskExit(TaskExitParams {
                            task_id,
                            exit_code: s.code(),
                            signal: s.signal(),
                        });
                        let notification = Notification {
                            jsonrpc: JsonRpcVersion {},
                            body,
                        };
                        let notification_str = serde_json::to_string(&notification)
                            .expect("Serialization shouldn't fail");
                        async move {
                            let _ = connection_writer.write(&notification_str).await;
                        }
                    }
                });
                task_builder
                    .submit()
                    .map(|_| ResponseResult::StartTaskResult { task_id })
            }
            RequestBody::TaskSendSignal(task_send_signal_params) => {
                let task = match self
                    .task_manager
                    .get_task(TaskId(task_send_signal_params.task_id))
                {
                    Some(t) => t,
                    // TODO: return not found here
                    None => return Err(TaskError::AlreadyExited),
                };
                task.send_signal(task_send_signal_params.signal)
                    .map(|_| ResponseResult::SendSignalResult)
            }
        }
    }
}
