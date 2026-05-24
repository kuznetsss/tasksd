use tracing::info;

use crate::{
    api::{
        request::{Request, RequestBody},
        response::{Response, ResponseBody, ResponseResult},
    },
    tasks::{task_error::TaskError, task_manager::TaskManager},
    transport::connection::ConnectionWriter,
};

use std::sync::Arc;

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
            id: request.id,
            body: response_body,
        };
        let response_str = serde_json::to_string(&response)
            .expect(&format!("Error serializing response: {response:?}")); // TODO: fix
        if let Err(e) = self.connection_writer.write(&response_str).await {
            info!("Error writing to connection: {e}")
        }
    }

    fn process_request(&self, request_body: RequestBody) -> Result<ResponseResult, TaskError> {
        match request_body {
            RequestBody::TaskStart(task_start_params) => {
                let task_builder = self.task_manager.create_task(task_start_params.executable);
                if let Some(args) = task_start_params.args {
                    task_builder.args(args);
                }
                if let Some(working_dir) = task_start_params.working_dir {
                    task_builder.working_dir(working_dir);
                }
                if task_start_params.subscribe_to_output {
                    task_builder.on_output(todo!());
                }
                task_builder
                    .submit()
                    .map(|task_id| ResponseResult::StartTaskResult { task_id })
            }
            RequestBody::TaskSendSignal(task_send_signal_params) => {
                let task = match self.task_manager.get_task(task_send_signal_params.task_id) {
                    Some(t) => t,
                    None => return TaskError::AlreadyExited,
                };
                task.send_signal(signal)
            }
        }
    }
}
