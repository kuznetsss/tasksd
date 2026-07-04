use tracing::warn;

use crate::{
    api::{Request, RequestBody, Response, ResponseResult, TaskSendSignalParams, TaskStartParams},
    application::{error::ApplicationError, subscriber::Subscriber},
    tasks::{TaskError, TaskManager, TaskReadingGate},
    transport::ConnectionWriter,
    utils::tracker::SpawnerHandle,
};

use std::sync::Arc;

pub(in crate::application) struct Handler {
    connection_writer: ConnectionWriter,
    task_manager: Arc<TaskManager>,
    spawner: SpawnerHandle,
}

impl Handler {
    pub(in crate::application) fn new(
        connection_writer: ConnectionWriter,
        task_manager: Arc<TaskManager>,
        spawner: SpawnerHandle,
    ) -> Self {
        Self {
            connection_writer,
            task_manager,
            spawner,
        }
    }

    pub(in crate::application) async fn handle_request(&self, request: Request) {
        let (response_body, task_reading_gate) = match request.body {
            RequestBody::TaskStart(params) => self
                .start_task(params)
                .map(|(response, guard)| (response.into(), Some(guard))),
            RequestBody::TaskSendSignal(params) => {
                self.send_signal(params).map(|r| (r.into(), None))
            }
        }
        .unwrap_or_else(|e| (e.into(), None));

        let response = Response::new(Some(request.id), response_body).to_json_string();
        if let Err(e) = self.connection_writer.write(&response).await {
            warn!("Error writing to connection: {e}")
        }
        // Dropping after sending response to keep the order of messages:
        // response then task events
        drop(task_reading_gate);
    }

    fn start_task(
        &self,
        params: TaskStartParams,
    ) -> Result<(ResponseResult, TaskReadingGate), ApplicationError> {
        let (task, task_id, gate) = self.task_manager.create_task(
            params.executable,
            params.args.unwrap_or_default(),
            params.working_dir,
        )?;
        let task_events_stream = task
            .events_stream()
            .expect("Task couldn't exit while its gate is not dropped");
        let subscriber = Subscriber::new(
            self.connection_writer.clone(),
            task_id,
            params.subscribe_to_output,
            task_events_stream,
        );
        self.spawner
            .spawn(async move { subscriber.run().await })
            .map_err(|_| ApplicationError::Shutdown)?;
        let response_result = ResponseResult::StartTaskResult { task_id };
        Ok((response_result, gate))
    }

    fn send_signal(
        &self,
        params: TaskSendSignalParams,
    ) -> Result<ResponseResult, ApplicationError> {
        if let Some(task) = self.task_manager.get_task(params.task_id) {
            Ok(task
                .send_signal(params.signal)
                .map(|_| ResponseResult::SendSignalResult {})?)
        } else if self
            .task_manager
            .get_finished_task(params.task_id)
            .is_some()
        {
            Err(TaskError::AlreadyExited.into())
        } else {
            Err(TaskError::NotFound.into())
        }
    }
}
