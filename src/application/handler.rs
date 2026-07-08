use tracing::warn;

use crate::{
    api::{
        Request, RequestBody, Response, ResponseResult, TaskGetOutputParams, TaskSendSignalParams,
        TaskStartParams,
    },
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
            RequestBody::TaskGetOutput(params) => self.get_output(params).map(|r| (r.into(), None)),
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
        self.task_manager
            .get_task(params.task_id)
            .and_then(|task| {
                task.send_signal(params.signal)
                    .map(|_| ResponseResult::SendSignalResult {})
            })
            .map_err(Into::into)
    }

    fn get_output(&self, params: TaskGetOutputParams) -> Result<ResponseResult, ApplicationError> {
        let line_range = params.from_line..(params.from_line + params.lines_number);
        let lines = self
            .task_manager
            .get_running_task(params.task_id)
            .map({
                let line_range = line_range.clone();
                |t| t.output_buffer().get_line_range(line_range)
            })
            .or_else(|| {
                self.task_manager
                    .get_finished_task(params.task_id)
                    .map(|t| t.output_buffer.get_line_range(line_range))
            });
        match lines {
            Some(lines) => Ok(ResponseResult::GetOutputResult {
                task_id: params.task_id,
                lines,
            }),
            None => Err(TaskError::NotFound.into()),
        }
    }
}
