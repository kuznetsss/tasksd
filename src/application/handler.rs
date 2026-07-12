use tracing::warn;

use crate::{
    api::{
        Request, RequestBody, Response, ResponseResult, TaskGetOutputParams, TaskSendInputParams,
        TaskSendSignalParams, TaskStartParams, TaskSubscribeParams,
    },
    application::{
        error::ApplicationError, subscriber::Subscriber,
        subscription_registry::SubscriptionRegistry,
    },
    tasks::{TaskError, TaskManager, TaskReadingGate},
    transport::ConnectionWriter,
};

use std::sync::Arc;

pub(in crate::application) struct Handler {
    connection_writer: ConnectionWriter,
    task_manager: Arc<TaskManager>,
    subscription_registry: SubscriptionRegistry,
}

impl Handler {
    pub(in crate::application) fn new(
        connection_writer: ConnectionWriter,
        task_manager: Arc<TaskManager>,
        subscription_registry: SubscriptionRegistry,
    ) -> Self {
        Self {
            connection_writer,
            task_manager,
            subscription_registry,
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
            RequestBody::TaskSubscribe(params) => self.subscribe(params).map(|r| (r.into(), None)),
            RequestBody::TaskUnsubscribe(params) => {
                self.unsubscribe(params).map(|r| (r.into(), None))
            }
            RequestBody::TaskSendInput(params) => {
                self.send_input(params).await.map(|r| (r.into(), None))
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
        self.subscription_registry
            .spawn_subscriber(task_id, subscriber)?;
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
        let line_range = params.from_line..params.from_line.saturating_add(params.lines_number);
        let lines = if let Some(task) = self.task_manager.get_running_task(params.task_id) {
            task.output_buffer().get_line_range(line_range)
        } else if let Some(task) = self.task_manager.get_finished_task(params.task_id) {
            task.output_buffer.get_line_range(line_range)
        } else {
            return Err(TaskError::NotFound.into());
        };
        Ok(ResponseResult::GetOutputResult {
            task_id: params.task_id,
            lines,
        })
    }

    fn subscribe(&self, params: TaskSubscribeParams) -> Result<ResponseResult, ApplicationError> {
        self.subscription_registry
            .subscribe_or_spawn(&params.task_id, || {
                // Create new subscriber
                let task = self.task_manager.get_task(params.task_id)?;
                Ok(Subscriber::new(
                    self.connection_writer.clone(),
                    params.task_id,
                    true,
                    task.events_stream()?,
                ))
            })
            .map(|_| ResponseResult::SubscribeResult {})
    }

    fn unsubscribe(&self, params: TaskSubscribeParams) -> Result<ResponseResult, ApplicationError> {
        self.subscription_registry
            .unsubscribe(&params.task_id)
            .map(|_| ResponseResult::UnsubscribeResult {})
    }

    async fn send_input(
        &self,
        params: TaskSendInputParams,
    ) -> Result<ResponseResult, ApplicationError> {
        let task = self.task_manager.get_task(params.task_id)?;
        task.write_to_stdin(params.input.as_bytes()).await?;
        Ok(ResponseResult::SendInputResult {})
    }
}
