use tracing::{info, warn};

use crate::{
    api::{
        HelloParams, Request, RequestBody, Response, ResponseResult, TaskGetOutputParams,
        TaskInfoParams, TaskSendInputParams, TaskSendSignalParams, TaskSetSubscribeParams,
        TaskStartParams,
    },
    application::{
        ShutdownTrigger,
        error::ApplicationError,
        subscriber::{CreatingEvent, Subscriber, SubscriberSendingGateGuard},
        subscription_registry::SubscriptionRegistry,
    },
    tasks::{AnyTask, TaskEntry, TaskError, TaskManager},
    transport::ConnectionWriter,
};

use std::sync::Arc;

pub(in crate::application) struct Handler {
    connection_writer: ConnectionWriter,
    task_manager: Arc<TaskManager>,
    subscription_registry: SubscriptionRegistry,
    shutdown_trigger: ShutdownTrigger,
}

impl Handler {
    pub(in crate::application) fn new(
        connection_writer: ConnectionWriter,
        task_manager: Arc<TaskManager>,
        subscription_registry: SubscriptionRegistry,
        shutdown_trigger: ShutdownTrigger,
    ) -> Self {
        Self {
            connection_writer,
            task_manager,
            subscription_registry,
            shutdown_trigger,
        }
    }

    pub(in crate::application) async fn handle_request(&self, request: Request) {
        let (response_body, sending_gate_guard) = match request.body {
            RequestBody::TaskStart(params) => self
                .start_task(params)
                .map(|(response, guard)| (response.into(), guard)),
            RequestBody::TaskSendSignal(params) => {
                self.send_signal(params).map(|r| (r.into(), None))
            }
            RequestBody::TaskGetOutput(params) => self.get_output(params).map(|r| (r.into(), None)),
            RequestBody::TaskSetSubscribe(params) => self
                .set_subscribe(params)
                .await
                .map(|(r, guard)| (r.into(), guard)),
            RequestBody::TaskSendInput(params) => {
                self.send_input(params).await.map(|r| (r.into(), None))
            }
            RequestBody::TaskList(_) => Ok((self.task_list().into(), None)),
            RequestBody::TaskInfo(params) => self.task_info(params).map(|r| (r.into(), None)),
            RequestBody::Hello(params) => Ok((self.hello(params).into(), None)),
            RequestBody::Shutdown(_) => Ok((self.shutdown().into(), None)),
        }
        .unwrap_or_else(|e| (e.into(), None));

        let response = Response::new(Some(request.id), response_body).to_json_string();
        if let Err(e) = self.connection_writer.write(&response).await {
            warn!("Error writing to connection: {e}")
        }
        // Dropping after sending response to keep the order of messages:
        // response then task events
        drop(sending_gate_guard);
    }

    fn start_task(
        &self,
        params: TaskStartParams,
    ) -> Result<(ResponseResult, Option<SubscriberSendingGateGuard>), ApplicationError> {
        let task_info = self.task_manager.create_task(
            params.executable,
            params.args.unwrap_or_default(),
            params.working_dir,
        )?;
        let mut guard = None;
        if !params.subscribe_to_events.is_empty() {
            let (subscriber, sending_gate_guard) = Subscriber::new(
                self.connection_writer.clone(),
                task_info.task_id,
                &params.subscribe_to_events,
                task_info.events_stream,
                CreatingEvent::Start,
                task_info.exit_notification_gate,
            );
            guard = Some(sending_gate_guard);
            self.subscription_registry
                .spawn_subscriber(task_info.task_id, subscriber)?;
        }
        let response_result = ResponseResult::StartTaskResult {
            task_id: task_info.task_id,
        };
        Ok((response_result, guard))
    }

    fn send_signal(
        &self,
        params: TaskSendSignalParams,
    ) -> Result<ResponseResult, ApplicationError> {
        self.task_manager
            .get_running_task(params.task_id)
            .and_then(|task| {
                task.send_signal(params.signal)
                    .map(|_| ResponseResult::SendSignalResult {})
            })
            .map_err(Into::into)
    }

    fn get_output(&self, params: TaskGetOutputParams) -> Result<ResponseResult, ApplicationError> {
        let task = self
            .task_manager
            .find_task(params.task_id)
            .ok_or(TaskError::NotFound)?;
        let lines = if let Some(from_line) = params.from_line {
            let line_range = from_line..from_line.saturating_add(params.lines_number);
            task.output_buffer().get_range_of_lines(line_range)
        } else {
            task.output_buffer().get_last_lines(params.lines_number)
        };
        Ok(ResponseResult::GetOutputResult {
            task_id: params.task_id,
            lines,
        })
    }

    async fn set_subscribe(
        &self,
        params: TaskSetSubscribeParams,
    ) -> Result<(ResponseResult, Option<SubscriberSendingGateGuard>), ApplicationError> {
        if params.events.is_empty() {
            match self
                .subscription_registry
                .stop_subscriber(&params.task_id)
                .await
            {
                Ok(_) => return Ok((ResponseResult::SubscribeResult {}, None)),
                Err(ApplicationError::TaskError(TaskError::NotFound)) => {
                    match self.task_manager.find_task(params.task_id) {
                        Some(AnyTask::Running(_)) => {
                            return Ok((ResponseResult::SubscribeResult {}, None));
                        }
                        Some(AnyTask::Finished(_)) => return Err(TaskError::AlreadyExited.into()),
                        None => return Err(TaskError::NotFound.into()),
                    }
                }
                Err(e) => return Err(e),
            }
        }

        self.subscription_registry
            .update_or_spawn_subscriber(&params.task_id, &params.events, || {
                let task_info = self.task_manager.get_running_task_handle(params.task_id)?;
                let (subscriber, guard) = Subscriber::new(
                    self.connection_writer.clone(),
                    params.task_id,
                    &params.events,
                    task_info.task.events_stream()?,
                    CreatingEvent::Subscribe,
                    task_info.exit_notification_gate,
                );
                Ok((subscriber, guard))
            })
            .map(|guard| (ResponseResult::SubscribeResult {}, guard))
    }

    async fn send_input(
        &self,
        params: TaskSendInputParams,
    ) -> Result<ResponseResult, ApplicationError> {
        let task = self.task_manager.get_running_task(params.task_id)?;
        task.write_to_stdin(params.input.as_bytes()).await?;
        Ok(ResponseResult::SendInputResult {})
    }

    fn hello(&self, params: HelloParams) -> ResponseResult {
        info!(
            "Got hello from client {} {}",
            params.client_name, params.client_version
        );
        ResponseResult::HelloResponse {
            server_version: env!("CARGO_PKG_VERSION"),
        }
    }

    fn task_list(&self) -> ResponseResult {
        let list = self.task_manager.task_list();
        ResponseResult::TaskList { tasks: list }
    }

    fn task_info(&self, params: TaskInfoParams) -> Result<ResponseResult, ApplicationError> {
        let task_entry = self
            .task_manager
            .find_task(params.task_id)
            .map(|t| match t {
                AnyTask::Running(t) => TaskEntry::running(&t, params.task_id),
                AnyTask::Finished(t) => TaskEntry::finished(&t, params.task_id),
            })
            .ok_or(TaskError::NotFound)?;
        Ok(ResponseResult::TaskInfoResult { entry: task_entry })
    }

    fn shutdown(&self) -> ResponseResult {
        self.shutdown_trigger.request_shutdown();
        ResponseResult::Shutdown {}
    }
}
