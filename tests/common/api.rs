use monostate::MustBe;
use serde::Deserialize;

#[derive(Deserialize)]
pub struct TaskStartResponse {
    pub id: i64,
    pub result: TaskStartResult,
}

#[derive(Deserialize)]
pub struct TaskStartResult {
    pub task_id: usize,
}

#[derive(Deserialize)]
pub struct TaskSendSignalResponse {
    pub id: i64,
    pub result: TaskSendSignalResponseResult,
}

#[derive(Deserialize)]
pub struct TaskSendSignalResponseResult {}

#[derive(Deserialize)]
pub struct TaskOutputNotification {
    pub method: MustBe!("task.output"),
    pub params: TaskOutputNotificationParams,
}

#[derive(Deserialize)]
pub struct TaskOutputNotificationParams {
    pub task_id: usize,
    pub line: String,
}

#[derive(Deserialize)]
pub struct TaskExitNotification {
    pub method: MustBe!("task.exit"),
    pub params: TaskExitNotificationParams,
}

#[derive(Deserialize)]
pub struct TaskExitNotificationParams {
    pub task_id: usize,
    pub exit_code: Option<i32>,
    pub signal: Option<i32>,
}

#[derive(Deserialize)]
pub struct ErrorResponse {
    pub id: Option<i64>,
    pub error: ErrorResponseDetails,
}

#[derive(Deserialize)]
pub struct ErrorResponseDetails {
    pub code: i64,
    pub message: String,
    pub data: Option<String>,
}
