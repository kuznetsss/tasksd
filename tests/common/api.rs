use monostate::MustBe;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct TaskStartResponse {
    pub id: i64,
    pub result: TaskStartResult,
}

#[derive(Debug, Deserialize)]
pub struct TaskStartResult {
    pub task_id: usize,
}

#[derive(Debug, Deserialize)]
pub struct TaskSendSignalResponse {
    pub id: i64,
    pub result: TaskSendSignalResponseResult,
}

#[derive(Debug, Deserialize)]
pub struct TaskSendSignalResponseResult {}

#[derive(Debug, Deserialize)]
pub struct TaskOutputNotification {
    pub method: MustBe!("task.output"),
    pub params: TaskOutputNotificationParams,
}

#[derive(Debug, Deserialize)]
pub struct TaskOutputNotificationParams {
    pub task_id: usize,
    pub line: String,
    pub line_number: usize,
}

#[derive(Debug, Deserialize)]
pub struct TaskMissedOutputNotification {
    pub method: MustBe!("task.missed_output"),
    pub params: TaskMissedOutputNotificationParams,
}

#[derive(Debug, Deserialize)]
pub struct TaskMissedOutputNotificationParams {
    pub task_id: usize,
    pub from_line: usize,
    pub missed: usize,
}

#[derive(Debug, Deserialize)]
pub struct TaskExitNotification {
    pub method: MustBe!("task.exit"),
    pub params: TaskExitNotificationParams,
}

#[derive(Debug, Deserialize)]
pub struct TaskExitNotificationParams {
    pub task_id: usize,
    pub exit_code: Option<i32>,
    pub signal: Option<i32>,
}

#[derive(Debug, Deserialize)]
pub struct ErrorResponse {
    pub id: Option<i64>,
    pub error: ErrorResponseDetails,
}

#[derive(Debug, Deserialize)]
pub struct ErrorResponseDetails {
    pub code: i64,
    pub message: String,
    pub data: Option<String>,
}
