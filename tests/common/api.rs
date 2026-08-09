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
pub struct TaskGetOutputResponse {
    pub id: i64,
    pub result: TaskGetOutputResponseParams,
}

#[derive(Debug, Deserialize)]
pub struct TaskSendInputResponse {
    pub id: i64,
}

#[derive(Debug, Deserialize)]
pub struct TaskSubscribeResponse {
    pub id: i64,
}

#[derive(Debug, Deserialize)]
pub struct TaskUnsubscribeResponse {
    pub id: i64,
}

#[derive(Debug, Deserialize)]
pub struct ShutdownResponse {
    pub id: i64,
    pub result: ShutdownResponseResult,
}

#[derive(Debug, Deserialize)]
pub struct ShutdownResponseResult {}

#[derive(Debug, Deserialize)]
pub struct TaskListResponse {
    pub id: i64,
    pub result: TaskListResponseResult,
}

#[derive(Debug, Deserialize)]
pub struct TaskListResponseResult {
    pub tasks: TaskList,
}

#[derive(Debug, Deserialize)]
pub struct TaskList {
    pub running: Vec<TaskEntry>,
    pub finished: Vec<TaskEntry>,
}

#[derive(Debug, Deserialize)]
pub struct TaskEntry {
    pub id: usize,
    pub info: TaskInfo,
}

#[derive(Debug, Deserialize)]
pub struct TaskInfo {
    pub executable: String,
    pub args: Vec<String>,
    pub working_dir: String,
}

#[derive(Debug, Deserialize)]
pub struct OutputLine {
    pub line: String,
    pub line_number: usize,
}

#[derive(Debug, Deserialize)]
pub struct TaskGetOutputResponseParams {
    pub task_id: usize,
    pub lines: Vec<OutputLine>,
}

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
pub struct ShuttingDownNotification {
    pub method: MustBe!("shutting_down"),
}

#[derive(Debug, Deserialize)]
pub struct HelloResponse {
    pub id: i64,
    pub result: HelloResponseResult,
}

#[derive(Debug, Deserialize)]
pub struct HelloResponseResult {
    pub server_version: String,
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
