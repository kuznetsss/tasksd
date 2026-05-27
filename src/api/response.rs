use serde::Serialize;

use crate::{
    api::common::{JsonRpcVersion, RequestId},
    tasks::{task_error::TaskError, task_manager::TaskId},
};

#[derive(Debug, Serialize)]
pub struct Response {
    pub jsonrpc: JsonRpcVersion,
    pub id: Option<RequestId>,

    #[serde(flatten)]
    pub body: ResponseBody,
}

// TODO: find a more convenient way to build Response from a Result
#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResponseBody {
    Result(ResponseResult),
    Error(ResponseError),
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum ResponseResult {
    StartTaskResult { task_id: TaskId },
    SendSignalResult,
}

#[derive(Debug, Serialize)]
pub struct ResponseError {
    code: i32,
    message: &'static str,
    data: Option<String>,
}

impl ResponseError {
    pub fn invalid_request(details: String) -> Self {
        Self {
            code: -32600,
            message: "Invalid Request",
            data: Some(details),
        }
    }
}

impl From<TaskError> for ResponseBody {
    fn from(value: TaskError) -> Self {
        match value {
            TaskError::InvalidDirectory => ResponseBody::Error(ResponseError {
                code: 1,
                message: "Invalid working directory",
                data: None,
            }),
            TaskError::PtyCreationError(e) => ResponseBody::Error(ResponseError {
                code: 2,
                message: "Error creating a new pty",
                data: Some(e),
            }),

            TaskError::StartingChildProcessError(e) => ResponseBody::Error(ResponseError {
                code: 3,
                message: "Error starting child process",
                data: Some(e),
            }),
            TaskError::WriteError(e) => ResponseBody::Error(ResponseError {
                code: 4,
                message: "Error writing to process",
                data: Some(e),
            }),
            TaskError::AlreadyExited => ResponseBody::Error(ResponseError {
                code: 5,
                message: "The task has already exited",
                data: None,
            }),
            TaskError::SendSignalError(e) => ResponseBody::Error(ResponseError {
                code: 6,
                message: "Error sending signal to the task",
                data: Some(e),
            }),
        }
    }
}
