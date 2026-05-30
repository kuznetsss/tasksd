use std::fmt;

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
    pub const PARSE_ERROR_CODE: i32 = -32700;
    pub const INVALID_REQUEST_CODE: i32 = -32600;
    pub const METHOD_NOT_FOUND_CODE: i32 = -32601;
    pub const INVALID_PARAMS_CODE: i32 = -32602;
    pub const INTERNAL_ERROR_CODE: i32 = -32603;

    pub fn parse_error(error: &impl fmt::Display) -> Self {
        Self {
            code: Self::PARSE_ERROR_CODE,
            message: "Invalid JSON",
            data: Some(format!("Error parsing request: {error}")),
        }
    }

    pub fn invalid_request(request: &str, reason: &impl fmt::Display) -> Self {
        Self {
            code: Self::INVALID_REQUEST_CODE,
            message: "Invalid Request",
            data: Some(format!("Request '{request}' is invalid: {reason}")),
        }
    }

    pub fn method_not_found(method: &str) -> Self {
        Self {
            code: Self::METHOD_NOT_FOUND_CODE,
            message: "Method not found",
            data: Some(format!("No such method: '{method}'")),
        }
    }

    pub fn invalid_params(method: &str, error: &impl fmt::Display) -> Self {
        Self {
            code: Self::INVALID_PARAMS_CODE,
            message: "Invalid params",
            data: Some(format!("Invalid params for method '{method}': {error}")),
        }
    }

    pub fn internal_error(details: &impl fmt::Display) -> Self {
        Self {
            code: Self::INTERNAL_ERROR_CODE,
            message: "Internal error",
            data: Some(details.to_string()),
        }
    }

    pub fn into_response(self, id: Option<RequestId>) -> Response {
        Response {
            jsonrpc: JsonRpcVersion {},
            id,
            body: ResponseBody::Error(self),
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
