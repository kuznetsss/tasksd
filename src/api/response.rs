use std::fmt;

use serde::Serialize;

use crate::{
    api::common::{JsonRpcVersion, RequestId},
    application::ApplicationError,
    tasks::{TaskError, TaskId},
};

#[derive(Debug, Serialize)]
pub struct Response {
    pub jsonrpc: JsonRpcVersion,
    pub id: Option<RequestId>,

    #[serde(flatten)]
    pub body: ResponseBody,
}

impl Response {
    pub fn new(id: Option<RequestId>, body: ResponseBody) -> Self {
        Response {
            jsonrpc: JsonRpcVersion {},
            id,
            body,
        }
    }

    pub fn to_json_string(&self) -> String {
        serde_json::to_string(self)
            .unwrap_or_else(|e| panic!("Error serializing response '{self:?}': {e}"))
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResponseBody {
    Result(ResponseResult),
    Error(ResponseError),
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ErrorCode {
    ParseError = -32700,
    InvalidRequest = -32600,
    MethodNotFound = -32601,
    InvalidParams = -32602,
    InternalError = -32603,

    InvalidDirectory = 1,
    PtyCreationError = 2,
    StartingChildProcessError = 3,
    WriteError = 4,
    AlreadyExited = 5,
    SendSignalError = 6,
    NotFoundError = 7,
    Shutdown = 8,
}

fn error_code_serializer<S>(code: &ErrorCode, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_i32(*code as i32)
}

impl From<ResponseResult> for ResponseBody {
    fn from(value: ResponseResult) -> Self {
        Self::Result(value)
    }
}

impl From<ApplicationError> for ResponseBody {
    fn from(value: ApplicationError) -> Self {
        match value {
            ApplicationError::Shutdown => ResponseBody::Error(ResponseError {
                code: ErrorCode::Shutdown,
                message: "Tasksd is shutting down",
                data: None,
            }),
            ApplicationError::TaskError(task_error) => task_error.into(),
        }
    }
}

impl From<TaskError> for ResponseBody {
    fn from(value: TaskError) -> Self {
        match value {
            TaskError::InvalidDirectory => ResponseBody::Error(ResponseError {
                code: ErrorCode::InvalidDirectory,
                message: "Invalid working directory",
                data: None,
            }),
            TaskError::PtyCreationError(e) => ResponseBody::Error(ResponseError {
                code: ErrorCode::PtyCreationError,
                message: "Error creating a new pty",
                data: Some(e),
            }),
            TaskError::StartingChildProcessError(e) => ResponseBody::Error(ResponseError {
                code: ErrorCode::StartingChildProcessError,
                message: "Error starting child process",
                data: Some(e),
            }),
            TaskError::WriteError(e) => ResponseBody::Error(ResponseError {
                code: ErrorCode::WriteError,
                message: "Error writing to process",
                data: Some(e),
            }),
            TaskError::AlreadyExited => ResponseBody::Error(ResponseError {
                code: ErrorCode::AlreadyExited,
                message: "The task has already exited",
                data: None,
            }),
            TaskError::SendSignalError(e) => ResponseBody::Error(ResponseError {
                code: ErrorCode::SendSignalError,
                message: "Error sending signal to the task",
                data: Some(e),
            }),
            TaskError::NotFound => ResponseBody::Error(ResponseError {
                code: ErrorCode::NotFoundError,
                message: "Task not found",
                data: None,
            }),
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum ResponseResult {
    StartTaskResult { task_id: TaskId },
    SendSignalResult {},
}

#[derive(Debug, Serialize)]
pub struct ResponseError {
    #[serde(serialize_with = "error_code_serializer")]
    pub code: ErrorCode,
    pub message: &'static str,
    pub data: Option<String>,
}

impl ResponseError {
    pub fn parse_error(error: &impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::ParseError,
            message: "Invalid JSON",
            data: Some(format!("Error parsing request: {error}")),
        }
    }

    pub fn invalid_request(request: &str, reason: &impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::InvalidRequest,
            message: "Invalid Request",
            data: Some(format!("Request '{request}' is invalid: {reason}")),
        }
    }

    pub fn method_not_found(method: &str) -> Self {
        Self {
            code: ErrorCode::MethodNotFound,
            message: "Method not found",
            data: Some(format!("No such method: '{method}'")),
        }
    }

    pub fn invalid_params(method: &str, error: &impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::InvalidParams,
            message: "Invalid params",
            data: Some(format!("Invalid params for method '{method}': {error}")),
        }
    }

    pub fn internal_error(details: &impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::InternalError,
            message: "Internal error",
            data: Some(details.to_string()),
        }
    }

    pub fn into_response(self, id: Option<RequestId>) -> Response {
        Response::new(id, ResponseBody::Error(self))
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    #[test]
    fn response_result_serialization() {
        let result = ResponseResult::StartTaskResult {
            task_id: TaskId(123),
        };
        let request_id = Some(RequestId::String("some id".to_string()));
        let response = Response::new(request_id, result.into());
        let json_str = serde_json::to_string(&response).unwrap();
        assert_eq!(
            json_str,
            r#"{"jsonrpc":"2.0","id":"some id","result":{"task_id":123}}"#
        )
    }

    #[test]
    fn response_error_serialization() {
        let error: ApplicationError = TaskError::PtyCreationError("some error".to_string()).into();
        let request_id = Some(RequestId::String("some id".to_string()));
        let response = Response::new(request_id, error.into());
        let json_str = serde_json::to_string(&response).unwrap();
        assert_eq!(
            json_str,
            r#"{"jsonrpc":"2.0","id":"some id","error":{"code":2,"message":"Error creating a new pty","data":"some error"}}"#
        )
    }
}
