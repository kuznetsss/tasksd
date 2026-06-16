use std::fmt;

use serde::Serialize;

use crate::{
    api::common::{JsonRpcVersion, RequestId},
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

impl ResponseBody {
    const INVALID_DIRECTORY_CODE: i32 = 1;
    const PTY_CREATION_ERROR_CODE: i32 = 2;
    const STARTING_CHILD_PROCESS_ERROR_CODE: i32 = 3;
    const WRITE_ERROR_CODE: i32 = 4;
    const ALREADY_EXITED_CODE: i32 = 5;
    const SEND_SIGNAL_ERROR_CODE: i32 = 6;
}

impl From<ResponseResult> for ResponseBody {
    fn from(value: ResponseResult) -> Self {
        Self::Result(value)
    }
}

impl From<TaskError> for ResponseBody {
    fn from(value: TaskError) -> Self {
        match value {
            TaskError::InvalidDirectory => ResponseBody::Error(ResponseError {
                code: Self::INVALID_DIRECTORY_CODE,
                message: "Invalid working directory",
                data: None,
            }),
            TaskError::PtyCreationError(e) => ResponseBody::Error(ResponseError {
                code: Self::PTY_CREATION_ERROR_CODE,
                message: "Error creating a new pty",
                data: Some(e),
            }),
            TaskError::StartingChildProcessError(e) => ResponseBody::Error(ResponseError {
                code: Self::STARTING_CHILD_PROCESS_ERROR_CODE,
                message: "Error starting child process",
                data: Some(e),
            }),
            TaskError::WriteError(e) => ResponseBody::Error(ResponseError {
                code: Self::WRITE_ERROR_CODE,
                message: "Error writing to process",
                data: Some(e),
            }),
            TaskError::AlreadyExited => ResponseBody::Error(ResponseError {
                code: Self::ALREADY_EXITED_CODE,
                message: "The task has already exited",
                data: None,
            }),
            TaskError::SendSignalError(e) => ResponseBody::Error(ResponseError {
                code: Self::SEND_SIGNAL_ERROR_CODE,
                message: "Error sending signal to the task",
                data: Some(e),
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
        Response::new(id, ResponseBody::Error(self))
    }
}

#[cfg(test)]
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
        let error = TaskError::PtyCreationError("some error".to_string());
        let request_id = Some(RequestId::String("some id".to_string()));
        let response = Response::new(request_id, error.into());
        let json_str = serde_json::to_string(&response).unwrap();
        assert_eq!(
            json_str,
            r#"{"jsonrpc":"2.0","id":"some id","error":{"code":2,"message":"Error creating a new pty","data":"some error"}}"#
        )
    }
}
