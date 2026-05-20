use serde::Serialize;

use crate::api::common::RequestId;

#[derive(Serialize)]
pub struct Response {
    pub id: RequestId,

    #[serde(flatten)]
    pub body: ResponseBody,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResponseBody {
    Result(ResponseResult),
    Error(ResponseError),
}

#[derive(Serialize)]
#[serde(untagged)]
pub enum ResponseResult {
    StartTaskResult { task_id: usize },
    SendSignalResult,
}

#[derive(Serialize)]
pub struct ResponseError {
    code: usize,
    message: &'static str,
}
