use rustix::process::Signal;
use serde::{Deserialize, Deserializer};

use crate::{
    api::{
        common::{JsonRpcVersion, RequestId},
        response::{Response, ResponseError},
    },
    tasks::TaskId,
};

pub struct Request {
    pub id: RequestId,
    pub body: RequestBody,
}

impl Request {
    pub fn parse(s: &str) -> Result<Self, Response> {
        let value: serde_json::Value = serde_json::from_str(s)
            .map_err(|e| ResponseError::parse_error(&e).into_response(None))?;
        let raw: RequestRaw = serde_json::from_value(value)
            .map_err(|e| ResponseError::invalid_request(s, &e).into_response(None))?;
        raw.parse_into_request()
    }
}

#[derive(Deserialize)]
struct RequestRaw {
    jsonrpc: JsonRpcVersion,
    id: RequestId,
    method: String,
    #[serde(default)]
    params: serde_json::Value,
}

impl RequestRaw {
    fn parse_into_request(self) -> Result<Request, Response> {
        match self.method.as_str() {
            "task.start" => self.parse_params(RequestBody::TaskStart),
            "task.send_signal" => self.parse_params(RequestBody::TaskSendSignal),
            unknown => Err(ResponseError::method_not_found(unknown).into_response(Some(self.id))),
        }
    }

    fn parse_params<T: serde::de::DeserializeOwned>(
        self,
        constructor: fn(T) -> RequestBody,
    ) -> Result<Request, Response> {
        let params = serde_json::from_value(self.params).map_err(|e| {
            ResponseError::invalid_params(&self.method, &e).into_response(Some(self.id.clone()))
        })?;
        Ok(Request {
            id: self.id,
            body: constructor(params),
        })
    }
}

pub enum RequestBody {
    TaskStart(TaskStartParams),
    TaskSendSignal(TaskSendSignalParams),
}

#[derive(Deserialize)]
pub struct TaskStartParams {
    pub executable: String,
    pub args: Option<Vec<String>>,
    pub working_dir: Option<String>,

    #[serde(default = "TaskStartParams::default_subscribe_to_output")]
    pub subscribe_to_output: bool,
}

impl TaskStartParams {
    fn default_subscribe_to_output() -> bool {
        true
    }
}

#[derive(Deserialize)]
pub struct TaskSendSignalParams {
    pub task_id: TaskId,

    #[serde(deserialize_with = "deserialize_signal")]
    pub signal: Signal,
}

fn deserialize_signal<'de, D>(d: D) -> Result<Signal, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::Error;
    let n = i32::deserialize(d)?;
    match Signal::from_named_raw(n) {
        Some(s) => Ok(s),
        None => Err(D::Error::invalid_value(
            serde::de::Unexpected::Signed(n as i64),
            &"a valid signal number",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn start_task_deserialize() {
        let json_str = r#"{
            "jsonrpc":"2.0",
            "id": 123,
            "method": "task.start",
            "params":{
                "executable": "ls",
                "working_dir":"/tmp"
            }
        }"#;
        let parsed = Request::parse(json_str).unwrap();
        assert_eq!(parsed.id, RequestId::Number(123));
        let RequestBody::TaskStart(body) = parsed.body else {
            panic!("Invalid body variant")
        };
        assert_eq!(body.executable, "ls");
        assert_eq!(body.args, None);
        assert_eq!(body.working_dir, Some("/tmp".to_string()));
    }

    #[test]
    fn send_signal_deserialize() {
        let json_str = r#"{
            "jsonrpc":"2.0",
            "id": 123,
            "method": "task.send_signal",
            "params":{
                "task_id": 456,
                "signal": 9
            }
        }"#;
        let parsed = Request::parse(json_str).unwrap();
        assert_eq!(parsed.id, RequestId::Number(123));
        let RequestBody::TaskSendSignal(body) = parsed.body else {
            panic!("Invalid body variant")
        };
        assert_eq!(body.task_id, TaskId(456));
        assert_eq!(body.signal.as_raw(), 9);
    }
}
