use rustix::process::Signal;
use serde::{Deserialize, Deserializer};

use crate::{
    api::{
        common::{JsonRpcVersion, RequestId},
        response::{Response, ResponseError},
    },
    tasks::TaskId,
};

#[derive(Debug)]
pub struct Request {
    pub id: RequestId,
    pub method: String,
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
            "task.get_output" => self.parse_params(RequestBody::TaskGetOutput),
            "task.subscribe" => self.parse_params(RequestBody::TaskSubscribe),
            "task.unsubscribe" => self.parse_params(RequestBody::TaskUnsubscribe),
            "task.send_input" => self.parse_params(RequestBody::TaskSendInput),
            "hello" => self.parse_params(RequestBody::Hello),
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
            method: self.method,
            body: constructor(params),
        })
    }
}

#[derive(Debug)]
pub enum RequestBody {
    TaskStart(TaskStartParams),
    TaskSendSignal(TaskSendSignalParams),
    TaskGetOutput(TaskGetOutputParams),
    TaskSubscribe(TaskSubscribeParams),
    TaskUnsubscribe(TaskSubscribeParams),
    TaskSendInput(TaskSendInputParams),
    Hello(HelloParams),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskSendSignalParams {
    pub task_id: TaskId,

    #[serde(deserialize_with = "deserialize_signal")]
    pub signal: Signal,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskGetOutputParams {
    pub task_id: TaskId,
    pub from_line: usize,
    pub lines_number: usize,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskSubscribeParams {
    pub task_id: TaskId,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskSendInputParams {
    pub task_id: TaskId,
    pub input: String,
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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HelloParams {
    pub client_name: String,
    pub client_version: String,
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use serde_json::json;

    use crate::api::response::{ErrorCode, ResponseBody};

    use super::*;

    #[test]
    fn request_parse_invalid_json() {
        let invalid_json_str = "{";
        let err = Request::parse(invalid_json_str).unwrap_err();
        assert_eq!(err.id, None);
        let body = match err.body {
            ResponseBody::Error(b) => b,
            b => panic!("Unexpected response body {b:?}"),
        };
        assert_eq!(body.code, ErrorCode::ParseError);
    }

    #[test]
    fn request_parse_invalid_request() {
        let invalid_json_str = "{}";
        let err = Request::parse(invalid_json_str).unwrap_err();
        assert_eq!(err.id, None);
        let body = match err.body {
            ResponseBody::Error(b) => b,
            b => panic!("Unexpected response body {b:?}"),
        };
        assert_eq!(body.code, ErrorCode::InvalidRequest);
    }

    #[test]
    fn request_parse_invalid_method() {
        let invalid_json_str =
            r#"{ "jsonrpc":"2.0", "id":123, "method":"invalid_method", "params":{} }"#;
        let err = Request::parse(invalid_json_str).unwrap_err();
        assert_eq!(err.id, Some(RequestId::Number(123)));
        let body = match err.body {
            ResponseBody::Error(b) => b,
            b => panic!("Unexpected response body {b:?}"),
        };
        assert_eq!(body.code, ErrorCode::MethodNotFound);
    }

    #[test]
    fn request_parse_invalid_params() {
        let invalid_json_str = r#"{ "jsonrpc":"2.0", "id":123, "method":"task.start", "params": { "invalid_param":123 } }"#;
        let err = Request::parse(invalid_json_str).unwrap_err();
        assert_eq!(err.id, Some(RequestId::Number(123)));
        let body = match err.body {
            ResponseBody::Error(b) => b,
            b => panic!("Unexpected response body {b:?}"),
        };
        assert_eq!(body.code, ErrorCode::InvalidParams);
    }

    #[test]
    fn start_task_deserialize() {
        let json = json![{
            "jsonrpc":"2.0",
            "id": 123,
            "method": "task.start",
            "params":{
                "executable": "ls",
                "working_dir":"/tmp"
            }
        }];
        let parsed = Request::parse(&json.to_string()).unwrap();
        assert_eq!(parsed.id, RequestId::Number(123));
        let RequestBody::TaskStart(body) = parsed.body else {
            panic!("Invalid body variant")
        };
        assert_eq!(body.executable, "ls");
        assert_eq!(body.args, None);
        assert_eq!(body.working_dir, Some("/tmp".to_string()));
        assert!(body.subscribe_to_output);
    }

    #[test]
    fn start_task_params_denies_extra_fields() {
        let json = json![{
            "executable": "ls",
            "working_dir":"/tmp",
            "extra_field": "some value"
        }];
        let err = serde_json::from_str::<TaskStartParams>(&json.to_string()).unwrap_err();
        assert!(err.to_string().contains("unknown field"));
    }

    #[test]
    fn send_signal_deserialize() {
        let json = json![{
            "jsonrpc":"2.0",
            "id": 123,
            "method": "task.send_signal",
            "params":{
                "task_id": 456,
                "signal": 9
            }
        }];
        let parsed = Request::parse(&json.to_string()).unwrap();
        assert_eq!(parsed.id, RequestId::Number(123));
        let RequestBody::TaskSendSignal(body) = parsed.body else {
            panic!("Invalid body variant")
        };
        assert_eq!(body.task_id, TaskId(456));
        assert_eq!(body.signal.as_raw(), 9);
    }

    #[test]
    fn send_signal_params_denies_extra_fields() {
        let json = json!({
            "task_id": 123,
            "signal": 9,
            "extra_field": "some value"
        });

        let err = serde_json::from_str::<TaskSendSignalParams>(&json.to_string()).unwrap_err();
        assert!(err.to_string().contains("unknown field"));
    }

    #[test]
    fn get_output_deserialize() {
        let json = json![{
            "jsonrpc":"2.0",
            "id": 123,
            "method": "task.get_output",
            "params":{
                "task_id": 456,
                "from_line": 789,
                "lines_number": 324
            }
        }];

        let parsed = Request::parse(&json.to_string()).unwrap();
        assert_eq!(parsed.id, RequestId::Number(123));
        let RequestBody::TaskGetOutput(params) = parsed.body else {
            panic!("Invalid request body");
        };
        assert_eq!(params.task_id.0, 456);
        assert_eq!(params.from_line, 789);
        assert_eq!(params.lines_number, 324);
    }

    #[test]
    fn deserialize_task_subscribe() {
        let json = json! {{
            "jsonrpc":"2.0",
            "id": 123,
            "method": "task.subscribe",
            "params":{
                "task_id": 456,
            }
        }};
        let parsed = Request::parse(&json.to_string()).unwrap();
        assert_eq!(parsed.id, RequestId::Number(123));
        let RequestBody::TaskSubscribe(params) = parsed.body else {
            panic!("Invalid request body");
        };
        assert_eq!(params.task_id.0, 456);
    }

    #[test]
    fn deserialize_task_unsubscribe() {
        let json = json! {{
            "jsonrpc":"2.0",
            "id": 123,
            "method": "task.unsubscribe",
            "params":{
                "task_id": 456,
            }
        }};
        let parsed = Request::parse(&json.to_string()).unwrap();
        assert_eq!(parsed.id, RequestId::Number(123));
        let RequestBody::TaskUnsubscribe(params) = parsed.body else {
            panic!("Invalid request body");
        };
        assert_eq!(params.task_id.0, 456);
    }

    #[test]
    fn deserialize_task_send_input() {
        let json = json! {{
            "jsonrpc":"2.0",
            "id": 123,
            "method": "task.send_input",
            "params":{
                "task_id": 456,
                "input": "some input"
            }
        }};
        let parsed = Request::parse(&json.to_string()).unwrap();
        assert_eq!(parsed.id, RequestId::Number(123));
        let RequestBody::TaskSendInput(params) = parsed.body else {
            panic!("Invalid request body");
        };
        assert_eq!(params.task_id.0, 456);
        assert_eq!(params.input, "some input");
    }

    #[test]
    fn deserialize_hello() {
        let json = json! {{
            "jsonrpc":"2.0",
            "id": 123,
            "method": "hello",
            "params":{
                "client_name": "some client",
                "client_version": "3.4.5"
            }
        }};
        let parsed = Request::parse(&json.to_string()).unwrap();
        assert_eq!(parsed.id, RequestId::Number(123));
        let RequestBody::Hello(params) = parsed.body else {
            panic!("Invalid request body");
        };
        assert_eq!(params.client_name, "some client");
        assert_eq!(params.client_version, "3.4.5");
    }

    #[test]
    fn deserialize_signal_success() {
        let original_signal = 12;
        let json_str = format!(r#"{{"task_id": 456, "signal":{original_signal}}}"#);
        let TaskSendSignalParams { signal, .. } = serde_json::from_str(&json_str).unwrap();
        assert_eq!(signal.as_raw(), original_signal);
    }

    #[test]
    fn deserialize_signal_error() {
        let original_signal = 12;
        let json_str = format!(r#"{{"task_id": 456, "signal":"{original_signal}"}}"#);
        let e = serde_json::from_str::<TaskSendSignalParams>(&json_str).unwrap_err();
        assert!(e.to_string().contains("invalid type"));
    }
}
