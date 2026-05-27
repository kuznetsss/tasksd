use rustix::process::Signal;
use serde::{Deserialize, Deserializer};

use crate::api::common::{JsonRpcVersion, RequestId};

#[derive(Deserialize)]
pub struct Request {
    pub jsonrpc: JsonRpcVersion,
    pub id: RequestId,

    #[serde(flatten)]
    pub body: RequestBody,
}

impl Request {
    pub fn parse(s: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(s)
    }
}

#[derive(Deserialize)]
#[serde(tag = "method", content = "params")]
pub enum RequestBody {
    #[serde(rename = "task.start")]
    TaskStart(TaskStartParams),
    #[serde(rename = "task.send_signal")]
    TaskSendSignal(TaskSendSignalParams),
}

#[derive(Deserialize)]
pub struct TaskStartParams {
    pub executable: String,
    pub args: Option<Vec<String>>,
    pub working_dir: Option<String>,

    #[serde(default = "TaskStartParams ::default_subscribe_to_output")]
    pub subscribe_to_output: bool,
}

impl TaskStartParams {
    fn default_subscribe_to_output() -> bool {
        true
    }
}

#[derive(Deserialize)]
pub struct TaskSendSignalParams {
    pub task_id: usize,

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
        let parsed: Request = serde_json::from_str(json_str).unwrap();
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
        let parsed: Request = serde_json::from_str(json_str).unwrap();
        assert_eq!(parsed.id, RequestId::Number(123));
        let RequestBody::TaskSendSignal(body) = parsed.body else {
            panic!("Invalid body variant")
        };
        assert_eq!(body.task_id, 456);
        assert_eq!(body.signal.as_raw(), 9);
    }
}
