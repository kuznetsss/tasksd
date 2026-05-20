use serde::Deserialize;

use crate::api::common::JsonRpcVersion;

#[derive(Deserialize)]
pub struct Request {
    pub jsonrpc: JsonRpcVersion,
    pub id: i64,

    #[serde(flatten)]
    pub body: RequestBody,
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
    pub signal: u8,
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
        assert_eq!(parsed.id, 123);
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
        assert_eq!(parsed.id, 123);
        let RequestBody::TaskSendSignal(body) = parsed.body else {
            panic!("Invalid body variant")
        };
        assert_eq!(body.task_id, 456);
        assert_eq!(body.signal, 9);
    }
}
