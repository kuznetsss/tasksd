use serde::Deserialize;

use crate::api::common::JsonRpcVersion;

#[derive(Deserialize)]
struct Request {
    jsonrpc: JsonRpcVersion,
    id: i64,

    #[serde(flatten)]
    body: RequestBody,
}

#[derive(Deserialize)]
#[serde(tag = "method", content = "params", rename_all = "snake_case")]
enum RequestBody {
    StartTask(StartTaskParams),
    SendSignal(SendSignalParams),
}

#[derive(Deserialize)]
struct StartTaskParams {
    executable: String,
    args: Option<Vec<String>>,
    working_dir: Option<String>,

    #[serde(default = "StartTaskParams::default_subscribe_to_output")]
    subscribe_to_output: bool,
}

impl StartTaskParams {
    fn default_subscribe_to_output() -> bool {
        true
    }
}

#[derive(Deserialize)]
struct SendSignalParams {
    task_id: usize,
    signal: u8,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn start_task_deserialize() {
        let json_str = r#"{
            "jsonrpc":"2.0",
            "id": 123,
            "method": "start_task",
            "params":{
                "executable": "ls",
                "working_dir":"/tmp"
            }
        }"#;
        let parsed: Request = serde_json::from_str(json_str).unwrap();
        assert_eq!(parsed.id, 123);
        let RequestBody::StartTask(body) = parsed.body else {
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
            "method": "send_signal",
            "params":{
                "task_id": 456,
                "signal": 9
            }
        }"#;
        let parsed: Request = serde_json::from_str(json_str).unwrap();
        assert_eq!(parsed.id, 123);
        let RequestBody::SendSignal(body) = parsed.body else {
            panic!("Invalid body variant")
        };
        assert_eq!(body.task_id, 456);
        assert_eq!(body.signal, 9);
    }
}
