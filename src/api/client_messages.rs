use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct ClientRequest {
    #[serde(flatten)]
    pub method: Method,
    pub id: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case", tag = "method", content = "params")]
pub enum Method {
    Run {
        executable: String,
        args: Vec<String>,
        working_directory: String,
    },
    SendSignal {
        pid: u32,
        signal: u32,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_request_deserialization() {
        let message = r#"{
            "id": 123,
            "method": "run",
            "params":{
                "executable": "ls",
                "args": ["-l", "-a"],
                "working_directory": "/tmp"
            }
        }"#;

        let parsed_message: ClientRequest = serde_json::from_str(message).unwrap();
        assert_eq!(parsed_message.id, 123);
        if let Method::Run {
            executable,
            args,
            working_directory,
        } = parsed_message.method
        {
            assert_eq!(executable, "ls");
            assert_eq!(args, ["-l", "-a"]);
            assert_eq!(working_directory, "/tmp");
        } else {
            panic!("Expected Method::Run");
        }
    }

    #[test]
    fn send_signal_request_deserialization() {
        let message = r#"{
            "id": 123,
            "method": "send_signal",
            "params":{
                "pid": 456,
                "signal": 9
            }
        }"#;

        let parsed_message: ClientRequest = serde_json::from_str(message).unwrap();
        assert_eq!(parsed_message.id, 123);
        if let Method::SendSignal { pid, signal } = parsed_message.method {
            assert_eq!(pid, 456);
            assert_eq!(signal, 9);
        } else {
            panic!("Expected Method::Run");
        }
    }
}
