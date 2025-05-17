use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
struct ServerMessage {
    #[serde(flatten)]
    payload: Payload,
    id: usize,
}

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
enum Payload {
    Status(Status),
    RunOutput { output: String },
    Error { code: i32, message: String },
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "result", rename_all = "snake_case")]
enum Status {
    Ok,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ok_message() {
        let message = ServerMessage {
            payload: Payload::Status(Status::Ok),
            id: 123,
        };
        let message_string = serde_json::to_string(&message).unwrap();
        assert_eq!(message_string, r#"{"result":"ok","id":123}"#);
    }

    #[test]
    fn run_output_message() {
        let message = ServerMessage {
            payload: Payload::RunOutput{output: "some output".to_string()},
            id: 321,
        };
        let message_string = serde_json::to_string(&message).unwrap();
        assert_eq!(
            message_string,
            r#"{"result":{"run_output":"some output"},"id":321}"#
        );
    }
}
