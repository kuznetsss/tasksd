use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
struct ServerResponse {
    #[serde(flatten)]
    payload: Payload,
    id: usize,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
enum Payload {
    Result(Result),
    Error { code: usize, message: String },
}

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
enum Result {
    RunResponse { pid: usize },
    SendSignalResult(Status),
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
enum Status {
    Ok,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case", tag = "method", content = "params")]
enum ServerNotification {
    ProcessOutput { pid: usize, line: String },
    ProcessExited { pid: usize, exit_code: usize },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_response_serialization() {
        let message = ServerResponse {
            payload: Payload::Result(Result::RunResponse { pid: 456 }),
            id: 123,
        };
        let serialized_message = serde_json::to_string(&message).unwrap();
        assert_eq!(serialized_message, r#"{"result":{"pid":456},"id":123}"#);
    }

    #[test]
    fn send_siganl_response_serialization() {
        let message = ServerResponse {
            payload: Payload::Result(Result::SendSignalResult(Status::Ok)),
            id: 123,
        };
        let serialized_message = serde_json::to_string(&message).unwrap();
        assert_eq!(serialized_message, r#"{"result":"ok","id":123}"#);
    }

    #[test]
    fn error_serialization() {
        let message = ServerResponse {
            payload: Payload::Error {
                code: 123,
                message: "some message".to_string(),
            },
            id: 456,
        };
        let serialized_message = serde_json::to_string(&message).unwrap();
        assert_eq!(
            serialized_message,
            r#"{"error":{"code":123,"message":"some message"},"id":456}"#
        )
    }

    #[test]
    fn process_output_serialization() {
        let message = ServerNotification::ProcessOutput {
            pid: 123,
            line: "some output".to_string(),
        };
        let serialized_message = serde_json::to_string(&message).unwrap();
        assert_eq!(
            serialized_message,
            r#"{"method":"process_output","params":{"pid":123,"line":"some output"}}"#
        )
    }

    #[test]
    fn process_exited_serialization() {
        let message = ServerNotification::ProcessExited {
            pid: 123,
            exit_code: 456,
        };
        let serialized_message = serde_json::to_string(&message).unwrap();
        assert_eq!(
            serialized_message,
            r#"{"method":"process_exited","params":{"pid":123,"exit_code":456}}"#
        )
    }
}
