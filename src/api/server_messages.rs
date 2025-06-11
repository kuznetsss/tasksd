use serde::Serialize;

use crate::types::{MessageID, ProcessID};

#[derive(Debug, Clone, Serialize)]
pub struct ServerResponse {
    #[serde(flatten)]
    payload: Payload,
    id: MessageID,
}

impl ServerResponse {
    pub fn run_response(id: MessageID, pid: ProcessID) -> ServerResponse {
        ServerResponse {
            payload: Payload::Result(Result::RunResponse { pid }),
            id,
        }
    }

    pub fn send_signal_response(id: MessageID) -> ServerResponse {
        ServerResponse {
            payload: Payload::Result(Result::SendSignalResponse(Status::Ok)),
            id,
        }
    }

    pub fn error(id: MessageID, error_code: usize) -> ServerResponse {
        ServerResponse {
            payload: Payload::Error {
                code: error_code,
                message: "todo".to_string(),
            },
            id,
        }
    }
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
    RunResponse { pid: ProcessID },
    SendSignalResponse(Status),
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
enum Status {
    Ok,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case", tag = "method", content = "params")]
pub enum ServerNotification {
    ProcessOutput { pid: ProcessID, line: String },
    ProcessExited { pid: ProcessID, exit_code: i32 },
}

impl ServerNotification {
    pub fn process_output(pid: ProcessID, line: String) -> ServerNotification {
        ServerNotification::ProcessOutput { pid, line }
    }

    pub fn process_exited(pid: ProcessID, exit_code: i32) -> ServerNotification {
        ServerNotification::ProcessExited { pid, exit_code }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_response_serialization() {
        let response = ServerResponse::run_response(MessageID(123), ProcessID(456));
        assert_eq!(
            serde_json::to_string(&response).unwrap(),
            r#"{"result":{"pid":456},"id":123}"#
        );
    }

    #[test]
    fn send_signal_response_serialization() {
        let response = ServerResponse::send_signal_response(MessageID(123));
        assert_eq!(
            serde_json::to_string(&response).unwrap(),
            r#"{"result":"ok","id":123}"#
        );
    }

    #[test]
    fn error_serialization() {
        let error = ServerResponse::error(MessageID(456), 123);
        assert_eq!(
            serde_json::to_string(&error).unwrap(),
            r#"{"error":{"code":123,"message":"todo"},"id":456}"#
        )
    }

    #[test]
    fn process_output_serialization() {
        let notification =
            ServerNotification::process_output(ProcessID(123), "some output".to_string());
        assert_eq!(
            serde_json::to_string(&notification).unwrap(),
            r#"{"method":"process_output","params":{"pid":123,"line":"some output"}}"#
        )
    }

    #[test]
    fn process_exited_serialization() {
        let notification = ServerNotification::process_exited(ProcessID(123), 456);
        assert_eq!(
            serde_json::to_string(&notification).unwrap(),
            r#"{"method":"process_exited","params":{"pid":123,"exit_code":456}}"#
        )
    }
}
