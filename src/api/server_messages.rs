use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct ServerResponse {
    #[serde(flatten)]
    payload: Payload,
    id: usize,
}

impl ServerResponse {
    pub fn run_response(id: usize, pid: usize) -> String {
        let message = ServerResponse {
            payload: Payload::Result(Result::RunResponse { pid }),
            id,
        };
        serde_json::to_string(&message).unwrap()
    }

    pub fn send_signal_response(id: usize) -> String {
        let message = ServerResponse {
            payload: Payload::Result(Result::SendSignalResponse(Status::Ok)),
            id: 123,
        };
        serde_json::to_string(&message).unwrap()
    }

    pub fn error(id: usize, error_code: usize) -> String {
        let message = ServerResponse {
            payload: Payload::Error {
                code: error_code,
                message: "todo".to_string(),
            },
            id,
        };
        serde_json::to_string(&message).unwrap()
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
    RunResponse { pid: usize },
    SendSignalResponse(Status),
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
    ProcessExited { pid: usize, exit_code: i32 },
}

impl ServerNotification {
    pub fn process_output(pid: usize, line: String) -> String {
        let message = ServerNotification::ProcessOutput { pid, line };
        serde_json::to_string(&message).unwrap()
    }

    pub fn process_exited(pid: usize, exit_code: i32) -> String {
        let message = ServerNotification::ProcessExited { pid, exit_code };
        serde_json::to_string(&message).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_response_serialization() {
        let serialized_message = ServerResponse::run_response(123, 456);
        assert_eq!(serialized_message, r#"{"result":{"pid":456},"id":123}"#);
    }

    #[test]
    fn send_siganl_response_serialization() {
        let serialized_message = ServerResponse::send_signal_response(123);
        assert_eq!(serialized_message, r#"{"result":"ok","id":123}"#);
    }

    #[test]
    fn error_serialization() {
        let serialized_message = ServerResponse::error(456, 123);
        assert_eq!(
            serialized_message,
            r#"{"error":{"code":123,"message":"todo"},"id":456}"#
        )
    }

    #[test]
    fn process_output_serialization() {
        let serialized_message = ServerNotification::process_output(123, "some output".to_string());
        assert_eq!(
            serialized_message,
            r#"{"method":"process_output","params":{"pid":123,"line":"some output"}}"#
        )
    }

    #[test]
    fn process_exited_serialization() {
        let serialized_message = ServerNotification::process_exited(123, 456);
        assert_eq!(
            serialized_message,
            r#"{"method":"process_exited","params":{"pid":123,"exit_code":456}}"#
        )
    }
}
