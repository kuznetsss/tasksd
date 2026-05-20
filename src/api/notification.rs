use std::sync::Arc;

use serde::Serialize;

use crate::{api::common::JsonRpcVersion, tasks::task_manager::TaskId};

#[derive(Serialize)]
pub struct Notification {
    pub jsonrpc: JsonRpcVersion,

    #[serde(flatten)]
    pub body: NotificationBody,
}

#[derive(Serialize)]
#[serde(tag = "method", content = "params")]
pub enum NotificationBody {
    #[serde(rename = "task.output")]
    TaskOutput(TaskOutputParams),
    #[serde(rename = "task.exit")]
    TaskExit(TaskExitParams),
}

#[derive(Serialize)]
pub struct TaskOutputParams {
    task_id: TaskId,
    line_number: usize,
    line: Arc<String>,
}

#[derive(Serialize)]
pub struct TaskExitParams {
    task_id: TaskId,
    exit_code: Option<i32>,
    exited_by_signal: Option<i32>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_output_serialization() {
        let task_id = TaskId(123);
        let line_number = 456;
        let line = "some_line";
        let body = NotificationBody::TaskOutput(TaskOutputParams {
            task_id,
            line_number,
            line: Arc::new(line.to_string()),
        });
        let notification = Notification {
            jsonrpc: JsonRpcVersion {},
            body,
        };
        let json_str = serde_json::to_string(&notification).unwrap();
        assert_eq!(
            json_str,
            format!(
                r#"{{"jsonrpc":"2.0","method":"task.output","params":{{"task_id":{task_id},"line_number":{line_number},"line":"{line}"}}}}"#
            )
        )
    }

    #[test]
    fn task_exit_serialization() {
        let task_id = TaskId(123);
        let exit_code = 456;
        let body = NotificationBody::TaskExit(TaskExitParams {
            task_id,
            exit_code: Some(exit_code),
            exited_by_signal: None,
        });
        let notification = Notification {
            jsonrpc: JsonRpcVersion {},
            body,
        };
        let json_str = serde_json::to_string(&notification).unwrap();
        assert_eq!(
            json_str,
            format!(
                r#"{{"jsonrpc":"2.0","method":"task.exit","params":{{"task_id":{task_id},"exit_code":{exit_code},"exited_by_signal":null}}}}"#
            )
        )
    }
}
