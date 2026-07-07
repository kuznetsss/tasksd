use std::{os::unix::process::ExitStatusExt, process::ExitStatus, sync::Arc};

use serde::Serialize;

use crate::{
    api::common::JsonRpcVersion,
    tasks::{OutputLine, TaskId},
};

#[derive(Debug, Serialize)]
pub struct Notification {
    pub jsonrpc: JsonRpcVersion,

    #[serde(flatten)]
    pub body: NotificationBody,
}

impl Notification {
    pub fn to_json_string(&self) -> String {
        serde_json::to_string(self)
            .unwrap_or_else(|e| panic!("Error serializing notification '{self:?}': {e}"))
    }
}

impl From<NotificationBody> for Notification {
    fn from(value: NotificationBody) -> Self {
        Self {
            jsonrpc: JsonRpcVersion {},
            body: value,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(tag = "method", content = "params")]
pub enum NotificationBody {
    #[serde(rename = "task.output")]
    TaskOutput(TaskOutputParams),

    #[serde(rename = "task.missed_output")]
    TaskMissedOutput(TaskMissedOutputParams),

    #[serde(rename = "task.exit")]
    TaskExit(TaskExitParams),
}

impl NotificationBody {
    pub fn task_output(task_id: TaskId, line: Arc<OutputLine>) -> Self {
        Self::TaskOutput(TaskOutputParams {
            task_id,
            content: line,
        })
    }

    pub fn task_missed_output(task_id: TaskId, from_line: usize, missed: usize) -> Self {
        Self::TaskMissedOutput(TaskMissedOutputParams {
            task_id,
            from_line,
            missed,
        })
    }

    pub fn task_exit(task_id: TaskId, exit_status: ExitStatus) -> Self {
        Self::TaskExit(TaskExitParams {
            task_id,
            exit_code: exit_status.code(),
            signal: exit_status.signal(),
        })
    }
}

#[derive(Debug, Serialize)]
pub struct TaskOutputParams {
    task_id: TaskId,
    #[serde(flatten)]
    content: Arc<OutputLine>,
}

#[derive(Debug, Serialize)]
pub struct TaskExitParams {
    task_id: TaskId,
    exit_code: Option<i32>,
    signal: Option<i32>,
}

#[derive(Debug, Serialize)]
pub struct TaskMissedOutputParams {
    task_id: TaskId,
    from_line: usize,
    missed: usize,
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    #[test]
    fn task_output_serialization() {
        let task_id = TaskId(123);
        let line = "some_line";
        let line_number = 456;
        let notification: Notification = NotificationBody::task_output(
            task_id,
            Arc::new(OutputLine {
                content: line.to_string(),
                line_number,
            }),
        )
        .into();
        let json_str = notification.to_json_string();
        assert_eq!(
            json_str,
            format!(
                r#"{{"jsonrpc":"2.0","method":"task.output","params":{{"task_id":{task_id},"line":"{line}","line_number":{line_number}}}}}"#
            )
        )
    }

    #[test]
    fn task_missed_output_serialization() {
        let task_id = TaskId(123);
        let from_line = 456;
        let number_of_missed_lines = 789;
        let notification: Notification =
            NotificationBody::task_missed_output(task_id, from_line, number_of_missed_lines).into();
        let json_str = notification.to_json_string();
        assert_eq!(
            json_str,
            format!(
                r#"{{"jsonrpc":"2.0","method":"task.missed_output","params":{{"task_id":{task_id},"from_line":{from_line},"missed":{number_of_missed_lines}}}}}"#
            )
        );
    }

    #[test]
    fn task_exit_serialization() {
        let task_id = TaskId(123);
        let exit_code = 1;
        let body = NotificationBody::task_exit(
            task_id,
            // from_raw takes a raw Unix wait status
            std::process::ExitStatus::from_raw(exit_code << 8),
        );
        let notification = Notification {
            jsonrpc: JsonRpcVersion {},
            body,
        };
        let json_str = serde_json::to_string(&notification).unwrap();
        assert_eq!(
            json_str,
            format!(
                r#"{{"jsonrpc":"2.0","method":"task.exit","params":{{"task_id":{task_id},"exit_code":{exit_code},"signal":null}}}}"#
            )
        )
    }
}
