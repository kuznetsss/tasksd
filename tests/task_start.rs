mod common;

use std::{collections::HashMap, io::Write};

use rustix::path::Arg;
use serde::Deserialize;
use serde_json::json;

use crate::common::{
    api::{
        ErrorResponse, TaskExitNotification, TaskOutputNotification, TaskStartResponse,
        TaskStartResult,
    },
    running_app,
};

#[derive(Deserialize)]
#[serde(untagged)]
enum ServerEvent {
    Started(TaskStartResponse),
    Output(TaskOutputNotification),
    Exit(TaskExitNotification),
}

#[derive(Debug, PartialEq)]
enum EventKind {
    Started,
    Output,
    Exit,
}

impl ServerEvent {
    fn task_id(&self) -> usize {
        match self {
            ServerEvent::Started(t) => t.result.task_id,
            ServerEvent::Output(t) => t.params.task_id,
            ServerEvent::Exit(t) => t.params.task_id,
        }
    }

    fn kind(&self) -> EventKind {
        match self {
            ServerEvent::Started(_) => EventKind::Started,
            ServerEvent::Output(_) => EventKind::Output,
            ServerEvent::Exit(_) => EventKind::Exit,
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_task_output_exit_notifications() {
    let (ctx, mut client) = running_app().await;

    let mut tmp_file = tempfile::NamedTempFile::new().unwrap();
    let data = "line 1\nline 2";
    tmp_file.write_all(data.as_bytes()).unwrap();
    tmp_file.flush().unwrap();

    client
        .task_start("cat", &[tmp_file.path().as_str().unwrap()], true)
        .await
        .unwrap();

    let response: TaskStartResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, client.last_id());
    let task_id = response.result.task_id;

    let line: TaskOutputNotification = client.read_struct().await.unwrap();
    assert_eq!(line.params.task_id, task_id);
    assert_eq!(line.params.line, "line 1\r\n");

    let line: TaskOutputNotification = client.read_struct().await.unwrap();
    assert_eq!(line.params.task_id, task_id);
    assert_eq!(line.params.line, "line 2");

    let exit: TaskExitNotification = client.read_struct().await.unwrap();
    assert_eq!(exit.params.task_id, task_id);
    assert_eq!(exit.params.exit_code, Some(0));
    assert_eq!(exit.params.signal, None);

    ctx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_task_string_request_id() {
    let (ctx, mut client) = running_app().await;

    let id = "some_id";
    let request = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "task.start",
            "params": {
                "executable": "echo",
                "args": ["hi"],
                "subscribe_to_output": true
            }
    });
    client.send_json(&request).await.unwrap();

    #[derive(Deserialize)]
    struct TaskStartResponseStringId {
        pub id: String,
        pub result: TaskStartResult,
    }

    let response: TaskStartResponseStringId = client.read_struct().await.unwrap();
    assert_eq!(response.id, id);
    let task_id = response.result.task_id;

    let line: TaskOutputNotification = client.read_struct().await.unwrap();
    assert_eq!(line.params.task_id, task_id);
    assert_eq!(line.params.line, "hi\r\n");

    let exit: TaskExitNotification = client.read_struct().await.unwrap();
    assert_eq!(exit.params.task_id, task_id);
    assert_eq!(exit.params.exit_code, Some(0));
    assert_eq!(exit.params.signal, None);

    ctx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_task_custom_working_dir() {
    let (ctx, mut client) = running_app().await;

    let tmp = tempfile::tempdir().unwrap();
    let dir = std::fs::canonicalize(tmp.path())
        .unwrap()
        .as_str()
        .unwrap()
        .to_owned();

    let id = 123;
    let request = json!({
            "jsonrpc": "2.0",
            "id": 123,
            "method": "task.start",
            "params": {
                "executable": "pwd",
                "args": [],
                "working_dir": dir,
                "subscribe_to_output": true
            }
    });
    client.send_json(&request).await.unwrap();

    let response: TaskStartResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, id);
    let task_id = response.result.task_id;

    let line: TaskOutputNotification = client.read_struct().await.unwrap();
    assert_eq!(line.params.task_id, task_id);
    assert_eq!(line.params.line, format!("{dir}\r\n"));

    let exit: TaskExitNotification = client.read_struct().await.unwrap();
    assert_eq!(exit.params.task_id, task_id);
    assert_eq!(exit.params.exit_code, Some(0));
    assert_eq!(exit.params.signal, None);

    ctx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_task_invalid_working_dir() {
    let (ctx, mut client) = running_app().await;

    let id = 123;
    let request = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "task.start",
            "params": {
                "executable": "ls",
                "args": [],
                "working_dir": "/invalid/working/dir",
                "subscribe_to_output": true
            }
    });
    client.send_json(&request).await.unwrap();

    let response: ErrorResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id.unwrap(), id);
    assert_eq!(response.error.code, 3);

    ctx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_task_not_subscribed_to_output() {
    let (ctx, mut client) = running_app().await;

    client.task_start("ls", &[], false).await.unwrap();

    let response: TaskStartResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, client.last_id());
    let task_id = response.result.task_id;

    let exit: TaskExitNotification = client.read_struct().await.unwrap();
    assert_eq!(exit.params.task_id, task_id);
    assert_eq!(exit.params.exit_code, Some(0));
    assert_eq!(exit.params.signal, None);

    ctx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_task_failed() {
    let (ctx, mut client) = running_app().await;

    client
        .task_start("non_existing_executable", &[], false)
        .await
        .unwrap();

    let response: ErrorResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, Some(client.last_id()));
    assert_eq!(response.error.code, 3);

    ctx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_task_skipped_output() {
    // tasks::senders::CHANNEL_CAPACITY = 16 so in case of output burst
    // only the last 16 lines will be sent as a notification
    let (ctx, mut client) = running_app().await;

    let mut tmp_file = tempfile::NamedTempFile::new().unwrap();
    let data: String = (1..=32).map(|i| format!("line {i}\n")).collect();
    tmp_file.write_all(data.as_bytes()).unwrap();
    tmp_file.flush().unwrap();

    client
        .task_start("cat", &[tmp_file.path().as_str().unwrap()], true)
        .await
        .unwrap();

    let response: TaskStartResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, client.last_id());
    let task_id = response.result.task_id;

    let mut last_line_num = None;
    loop {
        match client.read_struct::<ServerEvent>().await.unwrap() {
            ServerEvent::Output(o) => {
                assert_eq!(o.params.task_id, task_id);
                let line_num: i32 = o
                    .params
                    .line
                    .trim_end()
                    .strip_prefix("line ")
                    .unwrap()
                    .parse()
                    .unwrap();
                assert!((1..=32).contains(&line_num));
                if let Some(last_line_num) = last_line_num {
                    assert!(line_num > last_line_num);
                }
                last_line_num = Some(line_num);
            }
            ServerEvent::Exit(e) => {
                assert_eq!(e.params.task_id, task_id);
                assert_eq!(e.params.exit_code, Some(0));
                break;
            }
            ServerEvent::Started(_) => panic!("Unexpected second start"),
        }
    }
    assert_eq!(last_line_num, Some(32));

    ctx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multiple_clients_start_tasks() {
    let (ctx, mut client1) = running_app().await;
    let mut client2 = ctx.make_client().await;

    client1.task_start("ls", &[], false).await.unwrap();
    client2.task_start("ls", &[], false).await.unwrap();

    let response: TaskStartResponse = client1.read_struct().await.unwrap();
    assert_eq!(response.id, client1.last_id());
    let task_id1 = response.result.task_id;

    let response: TaskStartResponse = client2.read_struct().await.unwrap();
    assert_eq!(response.id, client2.last_id());
    let task_id2 = response.result.task_id;

    let exit: TaskExitNotification = client1.read_struct().await.unwrap();
    assert_eq!(exit.params.task_id, task_id1);
    assert_eq!(exit.params.exit_code, Some(0));
    assert_eq!(exit.params.signal, None);

    let exit: TaskExitNotification = client2.read_struct().await.unwrap();
    assert_eq!(exit.params.task_id, task_id2);
    assert_eq!(exit.params.exit_code, Some(0));
    assert_eq!(exit.params.signal, None);

    ctx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_two_tasks_on_one_connection() {
    let (ctx, mut client) = running_app().await;

    client.task_start("echo", &["1"], true).await.unwrap();
    client.task_start("echo", &["2"], true).await.unwrap();

    let mut events_by_task: HashMap<usize, Vec<_>> = HashMap::new();
    for _ in 0..6 {
        let event: ServerEvent = client.read_struct().await.unwrap();
        events_by_task
            .entry(event.task_id())
            .or_default()
            .push(event.kind());
    }

    assert_eq!(events_by_task.len(), 2);
    for events in events_by_task.values() {
        assert_eq!(
            events,
            &[EventKind::Started, EventKind::Output, EventKind::Exit]
        );
    }

    ctx.shutdown().await;
}
