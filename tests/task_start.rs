mod common;

use std::io::Write;

use rustix::path::Arg;

use crate::common::{
    api::{ErrorResponse, TaskExitNotification, TaskOutputNotification, TaskStartResponse},
    running_app,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_task_output_exit_notifications() {
    let (ctx, mut client) = running_app().await;

    let mut tmp_file = tempfile::NamedTempFile::new().unwrap();
    let data = "line 1\nline 2";
    tmp_file.write_all(data.as_bytes()).unwrap();
    tmp_file.flush().unwrap();

    client
        .task_start("cat", &[tmp_file.path().as_str().unwrap()], None, true)
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
async fn start_task_not_subscribed_to_output() {
    let (ctx, mut client) = running_app().await;

    client.task_start("ls", &[], None, false).await.unwrap();

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
        .task_start("non_existing_executable", &[], None, false)
        .await
        .unwrap();

    let response: ErrorResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, client.last_id());
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
        .task_start("cat", &[tmp_file.path().as_str().unwrap()], None, true)
        .await
        .unwrap();

    let response: TaskStartResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, client.last_id());
    let task_id = response.result.task_id;

    for i in 17..=32 {
        let line: TaskOutputNotification = client.read_struct().await.unwrap();
        assert_eq!(line.params.task_id, task_id);
        assert_eq!(line.params.line, format!("line {i}\r\n"));
    }

    let exit: TaskExitNotification = client.read_struct().await.unwrap();
    assert_eq!(exit.params.task_id, task_id);
    assert_eq!(exit.params.exit_code, Some(0));
    assert_eq!(exit.params.signal, None);

    ctx.shutdown().await;
}
