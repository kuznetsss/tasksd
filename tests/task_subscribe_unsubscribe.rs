mod common;

use std::time::Duration;

use rustix::path::Arg;
use tasksd::tasks::CHANNEL_CAPACITY;

use crate::common::{
    api::{
        ErrorResponse, TaskExitNotification, TaskOutputNotification, TaskSendInputResponse,
        TaskSendSignalResponse, TaskStartResponse, TaskSubscribeResponse, TaskUnsubscribeResponse,
    },
    running_app,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsubscribe_unsubscribes_from_output() {
    let (ctx, mut client) = running_app().await;

    client.task_start("cat", &[], true).await.unwrap();

    let response: TaskStartResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, client.last_id());
    let task_id = response.result.task_id;

    client.send_input(task_id, "input 1\n").await.unwrap();

    let last_id = client.last_id();
    client
        .expect_unordered()
        .message(move |r: TaskSendInputResponse| r.id == last_id)
        .message(move |o: TaskOutputNotification| {
            o.params.task_id == task_id && o.params.line == "input 1\n" && o.params.line_number == 0
        })
        .check()
        .await
        .unwrap();

    client.unsubscribe(task_id).await.unwrap();

    let response: TaskUnsubscribeResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, client.last_id());

    client.send_input(task_id, "input 2\n").await.unwrap();
    let response: TaskSendInputResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, client.last_id());

    client.send_signal(task_id, 9).await.unwrap();

    let last_id = client.last_id();
    client
        .expect_unordered()
        .message(move |s: TaskSendSignalResponse| s.id == last_id)
        // Exit notification guarantees no output is missing
        .message(move |e: TaskExitNotification| e.params.task_id == task_id)
        .check()
        .await
        .unwrap();

    ctx.shutdown().await;
}

async fn wait_for_line_in_file(line: &str, file_path: &std::path::Path) {
    const MAX_RETRIES: usize = 2000;
    let mut it = 0;

    loop {
        let buf = std::fs::read_to_string(file_path).unwrap();
        if buf.ends_with(line) {
            break;
        }
        it += 1;
        assert!(it < MAX_RETRIES);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_subscribes_to_output() {
    let tmp_file = tempfile::NamedTempFile::new().unwrap();
    let tmp_file_path = tmp_file.path().as_str().unwrap().to_string();

    let (ctx, mut client) = running_app().await;

    client
        .task_start("sh", &["-c", &format!("cat | tee {tmp_file_path}")], false)
        .await
        .unwrap();

    let response: TaskStartResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, client.last_id());
    let task_id = response.result.task_id;

    let input = "input 1\n";
    client.send_input(task_id, input).await.unwrap();

    let response: TaskSendInputResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, client.last_id());

    wait_for_line_in_file(input, tmp_file.path()).await;

    client.subscribe(task_id).await.unwrap();

    let response: TaskSubscribeResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, client.last_id());

    let input = "input 2\n";
    client.send_input(task_id, input).await.unwrap();
    let last_id = client.last_id();
    client
        .expect_unordered()
        .message(move |r: TaskSendInputResponse| r.id == last_id)
        .message(move |o: TaskOutputNotification| {
            o.params.task_id == task_id && o.params.line == input && o.params.line_number == 1
        })
        .check()
        .await
        .unwrap();

    client.send_signal(task_id, 9).await.unwrap();

    let last_id = client.last_id();
    client
        .expect_unordered()
        .message(move |e: TaskExitNotification| e.params.task_id == task_id)
        .message(move |r: TaskSendSignalResponse| r.id == last_id)
        .check()
        .await
        .unwrap();

    ctx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_to_already_running_task() {
    let tmp_file = tempfile::NamedTempFile::new().unwrap();
    let tmp_file_path = tmp_file.path().as_str().unwrap().to_string();

    let (ctx, mut client) = running_app().await;

    client
        .task_start("sh", &["-c", &format!("cat | tee {tmp_file_path}")], false)
        .await
        .unwrap();

    let response: TaskStartResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, client.last_id());
    let task_id = response.result.task_id;

    let input = "input 1\n";
    client.send_input(task_id, input).await.unwrap();

    let response: TaskSendInputResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, client.last_id());

    wait_for_line_in_file(input, tmp_file.path()).await;
    drop(client);

    let mut client = ctx.make_client().await;

    client.subscribe(task_id).await.unwrap();
    let response: TaskSendInputResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, client.last_id());

    let input = "input 2\n";
    client.send_input(task_id, input).await.unwrap();

    let last_id = client.last_id();
    client
        .expect_unordered()
        .message(move |r: TaskSendInputResponse| r.id == last_id)
        .message(move |o: TaskOutputNotification| {
            o.params.task_id == task_id && o.params.line == input && o.params.line_number == 1
        })
        .check()
        .await
        .unwrap();

    ctx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_unsubscribe_non_existing_task() {
    let (ctx, mut client) = running_app().await;

    client.subscribe(123).await.unwrap();

    let err: ErrorResponse = client.read_struct().await.unwrap();
    assert_eq!(err.id.unwrap(), client.last_id());
    assert_eq!(err.error.code, 7);

    client.unsubscribe(123).await.unwrap();

    let err: ErrorResponse = client.read_struct().await.unwrap();
    assert_eq!(err.id.unwrap(), client.last_id());
    assert_eq!(err.error.code, 7);

    ctx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_unsubscribe_finished_task() {
    let (ctx, mut client) = running_app().await;

    client.task_start("ls", &[], false).await.unwrap();
    let response: TaskStartResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, client.last_id());
    let task_id = response.result.task_id;

    let exit: TaskExitNotification = client.read_struct().await.unwrap();
    assert_eq!(exit.params.task_id, task_id);

    client.subscribe(task_id).await.unwrap();
    let err: ErrorResponse = client.read_struct().await.unwrap();
    assert_eq!(err.id.unwrap(), client.last_id());
    assert_eq!(err.error.code, 5);

    client.unsubscribe(task_id).await.unwrap();
    let err: ErrorResponse = client.read_struct().await.unwrap();
    assert_eq!(err.id.unwrap(), client.last_id());
    assert_eq!(err.error.code, 7);

    ctx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_to_running_task_first_message_is_lag() {
    let (ctx, mut client) = running_app().await;

    client
        .task_start("sh", &["-c", "echo ready; cat"], true)
        .await
        .unwrap();

    let response: TaskStartResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, client.last_id());
    let task_id = response.result.task_id;

    let output: TaskOutputNotification = client.read_struct().await.unwrap();
    assert_eq!(output.params.task_id, task_id);
    assert_eq!(output.params.line, "ready\n");
    assert_eq!(output.params.line_number, 0);

    client.unsubscribe(task_id).await.unwrap();
    let response: TaskSubscribeResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, client.last_id());

    let mut new_client = ctx.make_client().await;

    new_client.subscribe(task_id).await.unwrap();
    let response: TaskSubscribeResponse = new_client.read_struct().await.unwrap();
    assert_eq!(response.id, new_client.last_id());

    let input: String = (0..CHANNEL_CAPACITY * 2)
        .map(|i| format!("line {i}\n"))
        .collect();

    client.send_input(task_id, input.as_str()).await.unwrap();
    let response: TaskSendInputResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, client.last_id());

    let output: TaskOutputNotification = new_client.read_struct().await.unwrap();
    assert_eq!(output.params.task_id, task_id);
    assert!(output.params.line_number >= 1);
    assert!(output.params.line_number <= CHANNEL_CAPACITY + 1);

    ctx.shutdown().await;
}
