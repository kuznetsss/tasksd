mod common;

use std::time::Duration;

use rustix::path::Arg;

use crate::common::{
    api::{
        TaskExitNotification, TaskOutputNotification, TaskSendInputResponse,
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
