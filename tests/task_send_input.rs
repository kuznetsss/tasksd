mod common;

use crate::common::{
    api::{
        ErrorResponse, TaskExitNotification, TaskOutputNotification, TaskSendInputResponse,
        TaskStartResponse,
    },
    running_app,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_input_sends_input() {
    let (ctx, mut client) = running_app().await;

    client.task_start("cat", &[], true).await.unwrap();

    let response: TaskStartResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, client.last_id());
    let task_id = response.result.task_id;

    let input = "some input\n";
    client.send_input(task_id, input).await.unwrap();

    let last_id = client.last_id();
    client
        .expect_unordered()
        .message(move |r: TaskSendInputResponse| r.id == last_id)
        .message(move |o: TaskOutputNotification| {
            o.params.task_id == task_id && o.params.line == input && o.params.line_number == 0
        })
        .check()
        .await
        .unwrap();

    ctx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_input_non_existing_task() {
    let (ctx, mut client) = running_app().await;

    client.send_input(123, "some input").await.unwrap();

    let err: ErrorResponse = client.read_struct().await.unwrap();
    assert_eq!(err.id.unwrap(), client.last_id());
    assert_eq!(err.error.code, 7);

    ctx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_input_already_finished_task() {
    let (ctx, mut client) = running_app().await;

    client.task_start("echo", &[], false).await.unwrap();

    let response: TaskStartResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, client.last_id());
    let task_id = response.result.task_id;

    let exit: TaskExitNotification = client.read_struct().await.unwrap();
    assert_eq!(exit.params.task_id, task_id);

    client.send_input(task_id, "some input").await.unwrap();

    let err: ErrorResponse = client.read_struct().await.unwrap();
    assert_eq!(err.id.unwrap(), client.last_id());
    assert_eq!(err.error.code, 5);

    ctx.shutdown().await;
}
