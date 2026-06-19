mod common;

use crate::common::{
    api::{ErrorResponse, TaskExitNotification, TaskSendSignalResponse, TaskStartResponse},
    running_app,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_signal_success() {
    let (ctx, mut client) = running_app().await;

    client.task_start("cat", &[], None, true).await.unwrap();

    let response: TaskStartResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, client.last_id());
    let task_id = response.result.task_id;

    let signal = 9;
    client.send_signal(task_id, signal).await.unwrap();
    let response: TaskSendSignalResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, client.last_id());

    let exit_notification: TaskExitNotification = client.read_struct().await.unwrap();
    assert_eq!(exit_notification.params.task_id, task_id);
    assert_eq!(exit_notification.params.exit_code, None);
    assert_eq!(exit_notification.params.signal, Some(signal));

    ctx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_signal_to_non_existing_task() {
    let (ctx, mut client) = running_app().await;

    client.send_signal(123, 9).await.unwrap();

    let response: ErrorResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, client.last_id());
    assert_eq!(response.error.code, 5);

    ctx.shutdown().await;
}
