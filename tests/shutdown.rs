mod common;

use std::time::Duration;

use serde_json::json;

use crate::common::{
    Client, TestContext, TestContextBuilder,
    api::{
        ErrorResponse, HelloResponse, ShutdownResponse, ShuttingDownNotification,
        TaskExitNotification, TaskOutputNotification, TaskSendSignalResponse, TaskStartResponse,
    },
    running_app,
};

async fn connected_client(ctx: &TestContext) -> Client {
    let mut client = ctx.make_client().await;
    client.hello().await.unwrap();
    let response: HelloResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, client.last_id());
    client
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_sends_sigterm_to_running_tasks() {
    let (ctx, mut client) = running_app().await;

    client.task_start("cat", &[], true).await.unwrap();
    let response: TaskStartResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, client.last_id());
    let task_id = response.result.task_id;

    tokio::time::timeout(Duration::from_secs(1), ctx.shutdown())
        .await
        .unwrap();

    let exit_notification: TaskExitNotification = client.read_struct().await.unwrap();
    assert_eq!(exit_notification.params.task_id, task_id);
    assert_eq!(exit_notification.params.exit_code, None);
    assert_eq!(exit_notification.params.signal, Some(15));

    let _: ShuttingDownNotification = client.read_struct().await.unwrap();

    assert!(client.is_disconnected().await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutting_down_notification_is_sent_to_every_client() {
    let ctx = TestContextBuilder::new().build().unwrap();
    ctx.spawn_app_run();
    let mut first_client = connected_client(&ctx).await;
    let mut second_client = connected_client(&ctx).await;

    tokio::time::timeout(Duration::from_secs(1), ctx.shutdown())
        .await
        .unwrap();

    let _: ShuttingDownNotification = first_client.read_struct().await.unwrap();
    let _: ShuttingDownNotification = second_client.read_struct().await.unwrap();

    assert!(first_client.is_disconnected().await);
    assert!(second_client.is_disconnected().await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_sends_sigkill_after_ignoring_sigterm() {
    let ctx = TestContextBuilder::new()
        .adjust_cli_args(|cli| {
            cli.graceful_period = 0;
        })
        .build()
        .unwrap();
    ctx.spawn_app_run();
    let mut client = ctx.make_client().await;

    client
        .task_start(
            "sh",
            &[
                "-c",
                r#"trap "" TERM; echo ready; while :; do sleep 1; done"#,
            ],
            true,
        )
        .await
        .unwrap();

    let response: TaskStartResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, client.last_id());
    let task_id = response.result.task_id;

    // Synchronize with the shell by waiting for the output
    let notification: TaskOutputNotification = client.read_struct().await.unwrap();
    assert_eq!(notification.params.task_id, task_id);
    assert_eq!(notification.params.line, "ready\n");

    tokio::time::timeout(Duration::from_secs(2), ctx.shutdown())
        .await
        .unwrap();

    let exit_notification: TaskExitNotification = client.read_struct().await.unwrap();
    assert_eq!(exit_notification.params.task_id, task_id);
    assert_eq!(exit_notification.params.exit_code, None);
    assert_eq!(exit_notification.params.signal, Some(9));

    let _: ShuttingDownNotification = client.read_struct().await.unwrap();

    assert!(client.is_disconnected().await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_request_is_answered_before_exiting() {
    let (mut ctx, mut client) = running_app().await;
    let watcher = ctx.spawn_shutdown_watcher();

    client.shutdown().await.unwrap();

    tokio::time::timeout(Duration::from_secs(5), watcher)
        .await
        .unwrap()
        .unwrap();

    let response: ShutdownResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, client.last_id());

    let _: ShuttingDownNotification = client.read_struct().await.unwrap();

    assert!(client.is_disconnected().await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_request_sends_sigterm_to_running_tasks() {
    let (mut ctx, mut client) = running_app().await;
    let watcher = ctx.spawn_shutdown_watcher();

    client.task_start("cat", &[], true).await.unwrap();
    let response: TaskStartResponse = client.read_struct().await.unwrap();
    let task_id = response.result.task_id;

    client.shutdown().await.unwrap();
    let last_id = client.last_id();

    client
        .expect_unordered()
        .message(move |r: ShutdownResponse| r.id == last_id)
        .message(move |e: TaskExitNotification| {
            e.params.task_id == task_id
                && e.params.exit_code.is_none()
                && e.params.signal == Some(15)
        })
        .check()
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(5), watcher)
        .await
        .unwrap()
        .unwrap();

    let _: ShuttingDownNotification = client.read_struct().await.unwrap();

    assert!(client.is_disconnected().await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn second_shutdown_request_while_shutting_down_is_harmless() {
    let mut ctx = TestContextBuilder::new()
        .adjust_cli_args(|c| c.graceful_period = 10)
        .build()
        .unwrap();
    ctx.spawn_app_run();
    let mut client = ctx.make_client().await;
    let watcher = ctx.spawn_shutdown_watcher();
    let mut other_client = ctx.make_client().await;

    // Ignores SIGTERM, so the daemon stays inside its graceful period while the
    // second shutdown request is handled. `trap ""` is inherited as SIG_IGN
    // across exec, so `cat` ignores the SIGTERM sent to the process group too.
    client
        .task_start("sh", &["-c", r#"trap "" TERM; echo ready; cat"#], true)
        .await
        .unwrap();
    let response: TaskStartResponse = client.read_struct().await.unwrap();
    let blocking_task_id = response.result.task_id;

    // Synchronize with the shell: the trap is installed before "ready" is printed
    let notification: TaskOutputNotification = client.read_struct().await.unwrap();
    assert_eq!(notification.params.task_id, blocking_task_id);
    assert_eq!(notification.params.line, "ready\n");

    // Dies on SIGTERM, so its exit notification proves the daemon has entered
    // the shutdown and already signalled every running task.
    client.task_start("cat", &[], true).await.unwrap();
    let response: TaskStartResponse = client.read_struct().await.unwrap();
    let signalled_task_id = response.result.task_id;

    client.shutdown().await.unwrap();
    let last_id = client.last_id();
    client
        .expect_unordered()
        .message(move |s: ShutdownResponse| s.id == last_id)
        .message(move |e: TaskExitNotification| {
            e.params.task_id == signalled_task_id
                && e.params.exit_code.is_none()
                && e.params.signal == Some(15)
        })
        .check()
        .await
        .unwrap();

    // The shutdown is in progress now: a second request must be answered and
    // must not disturb it.
    other_client.shutdown().await.unwrap();
    let response: ShutdownResponse = other_client.read_struct().await.unwrap();
    assert_eq!(response.id, other_client.last_id());

    client.send_signal(blocking_task_id, 9).await.unwrap();
    let response: TaskSendSignalResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, client.last_id());

    tokio::time::timeout(Duration::from_secs(10), watcher)
        .await
        .unwrap()
        .unwrap();

    let exit_notification: TaskExitNotification = client.read_struct().await.unwrap();
    assert_eq!(exit_notification.params.task_id, blocking_task_id);
    assert_eq!(exit_notification.params.exit_code, None);
    assert_eq!(exit_notification.params.signal, Some(9));

    assert!(client.is_disconnected().await);
    assert!(other_client.is_disconnected().await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_request_with_unknown_params_is_rejected() {
    let (ctx, mut client) = running_app().await;

    let id = 123;
    client
        .send_json(&json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "shutdown",
            "params": { "unexpected": true }
        }))
        .await
        .unwrap();

    let response: ErrorResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, Some(id));
    assert_eq!(response.error.code, -32602);

    client.task_start("ls", &[], false).await.unwrap();
    let response: TaskStartResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, client.last_id());

    ctx.shutdown().await;
}
