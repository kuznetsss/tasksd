mod common;

use std::time::Duration;

use crate::common::{
    TestContextBuilder,
    api::{TaskExitNotification, TaskOutputNotification, TaskStartResponse},
    running_app,
};

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

    assert!(client.is_disconnected().await);
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

    tokio::time::timeout(Duration::from_secs(1), ctx.shutdown())
        .await
        .unwrap();

    let exit_notification: TaskExitNotification = client.read_struct().await.unwrap();
    assert_eq!(exit_notification.params.task_id, task_id);
    assert_eq!(exit_notification.params.exit_code, None);
    assert_eq!(exit_notification.params.signal, Some(9));

    assert!(client.is_disconnected().await);
}
