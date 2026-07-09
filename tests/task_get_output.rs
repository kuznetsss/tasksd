mod common;

use crate::common::{
    api::{
        ErrorResponse, TaskExitNotification, TaskGetOutputResponse, TaskOutputNotification,
        TaskStartResponse,
    },
    running_app,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_output_returns_lines() {
    let (ctx, mut client) = running_app().await;
    client
        .task_start("echo", &["line 1\nline 2\nline 3"], false)
        .await
        .unwrap();

    let response: TaskStartResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, client.last_id());
    let task_id = response.result.task_id;

    let exit: TaskExitNotification = client.read_struct().await.unwrap();
    assert_eq!(exit.params.task_id, task_id);
    assert_eq!(exit.params.exit_code.unwrap(), 0);

    client.get_output(task_id, 1, 2).await.unwrap();

    let output: TaskGetOutputResponse = client.read_struct().await.unwrap();
    assert_eq!(output.id, client.last_id());
    assert_eq!(output.result.task_id, task_id);
    assert_eq!(output.result.lines.len(), 2);
    assert_eq!(output.result.lines[0].line_number, 1);
    assert_eq!(output.result.lines[0].line, "line 2\n");
    assert_eq!(output.result.lines[1].line_number, 2);
    assert_eq!(output.result.lines[1].line, "line 3\n");

    ctx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_output_non_existing_task() {
    let (ctx, mut client) = running_app().await;
    client.get_output(123, 1, 2).await.unwrap();

    let error: ErrorResponse = client.read_struct().await.unwrap();
    assert_eq!(error.id.unwrap(), client.last_id());
    assert_eq!(error.error.code, 7);
    ctx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_output_running_task() {
    let (ctx, mut client) = running_app().await;
    client
        .task_start("sh", &["-c", "echo 'line 1\nline 2\nline 3'; cat"], true)
        .await
        .unwrap();

    let response: TaskStartResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, client.last_id());
    let task_id = response.result.task_id;

    for _ in 0..3 {
        let _: TaskOutputNotification = client.read_struct().await.unwrap();
    }

    client.get_output(task_id, 0, 2).await.unwrap();

    let output: TaskGetOutputResponse = client.read_struct().await.unwrap();
    assert_eq!(output.id, client.last_id());
    assert_eq!(output.result.task_id, task_id);
    assert_eq!(output.result.lines.len(), 2);
    assert_eq!(output.result.lines[0].line_number, 0);
    assert_eq!(output.result.lines[0].line, "line 1\n");
    assert_eq!(output.result.lines[1].line_number, 1);
    assert_eq!(output.result.lines[1].line, "line 2\n");

    client.send_signal(task_id, 9).await.unwrap();

    ctx.shutdown().await
}
