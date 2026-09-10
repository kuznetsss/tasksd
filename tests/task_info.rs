mod common;

use std::env::current_dir;

use crate::common::{
    api::{ErrorResponse, TaskExitNotification, TaskInfoResponse, TaskStartResponse},
    running_app,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn task_info_returns_info_of_running_task() {
    let (ctx, mut client) = running_app().await;

    client.task_start("cat", &[], false).await.unwrap();
    let response: TaskStartResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, client.last_id());
    let task_id = response.result.task_id;

    client.task_info(task_id).await.unwrap();
    let task_info: TaskInfoResponse = client.read_struct().await.unwrap();
    assert_eq!(task_info.id, client.last_id());
    assert_eq!(task_info.result.info.executable, "cat");
    assert!(task_info.result.info.args.is_empty());
    assert_eq!(
        task_info.result.info.working_dir,
        current_dir().unwrap().to_str().unwrap()
    );

    ctx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn task_info_returns_info_of_finished_task() {
    let (ctx, mut client) = running_app().await;

    client.task_start("ls", &["-la"], false).await.unwrap();
    let response: TaskStartResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, client.last_id());
    let task_id = response.result.task_id;

    let exit: TaskExitNotification = client.read_struct().await.unwrap();
    assert_eq!(exit.params.task_id, task_id);
    assert_eq!(exit.params.exit_code.unwrap(), 0);

    // Wait for the task to appear in finished to make sure it is finished
    client
        .wait_for_task_list(|l| l.finished.iter().find(|t| t.id == task_id).is_some())
        .await;

    client.task_info(task_id).await.unwrap();
    let task_info: TaskInfoResponse = client.read_struct().await.unwrap();
    assert_eq!(task_info.id, client.last_id());
    assert_eq!(task_info.result.info.executable, "ls");
    assert_eq!(task_info.result.info.args, &["-la"]);
    assert_eq!(
        task_info.result.info.working_dir,
        current_dir().unwrap().to_str().unwrap()
    );

    ctx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn task_info_returns_on_non_existing_task() {
    let (ctx, mut client) = running_app().await;

    client.task_info(123).await.unwrap();
    let error: ErrorResponse = client.read_struct().await.unwrap();
    assert_eq!(error.id.unwrap(), client.last_id());
    assert_eq!(error.error.code, 7);

    ctx.shutdown().await;
}
