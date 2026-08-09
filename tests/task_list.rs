mod common;

use std::env::current_dir;

use crate::common::{
    api::{TaskExitNotification, TaskListResponse, TaskStartResponse},
    running_app,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn task_list_returns_list_of_tasks() {
    let (ctx, mut client) = running_app().await;

    client.task_start("ls", &["-la"], false).await.unwrap();
    let response: TaskStartResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, client.last_id());
    let finished_task_id = response.result.task_id;

    let exit: TaskExitNotification = client.read_struct().await.unwrap();
    assert_eq!(exit.params.task_id, finished_task_id);
    assert_eq!(exit.params.exit_code.unwrap(), 0);

    client.task_start("cat", &[], false).await.unwrap();
    let response: TaskStartResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, client.last_id());
    let running_task_id = response.result.task_id;

    client.task_list().await.unwrap();
    let list: TaskListResponse = client.read_struct().await.unwrap();
    assert_eq!(list.id, client.last_id());

    assert_eq!(list.result.tasks.running.len(), 1);
    let running_task_entry = &list.result.tasks.running[0];
    assert_eq!(running_task_entry.id, running_task_id);
    assert_eq!(running_task_entry.info.executable, "cat");
    assert!(running_task_entry.info.args.is_empty());
    let current_dir = current_dir().unwrap().to_string_lossy().to_string();
    assert_eq!(running_task_entry.info.working_dir, current_dir);

    assert_eq!(list.result.tasks.finished.len(), 1);
    let finished_task_entry = &list.result.tasks.finished[0];
    assert_eq!(finished_task_entry.info.executable, "ls");
    assert_eq!(finished_task_entry.info.args, &["-la".to_string()]);
    assert_eq!(finished_task_entry.info.working_dir, current_dir);

    ctx.shutdown().await;
}
