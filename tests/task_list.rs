mod common;

use std::{env::current_dir, time::Duration};

use crate::common::{
    Client,
    api::{TaskExitNotification, TaskList, TaskListResponse, TaskStartResponse},
    running_app,
};

async fn wait_for_task_list(
    client: &mut Client,
    predicate: impl Fn(&TaskList) -> bool,
) -> TaskList {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            client.task_list().await.unwrap();
            let response: TaskListResponse = client.read_struct().await.unwrap();
            assert_eq!(response.id, client.last_id());
            if predicate(&response.result.tasks) {
                return response.result.tasks;
            }
        }
    })
    .await
    .expect("task list should converge")
}

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

    let list = wait_for_task_list(&mut client, |t| t.finished.len() == 1).await;

    assert_eq!(list.running.len(), 1);
    let running_task_entry = &list.running[0];
    assert_eq!(running_task_entry.id, running_task_id);
    assert_eq!(running_task_entry.info.executable, "cat");
    assert!(running_task_entry.info.args.is_empty());
    let current_dir = current_dir().unwrap().to_string_lossy().to_string();
    assert_eq!(running_task_entry.info.working_dir, current_dir);

    assert_eq!(list.finished.len(), 1);
    let finished_task_entry = &list.finished[0];
    assert_eq!(finished_task_entry.info.executable, "ls");
    assert_eq!(finished_task_entry.info.args, &["-la".to_string()]);
    assert_eq!(finished_task_entry.info.working_dir, current_dir);

    ctx.shutdown().await;
}
