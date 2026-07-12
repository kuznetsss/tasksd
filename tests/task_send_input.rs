use serde::Deserialize;

use crate::common::{
    api::{
        ErrorResponse, TaskExitNotification, TaskOutputNotification, TaskSendInputResponse,
        TaskStartResponse,
    },
    running_app,
};

mod common;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_input_sends_input() {
    let (ctx, mut client) = running_app().await;

    client.task_start("cat", &[], true).await.unwrap();

    let response: TaskStartResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, client.last_id());
    let task_id = response.result.task_id;

    let input = "some input\n";
    client.send_input(task_id, input).await.unwrap();

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Either {
        Response(TaskSendInputResponse),
        Output(TaskOutputNotification),
    }

    let mut got_response = false;
    let mut got_output = false;
    for _ in 0..2 {
        match client.read_struct::<Either>().await.unwrap() {
            Either::Response(response) => {
                assert_eq!(response.id, client.last_id());
                got_response = true;
            }
            Either::Output(output) => {
                assert_eq!(output.params.task_id, task_id);
                assert_eq!(output.params.line, input);
                assert_eq!(output.params.line_number, 0);
                got_output = true;
            }
        }
    }

    assert!(got_output);
    assert!(got_response);

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
