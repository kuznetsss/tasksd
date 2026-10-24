use crate::common::{
    api::{
        ErrorResponse, TaskExitNotification, TaskOutputNotification, TaskSendInputResponse,
        TaskSendSignalResponse, TaskSetSubscribeResponse, TaskStartResponse,
    },
    running_app,
};

mod common;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn task_set_subscribe_stop_subscriber_success() {
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
        .message(move |n: TaskOutputNotification| {
            n.params.task_id == task_id && n.params.line == input
        })
        .check()
        .await
        .unwrap();

    client.set_subscribe(task_id, false, false).await.unwrap();
    let response: TaskSetSubscribeResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, client.last_id());

    let input = "another input\n";
    client.send_input(task_id, input).await.unwrap();
    let response: TaskSendInputResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, client.last_id());

    client.send_signal(task_id, 9).await.unwrap();
    let response: TaskSendSignalResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, client.last_id());

    client
        .wait_for_task_list(|l| {
            l.len() == 1 && l[0].task_id == task_id && l[0].status == "finished"
        })
        .await;

    ctx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn task_set_subscribe_stop_subscriber_when_no_subscriber() {
    let (ctx, mut client) = running_app().await;

    client.task_start("cat", &[], true).await.unwrap();
    let response: TaskStartResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, client.last_id());
    let task_id = response.result.task_id;

    for _ in 0..2 {
        client.set_subscribe(task_id, false, false).await.unwrap();
        let response: TaskSetSubscribeResponse = client.read_struct().await.unwrap();
        assert_eq!(response.id, client.last_id());
    }

    ctx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn task_set_subscribe_stop_subscriber_when_task_has_finished() {
    let (ctx, mut client) = running_app().await;

    client.task_start("ls", &[], false).await.unwrap();
    let response: TaskStartResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, client.last_id());
    let task_id = response.result.task_id;

    let exit: TaskExitNotification = client.read_struct().await.unwrap();
    assert_eq!(exit.params.task_id, task_id);
    assert_eq!(exit.params.exit_code, Some(0));

    client.set_subscribe(task_id, false, false).await.unwrap();
    let error: ErrorResponse = client.read_struct().await.unwrap();
    assert_eq!(error.id, Some(client.last_id()));
    assert_eq!(error.error.code, 5);

    ctx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn task_set_subscribe_stop_subscriber_non_existing_task() {
    let (ctx, mut client) = running_app().await;

    client.set_subscribe(123, false, false).await.unwrap();
    let error: ErrorResponse = client.read_struct().await.unwrap();
    assert_eq!(error.id, Some(client.last_id()));
    assert_eq!(error.error.code, 7);

    ctx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn task_set_subscribe_disable_output_subscription() {
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
        .message(move |n: TaskOutputNotification| {
            n.params.task_id == task_id && n.params.line == input
        })
        .check()
        .await
        .unwrap();

    client.set_subscribe(task_id, false, true).await.unwrap();
    let response: TaskSetSubscribeResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, client.last_id());

    let input = "some input\n";
    client.send_input(task_id, input).await.unwrap();
    let response: TaskSendInputResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, client.last_id());

    client.send_signal(task_id, 9).await.unwrap();
    let last_id = client.last_id();
    client
        .expect_unordered()
        .message(move |r: TaskSendSignalResponse| r.id == last_id)
        .message(move |e: TaskExitNotification| e.params.task_id == task_id)
        .check()
        .await
        .unwrap();

    ctx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn task_set_subscribe_spawn_new_subscriber() {
    let (ctx, mut client) = running_app().await;

    client.task_start("cat", &[], false).await.unwrap();
    let response: TaskStartResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, client.last_id());
    let task_id = response.result.task_id;

    drop(client);
    let mut client = ctx.make_client().await;

    client.set_subscribe(task_id, true, true).await.unwrap();
    let response: TaskSetSubscribeResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, client.last_id());

    let input = "some input\n";
    client.send_input(task_id, input).await.unwrap();
    let last_id = client.last_id();
    client
        .expect_unordered()
        .message(move |r: TaskSendInputResponse| r.id == last_id)
        .message(move |n: TaskOutputNotification| {
            n.params.task_id == task_id && n.params.line == input
        })
        .check()
        .await
        .unwrap();

    client.send_signal(task_id, 9).await.unwrap();
    let last_id = client.last_id();
    client
        .expect_unordered()
        .message(move |r: TaskSendSignalResponse| r.id == last_id)
        .message(move |e: TaskExitNotification| e.params.task_id == task_id)
        .check()
        .await
        .unwrap();

    ctx.shutdown().await;
}
