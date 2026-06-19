use serde_json::json;

use crate::common::{
    TestContextBuilder,
    api::{ErrorResponse, TaskSendSignalResponse, TaskStartResponse},
    running_app,
};

mod common;

#[tokio::test]
async fn invalid_unix_socket() {
    let invalid_path = "/proc/invalid_path";
    let err = TestContextBuilder::new()
        .adjust_cli_args(|args| {
            args.unix_socket_path = invalid_path.into();
        })
        .build()
        .unwrap_err();
    assert!(err.to_string().contains("Error opening unix socket"));
}

#[tokio::test]
#[should_panic(expected = "Application is dropped")]
async fn application_dropped_without_shutdown_panics() {
    let _ = TestContextBuilder::new().build().unwrap();
}

#[tokio::test]
#[should_panic(expected = "custom panic")]
async fn application_doesnt_double_panic_if_already_panicking() {
    let _ctx = TestContextBuilder::new().build().unwrap();
    panic!("custom panic")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn error_reading_from_client() {
    let (ctx, mut client) = running_app().await;
    client.send_str("invalid\n").await.unwrap();
    assert!(client.is_disconnected().await);

    ctx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_empty_line_between_header_and_body() {
    let (ctx, mut client) = running_app().await;
    let request = json![{
        "jsonrpc":"2.0",
        "id":123,
        "method":"task.start",
        "params":{
            "executable": "ls"
        }
    }]
    .to_string();
    let msg = format!("Content-Length: {}{request}\n", request.len());
    client.send_str(&msg).await.unwrap();
    assert!(client.is_disconnected().await);

    ctx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_json() {
    let (ctx, mut client) = running_app().await;

    client.send_msg("{").await.unwrap();
    let response: ErrorResponse = client.read_struct().await.unwrap();
    assert!(response.id.is_none());
    assert_eq!(response.error.code, -32700);

    ctx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_request() {
    let (ctx, mut client) = running_app().await;

    let invalid_request = json!({ // id is missing
        "jsonrpc":"2.0",
        "method":"task.start",
        "params":{
            "executable": "ls"
        }
    });
    client.send_json(&invalid_request).await.unwrap();
    let response: ErrorResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, None);
    assert_eq!(response.error.code, -32600);

    ctx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_method() {
    let (ctx, mut client) = running_app().await;

    let id = 123;
    let invalid_request = json!({
        "jsonrpc":"2.0",
        "id": id,
        "method":"task.invalid_method",
        "params":{
            "executable": "ls"
        }
    });
    client.send_json(&invalid_request).await.unwrap();
    let response: ErrorResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, Some(id));
    assert_eq!(response.error.code, -32601);

    ctx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_params() {
    let (ctx, mut client) = running_app().await;

    let id = 123;
    let invalid_request = json!({
        "jsonrpc":"2.0",
        "id": id,
        "method":"task.start",
        "params":{
            "executable": "ls",
            "invalid_param": "invalid_value"
        }
    });
    client.send_json(&invalid_request).await.unwrap();
    let response: ErrorResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, Some(id));
    assert_eq!(response.error.code, -32602);

    ctx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn running_task_survives_client_disconnect() {
    let (ctx, mut client) = running_app().await;

    client.task_start("cat", &[], true).await.unwrap();

    let response: TaskStartResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, client.last_id());
    let task_id = response.result.task_id;

    drop(client);
    let mut client = ctx.make_client().await;
    // TODO: subscribe new client on exit event of the running task when implemented

    let signal = 9;
    client.send_signal(task_id, signal).await.unwrap();
    let response: TaskSendSignalResponse = client.read_struct().await.unwrap();
    assert_eq!(response.id, client.last_id());

    ctx.shutdown().await;
}
