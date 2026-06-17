use crate::common::TestContextBuilder;

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
async fn application_doesnt_double_panic_if_already_panicing() {
    let _ctx = TestContextBuilder::new().build().unwrap();
    panic!("custom panic")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn error_reading_from_client() {
    let mut ctx = TestContextBuilder::new().build().unwrap();
    let app = ctx.app();
    tokio::spawn(async move {
        app.run().await;
    });
    let mut client = ctx.make_client().await;

    client.send_str("invalid\n").await.unwrap();
    // Client should be disconnected after the invalid message
    assert!(!client.is_connected().await);

    ctx.shutdown().await;
}
