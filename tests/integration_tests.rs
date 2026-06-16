use crate::common::{Client, TestContextBuilder};

mod common;

#[tokio::test]
async fn connect() {
    let ctx = TestContextBuilder::new().build().unwrap();

    let _client = Client::connect(ctx.socket_path()).await.unwrap();
    ctx.shutdown().await;
}
