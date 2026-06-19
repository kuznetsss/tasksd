mod client;
mod test_context;

pub use client::Client;
pub use test_context::{TestContext, TestContextBuilder};

pub async fn running_app() -> (TestContext, Client) {
    let ctx = TestContextBuilder::new().build().unwrap();
    ctx.spawn_app_run();
    let client = ctx.make_client().await;
    (ctx, client)
}
