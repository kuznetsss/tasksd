use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{api::request::Request, tasks::task_manager::TaskManager, transport};

pub struct Session {
    cancellation_token: CancellationToken,
    connection: transport::Connection,
    task_manager: Arc<TaskManager>,
}

impl Session {
    pub fn new(
        cancellation_token: CancellationToken,
        connection: transport::Connection,
        task_manager: Arc<TaskManager>,
    ) -> Self {
        Self {
            cancellation_token,
            connection,
            task_manager,
        }
    }

    pub async fn run(mut self) {
        while let Some(msg) = self
            .cancellation_token
            .run_until_cancelled(self.connection.read_message())
            .await
        {
            let request = match msg {
                Ok(msg) => Request::parse(msg),
                Err(e) => {
                    info!("Error reading from client: {e}");
                    break;
                }
            };
            match request {
                Ok(r) => self.handle_request(r),
                Err(e) => {
                    info!("Error parsing request: '{}': {e}", msg.unwrap());
                }
            }
        }
    }

    fn handle_request(&self, _request: Request) {
        tokio::spawn(async move { todo!() });
    }
}
