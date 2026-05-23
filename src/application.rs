use std::sync::Arc;

use anyhow::Result;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::{
    CliOptions, api, session::Session, tasks::task_manager::TaskManager,
    transport::UnixSocketServer,
};

pub struct Application {
    root_cancellation: CancellationToken,
    server: UnixSocketServer,
    task_manager: Arc<TaskManager>,
}

impl Application {
    pub fn new(root_cancellation: CancellationToken, cli_args: CliOptions) -> Result<Self> {
        let server = UnixSocketServer::new_unix_socket(
            &cli_args.unix_socket_path,
            root_cancellation.child_token(),
        )?;
        Ok(Self {
            root_cancellation,
            server,
            task_manager: TaskManager::new(cli_args.process_buffer_size),
        })
    }

    pub async fn run(&self) {
        self.run_server().await;
    }

    async fn run_server(&self) {
        while let Some(connection) = self
            .root_cancellation
            .run_until_cancelled(self.server.wait_for_connection())
            .await
        {
            let connection = match connection {
                Ok(c) => c,
                Err(e) => {
                    warn!("Error accepting unix socket connection: {e}");
                    continue;
                }
            };
            // TODO: create session here
            tokio::spawn({
                let cancellation_token = self.root_cancellation.child_token();
                async move {
                    let connection = api::connection::Connection::new(connection);
                    let session = Session::new(cancellation_token, connection);
                    session.run().await;
                }
            });
        }
    }
}
