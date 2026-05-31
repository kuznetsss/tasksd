mod handler;
mod session;

use std::sync::{Arc, atomic::AtomicBool};

use anyhow::Result;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::{CliOptions, tasks::TaskManager, transport::UnixSocketServer};
use session::Session;

pub struct Application {
    root_cancellation: CancellationToken,
    server: UnixSocketServer,
    task_manager: Arc<TaskManager>,
    shutdown_complete: AtomicBool,
}

impl Application {
    pub fn new(root_cancellation: CancellationToken, cli_args: CliOptions) -> Result<Self> {
        let server = UnixSocketServer::new_unix_socket(&cli_args.unix_socket_path)?;
        Ok(Self {
            root_cancellation,
            server,
            task_manager: TaskManager::new(cli_args.process_buffer_size),
            shutdown_complete: AtomicBool::new(false),
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
            let accepted_connection = match connection {
                Ok(c) => c,
                Err(e) => {
                    warn!("Error accepting unix socket connection: {e}");
                    continue;
                }
            };
            tokio::spawn({
                let cancellation_token = self.root_cancellation.child_token();
                let task_manager = self.task_manager.clone();
                async move {
                    let connection =
                        accepted_connection.into_connection(cancellation_token.clone());
                    let session = Session::new(cancellation_token, connection, task_manager);
                    session.run().await;
                }
            });
        }
    }

    pub async fn shutdown(&self) {
        if self
            .shutdown_complete
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return;
        }
        self.root_cancellation.cancel();
        // TODO: shutdown all the running tasks
        self.task_manager.join().await;
        self.shutdown_complete
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

impl Drop for Application {
    fn drop(&mut self) {
        assert!(
            self.shutdown_complete
                .load(std::sync::atomic::Ordering::Relaxed),
            "Application is dropped without calling shutdown()"
        );
    }
}
