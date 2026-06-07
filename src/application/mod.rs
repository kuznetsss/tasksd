mod handler;
mod logger;
mod session;

pub use logger::setup_logger;

use std::{
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};

use anyhow::Result;
use rustix::{path::Arg, process::Signal};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info, info_span, warn};

use crate::{CliOptions, tasks::TaskManager, transport::UnixSocketServer};
use session::Session;

pub struct Application {
    root_cancellation: CancellationToken,
    server: UnixSocketServer,
    task_manager: Arc<TaskManager>,
    shutdown_complete: AtomicBool,
    graceful_period: Duration,
}

impl Application {
    pub fn new(root_cancellation: CancellationToken, cli_args: CliOptions) -> Result<Self> {
        info!(
            "Opening unix socket: {}",
            &cli_args
                .unix_socket_path
                .as_str()
                .unwrap_or("<not displayable>")
        );
        let server = UnixSocketServer::new_unix_socket(&cli_args.unix_socket_path)?;
        Ok(Self {
            root_cancellation,
            server,
            task_manager: TaskManager::new(cli_args.process_buffer_size),
            shutdown_complete: AtomicBool::new(false),
            graceful_period: Duration::from_secs(cli_args.graceful_period),
        })
    }

    pub async fn run(&self) {
        self.run_server().await;
    }

    async fn run_server(&self) {
        info!("Listening for incoming connection");
        let mut client_id = 0_usize;
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
                let span = info_span!("client", client_id);
                let cancellation_token = self.root_cancellation.child_token();
                let task_manager = self.task_manager.clone();
                async move {
                    info!("Client connected");
                    let connection =
                        accepted_connection.into_connection(cancellation_token.clone());
                    let session = Session::new(cancellation_token, connection, task_manager);
                    session.run().await;
                    info!("Connection closed");
                }
                .instrument(span)
            });
            client_id += 1;
        }
    }

    pub async fn shutdown(&self) {
        if self
            .shutdown_complete
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
            )
            .is_err()
        {
            return;
        }

        info!("Shutdown, sending SIGTERM to all running tasks");

        self.root_cancellation.cancel();
        self.task_manager.send_signal_to_all_tasks(Signal::TERM);

        let mut parallel_jobs = JoinSet::new();
        #[derive(Debug)]
        enum Event {
            Timeout,
            Finish,
        }
        let graceful_period = self.graceful_period;
        parallel_jobs.spawn({
            let task_manager = self.task_manager.clone();
            async move {
                tokio::time::sleep(graceful_period).await;
                warn!("Some tasks are still running after graceful period {graceful_period:?}. Sending SIGKILL");
                task_manager.send_signal_to_all_tasks(Signal::KILL);
                const KILL_TIMEOUT: Duration = Duration::from_secs(2);
                tokio::time::sleep(KILL_TIMEOUT).await;
                Event::Timeout
            }
        });
        parallel_jobs.spawn({
            let task_manager = self.task_manager.clone();
            async move {
                task_manager.join().await;
                Event::Finish
            }
        });
        match parallel_jobs.join_next().await.unwrap() {
            Ok(Event::Finish) => {}
            Ok(Event::Timeout) => {
                warn!(
                    "Some tasks are still not completed after {:?} and SIGKILL",
                    graceful_period
                );
            }
            Err(e) => {
                warn!("Error joining shutdown jobs: {e}");
            }
        }
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
