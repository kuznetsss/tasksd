use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Result;
use tasksd::application::{Application, CliOptions};
use tempfile::TempDir;

use crate::common::Client;

#[derive(Debug)]
pub struct TestContext {
    _tmp_dir: TempDir,
    socket_path: PathBuf,
    app: Arc<Application>,
}

impl TestContext {
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    pub fn app(&self) -> Arc<Application> {
        self.app.clone()
    }

    pub fn spawn_app_run(&self) {
        let app = self.app();
        tokio::spawn(async move {
            app.run().await;
        });
    }

    pub async fn shutdown(&self) {
        self.app.shutdown().await;
    }

    pub async fn make_client(&self) -> Client {
        Client::connect(&self.socket_path()).await.unwrap()
    }
}

pub struct TestContextBuilder {
    cli_args: CliOptions,
}

impl TestContextBuilder {
    pub fn new() -> Self {
        let cli_args = CliOptions {
            thread_number: 1,
            process_buffer_size: 100,
            unix_socket_path: Default::default(),
            graceful_period: 1,
            quiet: false,
            log_file: None,
        };
        Self { cli_args }
    }

    pub fn adjust_cli_args<F: FnOnce(&mut CliOptions)>(mut self, f: F) -> Self {
        f(&mut self.cli_args);
        self
    }

    pub fn build(mut self) -> Result<TestContext> {
        let tmp_dir = tempfile::tempdir().unwrap();
        let mut socket_path = tmp_dir.path().join("t.sock");
        if self.cli_args.unix_socket_path == PathBuf::default() {
            self.cli_args.unix_socket_path = socket_path.clone();
        } else {
            socket_path = self.cli_args.unix_socket_path.clone();
        }
        let app = Application::new(self.cli_args)?;
        Ok(TestContext {
            _tmp_dir: tmp_dir,
            socket_path,
            app: Arc::new(app),
        })
    }
}
