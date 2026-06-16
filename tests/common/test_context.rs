use std::path::{Path, PathBuf};

use anyhow::Result;
use tasksd::application::{Application, CliOptions};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

pub struct TestContext {
    _tmp_dir: TempDir,
    socket_path: PathBuf,
    root_cancellation: CancellationToken,
    app: Application,
}

impl TestContext {
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    pub fn root_cancellation(&self) -> &CancellationToken {
        &self.root_cancellation
    }

    pub fn app(&self) -> &Application {
        &self.app
    }

    // TODO: add run server

    pub async fn shutdown(&self) {
        self.app.shutdown().await;
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
        let socket_path = tmp_dir.path().join("t.sock");
        let root_cancellation = CancellationToken::new();
        self.cli_args.unix_socket_path = socket_path.clone();
        let app = Application::new(root_cancellation.clone(), self.cli_args)?;
        Ok(TestContext {
            _tmp_dir: tmp_dir,
            socket_path,
            root_cancellation,
            app,
        })
    }
}
