#![cfg_attr(coverage_nightly, feature(coverage_attribute))]
#![cfg_attr(coverage_nightly, coverage(off))]
use std::sync::Arc;

use tasksd::application::{Application, CliOptions, ShutdownHandler, setup_logger};

use clap::Parser;
use tracing::info;

fn main() -> anyhow::Result<()> {
    let cli_args = CliOptions::parse();
    let _guard = setup_logger(cli_args.log_file.as_ref(), !cli_args.quiet)?;
    info!(
        "Starting {} {}",
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION")
    );

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(cli_args.thread_number)
        .build()
        .unwrap()
        .block_on(async move {
            let shutdown_handler = ShutdownHandler::new();
            let application = Arc::new(Application::new(cli_args, shutdown_handler.trigger())?);
            let app_run = tokio::spawn({
                let application = application.clone();
                async move {
                    application.run().await;
                }
            });
            let shutdown_handler = shutdown_handler.wait().await;
            tokio::select! {
                _ = application.shutdown() => {},
                _ = shutdown_handler.wait_force() => {}
            }
            app_run.abort();
            info!("Exit");
            Ok(())
        })
}
