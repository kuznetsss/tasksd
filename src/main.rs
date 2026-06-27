#![cfg_attr(coverage_nightly, feature(coverage_attribute))]
#![cfg_attr(coverage_nightly, coverage(off))]
use std::sync::Arc;

use tasksd::application::{Application, CliOptions, setup_logger};

use clap::Parser;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
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
            let root_cancellation = CancellationToken::new();
            let application = Arc::new(Application::new(root_cancellation.clone(), cli_args)?);
            let mut jobs = JoinSet::new();
            jobs.spawn(ctrl_c_handler(root_cancellation.clone()));
            jobs.spawn({
                let application = application.clone();
                async move { application.run().await }
            });
            jobs.join_next().await;
            application.shutdown().await;
            info!("Exit");
            Ok(())
        })
}

async fn ctrl_c_handler(root_cancellation: CancellationToken) {
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for Ctrl-C");
    info!("Got Ctrl-C, shutting down");
    root_cancellation.cancel();
    tokio::signal::ctrl_c().await.unwrap();
    info!("Force exit on the second Ctrl-C");
}
