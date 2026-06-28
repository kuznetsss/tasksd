#![cfg_attr(coverage_nightly, feature(coverage_attribute))]
#![cfg_attr(coverage_nightly, coverage(off))]
use std::sync::Arc;

use tasksd::application::{Application, CliOptions, setup_logger};

use clap::Parser;
use tokio::task::JoinSet;
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
            let application = Arc::new(Application::new(cli_args)?);
            let app_run = tokio::spawn({
                let application = application.clone();
                async move {
                    application.run().await;
                }
            });
            ctrl_c_handler(application.clone()).await;
            app_run.abort();
            info!("Exit");
            Ok(())
        })
}

async fn ctrl_c_handler(application: Arc<Application>) {
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for Ctrl-C");
    info!("Got Ctrl-C, shutting down");

    let mut jobs = JoinSet::new();
    jobs.spawn(async move {
        application.shutdown().await;
    });
    jobs.spawn(async {
        tokio::signal::ctrl_c().await.unwrap();
        info!("Force exit on the second Ctrl-C");
    });
    jobs.join_next().await;
}
