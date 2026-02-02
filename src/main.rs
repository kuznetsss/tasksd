#[allow(dead_code)] // prevent to many warnings while developing
// mod api;
mod server;

use std::path::PathBuf;

use clap::Parser;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::server::Server;

fn main() -> anyhow::Result<()> {
    let cli_args = CliOptions::parse();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(cli_args.thread_number)
        .build()
        .unwrap()
        .block_on(async move {
            let root_cancellation = CancellationToken::new();
            let server =
                Server::new_unix_socket(&cli_args.unix_socket_path, root_cancellation.clone())?;
            let mut tasks = JoinSet::new();
            tasks.spawn(ctrl_c_handler(root_cancellation.clone()));
            tasks.spawn(run_server(server));
            tasks.join_next().await;

            Ok(())
        })
}

async fn run_server(server: Server) {
    while let Ok(mut c) = server.wait_for_connection().await {
        println!("Client connected");
        tokio::spawn(async move {
            while let Ok(line) = c.reader.read_line().await {
                println!("Got message: '{line}'");
            }
            println!("Client disconnected");
        });
    }
}

/// tasksd - Editor companion to manage processes
#[derive(clap::Parser, Debug)]
#[command(version, about)]
struct CliOptions {
    /// Number of threads to use (default to the number of cpu cores available)
    #[arg(short = 'j', long, default_value_t = 4)]
    thread_number: usize,

    /// Buffer size in lines for each running process
    #[arg(short = 'b', long, default_value_t = 10000)]
    process_buffer_size: usize,

    /// Path to unix socket to open
    #[arg(short = 'u', long)]
    unix_socket_path: PathBuf,
}

async fn ctrl_c_handler(root_cancellation: CancellationToken) {
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for Ctrl-C");
    root_cancellation.cancel();
    tokio::signal::ctrl_c().await.unwrap();
}
