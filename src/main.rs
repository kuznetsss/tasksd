#[allow(dead_code)] // prevent to many warnings while developing
// mod api;
mod server;

use clap::Parser;

fn main() -> anyhow::Result<()> {
    let cli_args = CliOptions::parse();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(cli_args.thread_number)
        .build()
        .unwrap()
        .block_on(async move { Ok(()) })
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
}
