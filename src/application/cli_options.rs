use std::path::PathBuf;

/// tasksd - Editor companion to manage processes
#[derive(clap::Parser, Debug)]
#[command(version, about)]
pub struct CliOptions {
    /// Number of threads to use (default to the number of cpu cores available)
    #[arg(short = 'j', long, default_value_t = 4)]
    pub thread_number: usize,

    /// Buffer size in lines for each running process
    #[arg(short = 'b', long, default_value_t = 10000)]
    pub process_buffer_size: usize,

    /// Path to unix socket to open
    #[arg(short = 'u', long)]
    pub unix_socket_path: PathBuf,

    /// Shutdown graceful period, seconds
    #[arg(short = 'g', long, default_value_t = 5)]
    pub graceful_period: u64,

    /// Disable logging to the console
    #[arg(short = 'q', long, default_value_t = false)]
    pub quiet: bool,

    /// Log to file
    #[arg(short = 'l', long)]
    pub log_file: Option<PathBuf>,
}
