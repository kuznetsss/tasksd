use tokio::io::AsyncWriteExt;

mod service;
mod api;

fn main() -> anyhow::Result<()> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            run().await;
        });
    Ok(())
}

async fn run() {
    use tokio::io::{AsyncBufReadExt, BufReader, BufWriter};

    let mut stdin = BufReader::new(tokio::io::stdin());
    let mut stdout = BufWriter::new(tokio::io::stdout());
    let mut buffer = String::new();
    loop {
        if let Err(e) = stdin.read_line(&mut buffer).await {
            eprintln!("{e}");
            continue;
        }
        if let Err(e) = stdout.write_all(buffer.as_bytes()).await {
            eprintln!("Error writing: {e}");
        }
        stdout.flush().await.unwrap();
        buffer.clear();
    }
}
