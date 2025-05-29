use tokio::io::AsyncWriteExt;

mod api;
mod io;
mod service;

fn main() -> anyhow::Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .build()
        .unwrap()
        .block_on(async {
            // run().await;
            for _ in 0..40 {
                tokio::spawn(async {
                    print().await;
                });
            }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        });
    Ok(())
}

async fn print() {
    use tokio::io::{AsyncBufReadExt, BufReader, BufWriter};
    let mut out = tokio::io::stdout();
    out.write_all("hello world\n".as_bytes()).await.unwrap();
    out.flush().await.unwrap();
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
