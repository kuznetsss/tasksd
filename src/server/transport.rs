use std::pin::pin;
use std::{path::Path, sync::Arc};

use anyhow::{Context, Result};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
use tokio::net::unix::OwnedReadHalf;
use tokio::sync::mpsc::{Sender, channel};
use tokio::{
    io::{BufReader, BufWriter, Interest},
    net::UnixStream,
};
use tokio_util::sync::CancellationToken;
use tracing::error;


pub trait Output: Clone {
    async fn write_line(&mut self, line: Arc<String>) -> Result<()>;
}

pub struct Transport<In> {
    in_part: BufReader<In>,
    out_sender: Sender<Arc<String>>,
}

impl Transport<OwnedReadHalf> {
    pub async fn open_socket(
        socket_path: &Path,
        cancellation_token: CancellationToken,
    ) -> Result<Self> {
        let socket = UnixStream::connect(socket_path).await?;
        socket
            .ready(Interest::READABLE | Interest::WRITABLE)
            .await?;
        let (in_part, out_part) = socket.into_split();
        Ok(Self {
            in_part: BufReader::new(in_part),
            out_sender: Self::spawn_writing(out_part, cancellation_token),
        })
    }
}

impl<In> Transport<In>
where
    In: AsyncReadExt + Send + 'static,
{
    const CHANNEL_BUFFER_SIZE: usize = 16;

    fn spawn_writing<Writer>(
        writer: Writer,
        cancellation_token: CancellationToken,
    ) -> Sender<Arc<String>>
    where
        Writer: AsyncWriteExt + Send + 'static,
    {
        let writer = BufWriter::new(writer);
        let (sender, mut receiver) = channel::<Arc<String>>(Self::CHANNEL_BUFFER_SIZE);
        tokio::spawn(async move {
            let mut writer = pin!(writer);
            while let Some(Some(msg)) = cancellation_token
                .run_until_cancelled(receiver.recv())
                .await
            {
                if let Err(e) = writer.write(msg.as_bytes()).await {
                    error!("Error writing to output: {e}. Message: {msg}");
                    cancellation_token.cancel();
                    return;
                }
                if let Err(e) = writer.flush().await {
                    error!("Error writing to output: {e}. Message: {msg}");
                    cancellation_token.cancel();
                    return;
                }
            }
        });
        sender
    }

    pub fn split_into(self) -> (BufReader<In>, Sender<Arc<String>>) {
        (self.in_part, self.out_sender)
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::create_dir, path::PathBuf};

    use super::*;
    use tempfile::{TempDir, tempdir};
    use tokio::net::UnixListener;

    struct TestContext {
        dir: TempDir,
        socket_path: PathBuf,
        cancellation_token: CancellationToken,
        listener: UnixListener,
    }

    impl TestContext {
        fn new() -> Self {
            let dir = tempdir().unwrap();
            let socket_path = dir.path().join("socket_send_test.socket");
            let cancellation_token = CancellationToken::new();
            let listener = UnixListener::bind(&socket_path).unwrap();
            Self {
                dir,
                socket_path,
                cancellation_token,
                listener,
            }
        }
    }

    #[tokio::test]
    async fn socket_send() {
        let context = TestContext::new();
        let msg = Arc::new("hello world\n".to_string());
        let listener_handle = tokio::spawn({
            let listener = context.listener;
            let msg = Arc::clone(&msg);
            async move {
                let (stream, _) = listener.accept().await.unwrap();
                stream
                    .ready(Interest::READABLE | Interest::WRITABLE)
                    .await
                    .unwrap();
                let (input, output) = stream.into_split();
                let mut input = BufReader::new(input);
                let mut s = String::new();
                input.read_line(&mut s).await.unwrap();
                assert_eq!(s, *msg);
            }
        });
        let transport =
            Transport::open_socket(&context.socket_path, context.cancellation_token.clone())
                .await
                .unwrap();
        transport.out_sender.send(msg).await.unwrap();
        listener_handle.await.unwrap();
    }
}
