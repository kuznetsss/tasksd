use std::path::Path;

use anyhow::Result;
use tokio::{
    io::{BufReader, Interest},
    net::{UnixListener, unix::OwnedReadHalf},
};
use tokio_util::sync::CancellationToken;

use crate::server::line_io::BackgroundLineWriter;

pub type Input = BufReader<OwnedReadHalf>;
pub type Output = BackgroundLineWriter;

pub struct Server {
    listener: UnixListener,
}

impl Server {
    pub fn new(path: &Path) -> Result<Self> {
        Ok(Self {
            listener: UnixListener::bind(path)?,
        })
    }

    pub async fn wait_for_connection(
        &self,
        cancellation_token: CancellationToken,
    ) -> Result<(Input, Output)> {
        let (stream, _) = self.listener.accept().await?;
        stream
            .ready(Interest::READABLE | Interest::WRITABLE)
            .await?;
        let (in_part, out_part) = stream.into_split();
        Ok((
            Input::new(in_part),
            Output::spawn(out_part, cancellation_token),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, sync::Arc};

    use crate::server::line_io::{LineReader, LineWriter};

    use super::*;
    use tempfile::TempDir;
    use tokio::{io::AsyncWriteExt, net::UnixStream};

    struct ServerTestContext {
        temp_dir: TempDir,
        socket_path: PathBuf,
        server: Server,
    }

    impl ServerTestContext {
        fn new() -> Self {
            let temp_dir = TempDir::new().unwrap();
            let socket_path = temp_dir.path().join("test_socket");
            let server = Server::new(&socket_path).unwrap();
            Self {
                temp_dir,
                socket_path,
                server,
            }
        }
    }

    #[tokio::test]
    async fn server_accepts_connection() {
        let ctx = ServerTestContext::new();
        let handle = tokio::spawn({
            let server = ctx.server;
            async move {
                server
                    .wait_for_connection(CancellationToken::new())
                    .await
                    .unwrap();
            }
        });
        let _ = UnixStream::connect(ctx.socket_path).await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), handle)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn server_reads_writes() {
        let msg_to_read = "message to read\n";
        let msg_to_write = "message to write\n";
        let ctx = ServerTestContext::new();
        let handle = tokio::spawn({
            let server = ctx.server;
            async move {
                let (mut reader, mut writer) = server
                    .wait_for_connection(CancellationToken::new())
                    .await
                    .unwrap();
                writer
                    .write_line(Arc::new(msg_to_write.to_string()))
                    .await
                    .unwrap();
                let msg = reader.read_line().await.unwrap();
                assert_eq!(msg, msg_to_read);
            }
        });

        let stream = UnixStream::connect(ctx.socket_path).await.unwrap();
        let (reader, mut writer) = stream.into_split();
        writer.write_all(msg_to_read.as_bytes()).await.unwrap();
        let mut reader = BufReader::new(reader);
        let msg = reader.read_line().await.unwrap();
        assert_eq!(msg, msg_to_write);

        tokio::time::timeout(std::time::Duration::from_secs(1), handle)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn server_client_disconnects() {
        let ctx = ServerTestContext::new();
        let handle = tokio::spawn({
            let server = ctx.server;
            async move {
                let (mut reader, _) = server
                    .wait_for_connection(CancellationToken::new())
                    .await
                    .unwrap();
                reader.read_line().await.unwrap();
            }
        });

        let stream = UnixStream::connect(ctx.socket_path).await.unwrap();
        drop(stream);

        tokio::time::timeout(std::time::Duration::from_secs(1), handle)
            .await
            .unwrap()
            .unwrap();
    }
}
