use std::path::{Path, PathBuf};

use anyhow::Result;
use tokio::{
    io::{BufReader, Interest},
    net::{UnixListener, unix::OwnedReadHalf},
};
use tokio_util::sync::CancellationToken;

use crate::server::{ServerImpl, background_writer::BackgroundWriter};

pub struct UnixSocketServerImpl {
    listener: UnixListener,
    socket_path: PathBuf,
}

impl UnixSocketServerImpl {
    pub fn new(path: &Path) -> Result<Self> {
        if path.exists() {
            std::fs::remove_file(path)?;
        }

        Ok(Self {
            listener: UnixListener::bind(path)?,
            socket_path: path.to_path_buf(),
        })
    }
}

impl Drop for UnixSocketServerImpl {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.socket_path);
    }
}

impl ServerImpl for UnixSocketServerImpl {
    type Reader = BufReader<OwnedReadHalf>;
    type Writer = BackgroundWriter;

    async fn wait_for_connection(
        &self,
        cancellation_token: CancellationToken,
    ) -> Result<(Self::Reader, Self::Writer)> {
        let stream = match cancellation_token
            .run_until_cancelled(self.listener.accept())
            .await
        {
            Some(Ok((s, _))) => s,
            Some(Err(e)) => {
                return Err(e.into());
            }
            None => anyhow::bail!("Cancelled"),
        };
        stream
            .ready(Interest::READABLE | Interest::WRITABLE)
            .await?;
        let (reader, writer) = stream.into_split();
        Ok((
            Self::Reader::new(reader),
            Self::Writer::spawn(writer, cancellation_token),
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::server::io::{Reader, Writer};

    use super::*;
    use std::path::PathBuf;
    use tempfile::TempDir;
    use tokio::{io::AsyncWriteExt, net::UnixStream};

    struct ServerTestContext {
        temp_dir: TempDir,
        socket_path: PathBuf,
        server: UnixSocketServerImpl,
    }

    impl ServerTestContext {
        fn new() -> Self {
            let temp_dir = TempDir::new().unwrap();
            let socket_path = temp_dir.path().join("test_socket");
            let server = UnixSocketServerImpl::new(&socket_path).unwrap();
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
                writer.write(msg_to_write.to_string()).await.unwrap();
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
                reader.read_line().await.unwrap_err();
            }
        });

        let stream = UnixStream::connect(ctx.socket_path).await.unwrap();
        drop(stream);

        tokio::time::timeout(std::time::Duration::from_secs(1), handle)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn server_wait_for_connection_cancelled() {
        let ctx = ServerTestContext::new();
        let cancellation_token = CancellationToken::new();
        cancellation_token.cancel();
        let handle = tokio::spawn({
            let server = ctx.server;
            async move {
                server
                    .wait_for_connection(cancellation_token)
                    .await
                    .unwrap_err();
            }
        });

        tokio::time::timeout(std::time::Duration::from_secs(1), handle)
            .await
            .unwrap()
            .unwrap();
    }
}
