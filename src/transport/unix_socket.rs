use std::path::{Path, PathBuf};

use anyhow::Result;
use tokio::io::Interest;
use tokio::net::{
    UnixListener,
    unix::{OwnedReadHalf, OwnedWriteHalf},
};

use crate::transport::ServerImpl;

pub struct UnixSocketServerImpl {
    listener: UnixListener,
    socket_path: PathBuf,
}

impl UnixSocketServerImpl {
    pub fn new(path: &Path) -> Result<Self> {
        // TODO: if file already exist provide more verbose error or add some logic
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
    type ReaderHalf = OwnedReadHalf;
    type WriterHalf = OwnedWriteHalf;

    async fn wait_for_connection(&self) -> Result<(Self::ReaderHalf, Self::WriterHalf)> {
        let (stream, _) = self.listener.accept().await?;
        stream
            .ready(Interest::READABLE | Interest::WRITABLE)
            .await?;
        Ok(stream.into_split())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tempfile::TempDir;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt, BufReader},
        net::UnixStream,
    };

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
                server.wait_for_connection().await.unwrap();
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
                let (mut reader, mut writer) = server.wait_for_connection().await.unwrap();
                writer.write_all(msg_to_write.as_bytes()).await.unwrap();
                drop(writer);
                let mut msg = String::new();
                reader.read_to_string(&mut msg).await.unwrap();
                assert_eq!(msg, msg_to_read);
            }
        });

        let stream = UnixStream::connect(ctx.socket_path).await.unwrap();
        let (reader, mut writer) = stream.into_split();
        writer.write_all(msg_to_read.as_bytes()).await.unwrap();
        drop(writer);
        let mut reader = BufReader::new(reader);
        let mut msg = String::new();
        reader.read_to_string(&mut msg).await.unwrap();
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
                let (mut reader, _) = server.wait_for_connection().await.unwrap();
                let mut msg = String::new();
                assert_eq!(reader.read_to_string(&mut msg).await.unwrap(), 0);
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
