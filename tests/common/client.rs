use std::path::Path;

use anyhow::Result;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, Interest},
    net::{UnixSocket, UnixStream},
};

#[derive(Debug)]
pub struct Client {
    stream: UnixStream,
}

impl Client {
    pub async fn connect(socket_path: impl AsRef<Path>) -> Result<Self> {
        let stream = UnixSocket::new_stream()
            .unwrap()
            .connect(socket_path)
            .await?;
        stream
            .ready(Interest::READABLE | Interest::WRITABLE)
            .await?;
        Ok(Self { stream })
    }

    pub async fn send_str(&mut self, s: &str) -> Result<()> {
        self.stream
            .write_all(s.as_bytes())
            .await
            .map_err(Into::into)
    }

    pub async fn send_json(&mut self, value: serde_json::Value) -> Result<()> {
        let value_str = serde_json::to_string(&value)?;
        self.send_str(&format!(
            "Content-Length: {}\r\n\r\n{value_str}",
            value_str.len()
        ))
        .await
    }

    pub async fn is_connected(mut self) -> bool {
        let mut buf = [0u8; 16];
        self.stream.read(&mut buf).await.unwrap() != 0
    }
}
