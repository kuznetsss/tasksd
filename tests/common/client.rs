use std::path::Path;

use anyhow::Result;
use tokio::{
    io::AsyncWriteExt,
    net::{UnixSocket, UnixStream},
};

pub struct Client {
    stream: UnixStream,
}

impl Client {
    pub async fn connect(socket_path: impl AsRef<Path>) -> Result<Self> {
        let stream = UnixSocket::new_stream()
            .unwrap()
            .connect(socket_path)
            .await?;
        Ok(Self { stream })
    }

    pub async fn send_str(&mut self, s: &str) -> Result<()> {
        todo!()
    }

    pub async fn send_json(&mut self, value: serde_json::Value) -> Result<()> {
        // TODO: add header
        let value_str = serde_json::to_string(&value)?;
        self.stream
            .write_all(value_str.as_bytes())
            .await
            .map_err(Into::into)
    }
}
