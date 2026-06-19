use std::{path::Path, time::Duration};

use anyhow::{Result, bail};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, Interest},
    net::{
        UnixSocket,
        unix::{OwnedReadHalf, OwnedWriteHalf},
    },
};

#[derive(Debug)]
pub struct Client {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
}

pub const HEADER: &str = "Content-Length: ";
impl Client {
    pub async fn connect(socket_path: impl AsRef<Path>) -> Result<Self> {
        let stream = UnixSocket::new_stream()
            .unwrap()
            .connect(socket_path)
            .await?;
        stream
            .ready(Interest::READABLE | Interest::WRITABLE)
            .await?;
        let (reader, writer) = stream.into_split();
        Ok(Self {
            reader: BufReader::new(reader),
            writer,
        })
    }

    pub async fn send_str(&mut self, s: &str) -> Result<()> {
        self.writer
            .write_all(s.as_bytes())
            .await
            .map_err(Into::into)
    }

    pub async fn send_msg(&mut self, msg: &str) -> Result<()> {
        self.send_str(&format!("{HEADER}{}\r\n\r\n{msg}", msg.len()))
            .await
    }

    pub async fn send_json(&mut self, value: &serde_json::Value) -> Result<()> {
        self.send_msg(&value.to_string()).await
    }

    pub async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.reader.read(buf).await.map_err(Into::into)
    }

    pub async fn read_msg(&mut self) -> Result<String> {
        tokio::time::timeout(Duration::from_secs(1), async move {
            let mut buf = String::new();
            self.reader.read_line(&mut buf).await.unwrap();
            if !buf.starts_with(HEADER) {
                bail!("Unexpected header: {buf}");
            }
            let start = HEADER.len();
            let end = buf.len() - 2;
            let content_length: usize = buf[start..end].parse()?;
            buf.clear();
            self.reader.read_line(&mut buf).await.unwrap();
            if buf != "\r\n" {
                bail!("Expected new line, got: {buf}");
            }
            let mut buf = vec![0u8; content_length];
            let read_len = self.reader.read_exact(&mut buf).await?;
            if read_len != content_length {
                bail!("Unexpected EOF, read {read_len} of expected {content_length}");
            }
            Ok(buf.try_into()?)
        })
        .await?
    }

    pub async fn read_json(&mut self) -> Result<serde_json::Value> {
        let msg = self.read_msg().await?;
        let json: serde_json::Value = serde_json::from_str(&msg)?;
        // All messages should contain "jsonrpc": "2.0"
        assert_eq!(json.get("jsonrpc").unwrap().as_str().unwrap(), "2.0");
        Ok(json)
    }

    /// Takes 1 second if returning false
    pub async fn is_disconnected(&mut self) -> bool {
        let mut buf = Vec::new();
        let read_future = self.reader.read_to_end(&mut buf);
        matches!(
            tokio::time::timeout(Duration::from_secs(1), read_future).await,
            Ok(Ok(_))
        )
    }
}
