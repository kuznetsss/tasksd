use std::{marker::PhantomData, path::Path, time::Duration};

use anyhow::{Result, bail};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
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
    last_id: i64,
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
            last_id: 0,
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

    fn next_id(&mut self) -> i64 {
        self.last_id += 1;
        self.last_id
    }

    pub async fn task_start(
        &mut self,
        executable: &str,
        args: &[&str],
        subscribe_to_output: bool,
    ) -> Result<()> {
        let id = self.next_id();
        let json = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "task.start",
            "params": {
                "executable": executable,
                "args": args,
                "subscribe_to_output": subscribe_to_output
            }
        });

        self.send_json(&json).await
    }

    pub async fn send_signal(&mut self, task_id: usize, signal: i32) -> Result<()> {
        let id = self.next_id();
        let json = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "task.send_signal",
            "params": {
                "task_id": task_id,
                "signal": signal
            }
        });
        self.send_json(&json).await
    }

    pub async fn get_output(
        &mut self,
        task_id: usize,
        from_line: usize,
        lines_number: usize,
    ) -> Result<()> {
        let id = self.next_id();
        let json = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "task.get_output",
            "params": {
                "task_id": task_id,
                "from_line": from_line,
                "lines_number": lines_number
            }
        });
        self.send_json(&json).await
    }

    pub async fn send_input(&mut self, task_id: usize, input: impl Into<&str>) -> Result<()> {
        let id = self.next_id();
        let json = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "task.send_input",
            "params": {
                "task_id": task_id,
                "input": input.into()
            }
        });
        self.send_json(&json).await
    }

    pub async fn subscribe(&mut self, task_id: usize) -> Result<()> {
        let id = self.next_id();
        let json = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "task.subscribe",
            "params": {
                "task_id": task_id,
            }
        });
        self.send_json(&json).await
    }

    pub async fn unsubscribe(&mut self, task_id: usize) -> Result<()> {
        let id = self.next_id();
        let json = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "task.unsubscribe",
            "params": {
                "task_id": task_id,
            }
        });
        self.send_json(&json).await
    }

    pub fn last_id(&self) -> i64 {
        self.last_id
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

    pub async fn read_struct<S: DeserializeOwned>(&mut self) -> Result<S> {
        let json = self.read_json().await?;
        serde_json::from_value(json).map_err(Into::into)
    }

    pub fn expect_unordered(&mut self) -> ExpectationBuilder<'_> {
        ExpectationBuilder::new(self)
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

trait Expectation {
    fn verify(&self, v: &Value) -> bool;
}

pub struct Expect<T, F> {
    _type: PhantomData<T>,
    verify_fn: F,
}

impl<T, F> Expect<T, F>
where
    T: DeserializeOwned + 'static,
    F: Fn(T) -> bool + 'static,
{
    pub fn new(f: F) -> Self {
        Self {
            _type: PhantomData,
            verify_fn: f,
        }
    }
}

impl<T: DeserializeOwned, F: Fn(T) -> bool> Expectation for Expect<T, F> {
    fn verify(&self, v: &Value) -> bool {
        T::deserialize(v)
            .map(|t| (self.verify_fn)(t))
            .unwrap_or(false)
    }
}

#[must_use]
pub struct ExpectationBuilder<'c> {
    expectations: Vec<Box<dyn Expectation>>,
    client: &'c mut Client,
}

impl<'c> ExpectationBuilder<'c> {
    fn new(client: &'c mut Client) -> Self {
        Self {
            expectations: Default::default(),
            client,
        }
    }

    pub fn message<F, T>(mut self, f: F) -> Self
    where
        F: Fn(T) -> bool + 'static,
        T: DeserializeOwned + 'static,
    {
        let e = Expect::new(f);
        self.expectations.push(Box::new(e));
        self
    }

    pub async fn check(self) -> Result<()> {
        let Self {
            mut expectations,
            client,
        } = self;
        while !expectations.is_empty() {
            let json = client.read_json().await?;
            match expectations.iter().position(|e| e.verify(&json)) {
                Some(p) => expectations.remove(p),
                None => bail!("Unexpected message: {json}"),
            };
        }
        Ok(())
    }
}
