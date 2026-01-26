use std::{marker::PhantomData, sync::Arc};

use anyhow::Result;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter},
    sync::mpsc::{Sender, channel},
};
use tokio_util::sync::CancellationToken;
use tracing::error;

/// An abstraction for input (unix socket, stdin etc)
pub trait Input {
    async fn read_line(&mut self) -> Result<String>;
}

pub struct BufferedInput<In> {
    input: BufReader<In>,
}

impl<In> Input for BufferedInput<In>
where
    In: AsyncBufReadExt + Send + 'static + Unpin,
{
    async fn read_line(&mut self) -> Result<String> {
        let mut buf = String::new();
        self.input.read_line(&mut buf).await?;
        Ok(buf)
    }
}

pub trait Output: Clone {
    type MessageType;
    type WriterType: AsyncWriteExt + Send + 'static + Unpin;
    fn new(writer: Self::WriterType, cancellation_token: CancellationToken) -> Self;

    async fn write_line(&mut self, msg: Self::MessageType) -> Result<()>;
}

pub struct BufferedOutput<Writer> {
    tx: Sender<Arc<String>>,
    _phantom: PhantomData<Writer>,
}

impl<Writer> Clone for BufferedOutput<Writer> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            _phantom: self._phantom,
        }
    }
}

const CHANNEL_BUFFER_SIZE: usize = 16;

impl<Writer> Output for BufferedOutput<Writer>
where
    Writer: AsyncWriteExt + Send + 'static + Unpin,
{
    type MessageType = Arc<String>;
    type WriterType = Writer;

    async fn write_line(&mut self, msg: Self::MessageType) -> Result<()> {
        self.tx.send(msg).await?;
        Ok(())
    }

    fn new(writer: Writer, cancellation_token: CancellationToken) -> Self {
        Self {
            tx: spawn_writing(writer, cancellation_token),
            _phantom: Default::default(),
        }
    }
}

fn spawn_writing<Writer>(
    writer: Writer,
    cancellation_token: CancellationToken,
) -> Sender<Arc<String>>
where
    Writer: AsyncWriteExt + Send + 'static + Unpin,
{
    let mut writer = BufWriter::new(writer);
    let (sender, mut receiver) = channel::<Arc<String>>(CHANNEL_BUFFER_SIZE);
    tokio::spawn(async move {
        while let Some(Some(msg)) = cancellation_token
            .run_until_cancelled(receiver.recv())
            .await
        {
            if let Err(e) = write_impl(&mut writer, msg.clone()).await {
                error!("Error writing to output: {e}. Message: {msg}");
                cancellation_token.cancel();
                return;
            }
        }
    });
    sender
}

async fn write_impl<Writer>(writer: &mut Writer, msg: Arc<String>) -> Result<()>
where
    Writer: AsyncWriteExt + Send + 'static + Unpin,
{
    writer.write_all(msg.as_bytes()).await?;
    if !msg.ends_with('\n') {
        writer.write_all(b"\n").await?;
    }
    writer.flush().await?;
    Ok(())
}

// TODO: add tests
