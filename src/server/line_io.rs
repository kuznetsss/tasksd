use anyhow::Result;
use tokio::{
    io::AsyncWriteExt,
    sync::mpsc::{Sender, channel},
};
use tokio_util::sync::CancellationToken;
use tracing::error;

use super::types::OutputMessage;

pub trait LineReader {
    async fn read_line(&mut self) -> Result<String>;
}

pub trait LineWriter {
    fn write_line(
        &mut self,
        message: OutputMessage,
    ) -> impl std::future::Future<Output = Result<()>> + std::marker::Send;
}

pub struct BackgroundLineWriter {
    tx: Sender<OutputMessage>,
}

impl BackgroundLineWriter {
    const CHANNEL_BUFFER_SIZE: usize = 16;

    pub fn spawn<W>(mut writer: W, cancellation_token: CancellationToken) -> Self
    where
        W: AsyncWriteExt + 'static + Send + Unpin,
    {
        let (sender, mut receiver) = channel::<OutputMessage>(Self::CHANNEL_BUFFER_SIZE);

        tokio::spawn(async move {
            while let Some(Some(msg)) = cancellation_token
                .run_until_cancelled(receiver.recv())
                .await
            {
                if let Err(e) = write_line(&mut writer, &msg).await {
                    error!("Error writing message: {e}. Message: {msg}");
                    cancellation_token.cancel();
                    return;
                }
            }
        });
        Self { tx: sender }
    }
}

async fn write_line<W>(writer: &mut W, msg: &OutputMessage) -> Result<()>
where
    W: AsyncWriteExt + Unpin,
{
    writer.write_all(msg.as_bytes()).await?;
    if !msg.ends_with('\n') {
        writer.write_all(b"\n").await?;
    }
    writer.flush().await?;
    Ok(())
}

impl LineWriter for BackgroundLineWriter {
    async fn write_line(&mut self, message: OutputMessage) -> Result<()> {
        self.tx.send(message).await.map_err(Into::into)
    }
}

// TODO: add tests
