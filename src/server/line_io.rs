use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader},
    sync::{
        Notify,
        mpsc::{Sender, channel},
    },
};
use tokio_util::sync::CancellationToken;
use tracing::error;

use super::types::OutputMessage;

// TODO: rename to message reader
#[async_trait]
pub trait LineReader {
    async fn read_line(&mut self) -> Result<String>;

    // async fn read_header(&mut self) -> Result<String>
    // async fn read_body(&mut self, n: usize) -> Result<String>;
    // or
    // async fn read_message(&mut self) -> Result<String>; // returns body only
}

#[async_trait]
impl<R> LineReader for BufReader<R>
where
    R: AsyncRead + Send + Unpin,
{
    async fn read_line(&mut self) -> Result<String> {
        let mut buf = String::new();
        AsyncBufReadExt::read_line(self, &mut buf).await?;
        // TODO: maybe we should that buf ends with '\n'. Otherwise means EOF
        // TODO: add test case for an empty buffer
        if buf.is_empty() {
            Err(anyhow::anyhow!("EOF"))
        } else {
            Ok(buf)
        }
    }
}

#[async_trait]
pub trait LineWriter {
    async fn write_line(&mut self, message: OutputMessage) -> Result<()>;
}

pub struct BackgroundLineWriter {
    tx: Sender<OutputMessage>,
    cancellation_token: CancellationToken,
    completion_notification: Arc<Notify>,
}

impl BackgroundLineWriter {
    const CHANNEL_BUFFER_SIZE: usize = 16;

    pub fn spawn<W>(mut writer: W, cancellation_token: CancellationToken) -> Self
    where
        W: AsyncWrite + 'static + Send + Unpin,
    {
        let (sender, mut receiver) = channel::<OutputMessage>(Self::CHANNEL_BUFFER_SIZE);

        let completion_notification = Arc::new(Notify::new());
        tokio::spawn({
            let cancellation_token = cancellation_token.clone();
            let completion_notification = completion_notification.clone();
            async move {
                while let Some(Some(msg)) = cancellation_token
                    .run_until_cancelled(receiver.recv())
                    .await
                {
                    if let Err(e) = write_line(&mut writer, &msg).await {
                        error!("Error writing message: {e}. Message: {msg}");
                        cancellation_token.cancel();
                        break;
                    }
                }
                completion_notification.notify_one();
            }
        });
        Self {
            tx: sender,
            cancellation_token,
            completion_notification,
        }
    }

    /// Stops the writer as soon as possible
    pub async fn stop(self) {
        self.cancellation_token.cancel();
        self.completion_notification.notified().await;
    }

    /// Writes everything queued and then stops
    /// NOTE: this method may hung if there are other senders
    pub async fn finish(self) {
        drop(self.tx);
        self.completion_notification.notified().await;
    }
}

async fn write_line<W>(writer: &mut W, msg: &OutputMessage) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    writer.write_all(msg.as_bytes()).await?;
    if !msg.ends_with('\n') {
        writer.write_all(b"\n").await?;
    }
    writer.flush().await?;
    Ok(())
}

#[async_trait]
impl LineWriter for BackgroundLineWriter {
    async fn write_line(&mut self, message: OutputMessage) -> Result<()> {
        self.tx.send(message).await.map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::io::BufReader;
    use tokio_test::io::Builder;
    use tokio_util::sync::CancellationToken;

    use crate::server::line_io::{BackgroundLineWriter, LineReader, LineWriter};

    struct BackgroundLineWriterTestCtx {
        writer: BackgroundLineWriter,
        token: CancellationToken,
    }

    impl BackgroundLineWriterTestCtx {
        fn new(expected_messages: &[&str]) -> Self {
            let mut builder = Builder::new();
            expected_messages.iter().for_each(|s| {
                builder.write(s.as_bytes());
            });
            Self::from_builder(builder)
        }

        fn from_builder(mut builder: Builder) -> Self {
            let token = CancellationToken::new();
            Self {
                writer: BackgroundLineWriter::spawn(builder.build(), token.clone()),
                token,
            }
        }
    }

    #[tokio::test]
    async fn background_line_writer_write_test() {
        let msg = "test";
        let mut ctx = BackgroundLineWriterTestCtx::new(&[format!("{msg}\n").as_str()]);
        ctx.writer
            .write_line(Arc::new(msg.to_string()))
            .await
            .unwrap();
        ctx.writer.finish().await;
    }

    #[tokio::test]
    async fn background_line_writer_doesnt_add_new_line_test() {
        let msg = "test\n";
        let mut ctx = BackgroundLineWriterTestCtx::new(&[msg]);
        ctx.writer
            .write_line(Arc::new(msg.to_string()))
            .await
            .unwrap();
        ctx.writer.finish().await;
    }

    #[tokio::test]
    async fn background_line_writer_doesnt_write_when_cancelled() {
        let mut ctx = BackgroundLineWriterTestCtx::new(&[]);
        ctx.token.cancel();
        // It's not deterministic if there will be an error here
        let _ = ctx.writer.write_line(Arc::new("msg".to_string())).await;
        ctx.writer.finish().await;
    }

    #[tokio::test]
    async fn background_line_writer_cancels_token_when_write_error() {
        use std::io::{Error, ErrorKind};
        let mut builder = Builder::new();
        builder.write_error(Error::from(ErrorKind::PermissionDenied));
        let mut ctx = BackgroundLineWriterTestCtx::from_builder(builder);
        ctx.writer
            .write_line(Arc::new("msg".to_string()))
            .await
            .unwrap();
        ctx.writer.finish().await;
        assert!(ctx.token.is_cancelled());
    }

    #[tokio::test]
    async fn line_reader_for_bufread_reads() {
        let msg = "test\n";
        let mock = Builder::new().read(msg.as_bytes()).build();
        let mut reader = BufReader::new(mock);
        assert_eq!(reader.read_line().await.unwrap(), msg);
    }

    #[tokio::test]
    async fn line_reader_for_bufread_propagates_error() {
        use std::io::{Error, ErrorKind};
        let mock = Builder::new()
            .read_error(Error::from(ErrorKind::ConnectionRefused))
            .build();
        let mut reader = BufReader::new(mock);
        reader.read_line().await.unwrap_err();
    }
}
