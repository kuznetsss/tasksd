use std::sync::Arc;

use anyhow::Result;
use tokio::{
    io::{AsyncWrite, AsyncWriteExt},
    sync::{
        Notify,
        mpsc::{Sender, channel},
    },
};
use tokio_util::sync::CancellationToken;
use tracing::error;

use crate::transport::io::{OutputMessage, Writer};

#[derive(Debug)]
pub struct BackgroundWriter {
    tx: Sender<OutputMessage>,
    cancellation_token: CancellationToken,
    completion_notification: Arc<Notify>,
}

impl BackgroundWriter {
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
                    if let Err(e) = writer.write_all(msg.as_bytes()).await {
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
    async fn finish(self) {
        drop(self.tx);
        self.completion_notification.notified().await;
    }
}

impl Writer for BackgroundWriter {
    async fn write(&mut self, message: OutputMessage) -> Result<()> {
        self.tx.send(message).await.map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tokio_test::io::Builder;

    struct BackgroundWriterTestCtx {
        writer: BackgroundWriter,
        token: CancellationToken,
    }

    impl BackgroundWriterTestCtx {
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
                writer: BackgroundWriter::spawn(builder.build(), token.clone()),
                token,
            }
        }
    }

    #[tokio::test]
    async fn background_line_writer_write_test() {
        let msg = "test";
        let mut ctx = BackgroundWriterTestCtx::new(&[format!("{msg}\n").as_str()]);
        ctx.writer.write(msg.to_string()).await.unwrap();
        ctx.writer.finish().await;
    }

    #[tokio::test]
    async fn background_line_writer_doesnt_write_when_cancelled() {
        let mut ctx = BackgroundWriterTestCtx::new(&[]);
        ctx.token.cancel();
        // It's not deterministic if there will be an error here
        let _ = ctx.writer.write("msg".to_string()).await;
        ctx.writer.finish().await;
    }

    #[tokio::test]
    async fn background_line_writer_cancels_token_when_write_error() {
        use std::io::{Error, ErrorKind};
        let mut builder = Builder::new();
        builder.write_error(Error::from(ErrorKind::PermissionDenied));
        let mut ctx = BackgroundWriterTestCtx::from_builder(builder);
        ctx.writer.write("msg".to_string()).await.unwrap();
        ctx.writer.finish().await;
        assert!(ctx.token.is_cancelled());
    }
}
