use tokio::{
    io::{AsyncWrite, AsyncWriteExt},
    sync::mpsc::{Sender, channel},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::error;

use crate::transport::error::TransportError;

pub trait WriterImpl: AsyncWrite + Send + Unpin + 'static {}

impl<T: AsyncWrite + Send + Unpin + 'static> WriterImpl for T {}

#[derive(Debug)]
pub(in crate::transport) struct BackgroundWriter {
    write_handle: WriteHandle,
    cancellation_token: CancellationToken,
    join_handle: JoinHandle<()>,
    drop_guard: tokio_util::sync::DropGuard,
}

#[derive(Debug, Clone)]
pub(in crate::transport) struct WriteHandle {
    inner: Sender<String>,
}

impl WriteHandle {
    pub(in crate::transport) async fn write(
        &self,
        message: impl Into<String>,
    ) -> Result<(), TransportError> {
        Ok(self.inner.send(message.into()).await?)
    }
}

impl BackgroundWriter {
    const CHANNEL_BUFFER_SIZE: usize = 16;

    pub(in crate::transport) fn spawn<D>(mut dst: D, cancellation_token: CancellationToken) -> Self
    where
        D: WriterImpl,
    {
        let (sender, mut receiver) = channel::<String>(Self::CHANNEL_BUFFER_SIZE);

        let handle = tokio::spawn({
            let cancellation_token = cancellation_token.clone();
            async move {
                while let Some(Some(msg)) = cancellation_token
                    .run_until_cancelled(receiver.recv())
                    .await
                {
                    if let Err(e) = dst.write_all(msg.as_bytes()).await {
                        error!("Error writing message: {e}. Message: {msg}");
                        cancellation_token.cancel();
                        break;
                    }
                }
            }
        });
        let drop_guard = cancellation_token.clone().drop_guard();
        Self {
            write_handle: WriteHandle { inner: sender },
            cancellation_token,
            join_handle: handle,
            drop_guard,
        }
    }

    pub(in crate::transport) fn handle(&self) -> WriteHandle {
        self.write_handle.clone()
    }

    /// Stops the writer as soon as possible
    pub(in crate::transport) async fn stop(self) {
        self.cancellation_token.cancel();
        self.join_handle.await.unwrap();
    }

    /// Writes everything queued and then stops
    /// NOTE: this method may hung if there are other senders
    async fn finish(self) {
        drop(self.write_handle);
        self.join_handle.await.unwrap();
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
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
    async fn background_writer_write_test() {
        let msg = "test";
        let ctx = BackgroundWriterTestCtx::new(&[msg]);
        ctx.writer.handle().write(msg).await.unwrap();
        ctx.writer.finish().await;
    }

    #[tokio::test]
    async fn background_writer_doesnt_write_when_cancelled() {
        let ctx = BackgroundWriterTestCtx::new(&[]);
        ctx.token.cancel();
        // It's not deterministic if there will be an error here
        let _ = ctx.writer.handle().write("msg").await;
        ctx.writer.finish().await;
    }

    #[tokio::test]
    async fn background_writer_cancels_token_when_write_error() {
        use std::io::{Error, ErrorKind};
        let mut builder = Builder::new();
        builder.write_error(Error::from(ErrorKind::PermissionDenied));
        let ctx = BackgroundWriterTestCtx::from_builder(builder);
        ctx.writer.handle().write("msg".to_string()).await.unwrap();
        ctx.writer.finish().await;
        assert!(ctx.token.is_cancelled());
    }

    #[tokio::test]
    async fn write_handle_write_error_when_background_writer_is_dropped() {
        let builder = Builder::new();
        let ctx = BackgroundWriterTestCtx::from_builder(builder);
        let write_handle = ctx.writer.handle();
        drop(ctx.writer);
        assert!(ctx.token.is_cancelled());
        tokio::task::yield_now().await;
        let err = write_handle.write("some message").await.unwrap_err();
        assert!(matches!(err, TransportError::WriteError(_)));
        assert!(dbg!(err.to_string()).contains("closed"));
    }
}
