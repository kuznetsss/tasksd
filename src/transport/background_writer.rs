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
    _drop_guard: tokio_util::sync::DropGuard,
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

    pub(in crate::transport) fn spawn<D>(mut dst: D) -> Self
    where
        D: WriterImpl,
    {
        let (sender, mut receiver) = channel::<String>(Self::CHANNEL_BUFFER_SIZE);
        let cancellation_token = CancellationToken::new();

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
            _drop_guard: drop_guard,
        }
    }

    pub(in crate::transport) fn handle(&self) -> WriteHandle {
        self.write_handle.clone()
    }

    /// Writes everything queued and then stops
    /// NOTE: this method may hung if there are other senders
    pub(in crate::transport) async fn join(self) {
        drop(self.write_handle);
        self.join_handle.await.unwrap();
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::{
        sync::{Arc, atomic::AtomicBool},
        time::Duration,
    };

    use super::*;

    use tokio_test::io::Builder;

    fn new_expecting_writes(expected_messages: &[&str]) -> BackgroundWriter {
        let mut builder = Builder::new();
        expected_messages.iter().for_each(|s| {
            builder.write(s.as_bytes());
        });
        BackgroundWriter::spawn(builder.build())
    }

    #[tokio::test]
    async fn write_handle_writes() {
        let msg = "test";
        let writer = new_expecting_writes(&[msg]);
        writer.handle().write(msg).await.unwrap();
        writer.join().await;
    }

    #[tokio::test]
    async fn internal_cancellation_token_is_cancelled_after_drop() {
        let writer = BackgroundWriter::spawn(Builder::new().build());
        let token = writer.cancellation_token.clone();
        assert!(!token.is_cancelled());
        drop(writer);
        assert!(token.is_cancelled());
    }

    #[tokio::test]
    async fn join_waits_for_all_write_handles_to_be_dropped() {
        let writer = BackgroundWriter::spawn(Builder::new().build());
        let handle = writer.handle();
        let join_finished = Arc::new(AtomicBool::new(false));

        tokio::spawn({
            let join_finished = join_finished.clone();
            async move {
                writer.join().await;
                join_finished.store(true, std::sync::atomic::Ordering::Relaxed);
            }
        });
        tokio::task::yield_now().await;
        assert!(!join_finished.load(std::sync::atomic::Ordering::Relaxed));

        drop(handle);

        tokio::task::yield_now().await;
        assert!(join_finished.load(std::sync::atomic::Ordering::Relaxed));
    }

    #[tokio::test]
    async fn join_waits_for_all_messages_to_be_written() {
        let msgs = ["some message 1", "some message 2", "some message 2"];
        let writer = new_expecting_writes(&msgs);
        let handle = writer.handle();
        for m in msgs {
            handle.write(m).await.unwrap();
        }
        drop(handle);
        tokio::time::timeout(Duration::from_secs(1), writer.join())
            .await
            .unwrap();
    }
}
