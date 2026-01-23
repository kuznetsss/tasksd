use std::pin::pin;
use std::{path::Path, sync::Arc};

use anyhow::Result;
use tokio::io::AsyncWriteExt;
use tokio::net::unix::OwnedReadHalf;
use tokio::sync::mpsc::{Sender, channel};
use tokio::{
    io::{BufReader, BufWriter, Interest},
    net::UnixStream,
};
use tokio_util::sync::CancellationToken;
use tracing::error;

pub struct Transport<In> {
    in_part: BufReader<In>,
    out_sender: Sender<Arc<String>>,
}

impl Transport<OwnedReadHalf> {
    pub async fn open_socket(
        socket_path: &Path,
        cancellation_token: CancellationToken,
    ) -> Result<Self> {
        let socket = UnixStream::connect(socket_path).await?;
        socket
            .ready(Interest::READABLE | Interest::WRITABLE)
            .await?;
        let (in_part, out_part) = socket.into_split();
        Ok(Self {
            in_part: BufReader::new(in_part),
            out_sender: Self::spawn_writing(out_part, cancellation_token),
        })
    }

}

impl<In> Transport<In> {
    const CHANNEL_BUFFER_SIZE: usize = 16;

    fn spawn_writing<Writer>(
        writer: Writer,
        cancellation_token: CancellationToken,
    ) -> Sender<Arc<String>>
    where
        Writer: AsyncWriteExt + Send + 'static,
    {
        let writer = BufWriter::new(writer);
        let (sender, mut receiver) = channel::<Arc<String>>(Self::CHANNEL_BUFFER_SIZE);
        tokio::spawn(async move {
            let mut writer = pin!(writer);
            while let Some(Some(msg)) = cancellation_token
                .run_until_cancelled(receiver.recv())
                .await
            {
                if let Err(e) = writer.write(msg.as_bytes()).await {
                    error!("Error writing to output: {e}. Message: {msg}");
                    cancellation_token.cancel();
                    return;
                }
            }
        });
        sender
    }
}
