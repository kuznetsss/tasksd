use std::path::Path;

use anyhow::Result;
use tokio::{
    io::{BufReader, BufWriter, Interest},
    net::{UnixListener, unix::OwnedReadHalf},
};
use tokio_util::sync::CancellationToken;

use crate::server::line_io::BackgroundLineWriter;

pub type SocketInput = BufReader<OwnedReadHalf>;
pub type SocketOutput = BackgroundLineWriter;

pub async fn open_socket(
    path: &Path,
    cancellation_token: CancellationToken,
) -> Result<(SocketInput, SocketOutput)> {
    let listener = UnixListener::bind(path)?;
    let (stream, _) = listener.accept().await?;
    stream
        .ready(Interest::READABLE | Interest::WRITABLE)
        .await?;
    let (in_part, out_part) = stream.into_split();
    Ok((
        SocketInput::new(in_part),
        SocketOutput::spawn(BufWriter::new(out_part), cancellation_token),
    ))
}

// TODO: add tests
