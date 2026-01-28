use std::path::Path;

use anyhow::Result;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter, Interest},
    net::{
        UnixStream,
        unix::{OwnedReadHalf, OwnedWriteHalf},
    },
};
use tokio_util::sync::CancellationToken;

use crate::server::line_io::{BackgroundLineWriter, LineReader, LineWriter};

pub type SocketInput = BufReader<OwnedReadHalf>;
pub type SocketOutput = BackgroundLineWriter;

pub async fn open_socket(
    path: &Path,
    cancellation_token: CancellationToken,
) -> Result<(SocketInput, SocketOutput)> {
    let socket = UnixStream::connect(path).await?;
    socket
        .ready(Interest::READABLE | Interest::WRITABLE)
        .await?;
    let (in_part, out_part) = socket.into_split();
    Ok((
        SocketInput::new(in_part),
        SocketOutput::spawn(BufWriter::new(out_part), cancellation_token),
    ))
}

impl LineReader for SocketInput {
    async fn read_line(&mut self) -> Result<String> {
        let mut buf = String::new();
        AsyncBufReadExt::read_line(self, &mut buf).await?;
        Ok(buf)
    }
}

// TODO: add tests
