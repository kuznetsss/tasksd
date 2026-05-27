use std::path::Path;

use anyhow::Result;
use tokio_util::sync::CancellationToken;

use crate::transport::{background_writer::WriterImpl, reader::ReaderImpl};
pub use connection::Connection;

mod background_writer;
pub mod connection;
pub mod error;
mod reader;
mod unix_socket;

pub trait ServerImpl {
    type ReaderHalf: ReaderImpl;
    type WriterHalf: WriterImpl;

    fn wait_for_connection(
        &self,
    ) -> impl Future<Output = Result<(Self::ReaderHalf, Self::WriterHalf)>>;
}

pub struct Server<I> {
    inner: I,
}

impl<I: ServerImpl> Server<I> {
    pub async fn wait_for_connection(
        &self,
    ) -> Result<AcceptedConnection<I::ReaderHalf, I::WriterHalf>> {
        let (read_half, write_half) = self.inner.wait_for_connection().await?;
        Ok(AcceptedConnection {
            read_half,
            write_half,
        })
    }
}

pub struct AcceptedConnection<R, W> {
    read_half: R,
    write_half: W,
}

impl<R, W> AcceptedConnection<R, W>
where
    R: ReaderImpl,
    W: WriterImpl,
{
    pub fn into_connection(self, token: CancellationToken) -> Connection {
        Connection::new(self.read_half, self.write_half, token)
    }
}

pub type UnixSocketServer = Server<unix_socket::UnixSocketServerImpl>;

impl UnixSocketServer {
    pub fn new_unix_socket(path: &Path) -> Result<Self> {
        Ok(Self {
            inner: unix_socket::UnixSocketServerImpl::new(path)?,
        })
    }
}
