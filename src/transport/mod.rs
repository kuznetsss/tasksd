mod background_writer;
mod connection;
mod error;
mod reader;
mod unix_socket;

use std::path::Path;

use anyhow::Result;

use crate::transport::{background_writer::WriterImpl, reader::ReaderImpl};

pub use connection::{Connection, ConnectionWriter};
pub use error::TransportError;

pub trait ServerImpl: std::fmt::Debug {
    type ReaderHalf: ReaderImpl;
    type WriterHalf: WriterImpl;

    fn wait_for_connection(
        &self,
    ) -> impl Future<Output = Result<(Self::ReaderHalf, Self::WriterHalf)>>;
}

#[derive(Debug)]
pub struct Server<I> {
    inner: I,
}

impl<I: ServerImpl> Server<I> {
    pub async fn wait_for_connection(&self) -> Result<Connection> {
        let (read_half, write_half) = self.inner.wait_for_connection().await?;
        Ok(Connection::new(read_half, write_half))
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
