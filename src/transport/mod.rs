use std::path::Path;

pub use crate::transport::io::{Reader, Writer};
use anyhow::Result;
use tokio_util::sync::CancellationToken;

mod background_writer;
mod io;
mod unix_socket;

#[cfg(test)]
pub use io::{MockReader, MockWriter};

pub trait ServerImpl {
    type Reader: Reader + 'static;
    type Writer: Writer + 'static;

    fn wait_for_connection(
        &self,
        cancellation_token: CancellationToken,
    ) -> impl Future<Output = Result<(Self::Reader, Self::Writer)>>;
}

pub struct Server<I> {
    cancellation_token: CancellationToken,
    inner: I,
}

pub type UnixSocketServer = Server<unix_socket::UnixSocketServerImpl>;

impl UnixSocketServer {
    pub fn new_unix_socket(path: &Path, cancellation_token: CancellationToken) -> Result<Self> {
        Ok(Self {
            cancellation_token,
            inner: unix_socket::UnixSocketServerImpl::new(path)?,
        })
    }
}

impl<I: ServerImpl> Server<I> {
    pub async fn wait_for_connection(&self) -> Result<Connection<I::Reader, I::Writer>> {
        let connection_token = self.cancellation_token.child_token();
        let (reader, writer) = self
            .inner
            .wait_for_connection(connection_token.clone())
            .await?;
        Ok(Connection {
            reader,
            writer,
            cancellation_token: connection_token,
        })
    }
}

pub struct Connection<R, W> {
    pub reader: R,
    pub writer: W,
    pub cancellation_token: CancellationToken,
}
