use std::path::Path;

use crate::server::line_io::{LineReader, LineWriter};
use anyhow::Result;
use tokio_util::sync::CancellationToken;

mod line_io;
mod io;
mod types;
mod unix_socket;

pub struct Server {
    cancellation_token: CancellationToken,
    inner: Inner,
}

enum Inner {
    UnixSocket(unix_socket::Server),
}

impl Server {
    pub fn new_unix_socket(path: &Path, cancellation_token: CancellationToken) -> Result<Self> {
        Ok(Self {
            cancellation_token,
            inner: Inner::UnixSocket(unix_socket::Server::new(path)?),
        })
    }

    pub async fn wait_for_connection(&self) -> Result<Connection> {
        match &self.inner {
            Inner::UnixSocket(s) => {
                let connection_token = self.cancellation_token.child_token();
                let (reader, writer) = match self
                    .cancellation_token
                    .run_until_cancelled(s.wait_for_connection(connection_token.clone()))
                    .await
                {
                    Some(res) => res?,
                    None => {
                        anyhow::bail!("Cancelled");
                    }
                };
                Ok(Connection {
                    reader: Box::new(reader),
                    writer: Box::new(writer),
                    cancellation_token: connection_token,
                })
            }
        }
    }
}

pub struct Connection {
    pub reader: Box<dyn LineReader + Send>,
    pub writer: Box<dyn LineWriter + Send>,
    pub cancellation_token: CancellationToken,
}

// TODO: add tests
