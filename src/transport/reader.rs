use std::marker::Send;

use anyhow::{Result, bail};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, BufReader};

pub(in crate::transport) struct Reader<I> {
    inner: BufReader<I>,
    buffer: String,
}

pub(in crate::transport) trait ReaderImpl: AsyncRead + Send + Unpin {}

impl<T: AsyncRead + Send + Unpin> ReaderImpl for T {}

impl<I> Reader<I>
where
    I: ReaderImpl,
{
    pub(in crate::transport) fn new(inner: I) -> Self {
        Self {
            inner: BufReader::new(inner),
            buffer: String::new(),
        }
    }

    pub(in crate::transport) async fn read_line(&mut self) -> Result<&str> {
        self.buffer.clear();
        self.inner.read_line(&mut self.buffer).await?;
        if !self.buffer.ends_with('\n') {
            Err(anyhow::anyhow!("EOF"))
        } else {
            Ok(&self.buffer)
        }
    }

    pub(in crate::transport) async fn read_some(&mut self, n: usize) -> Result<&str> {
        self.buffer.clear();
        self.buffer.reserve(n);
        let mut buffer = unsafe {
            // Safety: verify buffer content is a valid UTF-8 (done by checking str::from_utf8())
            self.buffer.as_bytes_mut()
        };
        self.inner.read_exact(&mut buffer).await?;
        if str::from_utf8(&buffer).is_err() {
            bail!("Got invalid UTF-8 string: {buffer:?}")
        } else {
            Ok(&self.buffer)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tokio_test::io::Builder;

    #[tokio::test]
    async fn reader_read_line_reads() {
        let msg = "test\n";
        let mock = Builder::new().read(msg.as_bytes()).build();
        let mut reader = Reader::new(mock);
        assert_eq!(reader.read_line().await.unwrap(), msg);
    }

    #[tokio::test]
    async fn reader_read_line_eof_empty_line() {
        let mock = Builder::new().build();
        let mut reader = Reader::new(mock);
        let err = reader.read_line().await.unwrap_err();
        assert!(err.to_string().contains("EOF"));
    }

    #[tokio::test]
    async fn reader_read_line_eof_before_new_line() {
        let mock = Builder::new().read("test".as_bytes()).build();
        let mut reader = Reader::new(mock);
        let err = reader.read_line().await.unwrap_err();
        assert!(err.to_string().contains("EOF"));
    }

    #[tokio::test]
    async fn reader_read_line_propagates_error() {
        use std::io::{Error, ErrorKind};
        let mock = Builder::new()
            .read_error(Error::from(ErrorKind::ConnectionRefused))
            .build();
        let mut reader = Reader::new(mock);
        let err = reader.read_line().await.unwrap_err();
        let err = err.downcast::<std::io::Error>().unwrap();
        assert_eq!(err.kind(), ErrorKind::ConnectionRefused);
    }

    #[tokio::test]
    async fn reader_read_some_reads() {
        let msg = "test";
        let mock = Builder::new().read(msg.as_bytes()).build();
        let mut reader = Reader::new(mock);
        assert_eq!(reader.read_some(msg.len()).await.unwrap(), msg);
    }

    #[tokio::test]
    async fn reader_read_some_eol() {
        let msg = "test";
        let mock = Builder::new().read(msg.as_bytes()).build();
        let mut reader = Reader::new(mock);
        let err = reader.read_some(msg.len() + 1).await.unwrap_err();
        assert!(err.to_string().contains("EOF"));
    }

    #[tokio::test]
    async fn reader_read_some_propagates_error() {
        use std::io::{Error, ErrorKind};
        let mock = Builder::new()
            .read_error(Error::from(ErrorKind::ConnectionRefused))
            .build();
        let mut reader = Reader::new(mock);
        let err = reader.read_some(123).await.unwrap_err();
        let err = err.downcast::<std::io::Error>().unwrap();
        assert_eq!(err.kind(), ErrorKind::ConnectionRefused);
    }
}
