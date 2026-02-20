use std::{fmt::Debug, future::Future, marker::Send};

use anyhow::Result;
use mockall::automock;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, BufReader};

#[automock]
pub trait Reader: Debug + Send {
    fn read_line(&mut self) -> impl Future<Output = Result<String>> + Send;
    fn read_some(&mut self, n: usize) -> impl Future<Output = Result<String>> + Send;
}

pub type OutputMessage = String;

#[automock]
pub trait Writer: Debug + Send {
    fn write(&mut self, msg: OutputMessage) -> impl Future<Output = Result<()>> + Send;
}

impl<R> Reader for BufReader<R>
where
    R: AsyncRead + Send + Unpin + Debug,
{
    async fn read_line(&mut self) -> Result<String> {
        let mut buf = String::new();
        AsyncBufReadExt::read_line(self, &mut buf).await?;
        if !buf.ends_with('\n') {
            Err(anyhow::anyhow!("EOF"))
        } else {
            Ok(buf)
        }
    }

    async fn read_some(&mut self, n: usize) -> Result<String> {
        let mut buf = vec![0; n];
        self.read_exact(buf.as_mut_slice()).await?;
        Ok(String::from_utf8(buf)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tokio::io::BufReader;
    use tokio_test::io::Builder;

    #[tokio::test]
    async fn reader_for_bufread_read_line_reads() {
        let msg = "test\n";
        let mock = Builder::new().read(msg.as_bytes()).build();
        let mut reader = BufReader::new(mock);
        assert_eq!(Reader::read_line(&mut reader).await.unwrap(), msg);
    }

    #[tokio::test]
    async fn reader_for_bufread_read_line_eof_empty_line() {
        let mock = Builder::new().build();
        let mut reader = BufReader::new(mock);
        Reader::read_line(&mut reader).await.unwrap_err();
    }

    #[tokio::test]
    async fn reader_for_bufread_read_line_eof_before_new_line() {
        let mock = Builder::new().read("test".as_bytes()).build();
        let mut reader = BufReader::new(mock);
        Reader::read_line(&mut reader).await.unwrap_err();
    }

    #[tokio::test]
    async fn reader_for_bufread_read_line_propagates_error() {
        use std::io::{Error, ErrorKind};
        let mock = Builder::new()
            .read_error(Error::from(ErrorKind::ConnectionRefused))
            .build();
        let mut reader = BufReader::new(mock);
        Reader::read_line(&mut reader).await.unwrap_err();
    }

    #[tokio::test]
    async fn reader_for_bufread_read_some_reads() {
        let msg = "test";
        let mock = Builder::new().read(msg.as_bytes()).build();
        let mut reader = BufReader::new(mock);
        assert_eq!(reader.read_some(msg.len()).await.unwrap(), msg);
    }

    #[tokio::test]
    async fn reader_for_bufread_read_some_eol() {
        let msg = "test";
        let mock = Builder::new().read(msg.as_bytes()).build();
        let mut reader = BufReader::new(mock);
        reader.read_some(msg.len() + 1).await.unwrap_err();
    }

    #[tokio::test]
    async fn reader_for_bufread_read_some_propagates_error() {
        use std::io::{Error, ErrorKind};
        let mock = Builder::new()
            .read_error(Error::from(ErrorKind::ConnectionRefused))
            .build();
        let mut reader = BufReader::new(mock);
        reader.read_some(123).await.unwrap_err();
    }
}
