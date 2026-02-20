use crate::transport::{Reader, Writer};

use anyhow::Result;
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

pub const CONTENT_LENGTH_HEADER: &str = "Content-Length: ";
pub const END_LINE_SYMBOLS: &str = "\r\n";

#[async_trait]
pub trait MessageReader {
    async fn read_message(&mut self) -> Result<String>;
}

pub struct MessageReaderImpl<R> {
    inner: R,
}

impl<R> MessageReaderImpl<R>
where
    R: Reader + Send,
{
    pub fn new(inner: R) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl<R> MessageReader for MessageReaderImpl<R>
where
    R: Reader + Send,
{
    async fn read_message(&mut self) -> Result<String> {
        let header = self.inner.read_line().await?;
        if !header.starts_with(CONTENT_LENGTH_HEADER) || !header.ends_with(END_LINE_SYMBOLS) {
            anyhow::bail!("Got unexpected symbols: {header}");
        }
        let start = CONTENT_LENGTH_HEADER.len();
        let end = header.len() - END_LINE_SYMBOLS.len();
        let content_length: usize = header[start..end].parse()?;
        let empty_line = self.inner.read_line().await?;
        if empty_line != END_LINE_SYMBOLS {
            anyhow::bail!("Expected a new line, got: {empty_line}");
        }
        self.inner.read_some(content_length).await
    }
}

#[async_trait]
pub trait MessageWriter {
    async fn write_message(&mut self, s: &str) -> Result<()>;
}

pub struct MessageWriterImpl<W> {
    inner: W,
}

impl<W> MessageWriterImpl<W>
where
    W: Writer + Send,
{
    pub fn new(inner: W) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl<W> MessageWriter for MessageWriterImpl<W>
where
    W: Writer + Send,
{
    async fn write_message(&mut self, s: &str) -> Result<()> {
        let message = format!(
            "{CONTENT_LENGTH_HEADER}{}{END_LINE_SYMBOLS}{END_LINE_SYMBOLS}{s}",
            s.len()
        );
        self.inner.write(message).await
    }
}

pub struct Connection {
    pub reader: Box<dyn MessageReader + Send>,
    pub writer: Box<dyn MessageWriter + Send>,
    pub cancellation_token: CancellationToken,
}

impl Connection {
    pub fn new<R, W>(c: crate::transport::Connection<R, W>) -> Self
    where
        R: Reader + Send + 'static,
        W: Writer + Send + 'static,
    {
        Self {
            reader: Box::new(MessageReaderImpl::new(c.reader)),
            writer: Box::new(MessageWriterImpl::new(c.writer)),
            cancellation_token: c.cancellation_token,
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::anyhow;
    use mockall::{Sequence, predicate::eq};

    use super::*;
    use crate::transport::{MockReader, MockWriter};

    macro_rules! message_reader_read_error_test {
        ($test_name:ident, $($method:ident: $msg:expr),+ ; $error:expr) => {
            #[tokio::test]
            async fn $test_name() {
                let mut mock = MockReader::new();
                let mut seq = Sequence::new();
                $(
                    mock.$method().times(1).in_sequence(&mut seq).return_once(
                        || { Box::pin( async { $msg } ) }
                    );
                )*
                let mut reader = MessageReaderImpl::new(mock);
                let err = reader.read_message().await.unwrap_err().to_string();
                assert!(err.contains($error));
            }
        };
    }
    message_reader_read_error_test!(
        message_reader_read_error_not_header,
        expect_read_line: Ok("not header\r\n".to_string());
        "Got unexpected symbols"
    );
    message_reader_read_error_test!(
        message_reader_read_error_no_end_line_symbols,
        expect_read_line: Ok("no endl".to_string());
        "Got unexpected symbols"
    );
    message_reader_read_error_test!(
        message_reader_read_error_reading,
        expect_read_line: Err(anyhow!("some error"));
        "some error"
    );
    message_reader_read_error_test!(
        message_reader_read_error_invalid_content_length,
        expect_read_line: Ok(format!("{}123abc\r\n", CONTENT_LENGTH_HEADER));
        "invalid digit found"
    );
    message_reader_read_error_test!(
        message_reader_read_error_non_empty_line,
        expect_read_line: Ok(format!("{}123\r\n", CONTENT_LENGTH_HEADER)),
        expect_read_line: Ok("a\r\n".to_string());
        "Expected a new line"
    );
    message_reader_read_error_test!(
        message_reader_read_error_second_read_failed,
        expect_read_line: Ok(format!("{CONTENT_LENGTH_HEADER}123\r\n")),
        expect_read_line: Err(anyhow!("some error"));
        "some error"
    );

    #[tokio::test]
    async fn message_reader_read_error_third_read_failed() {
        let content_length = 123;
        let mut mock = MockReader::new();
        let mut seq = Sequence::new();
        mock.expect_read_line()
            .times(1)
            .in_sequence(&mut seq)
            .return_once({
                move || {
                    Box::pin(async move {
                        Ok(format!(
                            "{CONTENT_LENGTH_HEADER}{}{END_LINE_SYMBOLS}",
                            content_length
                        ))
                    })
                }
            });
        mock.expect_read_line()
            .times(1)
            .in_sequence(&mut seq)
            .return_once(|| Box::pin(async { Ok(END_LINE_SYMBOLS.to_string()) }));
        mock.expect_read_some()
            .times(1)
            .in_sequence(&mut seq)
            .with(eq(content_length))
            .return_once(|_| Box::pin(async { Err(anyhow!("some error")) }));
        let mut reader = MessageReaderImpl::new(mock);
        let err = reader.read_message().await.unwrap_err().to_string();
        assert!(err.contains("some error"));
    }

    #[tokio::test]
    async fn message_reader_read_success() {
        let msg = "some message";
        let mut mock = MockReader::new();
        let mut seq = Sequence::new();
        mock.expect_read_line()
            .times(1)
            .in_sequence(&mut seq)
            .return_once(|| {
                Box::pin(async {
                    Ok(format!(
                        "{CONTENT_LENGTH_HEADER}{}{END_LINE_SYMBOLS}",
                        msg.len()
                    ))
                })
            });
        mock.expect_read_line()
            .times(1)
            .in_sequence(&mut seq)
            .return_once(|| Box::pin(async { Ok(END_LINE_SYMBOLS.to_string()) }));
        mock.expect_read_some()
            .times(1)
            .in_sequence(&mut seq)
            .with(eq(msg.len()))
            .return_once(|_| Box::pin(async { Ok(msg.to_string()) }));
        let mut reader = MessageReaderImpl::new(mock);
        assert_eq!(reader.read_message().await.unwrap(), msg);
    }

    #[tokio::test]
    async fn message_writer_writes() {
        let mut mock = MockWriter::new();
        mock.expect_write()
            .with(eq("Content-Length: 4\r\n\r\ntest".to_string()))
            .returning(|_| Box::pin(async { Ok(()) }));
        let mut writer = MessageWriterImpl::new(mock);
        writer.write_message("test").await.unwrap();
    }

    #[tokio::test]
    async fn message_writer_write_error() {
        let error_msg = "some error".to_string();
        let mut mock = MockWriter::new();
        mock.expect_write()
            .with(eq("Content-Length: 4\r\n\r\ntest".to_string()))
            .returning({
                let error_msg = error_msg.clone();
                move |_| {
                    Box::pin({
                        let error_msg = error_msg.clone();
                        async move { Err(anyhow!("{}", error_msg)) }
                    })
                }
            });
        let mut writer = MessageWriterImpl::new(mock);
        assert_eq!(
            writer.write_message("test").await.unwrap_err().to_string(),
            error_msg
        );
    }
}
