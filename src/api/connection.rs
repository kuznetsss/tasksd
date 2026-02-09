use crate::server::{Reader, Writer};

use anyhow::Result;

pub const CONTENT_LENGTH_HEADER: &str = "Content length: ";
pub const END_LINE_SYMBOLS: &str = "\r\n";

pub struct MessageReader {
    inner: Box<dyn Reader + Send>,
}

impl MessageReader {
    pub fn new(inner: Box<dyn Reader + Send>) -> Self {
        Self { inner }
    }

    pub async fn read_message(&mut self) -> Result<String> {
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

pub struct MessageWriter {
    inner: Box<dyn Writer + Send>,
}

impl MessageWriter {
    pub fn new(inner: Box<dyn Writer + Send>) -> Self {
        Self { inner }
    }

    pub async fn write_message(&mut self, s: &str) -> Result<()> {
        let message = format!(
            "{CONTENT_LENGTH_HEADER}{}{END_LINE_SYMBOLS}{END_LINE_SYMBOLS}{s}",
            s.len()
        );
        self.inner.write(message).await
    }
}

#[cfg(test)]
mod tests {
    use anyhow::anyhow;
    use mockall::{Sequence, predicate::eq};

    use super::*;
    use crate::server::{MockReader, MockWriter};

    macro_rules! message_reader_read_error_test {
        ($test_name:ident, $($method:ident: $msg:expr),+ ; $error:expr) => {
            #[tokio::test]
            async fn $test_name() {
                let mut mock = MockReader::new();
                let mut seq = mockall::Sequence::new();
                $(
                    mock.$method().times(1).in_sequence(&mut seq).return_once(|| $msg);
                )*
                let mut reader = MessageReader::new(Box::new(mock));
                let err = reader.read_message().await.unwrap_err().to_string();
                assert!(dbg!(err).contains($error));
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

    macro_rules! message_reader_read_third_read_test {
        ($test_name:ident, $content_length:literal, $msg:expr) => {
            #[tokio::test]
            async fn $test_name() {
                let mut mock = MockReader::new();
                mock.expect_read_line()
                    .returning(|| Ok(format!("{CONTENT_LENGTH_HEADER}$content_length")));
                mock.expect_read_line()
                    .returning(|| Ok(END_LINE_SYMBOLS.to_string()));
                mock.expect_read_some()
                    .with(eq($content_length))
                    .returning(|_| $msg);
                let mut reader = MessageReader::new(Box::new(mock));
                reader.read_message().await.unwrap_err();
            }
        };
    }
    message_reader_read_third_read_test!(
        message_reader_read_error_third_read_failed,
        123,
        Err(anyhow!("some error"))
    );
    message_reader_read_third_read_test!(
        message_reader_read_error_third_read_too_short,
        123,
        Ok("some content".to_string())
    );

    #[tokio::test]
    async fn message_reader_read_success() {
        let msg = "some message";
        let mut mock = MockReader::new();
        let mut seq = Sequence::new();
        mock.expect_read_line()
            .times(1)
            .in_sequence(&mut seq)
            .return_once(|| {
                Ok(format!(
                    "{CONTENT_LENGTH_HEADER}{}{END_LINE_SYMBOLS}",
                    msg.len()
                ))
            });
        mock.expect_read_line()
            .times(1)
            .in_sequence(&mut seq)
            .return_once(|| Ok(END_LINE_SYMBOLS.to_string()));
        mock.expect_read_some()
            .with(eq(msg.len()))
            .returning(|_| Ok(msg.to_string()));
        let mut reader = MessageReader::new(Box::new(mock));
        assert_eq!(reader.read_message().await.unwrap(), msg);
    }

    #[tokio::test]
    async fn message_writer_writes() {
        let mut mock = MockWriter::new();
        mock.expect_write()
            .with(eq("Content length: 4\r\n\r\ntest".to_string()))
            .returning(|_| Ok(()));
        let mut writer = MessageWriter::new(Box::new(mock));
        writer.write_message("test").await.unwrap();
    }

    #[tokio::test]
    async fn message_writer_write_error() {
        let error_msg = "some error".to_string();
        let mut mock = MockWriter::new();
        mock.expect_write()
            .with(eq("Content length: 4\r\n\r\ntest".to_string()))
            .returning({
                let error_msg = error_msg.clone();
                move |_| Err(anyhow!("{}", error_msg))
            });
        let mut writer = MessageWriter::new(Box::new(mock));
        assert_eq!(
            writer.write_message("test").await.unwrap_err().to_string(),
            error_msg
        );
    }
}
