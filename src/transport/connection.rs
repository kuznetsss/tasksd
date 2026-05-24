use anyhow::Result;
use tokio_util::sync::CancellationToken;

use crate::transport::{
    WriterImpl,
    background_writer::{BackgroundWriter, WriteHandle},
    reader::{Reader, ReaderImpl},
};

pub const CONTENT_LENGTH_HEADER: &str = "Content-Length: ";
pub const END_LINE_SYMBOLS: &str = "\r\n";

pub struct Connection {
    reader: Reader<Box<dyn ReaderImpl>>,
    writer: BackgroundWriter,
}

impl Connection {
    pub(in crate::transport) fn new<R, W>(
        read_half: R,
        write_half: W,
        cancellation_token: CancellationToken,
    ) -> Self
    where
        R: ReaderImpl,
        W: WriterImpl,
    {
        Self {
            reader: Reader::new(Box::new(read_half)),
            writer: BackgroundWriter::spawn(write_half, cancellation_token),
        }
    }

    pub async fn read_message(&mut self) -> Result<&str> {
        let header = self.reader.read_line().await?;
        if !header.starts_with(CONTENT_LENGTH_HEADER) || !header.ends_with(END_LINE_SYMBOLS) {
            anyhow::bail!("Got unexpected symbols: {header}");
        }
        let start = CONTENT_LENGTH_HEADER.len();
        let end = header.len() - END_LINE_SYMBOLS.len();
        let content_length: usize = header[start..end].parse()?;
        let empty_line = self.reader.read_line().await?;
        if empty_line != END_LINE_SYMBOLS {
            anyhow::bail!("Expected a new line, got: {empty_line}");
        }
        self.reader.read_some(content_length).await
    }

    pub fn writer(&self) -> ConnectionWriter {
        ConnectionWriter {
            inner: self.writer.handle(),
        }
    }
}

#[derive(Clone)]
pub struct ConnectionWriter {
    inner: WriteHandle,
}

impl ConnectionWriter {
    pub async fn write(&self, s: &str) -> Result<()> {
        const MESSAGE_LEN_DIGITS_NUM: usize = 6;
        const PREFIX_LEN: usize =
            CONTENT_LENGTH_HEADER.len() + MESSAGE_LEN_DIGITS_NUM + END_LINE_SYMBOLS.len() * 2;
        let mut message = String::with_capacity(PREFIX_LEN + s.len());
        use std::fmt::Write;
        write!(
            &mut message,
            "{CONTENT_LENGTH_HEADER}{}{END_LINE_SYMBOLS}{END_LINE_SYMBOLS}{s}",
            s.len()
        )?;
        self.inner.write(message).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_test::io::Builder;

    macro_rules! read_message_error_test {
        ($test_name:ident, [$($m:expr),+] , $e:expr) => {
            #[tokio::test]
            async fn $test_name() {
                let mut reader_mock = Builder::new();
                $(
                    reader_mock.read($m.as_bytes());
                )+
                let reader_mock = reader_mock.build();
                let writer_mock = Builder::new().build();
                let mut connection = Connection::new(reader_mock, writer_mock, CancellationToken::new());
                let err = connection.read_message().await.unwrap_err();
                assert!(dbg!(err.to_string()).contains($e));
            }
        };
    }

    read_message_error_test!(read_message_early_eof, ["test"], "EOF");
    read_message_error_test!(
        read_message_not_a_header,
        ["test\r\n"],
        "unexpected symbols"
    );
    read_message_error_test!(
        read_message_no_endline_symbol,
        [format!("{CONTENT_LENGTH_HEADER}123\n")],
        "unexpected symbols"
    );
    read_message_error_test!(
        read_message_length_is_not_a_number,
        [format!("{CONTENT_LENGTH_HEADER}123abc{END_LINE_SYMBOLS}")],
        "invalid digit"
    );
    read_message_error_test!(
        read_message_no_empty_line,
        [
            format!("{CONTENT_LENGTH_HEADER}123{END_LINE_SYMBOLS}"),
            format!("not_an_empty_line{END_LINE_SYMBOLS}")
        ],
        "Expected a new line"
    );
    read_message_error_test!(
        read_message_too_short_content,
        [
            format!("{CONTENT_LENGTH_HEADER}123{END_LINE_SYMBOLS}"),
            END_LINE_SYMBOLS,
            "short_content"
        ],
        "eof"
    );

    #[tokio::test]
    async fn read_message_success() {
        let msg = "some message";

        let mut reader_mock = Builder::new();
        reader_mock
            .read(format!("{CONTENT_LENGTH_HEADER}{}{END_LINE_SYMBOLS}", msg.len()).as_bytes());
        reader_mock.read(END_LINE_SYMBOLS.as_bytes());
        reader_mock.read(msg.as_bytes());
        let reader_mock = reader_mock.build();
        let writer_mock = Builder::new().build();

        let mut connection = Connection::new(reader_mock, writer_mock, CancellationToken::new());
        let read_msg = connection.read_message().await.unwrap();
        assert_eq!(read_msg, msg)
    }

    #[tokio::test]
    async fn read_message_multiple_times() {
        let msgs = ["some message", "another message"];

        let mut reader_mock = Builder::new();
        for m in &msgs {
            reader_mock
                .read(format!("{CONTENT_LENGTH_HEADER}{}{END_LINE_SYMBOLS}", m.len()).as_bytes());
            reader_mock.read(END_LINE_SYMBOLS.as_bytes());
            reader_mock.read(m.as_bytes());
        }
        let reader_mock = reader_mock.build();
        let writer_mock = Builder::new().build();

        let mut connection = Connection::new(reader_mock, writer_mock, CancellationToken::new());
        for m in msgs {
            let read_msg = connection.read_message().await.unwrap();
            assert_eq!(read_msg, m);
        }
    }

    #[tokio::test]
    async fn write_message_error_after_connection_dropped() {
        let reader_mock = Builder::new().build();
        let writer_mock = Builder::new().build();
        let connection = Connection::new(reader_mock, writer_mock, CancellationToken::new());
        let writer = connection.writer();
        drop(connection);
        tokio::task::yield_now().await;
        let err = writer.write("some message").await.unwrap_err();
        assert!(dbg!(err.to_string()).contains("closed"));
    }

    #[tokio::test]
    async fn write_message_writes() {
        let msg = "some message";
        let reader_mock = Builder::new().build();
        let writer_mock = Builder::new()
            .write(
                format!(
                    "{CONTENT_LENGTH_HEADER}{}{END_LINE_SYMBOLS}{END_LINE_SYMBOLS}{msg}",
                    msg.len()
                )
                .as_bytes(),
            )
            .build();
        let connection = Connection::new(reader_mock, writer_mock, CancellationToken::new());
        let writer = connection.writer();
        writer.write(&msg).await.unwrap();
    }

    #[tokio::test]
    async fn write_message_error_after_token_is_cancelled() {
        let reader_mock = Builder::new().build();
        let writer_mock = Builder::new().build();
        let token = CancellationToken::new();
        let connection = Connection::new(reader_mock, writer_mock, token.clone());
        let writer = connection.writer();
        token.cancel();
        tokio::task::yield_now().await;
        let err = writer.write("some message").await.unwrap_err();
        assert!(dbg!(err.to_string()).contains("closed"));
    }
}
