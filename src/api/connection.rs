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
    use mockall::predicate::eq;

    use super::*;
    use crate::server::MockWriter;

    // TODO: write test for reader

    #[tokio::test]
    async fn lsp_writer_writes() {
        let mut mock = MockWriter::new();
        mock.expect_write()
            .with(eq("Content length: 4\r\n\r\ntest".to_string()))
            .returning(|_| Ok(()));
        let mut writer = MessageWriter::new(Box::new(mock));
        writer.write_message("test").await.unwrap();
    }

    #[tokio::test]
    async fn lsp_writer_write_error() {
        let mut mock = MockWriter::new();
        mock.expect_write()
            .with(eq("Content length: 4\r\n\r\ntest".to_string()))
            .returning(|_| Err(anyhow!("Some error")));
        let mut writer = MessageWriter::new(Box::new(mock));
        writer.write_message("test").await.unwrap_err();
    }
}
