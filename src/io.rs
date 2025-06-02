use std::error::Error;
use std::fmt::Display;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender, channel};

use crate::api::server_messages::{ServerNotification, ServerResponse};

#[derive(Debug, PartialEq, Eq)]
pub enum IoError {
    EOF,
}

impl Display for IoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IoError::EOF => write!(f, "EOF"),
        }
    }
}

impl Error for IoError {}

#[derive(Debug)]
pub struct Reader<Stdin> {
    stdin: BufReader<Stdin>,
}

impl<Stdin> Reader<Stdin>
where
    Stdin: tokio::io::AsyncRead + Unpin,
{
    pub fn new(stdin: Stdin) -> Self {
        Self {
            stdin: BufReader::new(stdin),
        }
    }

    pub async fn read_line(&mut self) -> Result<String, anyhow::Error> {
        let mut buffer = String::new();
        match self.stdin.read_line(&mut buffer).await {
            Ok(bytes_read) => {
                if bytes_read != 0 {
                    Ok(buffer)
                } else {
                    Err(IoError::EOF.into())
                }
            }
            Err(e) => Err(e.into()),
        }
    }
}

#[derive(Debug)]
pub struct Writer<Stdout> {
    stdout: BufWriter<Stdout>,
    response_receiver: Receiver<ServerResponse>,
    notification_receiver: Receiver<ServerNotification>,
}

impl<Stdout> Writer<Stdout>
where
    Stdout: tokio::io::AsyncWrite + Unpin,
{
    const CHANNEL_SIZE: usize = 1024;
    pub fn new(stdout: Stdout) -> (Self, Sender<ServerResponse>, Sender<ServerNotification>) {
        let (response_sender, response_receiver) = channel::<ServerResponse>(Self::CHANNEL_SIZE);
        let (notification_sender, notification_receiver) =
            channel::<ServerNotification>(Self::CHANNEL_SIZE);
        (
            Self {
                stdout: BufWriter::new(stdout),
                response_receiver,
                notification_receiver,
            },
            response_sender,
            notification_sender,
        )
    }

    pub async fn run(mut self) -> Result<(), anyhow::Error> {
        loop {
            let string_to_write = select! {
                response = self.response_receiver.recv() => {
                    match response {
                        None => break,
                        Some(response) => {
                            serde_json::to_string(&response)?
                        }
                    }
                }
                notification = self.notification_receiver.recv() => {
                    match notification {
                        None => break,
                        Some(notification) => {
                            serde_json::to_string(&notification)?
                        }
                    }
                }
            };
            self.stdout.write_all(string_to_write.as_bytes()).await?;
            self.stdout.flush().await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reader_read_line() {
        let input_data = "Some line\nAnother line\n\n".to_string();
        let mut reader = Reader::new(std::io::Cursor::new(input_data));
        assert_eq!(reader.read_line().await.unwrap(), "Some line\n");
        assert_eq!(reader.read_line().await.unwrap(), "Another line\n");
        assert_eq!(reader.read_line().await.unwrap(), "\n");
    }

    #[tokio::test]
    async fn reader_read_line_returns_eof_error_on_closed_input() {
        // Cursor returns EOF when there is no data to read
        let mut reader = Reader::new(std::io::Cursor::new(""));
        assert_eq!(
            reader
                .read_line()
                .await
                .unwrap_err()
                .downcast::<IoError>()
                .unwrap(),
            IoError::EOF
        );
    }
}
