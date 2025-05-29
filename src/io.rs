use tokio::io::{AsyncBufReadExt, BufReader, BufWriter, Stdin, Stdout};
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender, channel};

use crate::api::server_messages::{ServerNotification, ServerResponse};

#[derive(Debug)]
pub struct Reader {
    stdin: BufReader<Stdin>,
}

impl Reader {
    pub fn new() -> Self {
        Self {
            stdin: BufReader::new(tokio::io::stdin()),
        }
    }

    pub async fn read_line(&mut self) -> Result<String, anyhow::Error> {
        let mut buffer = String::new();
        match self.stdin.read_line(&mut buffer).await {
            Ok(_) => Ok(buffer),
            Err(e) => Err(e.into()),
        }
    }
}

#[derive(Debug)]
pub struct Writer {
    stdout: BufWriter<Stdout>,
    response_receiver: Receiver<ServerResponse>,
    notification_receiver: Receiver<ServerNotification>,
}

impl Writer {
    const CHANNEL_SIZE: usize = 1024;
    pub fn new() -> (Self, Sender<ServerResponse>, Sender<ServerNotification>) {
        let (response_sender, response_receiver) = channel::<ServerResponse>(Self::CHANNEL_SIZE);
        let (notification_sender, notification_receiver) =
            channel::<ServerNotification>(Self::CHANNEL_SIZE);
        (
            Self {
                stdout: BufWriter::new(tokio::io::stdout()),
                response_receiver,
                notification_receiver,
            },
            response_sender,
            notification_sender,
        )
    }

    pub fn run(mut self) -> Result<(), anyhow::Error> {
        loop {
            select!{
                self.response_receiver.recv().await => {

                },
                self.notification_receiver.recv().await => {
                }
            }
        }
    }
}
