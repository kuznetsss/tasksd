use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{api::request::Request, transport};

pub struct Session {
    cancellation_token: CancellationToken,
    connection: transport::Connection,
}

impl Session {
    pub fn new(cancellation_token: CancellationToken, connection: transport::Connection) -> Self {
        Self {
            cancellation_token,
            connection,
        }
    }

    pub async fn run(mut self) {
        loop {
            match self.connection.read_message().await {
                Ok(msg) => {
                    self.handle_msg(msg);
                }
                Err(e) => {
                    info!("Error reading from client {e}");
                    break;
                }
            }
        }
    }

    fn handle_msg(&self, msg: String) {
        tokio::spawn(async {
            let result: anyhow::Result<()> = async move {
                let _request = Request::parse(&msg)?;
                Ok(())
            }
            .await;
            if let Err(e) = result {
                warn!("Error processing request: {e}");
            }
        });
    }
}
