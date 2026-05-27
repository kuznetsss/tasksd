use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{
    api::{
        common::JsonRpcVersion,
        request::Request,
        response::{Response, ResponseBody, ResponseError},
    },
    handler::Handler,
    tasks::task_manager::TaskManager,
    transport::{self, connection::ConnectionWriter, error::TransportError},
};

pub struct Session {
    cancellation_token: CancellationToken,
    connection: transport::Connection,
    task_manager: Arc<TaskManager>,
}

impl Session {
    pub fn new(
        cancellation_token: CancellationToken,
        connection: transport::Connection,
        task_manager: Arc<TaskManager>,
    ) -> Self {
        Self {
            cancellation_token,
            connection,
            task_manager,
        }
    }

    pub async fn run(mut self) {
        let writer = self.connection.writer();
        while let Some(msg) = self
            .cancellation_token
            .run_until_cancelled(self.connection.read_message())
            .await
        {
            let msg = match msg {
                Ok(msg) => msg,
                Err(e) => {
                    info!("Error reading from client: {e}");
                    break;
                }
            };
            match Request::parse(msg) {
                Ok(r) => self.handle_request(writer.clone(), r),
                Err(e) => {
                    if Self::handle_parse_error(writer.clone(), msg, e)
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            }
        }
        self.cancellation_token.cancel();
    }

    fn handle_request(&self, connection_writer: ConnectionWriter, request: Request) {
        let task_manager = self.task_manager.clone();
        tokio::spawn(async move {
            let handler = Handler::new(connection_writer, task_manager);
            handler.handle_request(request).await;
        });
    }

    async fn handle_parse_error(
        writer: ConnectionWriter,
        msg: &str,
        e: serde_json::Error,
    ) -> Result<(), TransportError> {
        info!("Error parsing request: '{msg}': {e}");
        let response = Response {
            jsonrpc: JsonRpcVersion {},
            id: None,
            body: ResponseBody::Error(ResponseError::invalid_request(e.to_string())),
        };
        let notification_str =
            serde_json::to_string(&response).expect("Serialization shouldn't fail");
        writer.write(&notification_str).await
    }
}
