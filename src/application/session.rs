use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info, info_span, warn};

use crate::{
    api::{Request, Response},
    application::handler::Handler,
    tasks::TaskManager,
    transport::{self},
};

pub(in crate::application) struct Session {
    cancellation_token: CancellationToken,
    connection: transport::Connection,
    task_manager: Arc<TaskManager>,
}

impl Session {
    pub(in crate::application) fn new(
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

    pub(in crate::application) async fn run(mut self) {
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
                Ok(r) => self.handle_request(r),
                Err(e) => self.handle_parse_error(e),
            }
        }
        self.cancellation_token.cancel();
    }

    fn handle_request(&self, request: Request) {
        let span = info_span!("request", id = %request.id, method = %request.method);
        let task_manager = self.task_manager.clone();
        let connection_writer = self.connection.writer();
        tokio::spawn(
            async move {
                let handler = Handler::new(connection_writer, task_manager);
                handler.handle_request(request).await;
            }
            .instrument(span),
        );
    }

    fn handle_parse_error(&self, response: Response) {
        let id = response
            .id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| "null".to_string());
        let span = info_span!("parse_error", id);
        let connection_writer = self.connection.writer();
        tokio::spawn(
            async move {
                let response =
                    serde_json::to_string(&response).expect("Serialization shouldn't fail");
                if let Err(e) = connection_writer.write(&response).await {
                    warn!("Error sending error response: {e}");
                }
            }
            .instrument(span),
        );
    }
}
