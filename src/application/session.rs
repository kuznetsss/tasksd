use std::sync::Arc;

use tokio::task::AbortHandle;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info, info_span, warn};

use crate::{
    api::{Request, RequestId, Response},
    application::{ApplicationError, handler::Handler},
    tasks::TaskManager,
    transport::{self},
    utils::tracker::{PanicHandler, WrappedTaskTracker},
};

pub(in crate::application) struct Session {
    cancellation_token: CancellationToken,
    internal_coroutines: Arc<WrappedTaskTracker>,
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
            internal_coroutines: Arc::new(WrappedTaskTracker::new(PanicHandler::new_aborting())),
            connection,
            task_manager,
        }
    }

    pub(in crate::application) async fn run(mut self) {
        loop {
            let msg = match self
                .cancellation_token
                .run_until_cancelled(self.connection.read_message())
                .await
            {
                Some(m) => m,
                None => {
                    // Shutdown was initiated
                    self.shutdown().await;
                    return;
                }
            };
            let msg = match msg {
                Ok(msg) => msg,
                Err(e) => {
                    info!("Error reading from client: {e}");
                    // Client connection is broken, disconnect
                    self.internal_coroutines.shutdown();
                    return;
                }
            };
            match Request::parse(msg) {
                Ok(r) => self.handle_request(r),
                Err(e) => self.handle_parse_error(e),
            }
        }
    }

    async fn shutdown(self) {
        self.internal_coroutines.shutdown();
        self.internal_coroutines.join().await;
        // All tasks should be finished at this point, so subscribers are dead.
        // Currently this is guaranteed by the order in Application::shutdown()
        self.connection.join().await;
    }

    fn handle_request(&mut self, request: Request) {
        let span = info_span!("request", id = %request.id, method = %request.method);
        let request_id = request.id.clone();
        let task_manager = self.task_manager.clone();
        let connection_writer = self.connection.writer();
        let internal_coroutines = self.internal_coroutines.clone();
        let spawn_result = self.internal_coroutines.spawn(
            async move {
                let handler = Handler::new(connection_writer, task_manager, internal_coroutines);
                handler.handle_request(request).await;
            }
            .instrument(span),
        );
        self.handle_spawn_result(spawn_result, Some(request_id));
    }

    fn handle_parse_error(&self, response: Response) {
        let id = response
            .id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| "null".to_string());
        let span = info_span!("parse_error", id);
        let connection_writer = self.connection.writer();
        let spawn_result = self.internal_coroutines.spawn(
            async move {
                let response =
                    serde_json::to_string(&response).expect("Serialization shouldn't fail");
                if let Err(e) = connection_writer.write(&response).await {
                    warn!("Error sending error response: {e}");
                }
            }
            .instrument(span),
        );
        self.handle_spawn_result(spawn_result, None);
    }

    fn handle_spawn_result(
        &self,
        result: Result<AbortHandle, ApplicationError>,
        request_id: Option<RequestId>,
    ) {
        if let Err(e) = result {
            let writer = self.connection.writer();
            tokio::spawn(async move {
                let response = Response::new(request_id, e.into());
                if let Err(e) = writer.write(&response.to_json_string()).await {
                    warn!("Error writing to connection: {e}")
                }
            });
        }
    }
}
