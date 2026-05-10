use std::{any::Any, fmt::Debug, panic::AssertUnwindSafe, sync::Arc};

use futures::FutureExt;
use tokio::task::AbortHandle;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::error;

use crate::tasks::task_error::TaskError;

#[derive(Clone)]
pub(in crate::tasks) enum PanicHandler {
    Logging,
    Callback(Arc<dyn Fn(Panic) + Send + Sync>),
    Abort,
}

impl PanicHandler {
    pub(in crate::tasks) fn new_logging() -> Self {
        Self::Logging
    }

    pub(in crate::tasks) fn new_with_callback<F>(f: F) -> Self
    where
        F: Fn(Panic) + Send + Sync + 'static,
    {
        Self::Callback(Arc::new(f))
    }

    pub(in crate::tasks) fn new_aborting() -> Self {
        Self::Abort
    }
}

impl Debug for PanicHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Logging => write!(f, "PanicHandler::Logging"),
            Self::Callback(_) => write!(f, "PanicHandler::Callback"),
            Self::Abort => write!(f, "PanicHandler::Abort"),
        }
    }
}

pub(in crate::tasks) type Panic = Box<dyn Any + Send>;

fn panic_to_string(p: &Panic) -> &str {
    p.downcast_ref::<&str>()
        .copied()
        .or_else(|| p.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("<non-string panic payload>")
}

impl PanicHandler {
    fn handle(self, p: Panic) {
        match self {
            Self::Logging => {
                error!("Panic: {}", panic_to_string(&p))
            }
            Self::Callback(c) => {
                c(p);
            }
            Self::Abort => {
                let log_msg = format!("Abort on panic: {}", panic_to_string(&p));
                error!("{log_msg}");
                eprintln!("{log_msg}");
                std::process::abort();
            }
        }
    }
}

#[derive(Debug)]
pub(in crate::tasks) struct WrappedTaskTracker {
    inner: TaskTracker,
    panic_handler: PanicHandler,
    cancellation_token: CancellationToken,
}

impl WrappedTaskTracker {
    pub(in crate::tasks) fn new(panic_handler: PanicHandler) -> Self {
        Self {
            inner: TaskTracker::new(),
            panic_handler,
            cancellation_token: CancellationToken::new(),
        }
    }

    pub(in crate::tasks) fn spawn<F>(&self, f: F) -> Result<AbortHandle, TaskError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        // NOTE: There is data race between is_closed() and spawn() here.
        // Worst case a task will be spawned but never joined
        if self.inner.is_closed() {
            Err(TaskError::AlreadyExited)
        } else {
            Ok(self
                .inner
                .spawn({
                    let panic_handler = self.panic_handler.clone();
                    let cancellation_token = self.cancellation_token.clone();
                    async move {
                        let panic_catch_future = AssertUnwindSafe(f).catch_unwind();
                        if let Some(Err(panic)) = cancellation_token
                            .run_until_cancelled(panic_catch_future)
                            .await
                        {
                            panic_handler.handle(panic);
                        }
                    }
                })
                .abort_handle())
        }
    }

    pub(in crate::tasks) async fn join(&self) {
        self.inner.close();
        self.inner.wait().await;
    }

    pub(in crate::tasks) fn is_joined(&self) -> bool {
        self.inner.is_closed() && self.inner.is_empty()
    }
}

impl Drop for WrappedTaskTracker {
    fn drop(&mut self) {
        self.cancellation_token.cancel();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicI32, AtomicUsize, Ordering};

    use tokio::sync::Notify;

    use super::*;

    #[test]
    fn extract_panic_message() {
        let msg = "some panic";
        let p = std::panic::catch_unwind(|| panic!("{msg}")).unwrap_err();
        assert_eq!(panic_to_string(&p), msg);

        let n = 123;
        let p = std::panic::catch_unwind(|| panic!("{n}")).unwrap_err();
        assert_eq!(panic_to_string(&p), format!("{n}"));
    }

    #[tokio::test]
    async fn spawn_spawns_a_task() {
        let t = WrappedTaskTracker::new(PanicHandler::new_aborting());
        let call_count = Arc::new(AtomicI32::new(0));
        t.spawn({
            let call_count = call_count.clone();
            async move {
                call_count.fetch_add(1, Ordering::Relaxed);
            }
        })
        .unwrap();
        t.join().await;
        assert!(t.is_joined());
        assert_eq!(call_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn spawn_catches_panic() {
        let call_count = Arc::new(AtomicI32::new(0));
        let panic_msg = "some panic";
        let t = WrappedTaskTracker::new(PanicHandler::new_with_callback({
            let call_count = call_count.clone();
            move |p| {
                assert_eq!(panic_to_string(&p), panic_msg);
                call_count.fetch_add(1, Ordering::Relaxed);
            }
        }));
        t.spawn(async move { panic!("{panic_msg}") }).unwrap();
        t.join().await;
        assert_eq!(call_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn spawn_abort() {
        let t = WrappedTaskTracker::new(PanicHandler::new_aborting());
        let call_count = Arc::new(AtomicI32::new(0));
        t.spawn({
            let call_count = call_count.clone();
            async move {
                call_count.fetch_add(1, Ordering::Relaxed);
            }
        })
        .unwrap()
        .abort();
        t.join().await;
        assert_eq!(call_count.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn spawn_after_join_returns_error() {
        let t = WrappedTaskTracker::new(PanicHandler::new_aborting());
        t.join().await;
        let e = t.spawn(async {}).unwrap_err();
        assert!(matches!(e, TaskError::AlreadyExited));
    }

    #[tokio::test]
    async fn drop_without_join_cancels_the_task() {
        let t = WrappedTaskTracker::new(PanicHandler::new_logging());
        let started = Arc::new(Notify::new());
        let call_count = Arc::new(AtomicUsize::new(0));
        t.spawn({
            let started = started.clone();
            let call_count = call_count.clone();
            async move {
                started.notify_one();
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                call_count.fetch_add(1, Ordering::Relaxed);
            }
        })
        .unwrap();
        started.notified().await;
        drop(t);
        assert_eq!(call_count.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn multiple_join() {
        let t = WrappedTaskTracker::new(PanicHandler::new_aborting());
        t.join().await;
        tokio::time::timeout(std::time::Duration::from_secs(1), t.join())
            .await
            .unwrap()
    }
}
