use std::{any::Any, panic::AssertUnwindSafe};

use futures::FutureExt;
use tokio::task::AbortHandle;
use tokio_util::task::TaskTracker;
use tracing::error;

use crate::tasks::task_error::TaskError;

#[derive(Debug)]
pub(in crate::tasks) struct WrappedTaskTracker<C = fn(Panic)> {
    inner: TaskTracker,
    panic_handler: PanicHandler<C>,
}

#[derive(Debug, Clone)]
pub(in crate::tasks) enum PanicHandler<C> {
    Logging,
    Callback(C),
    Abort,
}

pub(in crate::tasks) type Panic = Box<dyn Any + Send + 'static>;

impl<C> PanicHandler<C>
where
    C: FnOnce(Panic) + Clone + Send + 'static,
{
    fn handle(self, p: Panic) {
        match self {
            Self::Logging => {
                error!("Panic: {}", Self::to_string(&p))
            }
            Self::Callback(c) => {
                c(p);
            }
            Self::Abort => {
                let log_msg = format!("Abort on panic: {}", Self::to_string(&p));
                error!("{log_msg}");
                eprintln!("{log_msg}");
                std::process::abort();
            }
        }
    }

    fn to_string(p: &Panic) -> String {
        p.downcast_ref::<&str>()
            .map(|&s| s.to_string())
            .or_else(|| p.downcast_ref::<String>().map(Clone::clone))
            .unwrap_or("<non-string panic payload>".to_string())
    }
}

impl<C> WrappedTaskTracker<C>
where
    C: FnOnce(Panic) + Clone + Send + 'static,
{
    pub(in crate::tasks) fn new(panic_handler: PanicHandler<C>) -> Self {
        Self {
            inner: TaskTracker::new(),
            panic_handler,
        }
    }

    pub(in crate::tasks) fn spawn<F>(&self, f: F) -> Result<AbortHandle, TaskError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        // TODO: there is a data race here: inner could get closed after the check and then spawn will create a detached task
        if self.inner.is_closed() {
            Err(TaskError::AlreadyExited)
        } else {
            Ok(self
                .inner
                .spawn({
                    let panic_handler = self.panic_handler.clone();
                    async move {
                        if let Err(panic) = AssertUnwindSafe(f).catch_unwind().await {
                            panic_handler.handle(panic);
                        }
                    }
                })
                .abort_handle())
        }
    }

    pub(in crate::tasks) async fn join_all(&self) {
        assert!(
            self.inner.close(),
            "WrappedTaskTracker closed more than one time"
        );
        self.inner.wait().await;
    }
}

/*
#[cfg(test)]
mod tests {
    use std::sync::{Arc, atomic::AtomicBool};

    use super::*;

    #[tokio::test]
    async fn spawn() {
        let tt = WrappedTaskTracker::new();
        let called = Arc::new(AtomicBool::new(false));
        tt.spawn({
            let called = called.clone();
            async move {
                called.store(true, std::sync::atomic::Ordering::Relaxed);
            }
        })
        .unwrap();
        tt.join_all().await;
        assert!(called.load(std::sync::atomic::Ordering::Relaxed));
    }
}
*/
