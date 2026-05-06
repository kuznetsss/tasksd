use std::sync::Mutex;

use tokio::task::{AbortHandle, JoinSet};
use tokio_util::task::TaskTracker;

use crate::tasks::task_error::TaskError;

pub(in crate::tasks) struct WrappedTaskTracker {
    inner: TaskTracker,
}

impl WrappedTaskTracker {
    pub(in crate::tasks) fn new() -> Self {
        Self {
            inner: TaskTracker::new(),
        }
    }

    pub(in crate::tasks) fn spawn<F>(&self, f: F) -> Result<AbortHandle, TaskError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        if self.inner.is_closed() {
            Err(TaskError::AlreadyExited)
        } else {
            Ok(self.inner.spawn(f).abort_handle())
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
