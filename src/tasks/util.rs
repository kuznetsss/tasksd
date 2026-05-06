use std::sync::Mutex;

use tokio::task::{AbortHandle, JoinSet};

pub(in crate::tasks) struct WrappedJoinSet {
    inner: Mutex<Option<JoinSet<()>>>,
}

impl WrappedJoinSet {
    pub(in crate::tasks) fn new() -> Self {
        Self {
            inner: Mutex::new(Some(JoinSet::new())),
        }
    }

    pub(in crate::tasks) fn spawn<F>(&self, f: F) -> AbortHandle
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.inner
            .lock()
            .unwrap()
            .as_mut()
            .expect("Spawning on joined WrappedJoinSet")
            .spawn(f)
    }

    pub(in crate::tasks) fn try_join_next(&self) -> Option<()> {
        let join_result = self
            .inner
            .lock()
            .unwrap()
            .as_mut()
            .expect("Spawning on joined WrappedJoinSet")
            .try_join_next();
        let join_result = match join_result {
            None => return None,
            Some(r) => r,
        };
        match join_result {
            Ok(v) => Some(v),
            Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
            Err(_) => None,
        }
    }

    pub(in crate::tasks) async fn join_all(&self) {
        let mut join_set = self
            .inner
            .lock()
            .unwrap()
            .take()
            .expect("Called join_all on already joined WrappedJoinSet");
        while let Some(j) = join_set.join_next().await {
            if let Err(e) = j
                && e.is_panic()
            {
                std::panic::resume_unwind(e.into_panic());
            }
        }
    }
}
