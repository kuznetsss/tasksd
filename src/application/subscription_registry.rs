use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use tokio::task::AbortHandle;

use crate::{
    application::{
        ApplicationError,
        subscriber::{Subscriber, SubscriberHandle},
    },
    tasks::{TaskError, TaskId},
    utils::tracker::WrappedTaskTracker,
};

#[derive(Clone)]
pub(in crate::application) struct SubscriptionRegistry {
    subs: Arc<Mutex<HashMap<TaskId, SubscriberHandle>>>,
    internal_coroutines: Arc<WrappedTaskTracker>,
}

impl SubscriptionRegistry {
    pub(in crate::application) fn new(internal_coroutines: Arc<WrappedTaskTracker>) -> Self {
        Self {
            subs: Default::default(),
            internal_coroutines,
        }
    }

    pub(in crate::application) fn spawn_subscriber(
        &self,
        task_id: TaskId,
        subscriber: Subscriber,
    ) -> Result<AbortHandle, ApplicationError> {
        let mut subs = self.subs.lock().expect("Poisoned mutex");
        assert!(
            subs.insert(task_id, subscriber.handle()).is_none(),
            "Duplicated subscriber for task {task_id}"
        );
        self.internal_coroutines
            .spawn({
                let subs = self.subs.clone();
                async move {
                    subscriber.run().await;
                    subs.lock()
                        .expect("Poisoned mutex")
                        .remove(&task_id)
                        .or_else(|| panic!("Missing subscriber handle for {task_id}"));
                }
            })
            .map_err(|_| ApplicationError::Shutdown)
    }

    pub(in crate::application) fn set_subscribe_to_output(
        &self,
        task_id: &TaskId,
        value: bool,
    ) -> Result<(), ApplicationError> {
        let subs = self.subs.lock().expect("Poisoned lock");
        let handle = match subs.get(&task_id) {
            Some(h) => h,
            None => return Err(TaskError::NotFound.into()),
        };
        handle.set_subscribe_to_output(value);
        Ok(())
    }
}
