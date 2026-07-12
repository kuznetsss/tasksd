use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard},
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
        let subs = self.subs.lock().expect("Poisoned mutex");
        self.spawn_subscriber_impl(subs, task_id, subscriber)
    }

    pub(in crate::application) fn subscribe_or_spawn(
        &self,
        task_id: &TaskId,
        subscriber_builder: impl FnOnce() -> Result<Subscriber, ApplicationError>,
    ) -> Result<(), ApplicationError> {
        let subs = self.subs.lock().expect("Poisoned lock");
        if let Some(handle) = subs.get(task_id) {
            handle.set_subscribe_to_output(true);
            return Ok(());
        }
        self.spawn_subscriber_impl(subs, *task_id, subscriber_builder()?)?;
        Ok(())
    }

    pub(in crate::application) fn unsubscribe(
        &self,
        task_id: &TaskId,
    ) -> Result<(), ApplicationError> {
        let subs = self.subs.lock().expect("Poisoned lock");
        let handle = match subs.get(task_id) {
            Some(h) => h,
            None => return Err(TaskError::NotFound.into()),
        };
        handle.set_subscribe_to_output(false);
        Ok(())
    }

    /// Spawns subscriber with locked subs
    fn spawn_subscriber_impl(
        &self,
        mut subs: MutexGuard<'_, HashMap<TaskId, SubscriberHandle>>,
        task_id: TaskId,
        subscriber: Subscriber,
    ) -> Result<AbortHandle, ApplicationError> {
        assert!(
            subs.insert(task_id, subscriber.handle()).is_none(),
            "Duplicated subscriber for task {task_id}"
        );
        self.internal_coroutines
            .spawn({
                let subs = self.subs.clone();
                async move {
                    subscriber.run().await;
                    assert!(
                        subs.lock()
                            .expect("Poisoned mutex")
                            .remove(&task_id)
                            .is_some(),
                        "Missing subscriber handle for {task_id}"
                    );
                }
            })
            .map_err(|_| ApplicationError::Shutdown)
    }
}
