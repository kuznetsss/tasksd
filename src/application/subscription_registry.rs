use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard},
};

use tokio::{sync::watch, task::AbortHandle};

use crate::{
    api::{SubscribeEventType, SubscribeEvents},
    application::{
        ApplicationError,
        subscriber::{Subscriber, SubscriberHandle},
    },
    tasks::{TaskError, TaskId},
    utils::tracker::WrappedTaskTracker,
};

#[derive(Clone)]
pub(in crate::application) struct SubscriptionRegistry {
    subs: Arc<Mutex<HashMap<TaskId, SubscriberEntry>>>,
    internal_coroutines: Arc<WrappedTaskTracker>,
}

#[derive(Clone)]
struct SubscriberEntry {
    handle: SubscriberHandle,
    completion_receiver: watch::Receiver<bool>,
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

    pub(in crate::application) fn update_or_spawn_subscriber(
        &self,
        task_id: &TaskId,
        events_to_subscribe: &SubscribeEvents,
        subscriber_builder: impl FnOnce() -> Result<Subscriber, ApplicationError>,
    ) -> Result<(), ApplicationError> {
        let subs = self.subs.lock().expect("Poisoned lock");
        if let Some(entry) = subs.get(task_id) {
            entry
                .handle
                .set_subscribe_to_output(events_to_subscribe.contains(&SubscribeEventType::Output));
            return Ok(());
        }
        self.spawn_subscriber_impl(subs, *task_id, subscriber_builder()?)?;
        Ok(())
    }

    pub(in crate::application) async fn stop_subscriber(
        &self,
        task_id: &TaskId,
    ) -> Result<(), ApplicationError> {
        let mut entry = {
            let subs = self.subs.lock().expect("Poisoned lock");
            match subs.get(task_id) {
                Some(h) => h.clone(),
                None => return Err(TaskError::NotFound.into()),
            }
        };
        entry.handle.stop();
        let _ = entry.completion_receiver.changed().await;
        Ok(())
    }

    /// Spawns subscriber with locked subs
    fn spawn_subscriber_impl(
        &self,
        mut subs: MutexGuard<'_, HashMap<TaskId, SubscriberEntry>>,
        task_id: TaskId,
        subscriber: Subscriber,
    ) -> Result<AbortHandle, ApplicationError> {
        let (completion_sender, completion_receiver) = watch::channel(false);
        let entry = SubscriberEntry {
            handle: subscriber.handle(),
            completion_receiver,
        };
        assert!(
            subs.insert(task_id, entry).is_none(),
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
                    let _ = completion_sender.send(true);
                }
            })
            .map_err(|_| ApplicationError::Shutdown)
    }
}
