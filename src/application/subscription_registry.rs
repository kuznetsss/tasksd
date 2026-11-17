use std::{collections::HashMap, sync::Arc};

use tokio::{sync::watch, task::AbortHandle};

use crate::{
    api::{SubscribeEventType, SubscribeEvents},
    application::{
        ApplicationError,
        subscriber::{Subscriber, SubscriberHandle, SubscriberSendingGateGuard},
    },
    tasks::{TaskError, TaskId},
    utils::tracker::WrappedTaskTracker,
};

#[derive(Clone)]
pub(in crate::application) struct SubscriptionRegistry {
    subs: Arc<tokio::sync::Mutex<HashMap<TaskId, SubscriberEntry>>>,
    internal_coroutines: Arc<WrappedTaskTracker>,
}

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

    pub(in crate::application) async fn spawn_subscriber(
        &self,
        task_id: TaskId,
        subscriber: Subscriber,
    ) -> Result<AbortHandle, ApplicationError> {
        let subs = self.subs.lock().await;
        self.spawn_subscriber_impl(subs, task_id, subscriber)
    }

    pub(in crate::application) async fn update_or_spawn_subscriber(
        &self,
        task_id: &TaskId,
        events_to_subscribe: &SubscribeEvents,
        subscriber_builder: impl FnOnce() -> Result<Subscriber, ApplicationError>,
    ) -> Result<Option<SubscriberSendingGateGuard>, ApplicationError> {
        let subs = self.subs.lock().await;
        if let Some(entry) = subs.get(task_id) {
            let mut lock = entry.handle.lock().await;
            lock.set_subscribe_to_output(events_to_subscribe.contains(&SubscribeEventType::Output));
            return Ok(Some(lock.into_gate_guard()));
        }
        let subscriber = subscriber_builder()?;
        let guard = subscriber.sending_guard().await;
        self.spawn_subscriber_impl(subs, *task_id, subscriber)?;
        Ok(Some(guard))
    }

    pub(in crate::application) async fn stop_subscriber(
        &self,
        task_id: &TaskId,
    ) -> Result<(), ApplicationError> {
        let mut completion_receiver = {
            let subs = self.subs.lock().await;
            match subs.get(task_id) {
                Some(h) => {
                    h.handle.stop();
                    h.completion_receiver.clone()
                }
                None => return Err(TaskError::NotFound.into()),
            }
        };
        let _ = completion_receiver.changed().await;
        Ok(())
    }

    pub(in crate::application) async fn subscriber_sending_gate_guard(
        &self,
        task_id: &TaskId,
    ) -> Option<SubscriberSendingGateGuard> {
        match self.subs.lock().await.get(task_id) {
            Some(entry) => Some(entry.handle.gate_guard().await),
            None => None,
        }
    }

    /// Spawns subscriber with locked subs
    fn spawn_subscriber_impl(
        &self,
        mut subs: tokio::sync::MutexGuard<'_, HashMap<TaskId, SubscriberEntry>>,
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
                    let exit_event = subscriber.run().await;
                    assert!(
                        subs.lock().await.remove(&task_id).is_some(),
                        "Missing subscriber handle for {task_id}"
                    );
                    // Sending notification only after removing Subscriber from subs so it is not available after exit notification is sent
                    if let Some(e) = exit_event {
                        e.send().await;
                    }
                    let _ = completion_sender.send(true);
                }
            })
            .map_err(|_| ApplicationError::Shutdown)
    }
}
