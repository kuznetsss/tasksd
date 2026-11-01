use std::marker::PhantomData;

use tokio::sync::watch;

#[derive(Debug)]
#[must_use]
pub struct GateLock<Tag> {
    sender: watch::Sender<()>,
    tag_: PhantomData<Tag>,
}

impl<Tag> GateLock<Tag> {
    pub fn gate(&self) -> Gate<Tag> {
        Gate {
            receiver: self.sender.subscribe(),
            tag_: Default::default(),
        }
    }
}

#[derive(Debug)]
pub struct Gate<Tag> {
    receiver: watch::Receiver<()>,
    tag_: PhantomData<Tag>,
}

impl<Tag> Gate<Tag> {
    pub async fn wait(&mut self) {
        let _ = self.receiver.changed().await;
    }
}

pub(in crate::tasks) fn gate<Tag>() -> (Gate<Tag>, GateLock<Tag>) {
    let (sender, receiver) = watch::channel(());
    (
        Gate {
            receiver,
            tag_: Default::default(),
        },
        GateLock {
            sender,
            tag_: Default::default(),
        },
    )
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    struct Tag;

    #[tokio::test]
    async fn dropping_lock_unlock_gate() {
        #[derive(Debug, PartialEq, Eq)]
        enum Event {
            Drop,
            Unlock,
        }
        let events = Arc::new(Mutex::new(Vec::new()));

        let (mut gate, lock) = gate::<Tag>();

        let handle = tokio::spawn({
            let events = events.clone();
            async move {
                gate.wait().await;
                events.lock().unwrap().push(Event::Unlock);
            }
        });
        tokio::task::yield_now().await;
        drop(lock);
        events.lock().unwrap().push(Event::Drop);
        handle.await.unwrap();
        assert_eq!(*events.lock().unwrap(), [Event::Drop, Event::Unlock]);
    }
}
