use std::marker::PhantomData;

use tokio::sync::watch;

#[derive(Debug)]
#[must_use]
pub struct GateGuard<Tag> {
    sender: watch::Sender<()>,
    tag_: PhantomData<Tag>,
}

impl<Tag> GateGuard<Tag> {
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

pub fn gate<Tag>() -> (Gate<Tag>, GateGuard<Tag>) {
    let (sender, receiver) = watch::channel(());
    (
        Gate {
            receiver,
            tag_: Default::default(),
        },
        GateGuard {
            sender,
            tag_: Default::default(),
        },
    )
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn dropping_guard_unlocks_gate() {
        struct Tag;
        let (mut gate, guard) = gate::<Tag>();

        let mut handle = tokio_test::task::spawn(async move {
            gate.wait().await;
        });
        assert!(handle.poll().is_pending());
        drop(guard);
        assert!(handle.is_woken());
        assert!(handle.poll().is_ready());
    }
}
