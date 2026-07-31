use tokio::sync::watch;
use tracing::info;

#[derive(Debug)]
pub struct First {
    tx: watch::Sender<bool>,
    rx: watch::Receiver<bool>,
    ctrl_c_count: usize,
}

#[derive(Debug)]
pub struct Second {
    ctrl_c_count: usize,
}

#[derive(Debug)]
pub struct ShutdownHandler<State> {
    state: State,
}

#[derive(Debug, Clone)]
pub struct ShutdownTrigger {
    tx: watch::Sender<bool>,
}

impl ShutdownTrigger {
    pub fn call_shutdown(&self) {
        // Already shutting down if failed
        let _ = self.tx.send(true);
    }
}

impl ShutdownHandler<First> {
    pub fn new() -> Self {
        let (tx, rx) = watch::channel(false);
        Self {
            state: First {
                tx,
                rx,
                ctrl_c_count: 0,
            },
        }
    }

    pub fn trigger(&self) -> ShutdownTrigger {
        ShutdownTrigger {
            tx: self.state.tx.clone(),
        }
    }

    pub async fn wait(self) -> ShutdownHandler<Second> {
        let First {
            tx: _tx,
            mut rx,
            mut ctrl_c_count,
        } = self.state;
        tokio::select! {
            _ = wait_for_ctrl_c() => {
                info!("Got Ctrl-C, initiating shutdown...");
                ctrl_c_count += 1;
            },
            r = rx.wait_for(|&v| v) => {
                info!("Received internal shutdown signal, initiating shutdown...");
                r.expect("At least one tx is alive");
            }
        }

        ShutdownHandler::<Second> {
            state: Second { ctrl_c_count },
        }
    }
}

impl Default for ShutdownHandler<First> {
    fn default() -> Self {
        Self::new()
    }
}

impl ShutdownHandler<Second> {
    pub async fn wait_force(mut self) {
        while self.state.ctrl_c_count < 2 {
            wait_for_ctrl_c().await;
            self.state.ctrl_c_count += 1;
            if self.state.ctrl_c_count == 1 {
                info!("Got Ctrl-C, but shutdown is already in progress, ignoring");
            }
        }
        info!("Got second Ctrl-C, force exit");
    }
}

async fn wait_for_ctrl_c() {
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for Ctrl-C");
}
