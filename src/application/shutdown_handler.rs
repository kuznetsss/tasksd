use tokio::{
    signal::unix::{Signal, SignalKind},
    sync::watch,
};
use tracing::info;

#[derive(Debug)]
pub struct Armed {
    tx: watch::Sender<bool>,
    rx: watch::Receiver<bool>,
    had_ctrl_c: bool,
    sigint: Signal,
}

#[derive(Debug)]
pub struct ShuttingDown {
    sigint: Signal,
    had_ctrl_c: bool,
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
    pub fn request_shutdown(&self) {
        // Already shutting down if failed
        let _ = self.tx.send(true);
    }
}

impl ShutdownHandler<Armed> {
    pub fn new() -> Self {
        let (tx, rx) = watch::channel(false);
        Self {
            state: Armed {
                tx,
                rx,
                sigint: tokio::signal::unix::signal(SignalKind::interrupt())
                    .expect("Failed to listen for Ctrl-C"),
                had_ctrl_c: false,
            },
        }
    }

    pub fn trigger(&self) -> ShutdownTrigger {
        ShutdownTrigger {
            tx: self.state.tx.clone(),
        }
    }

    pub async fn wait_for_shutdown(self) -> ShutdownHandler<ShuttingDown> {
        let Armed {
            tx: _tx,
            mut rx,
            mut sigint,
            mut had_ctrl_c,
        } = self.state;
        tokio::select! {
            _ = sigint.recv() => {
                info!("Got Ctrl-C, initiating shutdown...");
                had_ctrl_c = true;
            },
            r = rx.wait_for(|&v| v) => {
                info!("Received internal shutdown signal, initiating shutdown...");
                r.expect("At least one tx is alive");
            }
        }

        ShutdownHandler::<ShuttingDown> {
            state: ShuttingDown { sigint, had_ctrl_c },
        }
    }
}

impl Default for ShutdownHandler<Armed> {
    fn default() -> Self {
        Self::new()
    }
}

impl ShutdownHandler<ShuttingDown> {
    pub async fn wait_for_force_exit(mut self) {
        if !self.state.had_ctrl_c {
            self.state.sigint.recv().await;
            info!("Got Ctrl-C, but shutdown is already in progress, ignoring");
        }
        self.state.sigint.recv().await;
        info!("Got second Ctrl-C, force exit");
    }
}
