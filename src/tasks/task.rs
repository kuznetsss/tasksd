use std::{env::current_dir, path::PathBuf, process::ExitStatus, sync::Arc};

use anyhow::Result;
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::{Child, Command},
    sync::{broadcast, watch},
    task::{JoinHandle, JoinSet},
};
use tokio_util::{bytes::Buf, sync::CancellationToken};
use tracing::warn;

use crate::tasks::pty::{PtyReadPart, PtyWritePart, create_pty_pair};

pub struct Task {
    info: Arc<TaskInfo>,
    stdin: Arc<PtyWritePart>,
    on_exit_rx: watch::Receiver<Option<ExitStatus>>,
    stdout_rx: broadcast::Receiver<Arc<String>>,
    cancel: CancellationToken,
    // TODO: add output buffer
    related_tasks: Option<JoinSet<()>>,
}

pub struct TaskInfo {
    pub executable: String,
    pub args: Vec<String>,
    pub working_dir: PathBuf,
    pub pid: Option<u32>,
}

impl Task {
    const CHANNEL_CAPACITY: usize = 16;
    pub fn new(
        executable: String,
        args: Vec<String>,
        working_dir: Option<PathBuf>,
    ) -> Result<Self> {
        let working_dir = working_dir.unwrap_or(current_dir()?);
        let (pty, child_pty) = create_pty_pair()?;
        // Using unsafe because pre_exec() is not safe since it is running in a process after fork
        let child = unsafe {
            Command::new(&executable)
                .args(&args)
                .stdin(child_pty.try_clone()?)
                .stdout(child_pty.try_clone()?)
                .stderr(child_pty.try_clone()?)
                .current_dir(&working_dir)
                .pre_exec(move || {
                    rustix::process::setsid()?;
                    rustix::process::ioctl_tiocsctty(&child_pty)?;
                    Ok(())
                })
                .spawn()?
        };
        let pid = child.id();
        let cancel = CancellationToken::new();
        let (stdout, stdin) = pty.into_split()?;
        let (stdout_tx, stdout_rx) = broadcast::channel(Self::CHANNEL_CAPACITY);

        let mut related_tasks = JoinSet::new();
        let on_exit_rx = Self::spawn_waiting_for_finish(&mut related_tasks, child);

        let mut s = Self {
            info: Arc::new(TaskInfo {
                executable,
                args,
                working_dir,
                pid,
            }),
            stdin: Arc::new(stdin),
            on_exit_rx,
            stdout_rx,
            cancel: cancel.clone(),
            related_tasks: Some(JoinSet::new()),
        };
        s.spawn_stdout_reading(stdout, stdout_tx, cancel);
        // Spawning a task to throw away input
        // to not block the process while there is no output subscribers.
        // This should write to the output buffer after it is added.
        s.on_output(|_| {});
        Ok(s)
    }

    pub fn info(&self) -> Arc<TaskInfo> {
        Arc::clone(&self.info)
    }

    pub fn stdin(&self) -> Arc<PtyWritePart> {
        Arc::clone(&self.stdin)
    }

    pub fn on_output<F>(&mut self, f: F) -> tokio::task::AbortHandle
    where
        F: Fn(Arc<String>) + Send + 'static,
    {
        let mut stdout_rx = self.stdout_rx.resubscribe();
        let cancel = self.cancel.child_token();
        self.related_tasks
            .as_mut()
            .expect("on_output() called after finish()")
            .spawn(async move {
                while let Some(Ok(l)) = cancel.run_until_cancelled(stdout_rx.recv()).await {
                    f(l);
                }
            })
    }

    pub fn on_exit<F>(&mut self, f: F)
    where
        F: FnOnce(ExitStatus),
    {
        todo!()
    }

    pub fn send_signal(&self, signal: rustix::process::Signal) -> Result<()> {
        let Some(pid) = self.info.pid else {
            anyhow::bail!("No pid for the task. Maybe it failed to start");
        };
        if self.on_exit_rx.has_changed()? {
            anyhow::bail!("The task has already exited");
        }
        let pid =
            rustix::process::Pid::from_raw(pid.try_into()?).expect("Pid should be valid here");
        rustix::process::kill_process(pid, signal).map_err(Into::into)
    }

    pub async fn finish(&mut self) {
        if self.related_tasks.is_some() {
            self.related_tasks.take().unwrap().join_all().await;
        }
    }

    fn spawn_stdout_reading(
        &mut self,
        stdout: PtyReadPart,
        stdout_tx: broadcast::Sender<Arc<String>>,
        cancel: CancellationToken,
    ) {
        self.related_tasks
            .as_mut()
            .expect("spawn_stdout_reading() called after finish()")
            .spawn(async move {
                let mut stdout = BufReader::new(stdout);
                let mut buf = String::new();
                while let Some(Ok(read_bytes)) =
                    cancel.run_until_cancelled(stdout.read_line(&mut buf)).await
                {
                    if read_bytes == 0 {
                        // EOF
                        return;
                    }
                    let line = Arc::new(buf);
                    if stdout_tx.send(line).is_err() {
                        return;
                    }
                    buf = String::new();
                }
            });
    }

    fn spawn_waiting_for_finish(
        related_tasks: &mut JoinSet<()>,
        mut child: Child,
    ) -> watch::Receiver<Option<ExitStatus>> {
        let (tx, rx) = watch::channel(None);
        related_tasks.spawn(async move {
            match child.wait().await {
                Ok(exit_code) => tx
                    .send(Some(exit_code))
                    .expect("At least one receiver should be alive"),
                Err(e) => warn!("Error waiting for child process {e}"),
            }
        });
        rx
    }
}

impl Drop for Task {
    fn drop(&mut self) {
        assert!(
            self.related_tasks.is_none(),
            "task is dropped without calling finish()"
        );
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use super::*;

    #[tokio::test]
    async fn try_task() {
        let msg = "hello from pty";
        let mut t = Task::new("echo".to_string(), vec![msg.to_string()], None).unwrap();
        let called = Arc::new(AtomicBool::new(false));
        t.on_output({
            let called = called.clone();
            move |s| {
                assert_eq!(s.as_ref(), &format!("{msg}\r\n"));
                called
                    .as_ref()
                    .store(true, std::sync::atomic::Ordering::Relaxed);
            }
        });

        t.finish().await;
        assert!(called.as_ref().load(std::sync::atomic::Ordering::Relaxed))
    }
}
