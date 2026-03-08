use std::{env::current_dir, path::PathBuf, process::ExitStatus, sync::Arc};

use anyhow::Result;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, Command},
    sync::{Mutex, broadcast, watch},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::tasks::pty::{PtyReadPart, PtyWritePart, create_pty_pair};

#[derive(Debug)]
pub struct Task {
    info: Arc<TaskInfo>,

    // Mutex could be avoided if AsyncWrite is implemented for &PtyWritePart (reference is important).
    // But without mutex multiple parallel inputs may be mixed together.
    stdin: Arc<Mutex<PtyWritePart>>,

    on_exit_rx: watch::Receiver<Option<ExitStatus>>,
    stdout_rx: broadcast::Receiver<Arc<String>>,
    cancel: CancellationToken,
    // TODO: add output buffer
    related_tasks: Option<JoinSet<()>>,
}

#[derive(Debug)]
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
            stdin: Arc::new(Mutex::new(stdin)),
            on_exit_rx,
            stdout_rx,
            cancel: cancel.clone(),
            related_tasks: Some(related_tasks),
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

    pub fn stdin(&self) -> Arc<Mutex<PtyWritePart>> {
        Arc::clone(&self.stdin)
    }

    // TODO: maybe wrap related_tasks into Mutex and switch to &self everywhere
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
                // stdout_rx.recv() my return Lagged error, but it is silenced here
                while let Some(Ok(l)) = cancel.run_until_cancelled(stdout_rx.recv()).await {
                    f(l);
                }
            })
    }

    pub fn on_exit<F>(&mut self, f: F) -> Option<tokio::task::AbortHandle>
    where
        F: FnOnce(ExitStatus) + Send + 'static,
    {
        if self
            .on_exit_rx
            .has_changed()
            .expect("On exit channel is alive")
        {
            f(self.on_exit_rx.borrow().unwrap());
            return None;
        }
        let mut on_exit_rx = self.on_exit_rx.clone();
        let handle = self.related_tasks.as_mut().unwrap().spawn(async move {
            on_exit_rx.changed().await.expect("Changed shouldn't fail");
            // There will always be an exit code
            f(on_exit_rx.borrow_and_update().unwrap());
        });
        Some(handle)
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
        rustix::process::kill_process(pid, signal).map_err(|e| {
            if e == rustix::io::Errno::SRCH {
                anyhow::anyhow!("The task has already exited")
            } else {
                e.into()
            }
        })
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
            .spawn({
                let task_info = Arc::clone(&self.info);
                async move {
                    let mut stdout = BufReader::new(stdout);
                    let mut buf = String::new();
                    while let Some(read_bytes) =
                        cancel.run_until_cancelled(stdout.read_line(&mut buf)).await
                    {
                        let read_bytes = match read_bytes {
                            Ok(r) => r,
                            Err(e) => {
                                warn!(
                                    "Error reading from stdout for the task {:?}: {e}",
                                    task_info
                                );
                                return;
                            }
                        };
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
                }
            });
    }

    fn spawn_waiting_for_finish(
        related_tasks: &mut JoinSet<()>,
        mut child: Child,
    ) -> watch::Receiver<Option<ExitStatus>> {
        let (tx, rx) = watch::channel(None);
        related_tasks.spawn(async move {
            let exit_code = child
                .wait()
                .await
                .expect("Child process should finish normally");
            tx.send(Some(exit_code))
                .expect("At least one receiver should be alive");
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
    use std::{str::FromStr, sync::Mutex};

    use rustix::process::Signal;
    use tokio::io::AsyncWriteExt;

    use super::*;

    #[tokio::test]
    async fn task_new_non_existing_working_directory() {
        let t = Task::new(
            "ls".to_string(),
            Vec::new(),
            Some(PathBuf::from_str("./non_existing").unwrap()),
        );
        assert!(t.is_err());
        let err_str = t.unwrap_err().to_string();
        assert!(err_str.contains("No such file or directory"));
    }

    #[tokio::test]
    async fn task_new_invalid_executable() {
        let t = Task::new("non_existing".to_string(), Vec::new(), None);
        assert!(t.is_err());
        let err_str = t.unwrap_err().to_string();
        assert!(err_str.contains("No such file or directory"));
    }

    #[tokio::test]
    async fn task_info() {
        let executable = "ls";
        let args = vec!["-la".to_string()];
        let mut t = Task::new(executable.to_string(), args.clone(), None).unwrap();
        let info = t.info();
        assert_eq!(&info.executable, &executable);
        assert_eq!(&info.args, &args);
        assert_eq!(&info.working_dir, &current_dir().unwrap());
        assert!(info.pid.unwrap() > 0);
        t.finish().await;
    }

    #[tokio::test]
    async fn task_on_output() {
        let mut t = Task::new(
            "echo".to_string(),
            vec!["-ne".to_string(), "some\nmulti\nline".to_string()],
            None,
        )
        .unwrap();
        let output_lines = Arc::new(Mutex::new(Vec::new()));
        t.on_output({
            let output_lines = Arc::clone(&output_lines);
            move |line| {
                output_lines.lock().unwrap().push(line);
            }
        });
        t.finish().await;
        assert_eq!(output_lines.lock().unwrap().len(), 3);
        assert_eq!(output_lines.lock().unwrap()[0].as_str(), "some\r\n");
        assert_eq!(output_lines.lock().unwrap()[1].as_str(), "multi\r\n");
        assert_eq!(output_lines.lock().unwrap()[2].as_str(), "line");
    }

    #[tokio::test]
    async fn task_stdin() {
        let mut t = Task::new("cat".to_string(), Vec::new(), None).unwrap();
        let output_lines = Arc::new(Mutex::new(Vec::new()));
        t.on_output({
            let output_lines = Arc::clone(&output_lines);
            move |line| {
                output_lines.lock().unwrap().push(line);
            }
        });

        let stdin = t.stdin();
        stdin
            .lock()
            .await
            .write_all("some\n".as_bytes())
            .await
            .unwrap();
        stdin
            .lock()
            .await
            .write_all("multi\n".as_bytes())
            .await
            .unwrap();
        stdin
            .lock()
            .await
            .write_all("line".as_bytes())
            .await
            .unwrap();
        stdin.lock().await.write_all(&[0x04]).await.unwrap(); // EOF symbol - first flushes buffer
        stdin.lock().await.write_all(&[0x04]).await.unwrap(); // EOF symbol - second closes cat
        t.finish().await;
        assert_eq!(output_lines.lock().unwrap().len(), 3);
        assert_eq!(output_lines.lock().unwrap()[0].as_str(), "some\r\n");
        assert_eq!(output_lines.lock().unwrap()[1].as_str(), "multi\r\n");
        assert_eq!(output_lines.lock().unwrap()[2].as_str(), "line");
    }

    #[tokio::test]
    async fn task_on_exit() {
        let mut t = Task::new("echo".to_string(), Vec::new(), None).unwrap();
        let status = Arc::new(Mutex::new(Vec::new()));
        t.on_exit({
            let status = Arc::clone(&status);
            move |s| {
                status.lock().unwrap().push(s);
            }
        });
        t.finish().await;
        let status = status.lock().unwrap();
        assert_eq!(status.len(), 1);
        assert_eq!(status[0].code().unwrap(), 0);
    }

    #[tokio::test]
    async fn task_send_signal() {
        let start_time = std::time::Instant::now();
        let mut t = Task::new("sleep".to_string(), vec!["5".to_string()], None).unwrap();
        t.send_signal(Signal::TERM).unwrap();
        t.finish().await;
        assert!(
            std::time::Instant::now()
                .duration_since(start_time)
                .as_millis()
                < 5000
        );
    }
}
