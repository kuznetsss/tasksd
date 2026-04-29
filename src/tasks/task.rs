use std::{process::ExitStatus, sync::Arc};

use anyhow::Result;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, Command},
    sync::{broadcast, watch},
    task::{AbortHandle, JoinSet},
};
use tracing::warn;

use crate::tasks::{
    events::{TaskEvents, TaskExitCallback, TaskOutputCallback},
    finished_task::FinishedTask,
    info::TaskInfo,
    pty::{PtyChild, PtyReadPart, PtyWritePart, create_pty_pair},
    senders::TaskSenders,
    task_error::TaskError,
};

#[derive(Debug)]
pub struct Task {
    info: Arc<TaskInfo>,
    stdin: tokio::sync::Mutex<PtyWritePart>,
    pid: u32,
    events: TaskEvents,
    internal_tasks: Option<JoinSet<()>>,
}

impl Task {
    pub(in crate::tasks) fn new(
        info: TaskInfo,
        senders: TaskSenders,
        events: TaskEvents,
    ) -> Result<Self, TaskError> {
        let (pty, child_pty) = create_pty_pair().map_err(TaskError::pty_creation_error)?;
        let (stdout, stdin) = pty.into_split().map_err(TaskError::pty_creation_error)?;

        let info = Arc::new(info);
        let mut internal_tasks = JoinSet::new();
        Self::spawn_stdout_reading(&mut internal_tasks, info.clone(), stdout, senders.stdout_tx);

        // Spawning a task to throw away stdput
        // to not block the process while there is no output subscribers.
        // This should write to the output buffer after it is added.
        events.on_output(|_| {}).expect("Task shouldn't exit yet");

        let child = Self::spawn_child_process(&info, child_pty)?;
        let pid = child.id().expect("pid");
        Self::spawn_waiting_for_exit(&mut internal_tasks, senders.on_exit_tx, child);

        let task = Self {
            info,
            stdin: tokio::sync::Mutex::new(stdin),
            pid,
            events,
            internal_tasks: Some(internal_tasks),
        };

        Ok(task)
    }

    pub fn info(&self) -> Arc<TaskInfo> {
        Arc::clone(&self.info)
    }

    pub async fn write_to_stdin(&self, msg: &[u8]) -> Result<(), TaskError> {
        self.stdin
            .lock()
            .await
            .write_all(msg)
            .await
            .map_err(TaskError::write_error)
    }

    pub fn on_output<F>(&self, f: F) -> Result<AbortHandle, TaskError>
    where
        F: TaskOutputCallback,
    {
        self.events.on_output(f)
    }

    pub fn on_exit<F>(&self, f: F) -> Result<AbortHandle, TaskError>
    where
        F: TaskExitCallback,
    {
        self.events.on_exit(f)
    }

    pub fn send_signal(&self, signal: rustix::process::Signal) -> Result<(), TaskError> {
        let pid: rustix::process::RawPid =
            self.pid.try_into().map_err(TaskError::send_signal_error)?;
        let pid = rustix::process::Pid::from_raw(pid).expect("Pid should be valid here");
        rustix::process::kill_process(pid, signal).map_err(|e| {
            if e == rustix::io::Errno::SRCH {
                TaskError::AlreadyExited
            } else {
                TaskError::send_signal_error(e)
            }
        })
    }

    pub async fn wait(&self) {
        self.events.exit_status().await;
    }

    pub async fn finish(&mut self) -> FinishedTask {
        assert!(
            self.internal_tasks.is_some(),
            "Task::finish() called more than one time"
        );
        self.internal_tasks.take().unwrap().join_all().await;
        self.events.join_all().await;
        FinishedTask {
            info: self.info.clone(),
            exit_status: self.events.exit_status().await,
        }
    }

    fn spawn_child_process(info: &TaskInfo, child_pty: PtyChild) -> Result<Child, TaskError> {
        // Using unsafe because pre_exec() is not safe since it is running in a process after fork
        let child = unsafe {
            Command::new(&info.executable)
                .args(&info.args)
                .stdin(
                    child_pty
                        .try_clone()
                        .map_err(TaskError::pty_creation_error)?,
                )
                .stdout(
                    child_pty
                        .try_clone()
                        .map_err(TaskError::pty_creation_error)?,
                )
                .stderr(
                    child_pty
                        .try_clone()
                        .map_err(TaskError::pty_creation_error)?,
                )
                .current_dir(&info.working_dir)
                .pre_exec(move || {
                    rustix::process::setsid()?;
                    rustix::process::ioctl_tiocsctty(&child_pty)?;
                    Ok(())
                })
                .spawn()
                .map_err(TaskError::starting_child_process_error)?
        };
        Ok(child)
    }

    fn spawn_stdout_reading(
        internal_tasks: &mut JoinSet<()>,
        task_info: Arc<TaskInfo>,
        stdout: PtyReadPart,
        stdout_tx: broadcast::Sender<Arc<String>>,
    ) {
        internal_tasks.spawn({
            async move {
                let mut stdout = BufReader::new(stdout);
                loop {
                    let mut buf = String::new();
                    let read_bytes = stdout.read_line(&mut buf).await;
                    match read_bytes {
                        Ok(r) if r == 0 => {
                            // EOF
                            return;
                        }
                        Ok(_) => {}
                        Err(e) => {
                            warn!(
                                "Error reading from stdout for the task {:?}: {e}",
                                task_info
                            );
                            return;
                        }
                    };
                    let line = Arc::new(buf);
                    if stdout_tx.send(line).is_err() {
                        return;
                    }
                }
            }
        });
    }

    fn spawn_waiting_for_exit(
        internal_tasks: &mut JoinSet<()>,
        tx: watch::Sender<Option<ExitStatus>>,
        mut child: Child,
    ) {
        internal_tasks.spawn(async move {
            let exit_status = child
                .wait()
                .await
                .expect("Child process should finish normally");
            tx.send(Some(exit_status))
                .expect("At least one receiver should be alive");
        });
    }
}

impl Drop for Task {
    fn drop(&mut self) {
        assert!(
            self.internal_tasks.is_none(),
            "Task is dropped without calling finish()"
        );
    }
}

#[cfg(test)]
mod tests {
    use std::{env::current_dir, path::Path};

    use super::*;

    fn make_task(executable: &str, args: &[&str], working_dir: &Path) -> Result<Task, TaskError> {
        let senders = TaskSenders::new();
        let events = TaskEvents::new(&senders);
        let info = TaskInfo {
            executable: executable.to_string(),
            args: args.iter().map(|&s| String::from(s)).collect(),
            working_dir: working_dir.to_path_buf(),
        };
        Task::new(info, senders, events)
    }

    #[tokio::test]
    async fn new_non_existing_executable() {
        let err = make_task("non_existing", &[], &current_dir().unwrap()).unwrap_err();
        assert!(matches!(err, TaskError::StartingChildProcessError(_)));
    }

    #[tokio::test]
    async fn new_bad_args() {
        let err = make_task("ls", &["\0"], &current_dir().unwrap()).unwrap_err();
        assert!(matches!(err, TaskError::StartingChildProcessError(_)));
    }

    #[tokio::test]
    async fn new_invalid_directory() {
        let err = make_task(
            "ls",
            &["\0"],
            &current_dir().unwrap().join("non_existing_123"),
        )
        .unwrap_err();
        assert!(matches!(err, TaskError::StartingChildProcessError(_)));
    }
}
