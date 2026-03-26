use std::{
    env::current_dir,
    path::PathBuf,
    process::ExitStatus,
    sync::{Arc, Mutex, atomic::AtomicBool},
};

use anyhow::Result;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, Command},
    sync::{broadcast, watch},
    task::{AbortHandle, JoinSet},
};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::tasks::{
    common::{TaskEvents, TaskExitCallback, TaskInfo, TaskOutputCallback, TaskSenders},
    finished_task::FinishedTask,
    pty::{PtyChild, PtyReadPart, PtyWritePart, create_pty_pair},
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
        callbacks: TaskEvents,
    ) -> Result<Self, TaskError> {
        let (pty, child_pty) = create_pty_pair().map_err(TaskError::pty_creation_error)?;

        let info = Arc::new(info);

        let (stdout, stdin) = pty.into_split().map_err(TaskError::pty_creation_error)?;

        let mut internal_tasks = JoinSet::new();
        Self::spawn_stdout_reading(
            &mut internal_tasks,
            info.clone(),
            stdout,
            senders.stdout_tx,
            callbacks.cancel.clone(),
        );

        let child = Self::spawn_child_process(&info, child_pty)?;
        let pid = child.id().expect("pid");
        Self::spawn_waiting_for_exit(&mut internal_tasks, senders.on_exit_tx, child);

        let task = Self {
            info,
            stdin: tokio::sync::Mutex::new(stdin),
            pid,
            events: callbacks,
            internal_tasks: Some(internal_tasks),
        };

        // Spawning a task to throw away input
        // to not block the process while there is no output subscribers.
        // This should write to the output buffer after it is added.
        task.on_output(|_| {});
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
        related_tasks: &mut JoinSet<()>,
        task_info: Arc<TaskInfo>,
        stdout: PtyReadPart,
        stdout_tx: broadcast::Sender<Arc<String>>,
        cancel: CancellationToken,
    ) {
        related_tasks.spawn({
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

    fn spawn_waiting_for_exit(
        related_tasks: &mut JoinSet<()>,
        tx: watch::Sender<Option<ExitStatus>>,
        mut child: Child,
    ) {
        related_tasks.spawn(async move {
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

/*
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
*/
