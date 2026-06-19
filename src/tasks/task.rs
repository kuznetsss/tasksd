use std::{process::ExitStatus, sync::Arc};

use anyhow::Result;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, Command},
    sync::{broadcast, watch},
    task::AbortHandle,
};
use tracing::{Instrument, Span, info, info_span, warn};

use crate::tasks::{
    events::{TaskEvents, TaskExitCallback, TaskOutputCallback},
    finished_task::FinishedTask,
    info::TaskInfo,
    output_buffer::OutputBuffer,
    pty::{PtyChild, PtyReadPart, PtyWritePart, create_pty_pair},
    pty_reader::PtyReader,
    senders::TaskSenders,
    task_error::TaskError,
    tracker::{PanicHandler, WrappedTaskTracker},
};

/// Representation of a running child process.
/// All the output of the child process is captured into [`OutputBuffer`].
/// Subscription to output is lossy which means if a subscriber is too slow or the child process
/// produces too much output, subscribers may miss some output lines.
#[derive(Debug)]
pub struct Task {
    info: Arc<TaskInfo>,
    stdin: tokio::sync::Mutex<PtyWritePart>,
    pid: u32,
    events: TaskEvents,
    internal_tasks: WrappedTaskTracker,
    output_buffer: Arc<OutputBuffer>,
}

impl Task {
    pub(in crate::tasks) fn new(
        info: TaskInfo,
        senders: TaskSenders,
        events: TaskEvents,
        output_buffer_capacity: usize,
    ) -> Result<Self, TaskError> {
        let span = info_span!( "task",
                    executable = info.executable,
                    args = ?info.args);
        let _entered = span.enter();

        let (pty, child_pty) = create_pty_pair().map_err(TaskError::pty_creation_error)?;
        let (pty_read, pty_write) = pty
            .into_split()
            .map_err(TaskError::pty_creation_error)
            .inspect_err(|e| warn!("Error splitting pty: {e}"))?;

        let info = Arc::new(info);
        let internal_tasks = WrappedTaskTracker::new(PanicHandler::new_aborting());
        let output_buffer = Arc::new(OutputBuffer::new(output_buffer_capacity));
        Self::spawn_output_reading(
            &internal_tasks,
            pty_read,
            &child_pty,
            &senders.on_exit_tx,
            senders.output_tx,
            output_buffer.clone(),
            span.clone(),
        )?;

        let child = Self::spawn_child_process(&info, child_pty)
            .inspect_err(|e| warn!("Error spawning child process: {e}"))?;
        let pid = child.id().expect("pid");
        info!(pid, "Spawned a process");
        Self::spawn_waiting_for_exit(&internal_tasks, senders.on_exit_tx, child, span.clone());

        let task = Self {
            info,
            stdin: tokio::sync::Mutex::new(pty_write),
            pid,
            events,
            internal_tasks,
            output_buffer,
        };

        Ok(task)
    }

    pub fn info(&self) -> Arc<TaskInfo> {
        Arc::clone(&self.info)
    }

    pub async fn write_to_stdin(&self, msg: &[u8]) -> Result<(), TaskError> {
        if self.events.has_exited() {
            Err(TaskError::AlreadyExited)
        } else {
            self.stdin
                .lock()
                .await
                .write_all(msg)
                .await
                .map_err(TaskError::write_error)
        }
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

    pub fn output_buffer(&self) -> &Arc<OutputBuffer> {
        &self.output_buffer
    }

    pub async fn wait(&self) {
        self.events.exit_status().await;
    }

    pub async fn join(&self) -> FinishedTask {
        self.internal_tasks.join().await;
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

    fn spawn_output_reading(
        internal_tasks: &WrappedTaskTracker,
        pty_read_part: PtyReadPart,
        child_pty: &PtyChild,
        on_exit_sender: &watch::Sender<Option<ExitStatus>>,
        stdout_tx: broadcast::Sender<Arc<String>>,
        output_buffer: Arc<OutputBuffer>,
        span: Span,
    ) -> Result<(), TaskError> {
        let child_pty = child_pty
            .try_clone()
            .map_err(TaskError::pty_creation_error)?;
        let mut on_exit_receiver = on_exit_sender.subscribe();
        let child_process_exit_future = Box::pin(async move {
            let _ = on_exit_receiver.changed().await;
        });
        let pty_reader = PtyReader::new(pty_read_part, child_pty, child_process_exit_future);
        internal_tasks
            .spawn({
                async move {
                    let mut stdout = BufReader::new(pty_reader);
                    loop {
                        let mut buf = String::new();
                        let read_bytes = stdout.read_line(&mut buf).await;
                        match read_bytes {
                            Ok(0) => {
                                // EOF
                                return;
                            }
                            Ok(_) => {}
                            Err(e) => {
                                warn!("Error reading output for the task: {e}");
                                return;
                            }
                        };
                        let line = Arc::new(buf);
                        output_buffer.insert_line(line.clone());
                        if let Err(e) = stdout_tx.send(line) {
                            warn!("Error sending output line to subscribers: {e}");
                        }
                    }
                }
                .instrument(span)
            })
            .expect("Internal tasks should not be joined yet");
        Ok(())
    }

    fn spawn_waiting_for_exit(
        internal_tasks: &WrappedTaskTracker,
        tx: watch::Sender<Option<ExitStatus>>,
        mut child: Child,
        span: Span,
    ) {
        internal_tasks
            .spawn(
                async move {
                    let exit_status = child
                        .wait()
                        .await
                        .expect("Child process should finish normally");
                    info!("Task has exited. {exit_status}");
                    tx.send(Some(exit_status))
                        .expect("At least one receiver should be alive");
                }
                .instrument(span),
            )
            .expect("Internal tasks should not be joined yet");
    }
}

impl Drop for Task {
    fn drop(&mut self) {
        if std::thread::panicking() {
            return;
        }
        assert!(
            self.internal_tasks.is_joined(),
            "Task is dropped without calling join()"
        );
    }
}

#[cfg(test)]
mod tests {
    use crate::tasks::senders::CHANNEL_CAPACITY;

    use super::*;
    use std::{
        env::current_dir, io::Write, os::unix::process::ExitStatusExt, path::PathBuf, str::FromStr,
        sync::Mutex,
    };

    const OUTPUT_BUFFER_CAPACITY: usize = CHANNEL_CAPACITY * 2;

    fn make_task(
        executable: impl Into<String>,
        args: &[&str],
        working_dir: impl Into<PathBuf>,
        on_output: impl TaskOutputCallback,
    ) -> Result<Task, TaskError> {
        let senders = TaskSenders::new();
        let events = TaskEvents::new(&senders);
        events.on_output(on_output).unwrap();
        let info = TaskInfo {
            executable: executable.into(),
            args: args.iter().map(|&s| String::from(s)).collect(),
            working_dir: working_dir.into(),
        };
        Task::new(info, senders, events, OUTPUT_BUFFER_CAPACITY)
    }

    fn noop_callback() -> impl TaskOutputCallback {
        |_| async { Ok(()) }
    }

    #[tokio::test]
    async fn new_non_existing_executable() {
        let err =
            make_task("non_existing", &[], current_dir().unwrap(), noop_callback()).unwrap_err();
        assert!(matches!(err, TaskError::StartingChildProcessError(_)));
    }

    #[tokio::test]
    async fn new_bad_args() {
        let err = make_task("ls", &["\0"], current_dir().unwrap(), noop_callback()).unwrap_err();
        assert!(matches!(err, TaskError::StartingChildProcessError(_)));
    }

    #[tokio::test]
    async fn new_invalid_directory() {
        let err = make_task(
            "ls",
            &["\0"],
            current_dir().unwrap().join("non_existing_123"),
            noop_callback(),
        )
        .unwrap_err();
        assert!(matches!(err, TaskError::StartingChildProcessError(_)));
    }

    #[tokio::test]
    async fn new_success() {
        let captured_output = Arc::new(Mutex::new(Vec::new()));
        let on_output = {
            let captured_output = captured_output.clone();
            move |o| {
                captured_output.lock().unwrap().push(o);
                async { Ok(()) }
            }
        };
        let msg = "test";
        let task = make_task("echo", &[msg], current_dir().unwrap(), on_output).unwrap();
        task.join().await;
        let captured_output = captured_output.lock().unwrap();
        assert_eq!(captured_output.len(), 1);
        assert_eq!(*captured_output[0], format!("{msg}\r\n"));
    }

    #[tokio::test]
    async fn info() {
        let executable = "ls";
        let args = ["-la"];
        let directory = PathBuf::from_str("/tmp").unwrap();
        let task = make_task(executable, &args, &directory, noop_callback()).unwrap();
        let info = task.info();
        assert_eq!(&info.executable, executable);
        assert_eq!(info.args, args);
        assert_eq!(info.working_dir, directory);
        task.join().await;
    }

    #[tokio::test]
    async fn write_to_stdin_success() {
        let captured_output = Arc::new(Mutex::new(Vec::new()));
        let on_output = {
            let captured_output = captured_output.clone();
            move |o| {
                captured_output.lock().unwrap().push(o);
                async { Ok(()) }
            }
        };
        let task = make_task("cat", &[], current_dir().unwrap(), on_output).unwrap();
        for m in ["one", "two\n", "three"] {
            task.write_to_stdin(m.as_bytes()).await.unwrap();
        }
        task.write_to_stdin(b"\x04\x04").await.unwrap(); // flush "three" then EOF
        task.join().await;
        let captured_output = captured_output.lock().unwrap();
        assert_eq!(captured_output.len(), 2);
        assert_eq!(*captured_output[0], "onetwo\r\n");
        assert_eq!(*captured_output[1], "three");
    }

    #[tokio::test]
    async fn write_to_stdin_error() {
        let task = make_task("ls", &[], current_dir().unwrap(), noop_callback()).unwrap();
        task.wait().await;
        let err = task
            .write_to_stdin("some input".as_bytes())
            .await
            .unwrap_err();
        assert!(matches!(err, TaskError::AlreadyExited));
        task.join().await;
    }

    #[tokio::test]
    async fn on_output_after_exit_returns_error() {
        let task = make_task("ls", &[], current_dir().unwrap(), noop_callback()).unwrap();
        task.wait().await;
        let err = task.on_output(noop_callback()).unwrap_err();
        assert!(matches!(err, TaskError::AlreadyExited));
        task.join().await;
    }

    #[tokio::test]
    async fn output_buffer_captures_output() {
        let task = make_task(
            "echo",
            &["-n", "line1\nline2"],
            current_dir().unwrap(),
            noop_callback(),
        )
        .unwrap();
        task.join().await;
        let range = task.output_buffer().line_range();
        assert_eq!(range, 0..2);
        assert_eq!(
            task.output_buffer()
                .get_line_range(range)
                .into_iter()
                .map(|s| String::clone(s.as_ref()))
                .collect::<Vec<_>>(),
            ["line1\r\n", "line2"]
        );
    }

    #[tokio::test]
    async fn output_buffer_capacity() {
        let task = make_task("ls", &[], current_dir().unwrap(), noop_callback()).unwrap();
        assert_eq!(task.output_buffer().capacity(), OUTPUT_BUFFER_CAPACITY);
        task.join().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn output_buffer_captures_all_the_output() {
        const REPEAT_COUNT: usize = CHANNEL_CAPACITY * 2;
        let mut tmp_file = tempfile::NamedTempFile::new().unwrap();
        tmp_file
            .write_all("some text\n".repeat(REPEAT_COUNT).as_bytes())
            .unwrap();
        tmp_file.flush().unwrap();
        let captured_output = Arc::new(Mutex::new(Vec::new()));
        let task = make_task(
            "cat",
            &[tmp_file.path().to_str().unwrap()],
            current_dir().unwrap(),
            {
                let captured_output = captured_output.clone();
                move |o: Arc<String>| {
                    captured_output.lock().unwrap().push(o);
                    async { Ok(()) }
                }
            },
        )
        .unwrap();
        let output_buffer = task.output_buffer().clone();
        task.join().await;
        assert!(captured_output.lock().unwrap().len() <= REPEAT_COUNT);
        assert_eq!(output_buffer.line_range().end, REPEAT_COUNT);
    }

    #[tokio::test]
    async fn send_signal_success() {
        let task = make_task("cat", &[], current_dir().unwrap(), noop_callback()).unwrap();
        tokio::task::yield_now().await;
        task.send_signal(rustix::process::Signal::TERM).unwrap();
        let finished_task = task.join().await;
        assert_eq!(
            finished_task.exit_status.signal().unwrap(),
            rustix::process::Signal::TERM.as_raw()
        );
    }

    #[tokio::test]
    async fn send_signal_error() {
        let task = make_task("ls", &[], current_dir().unwrap(), noop_callback()).unwrap();
        task.wait().await;
        let err = task.send_signal(rustix::process::Signal::TERM).unwrap_err();
        assert!(matches!(err, TaskError::AlreadyExited));
        task.join().await;
    }

    #[tokio::test]
    async fn join_result() {
        let executable = "ls";
        let args = ["-la"];
        let dir = current_dir().unwrap().join("../");
        let task = make_task(executable, &args, &dir, noop_callback()).unwrap();
        let finished_task = task.join().await;
        assert_eq!(finished_task.info.executable, executable);
        assert_eq!(finished_task.info.args, &args);
        assert_eq!(finished_task.info.working_dir, dir);
        assert_eq!(finished_task.exit_status.code().unwrap(), 0);
    }

    #[tokio::test]
    #[should_panic(expected = "dropped without calling join()")]
    async fn panic_if_dropped_without_join() {
        let task = make_task("ls", &[], current_dir().unwrap(), noop_callback()).unwrap();
        task.wait().await;
    }

    #[tokio::test]
    #[should_panic(expected = "custom panic")]
    async fn doesnt_double_panic_if_already_panicking() {
        let task = make_task("ls", &[], current_dir().unwrap(), noop_callback()).unwrap();
        task.wait().await;
        panic!("custom panic");
    }

    #[tokio::test]
    async fn calling_join_twice() {
        let task = make_task("ls", &[], current_dir().unwrap(), noop_callback()).unwrap();
        task.join().await;
        task.join().await;
    }
}
