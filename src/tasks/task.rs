use std::{process::ExitStatus, sync::Arc};

use anyhow::Result;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, Command},
    sync::{broadcast, oneshot, watch},
};
use tracing::{Instrument, Span, info, info_span, warn};

use crate::tasks::{
    TaskEventsStream,
    finished_task::FinishedTask,
    info::TaskInfo,
    output_buffer::OutputBuffer,
    pty::{PtyChild, PtyWritePart, create_pty_pair},
    pty_reader::PtyReader,
    sender::{TaskEvent, TaskSender},
    task_error::TaskError,
    tracker::{PanicHandler, WrappedTaskTracker},
};

/// Gate of task event reading.
/// Dropping gate allows task to begin reading from PTY.
#[derive(Debug)]
#[must_use]
pub struct TaskReadingGate(oneshot::Sender<()>);

/// Representation of a running child process.
/// All the output of the child process is captured into [`OutputBuffer`].
/// Subscription to output is lossy which means if a subscriber is too slow or the child process
/// produces too much output, subscribers may miss some output lines.
/// There is a strong guarantee that on_exit notification is sent after
/// all the output notifications.
#[derive(Debug)]
pub struct Task {
    info: Arc<TaskInfo>,
    stdin: tokio::sync::Mutex<PtyWritePart>,
    pid: u32,
    exit_rx: watch::Receiver<Option<ExitStatus>>,
    internal_tasks: WrappedTaskTracker,
    output_buffer: Arc<OutputBuffer>,
    events_stream: TaskEventsStream,
}

impl Task {
    pub(in crate::tasks) fn new(
        info: TaskInfo,
        output_buffer_capacity: usize,
    ) -> Result<(Self, TaskReadingGate), TaskError> {
        let span = info_span!( "task",
                    executable = info.executable,
                    args = ?info.args);
        let _entered = span.enter();

        let sender = TaskSender::new();

        let (pty, child_pty) = create_pty_pair().map_err(TaskError::pty_creation_error)?;
        let (pty_read, pty_write) = pty
            .into_split()
            .map_err(TaskError::pty_creation_error)
            .inspect_err(|e| warn!("Error splitting pty: {e}"))?;

        let info = Arc::new(info);
        let internal_tasks = WrappedTaskTracker::new(PanicHandler::new_aborting());
        let output_buffer = Arc::new(OutputBuffer::new(output_buffer_capacity));

        let child_process_exit_future = Box::pin({
            let mut on_exit_internal_receiver = sender.exit_tx.subscribe();
            async move {
                let _ = on_exit_internal_receiver.changed().await;
            }
        });
        let pty_reader = PtyReader::new(
            pty_read,
            child_pty
                .try_clone()
                .map_err(TaskError::pty_creation_error)?,
            child_process_exit_future,
        );

        let (read_guard_tx, read_guard_rx) = oneshot::channel();

        let events_stream = sender.events_tx.subscribe();
        Self::spawn_output_reading(
            &internal_tasks,
            read_guard_rx,
            pty_reader,
            sender.exit_tx.subscribe(),
            sender.events_tx,
            output_buffer.clone(),
            span.clone(),
        );

        let child = Self::spawn_child_process(&info, child_pty)
            .inspect_err(|e| warn!("Error spawning child process: {e}"))?;
        let pid = child.id().expect("pid");
        info!(pid, "Spawned a process");
        let exit_rx = sender.exit_tx.subscribe();
        Self::spawn_waiting_for_exit(&internal_tasks, sender.exit_tx, child, span.clone());

        let task = Self {
            info,
            stdin: tokio::sync::Mutex::new(pty_write),
            pid,
            exit_rx,
            internal_tasks,
            output_buffer,
            events_stream,
        };

        Ok((task, TaskReadingGate(read_guard_tx)))
    }

    pub fn info(&self) -> Arc<TaskInfo> {
        Arc::clone(&self.info)
    }

    pub async fn write_to_stdin(&self, msg: &[u8]) -> Result<(), TaskError> {
        if self.has_exited() {
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

    pub fn events_stream(&self) -> Result<TaskEventsStream, TaskError> {
        if self.has_exited() {
            return Err(TaskError::AlreadyExited);
        }
        Ok(self.events_stream.resubscribe())
    }

    pub fn send_signal(&self, signal: rustix::process::Signal) -> Result<(), TaskError> {
        if self.has_exited() {
            return Err(TaskError::AlreadyExited);
        }
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

    pub fn has_exited(&self) -> bool {
        self.exit_rx.has_changed().unwrap_or(true)
    }

    async fn exit_status(&self) -> ExitStatus {
        self.wait().await;
        self.exit_rx
            .borrow()
            .to_owned()
            .expect("ExitStatus should always be Some")
    }

    pub async fn wait(&self) {
        let _ = self.exit_rx.clone().changed().await;
    }

    pub async fn join(&self) -> FinishedTask {
        self.internal_tasks.join().await;
        FinishedTask {
            info: self.info.clone(),
            exit_status: self.exit_status().await,
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
        read_guard_rx: oneshot::Receiver<()>,
        pty_reader: PtyReader,
        mut on_exit_receiver: watch::Receiver<Option<ExitStatus>>,
        events_tx: broadcast::Sender<TaskEvent>,
        output_buffer: Arc<OutputBuffer>,
        span: Span,
    ) {
        internal_tasks
            .spawn({
                async move {
                    let _ = read_guard_rx.await;
                    let mut stdout = BufReader::new(pty_reader);
                    loop {
                        let mut buf = String::new();
                        let read_bytes = stdout.read_line(&mut buf).await;
                        match read_bytes {
                            Ok(0) => {
                                // EOF
                                break;
                            }
                            Ok(_) => {}
                            Err(e) => {
                                warn!("Error reading output for the task: {e}");
                                break;
                            }
                        };
                        let line = Arc::new(buf);
                        output_buffer.insert_line(line.clone());
                        if let Err(e) = events_tx.send(TaskEvent::Output(line)) {
                            warn!("Error sending output line to subscribers: {e}");
                        }
                    }
                    if on_exit_receiver.changed().await.is_ok() {
                        let exit_status = on_exit_receiver
                            .borrow()
                            .to_owned()
                            .expect("Exit status should always be Some");
                        events_tx
                            .send(TaskEvent::Exit(exit_status))
                            .expect("One receiver in events should be alive");
                    }
                }
                .instrument(span)
            })
            .expect("Internal tasks should not be joined yet");
    }

    fn spawn_waiting_for_exit(
        internal_tasks: &WrappedTaskTracker,
        internal_on_exit_sender: watch::Sender<Option<ExitStatus>>,
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
                    // This sender notifies only internal components about the process exit:
                    // PtyReader and unlocks sending Exit event to subscribers
                    internal_on_exit_sender
                        .send(Some(exit_status))
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
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use rustix::{path::Arg, process::Signal};

    use crate::tasks::sender::CHANNEL_CAPACITY;

    use super::*;
    use std::{
        assert_matches, env::current_dir, io::Write, os::unix::process::ExitStatusExt,
        path::PathBuf, str::FromStr, sync::Mutex, time::Duration,
    };

    const OUTPUT_BUFFER_CAPACITY: usize = CHANNEL_CAPACITY * 2;

    fn make_task(
        executable: impl Into<String>,
        args: &[&str],
        working_dir: impl Into<PathBuf>,
    ) -> Result<(Task, TaskReadingGate), TaskError> {
        let info = TaskInfo {
            executable: executable.into(),
            args: args.iter().map(|&s| String::from(s)).collect(),
            working_dir: working_dir.into(),
        };
        Task::new(info, OUTPUT_BUFFER_CAPACITY)
    }

    fn collect_events(mut events: TaskEventsStream) -> Vec<TaskEvent> {
        std::iter::from_fn(|| events.try_recv().ok()).collect()
    }

    fn collect_output(mut events: TaskEventsStream) -> Vec<String> {
        std::iter::from_fn(|| events.try_recv().ok())
            .filter(|e| matches!(e, TaskEvent::Output(_)))
            .map(|e| match e {
                TaskEvent::Output(o) => String::clone(&o),
                other => panic!("Unexpected task event {other:?}"),
            })
            .collect()
    }

    fn get_exit_status(mut events: TaskEventsStream) -> ExitStatus {
        let events: Vec<_> = std::iter::from_fn(|| events.try_recv().ok()).collect();
        assert_eq!(
            events
                .iter()
                .filter(|e| matches!(e, TaskEvent::Exit(_)))
                .count(),
            1
        );
        events
            .last()
            .map(|e| match e {
                TaskEvent::Exit(e) => e.to_owned(),
                other => panic!("Unexpected task event {other:?}"),
            })
            .expect("No exit code event")
    }

    #[tokio::test]
    async fn new_non_existing_executable() {
        let err = make_task("non_existing", &[], current_dir().unwrap()).unwrap_err();
        assert!(matches!(err, TaskError::StartingChildProcessError(_)));
    }

    #[tokio::test]
    async fn new_bad_args() {
        let err = make_task("ls", &["\0"], current_dir().unwrap()).unwrap_err();
        assert!(matches!(err, TaskError::StartingChildProcessError(_)));
    }

    #[tokio::test]
    async fn new_invalid_directory() {
        let err = make_task(
            "ls",
            &["\0"],
            current_dir().unwrap().join("non_existing_123"),
        )
        .unwrap_err();
        assert!(matches!(err, TaskError::StartingChildProcessError(_)));
    }

    #[tokio::test]
    async fn new_success() {
        let msg = "test";
        let (task, gate) = make_task("echo", &[msg], current_dir().unwrap()).unwrap();
        let events = task.events_stream().unwrap();
        drop(gate);
        task.join().await;
        let events = collect_events(events);
        assert_eq!(events.len(), 2);
        let TaskEvent::Output(output) = events[0] else {
            panic!("Expected output");
        };
        assert_eq!(*output, msg);
        let TaskEvent::Exit(exit) = events[1] else {
            panic!("Expected exit");
        };
        assert_eq!(exit.code().unwrap(), 0);
    }

    #[tokio::test]
    async fn info() {
        let executable = "ls";
        let args = ["-la"];
        let directory = PathBuf::from_str("/tmp").unwrap();
        let (task, _) = make_task(executable, &args, &directory).unwrap();
        let info = task.info();
        assert_eq!(&info.executable, executable);
        assert_eq!(info.args, args);
        assert_eq!(info.working_dir, directory);
        task.join().await;
    }

    #[tokio::test]
    async fn write_to_stdin_success() {
        let (task, _) = make_task("cat", &[], current_dir().unwrap()).unwrap();
        let events = task.events_stream().unwrap();
        for m in ["one", "two\n", "three"] {
            task.write_to_stdin(m.as_bytes()).await.unwrap();
        }
        task.write_to_stdin(b"\x04\x04").await.unwrap(); // flush "three" then EOF
        task.join().await;
        let captured_output = collect_output(events);
        assert_eq!(captured_output.len(), 2);
        assert_eq!(*captured_output[0], "onetwo\r\n");
        assert_eq!(*captured_output[1], "three");
    }

    #[tokio::test]
    async fn write_to_stdin_error() {
        let (task, _) = make_task("ls", &[], current_dir().unwrap()).unwrap();
        task.wait().await;
        let err = task
            .write_to_stdin("some input".as_bytes())
            .await
            .unwrap_err();
        assert!(matches!(err, TaskError::AlreadyExited));
        task.join().await;
    }

    #[tokio::test]
    async fn events_stream_returns_error_after_exit_() {
        let (task, _) = make_task("ls", &[], current_dir().unwrap()).unwrap();
        task.wait().await;
        let err = task.events_stream().unwrap_err();
        assert!(matches!(err, TaskError::AlreadyExited));
        task.join().await;
    }

    #[tokio::test]
    async fn events_stream_when_task_is_running() {
        let (task, _) = make_task("cat", &[], current_dir().unwrap()).unwrap();
        tokio::task::yield_now().await;

        let events = task.events_stream().unwrap();

        task.send_signal(Signal::KILL).unwrap();
        task.join().await;

        let exit_status = get_exit_status(events);
        assert_eq!(exit_status.signal().unwrap(), Signal::KILL.as_raw());
    }

    #[tokio::test]
    async fn events_stream_returns_error_after_exit_events_based() {
        // This test checks that events_stream() will return error after Exit event was published
        let (task, guard) = make_task("echo", &["-n", "hello"], current_dir().unwrap()).unwrap();
        let mut events = task.events_stream().unwrap();
        let event = tokio::time::timeout(Duration::from_secs(1), events.recv())
            .await
            .unwrap()
            .unwrap();
        assert_matches!(event, TaskEvent::Output(_));
        let event = tokio::time::timeout(Duration::from_secs(1), events.recv())
            .await
            .unwrap()
            .unwrap();
        assert_matches!(event, TaskEvent::Exit(_));

        let err = task.events_stream().unwrap_err();
        assert!(matches!(err, TaskError::AlreadyExited));
        task.join().await;
    }

    #[tokio::test]
    async fn output_buffer_captures_output() {
        let (task, _) = make_task("echo", &["-n", "line1\nline2"], current_dir().unwrap()).unwrap();
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
        let (task, _) = make_task("ls", &[], current_dir().unwrap()).unwrap();
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
        let (task, guard) = make_task(
            "cat",
            &[tmp_file.path().to_str().unwrap()],
            current_dir().unwrap(),
        )
        .unwrap();

        let subscriber_handle = tokio::spawn({
            let events = task.events_stream().unwrap();
            let captured_output = captured_output.clone();
            async move {
                while let Ok(e) = events.recv().await {
                    if let TaskEvent::Output(o) = e {
                        captured_output.lock().unwrap().push(o);
                    }
                }
            }
        });
        drop(guard);
        let output_buffer = task.output_buffer().clone();
        task.join().await;
        tokio::time::timeout(Duration::from_secs(1), subscriber_handle)
            .await
            .unwrap();
        assert!(captured_output.lock().unwrap().len() <= REPEAT_COUNT);
        assert_eq!(output_buffer.line_range().end, REPEAT_COUNT);
    }

    #[tokio::test]
    async fn send_signal_success() {
        let (task, _) = make_task("cat", &[], current_dir().unwrap()).unwrap();
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
        let (task, _) = make_task("ls", &[], current_dir().unwrap()).unwrap();
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
        let (task, _) = make_task(executable, &args, &dir).unwrap();
        let finished_task = task.join().await;
        assert_eq!(finished_task.info.executable, executable);
        assert_eq!(finished_task.info.args, &args);
        assert_eq!(finished_task.info.working_dir, dir);
        assert_eq!(finished_task.exit_status.code().unwrap(), 0);
    }

    #[tokio::test]
    #[should_panic(expected = "dropped without calling join()")]
    async fn panic_if_dropped_without_join() {
        let (task, _) = make_task("ls", &[], current_dir().unwrap()).unwrap();
        task.wait().await;
    }

    #[tokio::test]
    #[should_panic(expected = "custom panic")]
    async fn doesnt_double_panic_if_already_panicking() {
        let (task, _) = make_task("ls", &[], current_dir().unwrap()).unwrap();
        task.wait().await;
        panic!("custom panic");
    }

    #[tokio::test]
    async fn calling_join_twice() {
        let (task, _) = make_task("ls", &[], current_dir().unwrap()).unwrap();
        task.join().await;
        task.join().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn exit_notification_is_after_output() {
        let mut tmp_file = tempfile::NamedTempFile::new().unwrap();
        tmp_file
            .write_all("line\n".repeat(CHANNEL_CAPACITY - 1).as_bytes())
            .unwrap();
        tmp_file.flush().unwrap();

        let (task, gate) = make_task(
            "cat",
            &[tmp_file.path().as_str().unwrap()],
            current_dir().unwrap(),
        )
        .unwrap();

        let captured_events = Arc::new(Mutex::new(Vec::new()));
        let subscriber_handle = tokio::spawn({
            let events = task.events_stream().unwrap();
            let captured_events = captured_events.clone();
            async move {
                while let Ok(e) = events.recv().await {
                    captured_events.lock().unwrap().push(e);
                }
            }
        });
        drop(gate);
        task.join().await;

        let captured_events = captured_events.lock().unwrap().to_owned();
        assert_eq!(captured_events.len(), CHANNEL_CAPACITY);
        assert!(
            captured_events[..captured_events.len() - 1]
                .iter()
                .all(|e| matches!(e, TaskEvent::Output(_)))
        );
        assert_matches!(captured_events.last().unwrap(), TaskEvent::Exit(_));
    }

    #[tokio::test]
    async fn has_exited_returns_true_after_exit() {
        let (task, _) = make_task("ls", &[], current_dir().unwrap()).unwrap();
        task.wait().await;
        assert!(task.has_exited());
        task.join().await;
    }

    #[tokio::test]
    async fn task_reading_gate_gates_events_sending() {
        let (task, gate) = make_task("echo", &["hello"], current_dir().unwrap()).unwrap();
        let events = task.events_stream().unwrap();
        let handle = tokio::spawn(async move {
            task.join().await;
        });

        tokio::task::yield_now().await;
        assert!(!handle.is_finished());
        assert!(events.try_recv().is_err());

        drop(gate);
        handle.await.unwrap();
        assert!(events.try_recv().is_ok());
    }
}
