use std::{
    process::{ExitStatus, Stdio},
    sync::Arc,
};

use anyhow::Result;
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncWriteExt, BufReader},
    process::{Child, ChildStderr, ChildStdin, ChildStdout, Command},
    sync::{broadcast, mpsc, watch},
};
use tracing::{Instrument, Span, info, info_span, warn};

use crate::{
    tasks::{
        OutputLine, TaskEventsStream,
        finished_task::FinishedTask,
        info::TaskInfo,
        output_buffer::OutputBuffer,
        sender::{TaskEvent, TaskSender},
        task_error::TaskError,
    },
    utils::tracker::{PanicHandler, WrappedTaskTracker},
};

/// Gate of task event reading.
/// Dropping gate allows task to begin reading from PTY and waiting for exit.
#[derive(Debug)]
#[must_use]
pub struct TaskReadingGate(watch::Sender<Option<()>>);

impl Drop for TaskReadingGate {
    fn drop(&mut self) {
        self.0.send(Some(())).expect("Gate shouldn't outlive Task");
    }
}

/// Representation of a running child process.
/// All the output of the child process is captured into [`OutputBuffer`].
/// Subscription to output is lossy which means if a subscriber is too slow or the child process
/// produces too much output, subscribers may miss some output lines.
/// There is a strong guarantee that on_exit notification is sent after
/// all the output notifications.
#[derive(Debug)]
pub struct Task {
    info: Arc<TaskInfo>,
    stdin: tokio::sync::Mutex<ChildStdin>,
    pid: u32,
    exit_rx: watch::Receiver<Option<ExitStatus>>,
    internal_tasks: WrappedTaskTracker,
    output_buffer: Arc<OutputBuffer>,
    events_stream: TaskEventsStream,
}

impl Task {
    const OUTPUT_LINE_CHANNEL_SIZE: usize = 16;

    pub(in crate::tasks) fn new(
        info: TaskInfo,
        output_buffer_capacity: usize,
    ) -> Result<(Self, TaskReadingGate), TaskError> {
        let span = info_span!( "task",
                    executable = info.executable,
                    args = ?info.args);
        let _entered = span.enter();

        let sender = TaskSender::new();

        let info = Arc::new(info);
        let internal_tasks = WrappedTaskTracker::new(PanicHandler::new_aborting());
        let output_buffer = Arc::new(OutputBuffer::new(output_buffer_capacity));

        let (read_guard_tx, read_guard_rx) = watch::channel(None);

        let mut child = Self::spawn_child_process(&info)
            .inspect_err(|e| warn!("Error spawning child process: {e}"))?;

        let pid = child.id().expect("pid");
        info!(pid, "Spawned a process");

        let stdout = child.stdout.take().expect("Child should have stdout");
        let stderr = child.stderr.take().expect("Child should have stderr");
        let stdin = child.stdin.take().expect("Child should have stdin");

        let events_stream = sender.events_tx.subscribe();
        let (line_stream_tx, line_stream_rx) = mpsc::channel(Self::OUTPUT_LINE_CHANNEL_SIZE);
        Self::spawn_output_reading(
            &internal_tasks,
            stdout,
            stderr,
            line_stream_tx,
            span.clone(),
        );

        Self::spawn_output_sending(
            &internal_tasks,
            line_stream_rx,
            read_guard_rx,
            sender.exit_tx.subscribe(),
            sender.events_tx,
            output_buffer.clone(),
            span.clone(),
        );

        let exit_rx = sender.exit_tx.subscribe();
        Self::spawn_waiting_for_exit(
            &internal_tasks,
            sender.exit_tx,
            read_guard_tx.subscribe(),
            child,
            span.clone(),
        );

        let task = Self {
            info,
            pid,
            stdin: tokio::sync::Mutex::new(stdin),
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
            let mut stdin = self.stdin.lock().await;
            stdin.write_all(msg).await.map_err(TaskError::write_error)?;
            stdin.flush().await.map_err(TaskError::write_error)
        }
    }

    pub fn events_stream(&self) -> Result<TaskEventsStream, TaskError> {
        if self.has_exited() {
            return Err(TaskError::AlreadyExited);
        }
        Ok(self.events_stream.resubscribe())
    }

    pub fn send_signal(&self, signal: rustix::process::Signal) -> Result<(), TaskError> {
        let pid: rustix::process::RawPid =
            self.pid.try_into().map_err(TaskError::send_signal_error)?;
        let pid = rustix::process::Pid::from_raw(pid).expect("Pid should be valid here");
        rustix::process::kill_process_group(pid, signal).map_err(|e| {
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
            output_buffer: self.output_buffer.clone(),
            exit_status: self.exit_status().await,
        }
    }

    fn spawn_child_process(info: &TaskInfo) -> Result<Child, TaskError> {
        // Using unsafe because pre_exec() is not safe since it is running in a process after fork
        let child = unsafe {
            Command::new(&info.executable)
                .args(&info.args)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .stdin(Stdio::piped())
                .current_dir(&info.working_dir)
                .pre_exec(move || {
                    rustix::process::setsid()?;
                    Ok(())
                })
                .spawn()
                .map_err(TaskError::starting_child_process_error)?
        };
        Ok(child)
    }

    fn spawn_output_reading(
        internal_tasks: &WrappedTaskTracker,
        stdout: ChildStdout,
        stderr: ChildStderr,
        line_stream_tx: mpsc::Sender<String>,
        span: Span,
    ) {
        internal_tasks
            .spawn({
                let tx = line_stream_tx.clone();
                async move {
                    Self::read_loop(stdout, tx).await;
                }
                .instrument(span.clone())
            })
            .expect("Internal tasks should not be joined yet");
        internal_tasks
            .spawn(
                {
                    async move {
                        Self::read_loop(stderr, line_stream_tx).await;
                    }
                }
                .instrument(span),
            )
            .expect("Internal tasks should not be joined yet");
    }

    fn spawn_output_sending(
        internal_tasks: &WrappedTaskTracker,
        mut line_stream_rx: mpsc::Receiver<String>,
        mut read_guard_rx: watch::Receiver<Option<()>>,
        mut on_exit_receiver: watch::Receiver<Option<ExitStatus>>,
        events_tx: broadcast::Sender<TaskEvent>,
        output_buffer: Arc<OutputBuffer>,
        span: Span,
    ) {
        internal_tasks
            .spawn(
                async move {
                    let _ = read_guard_rx.changed().await;
                    let mut line_number = 0;

                    while let Some(line) = line_stream_rx.recv().await {
                        let line = Arc::new(OutputLine {
                            content: line,
                            line_number,
                        });
                        line_number += 1;
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
                .instrument(span),
            )
            .expect("Internal task should not be joined yet");
    }

    async fn read_loop<R: AsyncRead + Unpin>(stream: R, sender: mpsc::Sender<String>) {
        let mut stream = BufReader::new(stream);
        loop {
            // TODO: switch to bytes
            // let mut buf = Vec::new();
            // let read_bytes = stream.read_until(b'\n', &mut buf).await;
            let mut buf = String::new();
            match stream.read_line(&mut buf).await {
                Ok(0) => break, // EOF
                Ok(_) => {
                    sender
                        .send(buf)
                        .await
                        .expect("Sending should be always fine");
                }
                Err(e) => {
                    warn!("Error reading output: {e}");
                    break;
                }
            };
        }
    }

    fn spawn_waiting_for_exit(
        internal_tasks: &WrappedTaskTracker,
        internal_on_exit_sender: watch::Sender<Option<ExitStatus>>,
        mut guard_rx: watch::Receiver<Option<()>>,
        mut child: Child,
        span: Span,
    ) {
        internal_tasks
            .spawn(
                async move {
                    let _ = guard_rx.changed().await;
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

    fn collect_output(mut events: TaskEventsStream) -> Vec<OutputLine> {
        std::iter::from_fn(|| events.try_recv().ok())
            .filter(|e| matches!(e, TaskEvent::Output(_)))
            .map(|e| match e {
                TaskEvent::Output(o) => OutputLine::clone(&o),
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
        assert_matches!(&events[0], TaskEvent::Output(o) if o.content == format!("{msg}\n"));
        assert_matches!(events[1], TaskEvent::Exit(e) if e.code().unwrap() == 0);
    }

    #[tokio::test]
    async fn output_lines_are_contiguous() {
        let (task, gate) = make_task("echo", &["line1\nline2"], current_dir().unwrap()).unwrap();
        let events = task.events_stream().unwrap();
        drop(gate);
        task.join().await;
        let output_lines = collect_output(events);
        assert_eq!(output_lines.len(), 2);
        assert_eq!(output_lines[0].line_number, 0);
        assert_eq!(output_lines[1].line_number, 1);
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
        let msgs = ["one", "two\n", "three"];
        let total_bytes: usize = msgs.iter().map(|m| m.len()).sum();
        let (task, _) = make_task(
            "head",
            &["-c", &total_bytes.to_string()],
            current_dir().unwrap(),
        )
        .unwrap();
        let events = task.events_stream().unwrap();
        for m in msgs {
            task.write_to_stdin(m.as_bytes()).await.unwrap();
        }
        task.join().await;
        let captured_output = collect_output(events);
        assert_eq!(captured_output.len(), 2);
        assert_eq!(captured_output[0].content, "onetwo\n");
        assert_eq!(captured_output[1].content, "three");
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
        drop(guard);
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
                .map(|s| s.content.clone())
                .collect::<Vec<_>>(),
            ["line1\n", "line2"]
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
            let mut events = task.events_stream().unwrap();
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
            .unwrap()
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
    async fn send_signal_kills_process_group() {
        let (task, _) = make_task("sh", &["-c", "cat"], current_dir().unwrap()).unwrap();
        tokio::task::yield_now().await;
        task.send_signal(rustix::process::Signal::TERM).unwrap();
        let exit_status = tokio::time::timeout(Duration::from_secs(1), task.join())
            .await
            .unwrap()
            .exit_status;
        assert_eq!(
            exit_status.signal().unwrap(),
            rustix::process::Signal::TERM.as_raw()
        );
    }

    #[tokio::test]
    async fn join_result() {
        let executable = "echo";
        let args = ["hello"];
        let dir = current_dir().unwrap();
        let (task, _) = make_task(executable, &args, &dir).unwrap();
        let finished_task = task.join().await;
        assert_eq!(finished_task.info.executable, executable);
        assert_eq!(finished_task.info.args, &args);
        assert_eq!(finished_task.info.working_dir, dir);
        assert_eq!(finished_task.exit_status.code().unwrap(), 0);
        assert_eq!(finished_task.output_buffer.line_range(), 0..1);
        assert_eq!(
            finished_task.output_buffer.get_line(0).unwrap().content,
            "hello\n"
        );
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
            let mut events = task.events_stream().unwrap();
            let captured_events = captured_events.clone();
            async move {
                while let Ok(e) = events.recv().await {
                    captured_events.lock().unwrap().push(e);
                }
            }
        });
        drop(gate);
        task.join().await;
        tokio::time::timeout(Duration::from_secs(1), subscriber_handle)
            .await
            .unwrap()
            .unwrap();

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
        let mut events = task.events_stream().unwrap();
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

    #[tokio::test]
    async fn task_doesnt_exit_while_gate_exists() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let tmp_file = tmp_dir.path().join("tmp_file");
        let tmp_file_path = tmp_file.as_str().unwrap();
        let (task, gate) = make_task("touch", &[tmp_file_path], current_dir().unwrap()).unwrap();
        let task = Arc::new(task);
        let handle = tokio::spawn({
            let task = task.clone();
            async move {
                task.join().await;
            }
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while !tmp_file.exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();

        assert!(!task.has_exited());
        assert!(!handle.is_finished());
        drop(gate);
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .unwrap()
            .unwrap();
        assert!(task.has_exited());
    }

    #[tokio::test]
    async fn dropping_task_reading_gate_sends() {
        let (tx, mut rx) = watch::channel(None);
        let gate = TaskReadingGate(tx);
        assert!(!rx.has_changed().unwrap());
        drop(gate);
        tokio::time::timeout(Duration::from_secs(1), rx.changed())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(rx.borrow().unwrap(), ());
    }
}
