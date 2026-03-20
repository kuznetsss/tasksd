use std::{env::current_dir, path::PathBuf, process::ExitStatus, sync::Arc};

use tokio::{
    process::{Child, Command},
    sync::{broadcast, watch},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::tasks::{
    common::{CHANNEL_CAPACITY, TaskInfo},
    pty::{PtyChild, create_pty_pair},
    task::Task,
    task_error::TaskError,
};

pub(in crate::tasks) struct TaskData {}

pub struct TaskBuilder {
    executable: String,
    args: Option<Vec<String>>,
    working_dir: Option<PathBuf>,

    data: TaskData,
    // TODO: put these field into TaskData struct
    cancel: CancellationToken,
    stdout_tx: broadcast::Sender<Arc<String>>,
    on_exit_tx: watch::Sender<Option<ExitStatus>>,
    related_tasks: JoinSet<()>,
}

impl TaskBuilder {
    pub fn new(executable: impl Into<String>) -> Self {
        let (stdout_tx, _) = broadcast::channel(CHANNEL_CAPACITY);
        let (on_exit_tx, _) = watch::channel(None);
        Self {
            executable: executable.into(),
            args: None,
            working_dir: None,
            data: TaskData {},
            cancel: CancellationToken::new(),
            stdout_tx,
            on_exit_tx,
            related_tasks: JoinSet::new(),
        }
    }

    pub fn arg(&mut self, arg: impl Into<String>) -> &mut Self {
        self.args.get_or_insert(Vec::new()).push(arg.into());
        self
    }

    pub fn args(&mut self, args: impl IntoIterator<Item = impl Into<String>>) -> &mut Self {
        match &mut self.args {
            Some(v) => v.extend(args.into_iter().map(Into::into)),
            None => self.args = Some(args.into_iter().map(Into::into).collect()),
        };
        self
    }

    pub fn working_dir(&mut self, path: impl Into<PathBuf>) -> &mut Self {
        self.working_dir = Some(path.into());
        self
    }

    pub fn on_output<F>(&mut self, mut f: F) -> &mut Self
    where
        F: FnMut(Arc<String>) + 'static + Send,
    {
        let mut stdout_rx = self.stdout_tx.subscribe();
        let cancel = self.cancel.child_token();
        self.related_tasks.spawn(async move {
            while let Some(input_line) = cancel.run_until_cancelled(stdout_rx.recv()).await {
                match input_line {
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!("Stdout receiver is too slow. Have to skip {n} lines");
                    }
                    Err(_) => break,
                    Ok(line) => f(line),
                };
            }
        });
        self
    }

    pub fn on_exit<F>(&mut self, f: F) -> &mut Self
    where
        F: FnOnce(ExitStatus) + 'static + Send,
    {
        let mut on_exit_rx = self.on_exit_tx.subscribe();
        self.related_tasks.spawn(async move {
            on_exit_rx.changed().await.expect("on_exit_rx.await");
            f(on_exit_rx.borrow_and_update().unwrap());
        });
        self
    }

    pub fn start_task(self) -> Result<Task, TaskError> {
        let working_dir = self
            .working_dir
            .unwrap_or(current_dir().map_err(|_| TaskError::InvalidDirectory)?);
        let info = TaskInfo {
            executable: self.executable,
            args: self.args.unwrap_or_default(),
            working_dir,
        };

        Task::start(info, self.data)
    }
}

#[cfg(test)]
mod tests {
    use std::{os::unix::process::ExitStatusExt, sync::Mutex};

    use tokio::sync::Notify;

    use super::*;

    #[test]
    fn arg_adds_arg() {
        let mut builder = TaskBuilder::new("some_executable");
        assert!(builder.args.is_none());
        let arg = "some_arg";
        builder.arg(arg);
        assert_eq!(builder.args.as_ref().unwrap().len(), 1);
        assert_eq!(builder.args.as_ref().unwrap()[0], arg);

        let another_arg = "some_arg";
        builder.arg(another_arg);
        assert_eq!(builder.args.as_ref().unwrap().len(), 2);
        assert_eq!(builder.args.as_ref().unwrap()[0], arg);
        assert_eq!(builder.args.as_ref().unwrap()[0], another_arg);
    }

    #[test]
    fn args_adds_args() {
        let mut builder = TaskBuilder::new("some_executable");
        assert!(builder.args.is_none());
        let args = ["some", "args"];
        builder.args(args);
        assert_eq!(builder.args.as_ref().unwrap().len(), args.len());
        assert_eq!(builder.args.as_ref().unwrap(), &args);

        let another_args = ["another", "args"];
        builder.args(another_args);
        assert_eq!(
            builder.args.as_ref().unwrap().len(),
            args.len() + another_args.len()
        );
        assert_eq!(builder.args.as_ref().unwrap()[..args.len()], args);
        assert_eq!(builder.args.as_ref().unwrap()[args.len()..], another_args);
    }

    #[test]
    fn working_dir_sets_working_dir() {
        let mut builder = TaskBuilder::new("some_executable");
        assert!(builder.working_dir.is_none());
        let wd = "/tmp";
        builder.working_dir(wd);
        assert_eq!(
            builder.working_dir.as_ref().unwrap(),
            &Into::<PathBuf>::into(wd)
        );
    }

    fn make_on_output_test_data() -> (TaskBuilder, Arc<Mutex<Vec<Arc<String>>>>) {
        let mut builder = TaskBuilder::new("some_executable");
        let captured_output = Arc::new(Mutex::new(Vec::new()));
        builder.on_output({
            let captured_output = captured_output.clone();
            move |line| {
                captured_output.lock().unwrap().push(line);
            }
        });
        (builder, captured_output)
    }

    #[tokio::test]
    async fn on_output_subscribes_to_stdout() {
        let (builder, captured_output) = make_on_output_test_data();
        let lines = ["some", "output", "lines"];
        for l in lines {
            assert_eq!(builder.stdout_tx.send(Arc::new(l.to_string())).unwrap(), 1);
        }
        drop(builder.stdout_tx);
        builder.related_tasks.join_all().await;
        let captured_output = captured_output.lock().unwrap();
        assert_eq!(captured_output.len(), lines.len());
        for (i, line) in lines.iter().enumerate() {
            assert_eq!(*line, *captured_output[i]);
        }
    }

    #[tokio::test]
    async fn on_output_sender_dropped() {
        let (builder, captured_output) = make_on_output_test_data();
        drop(builder.stdout_tx);
        builder.related_tasks.join_all().await;
        assert!(captured_output.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn on_output_cancellation() {
        let mut builder = TaskBuilder::new("some_executable");
        let got_message = Arc::new(Notify::new());
        let captured_output = Arc::new(Mutex::new(Vec::new()));
        builder.on_output({
            let captured_output = captured_output.clone();
            let got_message = got_message.clone();
            move |line| {
                captured_output.lock().unwrap().push(line);
                got_message.notify_one();
            }
        });
        let lines = ["some", "output", "lines"];
        assert_eq!(
            builder
                .stdout_tx
                .send(Arc::new(lines[0].to_string()))
                .unwrap(),
            1
        );
        got_message.notified().await;
        builder.cancel.cancel();
        builder.related_tasks.join_all().await;

        for l in lines.iter().skip(1) {
            builder.stdout_tx.send(Arc::new(l.to_string())).unwrap_err();
        }
        let captured_output = captured_output.lock().unwrap();
        assert_eq!(captured_output.len(), 1);
        assert_eq!(lines[0], *captured_output[0]);
    }

    #[tokio::test]
    async fn on_output_slow_subscriber() {
        // tokio::test is single threaded by default so this test is deterministic
        let (builder, captured_output) = make_on_output_test_data();
        let range = 0..(CHANNEL_CAPACITY + 2);
        for i in range.clone() {
            assert_eq!(builder.stdout_tx.send(Arc::new(i.to_string())).unwrap(), 1);
        }
        drop(builder.stdout_tx);
        builder.related_tasks.join_all().await;
        let captured_output = captured_output.lock().unwrap();
        assert_eq!(captured_output.len(), CHANNEL_CAPACITY);
        for (i, message) in range.skip(2).enumerate() {
            assert_eq!(message.to_string(), *captured_output[i]);
        }
    }

    #[tokio::test]
    async fn on_exit_subscribes_to_exit_tx() {
        let mut builder = TaskBuilder::new("some_executable");
        let captured_exit_code = Arc::new(Mutex::new(Option::<ExitStatus>::None));
        builder.on_exit({
            let captured_exit_code = captured_exit_code.clone();
            move |e| {
                *captured_exit_code.lock().unwrap() = Some(e);
            }
        });
        let exit_code = 123;
        builder
            .on_exit_tx
            .send(Some(ExitStatus::from_raw(exit_code)))
            .unwrap();
        builder.related_tasks.join_all().await;
        assert_eq!(
            captured_exit_code.lock().unwrap().unwrap().into_raw(),
            exit_code
        );
    }

    #[tokio::test]
    async fn on_exit_tx_never_called() {
        let mut builder = TaskBuilder::new("some_executable");
        let captured_exit_code = Arc::new(Mutex::new(Option::<ExitStatus>::None));
        builder.on_exit({
            let captured_exit_code = captured_exit_code.clone();
            move |e| {
                *captured_exit_code.lock().unwrap() = Some(e);
            }
        });
        drop(builder);
        assert!(captured_exit_code.lock().unwrap().is_none());
    }
}
