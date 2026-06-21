use std::{env::current_dir, path::PathBuf};

use crate::tasks::{
    events::{TaskEvents, TaskExitCallback, TaskOutputCallback},
    info::TaskInfo,
    senders::TaskSender,
    task::Task,
    task_error::TaskError,
};

pub struct TaskBuilder {
    executable: String,
    args: Option<Vec<String>>,
    working_dir: Option<PathBuf>,

    sender: TaskSender,
    events: TaskEvents,
    output_buffer_capacity: usize,
}

impl TaskBuilder {
    pub fn new(executable: impl Into<String>, output_buffer_capacity: usize) -> Self {
        let sender = TaskSender::new();
        let events = TaskEvents::new(&sender);
        Self {
            executable: executable.into(),
            args: None,
            working_dir: None,
            events,
            sender,
            output_buffer_capacity,
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

    pub fn on_output<F>(&mut self, f: F) -> &mut Self
    where
        F: TaskOutputCallback,
    {
        self.events
            .on_output(f)
            .expect("Task can't exit in builder");
        self
    }

    pub fn on_exit<F>(&mut self, f: F) -> &mut Self
    where
        F: TaskExitCallback,
    {
        self.events.on_exit(f).expect("Task can't exit in builder");
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

        Task::new(info, self.sender, self.events, self.output_buffer_capacity)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        os::unix::process::ExitStatusExt,
        process::ExitStatus,
        sync::{Arc, Mutex},
    };

    use super::*;

    const OUTPUT_BUFFER_CAPACITY: usize = 10;

    #[tokio::test]
    async fn arg_adds_arg() {
        let mut builder = TaskBuilder::new("some_executable", OUTPUT_BUFFER_CAPACITY);
        assert!(builder.args.is_none());
        let arg = "some_arg";
        builder.arg(arg);
        assert_eq!(builder.args.as_ref().unwrap().len(), 1);
        assert_eq!(builder.args.as_ref().unwrap()[0], arg);

        let another_arg = "another_arg";
        builder.arg(another_arg);
        assert_eq!(builder.args.as_ref().unwrap().len(), 2);
        assert_eq!(builder.args.as_ref().unwrap()[0], arg);
        assert_eq!(builder.args.as_ref().unwrap()[1], another_arg);
    }

    #[tokio::test]
    async fn args_adds_args() {
        let mut builder = TaskBuilder::new("some_executable", OUTPUT_BUFFER_CAPACITY);
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

    #[tokio::test]
    async fn working_dir_sets_working_dir() {
        let mut builder = TaskBuilder::new("some_executable", OUTPUT_BUFFER_CAPACITY);
        assert!(builder.working_dir.is_none());
        let wd = "/tmp";
        builder.working_dir(wd);
        assert_eq!(
            builder.working_dir.as_ref().unwrap(),
            &Into::<PathBuf>::into(wd)
        );
    }

    #[tokio::test]
    async fn on_output_subscribes_to_stdout() {
        let mut builder = TaskBuilder::new("some_executable", OUTPUT_BUFFER_CAPACITY);
        let captured_output = Arc::new(Mutex::new(Vec::new()));
        builder.on_output({
            let captured_output = captured_output.clone();
            move |o| {
                captured_output.lock().unwrap().push(o);
                async { Ok(()) }
            }
        });
        let output_lines = ["some output", "other output"];
        for l in &output_lines {
            assert_eq!(builder.sender.0.send(l.to_string().into()).unwrap(), 3);
        }
        drop(builder.sender);
        builder.events.join_all().await;
        let captured_output = captured_output.lock().unwrap();
        assert_eq!(captured_output.len(), output_lines.len());
        for i in 0..output_lines.len() {
            assert_eq!(captured_output[i].as_ref(), output_lines[i]);
        }
    }

    #[tokio::test]
    async fn on_exit_subscribes_to_exit_code() {
        let mut builder = TaskBuilder::new("some_executable", OUTPUT_BUFFER_CAPACITY);
        let captured_exit_code = Arc::new(Mutex::new(None));
        builder.on_exit({
            let captured_exit_code = captured_exit_code.clone();
            move |e| {
                *captured_exit_code.lock().unwrap() = Some(e);
                async {}
            }
        });
        let exit_code = ExitStatus::from_raw(123);
        builder.sender.0.send(exit_code.into()).unwrap();
        drop(builder.sender);
        builder.events.join_all().await;
        let captured_exit_code = captured_exit_code.lock().unwrap();
        assert!(captured_exit_code.is_some());
        assert_eq!(captured_exit_code.unwrap().into_raw(), exit_code.into_raw());
    }

    #[tokio::test]
    async fn output_buffer_capacity_passed_to_task() {
        let task = TaskBuilder::new("ls", OUTPUT_BUFFER_CAPACITY)
            .start_task()
            .unwrap();
        assert_eq!(task.output_buffer().capacity(), OUTPUT_BUFFER_CAPACITY);
        task.join().await;
    }
}
