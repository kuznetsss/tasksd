use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex, RwLock, atomic::AtomicUsize},
};

use crate::tasks::{
    TaskEventsSubscriber,
    finished_task::FinishedTask,
    recent_finished_tasks::RecentFinishedTasks,
    task::TaskReadingGate,
    task_builder::TaskBuilder,
    task_error::TaskError,
    tracker::{PanicHandler, WrappedTaskTracker},
};

use super::task::Task;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy, Serialize, Deserialize)]
pub struct TaskId(pub usize);

impl std::fmt::Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug)]
pub struct TaskManager {
    task_output_buffer_capacity: usize,
    tasks: RwLock<HashMap<TaskId, Arc<Task>>>,
    next_id: AtomicUsize,
    finished_tasks: RwLock<RecentFinishedTasks>,
    completion_coroutines: Mutex<Option<WrappedTaskTracker>>,
}

#[must_use = "TaskCreationHandle must be submitted with .submit() to register the task"]
pub struct TaskCreationHandle<'a> {
    manager: &'a Arc<TaskManager>,
    builder: TaskBuilder,
    task_id: TaskId,
}

impl<'a> TaskCreationHandle<'a> {
    pub fn args(&mut self, args: impl IntoIterator<Item = impl Into<String>>) -> &mut Self {
        self.builder.args(args);
        self
    }

    pub fn working_dir(&mut self, dir: impl Into<PathBuf>) -> &mut Self {
        self.builder.working_dir(dir);
        self
    }

    pub fn subscribe(&mut self, s: impl TaskEventsSubscriber) -> &mut Self {
        self.builder.subscribe(s);
        self
    }

    pub fn submit(self) -> Result<TaskReadingGate, TaskError> {
        self.manager.submit(self.builder, self.task_id)
    }

    pub fn task_id(&self) -> TaskId {
        self.task_id
    }
}

impl TaskManager {
    pub fn new(task_output_buffer_capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            task_output_buffer_capacity,
            tasks: Default::default(),
            next_id: AtomicUsize::new(0),
            finished_tasks: RwLock::new(RecentFinishedTasks::new(100)),
            completion_coroutines: Mutex::new(Some(WrappedTaskTracker::new(
                PanicHandler::new_aborting(),
            ))),
        })
    }

    pub fn create_task(self: &Arc<Self>, executable: impl Into<String>) -> TaskCreationHandle<'_> {
        let task_id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        TaskCreationHandle {
            manager: self,
            builder: TaskBuilder::new(executable.into(), self.task_output_buffer_capacity),
            task_id: TaskId(task_id),
        }
    }

    fn submit(
        self: &Arc<Self>,
        builder: TaskBuilder,
        task_id: TaskId,
    ) -> Result<TaskReadingGate, TaskError> {
        let lock = self.completion_coroutines.lock().unwrap();
        let completion_coroutines = lock.as_ref().ok_or(TaskError::AlreadyExited)?;

        let (task, reading_gate) = builder.start_task()?;
        let task = Arc::new(task);
        self.spawn_task_completion(completion_coroutines, task.clone(), task_id);
        self.tasks
            .write()
            .expect("RwLock is poisoned")
            .insert(task_id, task);
        Ok(reading_gate)
    }

    pub fn get_task(&self, id: TaskId) -> Option<Arc<Task>> {
        self.tasks
            .read()
            .expect("RwLock is poisoned")
            .get(&id)
            .cloned()
    }

    pub fn get_finished_task(&self, id: TaskId) -> Option<Arc<FinishedTask>> {
        self.finished_tasks.read().unwrap().get(id)
    }

    pub async fn join(&self) {
        let completion_coroutines = match self.completion_coroutines.lock().unwrap().take() {
            Some(c) => c,
            None => return, // Already joined
        };
        completion_coroutines.join().await;
    }

    pub fn send_signal_to_all_tasks(&self, signal: rustix::process::Signal) {
        let tasks_map = self.tasks.read().unwrap();
        for task in tasks_map.values() {
            let _ = task.send_signal(signal);
        }
    }

    fn spawn_task_completion(
        self: &Arc<Self>,
        completion_coroutines: &WrappedTaskTracker,
        task: Arc<Task>,
        task_id: TaskId,
    ) {
        completion_coroutines
            .spawn({
                let task = task.clone();
                let this = self.clone();
                async move {
                    let finished_task = task.join().await;
                    this.finished_tasks
                        .write()
                        .unwrap()
                        .insert(task_id, Arc::new(finished_task));
                    this.tasks
                        .write()
                        .unwrap()
                        .remove(&task_id)
                        .expect("Task should still be in the hmap");
                }
            })
            .expect("Should never happen because of mutex");
    }
}

impl Drop for TaskManager {
    fn drop(&mut self) {
        if std::thread::panicking() {
            return;
        }
        assert!(
            self.completion_coroutines.get_mut().unwrap().is_none(),
            "TaskManager is dropped without calling join()"
        );
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::{
        collections::HashSet, env::current_dir, os::unix::process::ExitStatusExt, pin::pin,
        task::Poll, time::Duration,
    };

    use futures::task::noop_waker;
    use rustix::{path::Arg, process::Signal};

    use crate::tasks::test_subscribers::CapturingSubscriber;

    use super::*;

    const TASK_OUTPUT_BUFFER_CAPACITY: usize = 10;

    #[tokio::test]
    async fn create_task_fails_if_task_couldnt_be_started() {
        let tm = TaskManager::new(TASK_OUTPUT_BUFFER_CAPACITY);
        let err = tm.create_task("non_existing").submit().unwrap_err();
        assert!(matches!(err, TaskError::StartingChildProcessError(_)));
        tm.join().await;
    }

    #[tokio::test]
    async fn create_task_output_callback_catches_output() {
        let tm = TaskManager::new(TASK_OUTPUT_BUFFER_CAPACITY);
        let captured_output = Arc::new(Mutex::new(Vec::new()));
        let mut builder = tm.create_task("echo");
        builder
            .args(["-n", "hello\nworld"])
            .subscribe(CapturingSubscriber {
                captured_output: captured_output.clone(),
                ..Default::default()
            });
        let _ = builder.submit().unwrap();
        tm.join().await;
        let captured_output = captured_output.lock().unwrap();
        assert_eq!(captured_output.len(), 2);
        assert_eq!(*captured_output[0], "hello\r\n");
        assert_eq!(*captured_output[1], "world");
    }

    #[tokio::test]
    async fn create_task_exit_callback_catches_exit_code() {
        let tm = TaskManager::new(TASK_OUTPUT_BUFFER_CAPACITY);
        let captured_exit_codes = Arc::new(Mutex::new(Vec::new()));
        let mut builder = tm.create_task("sh");
        builder
            .args(["-c", "exit 123"])
            .subscribe(CapturingSubscriber {
                captured_exit_codes: captured_exit_codes.clone(),
                ..Default::default()
            });
        let _ = builder.submit().unwrap();
        tm.join().await;
        let captured_output = captured_exit_codes.lock().unwrap();
        assert_eq!(captured_output.len(), 1);
        assert_eq!(captured_output[0].code().unwrap(), 123);
    }

    #[tokio::test]
    async fn create_task_after_join_returns_error() {
        let tm = TaskManager::new(TASK_OUTPUT_BUFFER_CAPACITY);
        tm.join().await;
        let err = tm.create_task("ls").submit().unwrap_err();
        assert!(matches!(err, TaskError::AlreadyExited));
    }

    #[tokio::test]
    async fn create_task_custom_working_dir() {
        let tm = TaskManager::new(TASK_OUTPUT_BUFFER_CAPACITY);
        let tmp_dir = tempfile::tempdir().unwrap();
        let dir = std::fs::canonicalize(tmp_dir.path())
            .unwrap()
            .as_str()
            .unwrap()
            .to_owned();

        let subscriber = CapturingSubscriber::default();
        let captured_output = subscriber.captured_output.clone();
        let mut builder = tm.create_task("pwd");
        builder.working_dir(&dir).subscribe(subscriber);
        let _ = builder.submit().unwrap();

        tm.join().await;
        let captured_output = captured_output.lock().unwrap();
        assert_eq!(captured_output.len(), 1);
        assert_eq!(*captured_output[0], format!("{dir}\r\n"));
    }

    #[tokio::test]
    async fn create_multiple_tasks() {
        let tm = TaskManager::new(TASK_OUTPUT_BUFFER_CAPACITY);
        let mut task_ids = HashSet::new();
        for _ in 0..3 {
            let builder = tm.create_task("ls");
            let id = builder.task_id();
            let _ = builder.submit().unwrap();
            assert!(task_ids.insert(id));
        }
        tokio::time::timeout(Duration::from_secs(1), tm.join())
            .await
            .unwrap();
        for id in &task_ids {
            assert!(tm.get_finished_task(*id).is_some());
        }
    }

    #[tokio::test]
    async fn get_methods_return_task() {
        let tm = TaskManager::new(TASK_OUTPUT_BUFFER_CAPACITY);
        let executable = "cat";
        let builder = tm.create_task(executable);
        let task_id = builder.task_id();
        let _ = builder.submit().unwrap();

        let task = tm.get_task(task_id).unwrap();
        assert!(tm.get_finished_task(task_id).is_none());

        let non_existing_id = TaskId(task_id.0 + 123);
        assert!(tm.get_task(non_existing_id).is_none());
        assert!(tm.get_finished_task(non_existing_id).is_none());

        let signal = Signal::TERM;
        task.send_signal(signal).unwrap();
        tokio::time::timeout(Duration::from_secs(1), tm.join())
            .await
            .unwrap();

        assert!(tm.get_task(task_id).is_none());
        let finished_task = tm.get_finished_task(task_id).unwrap();
        assert_eq!(&finished_task.info.executable, executable);
        assert_eq!(&finished_task.info.working_dir, &current_dir().unwrap());
        assert_eq!(finished_task.exit_status.signal().unwrap(), signal.as_raw());

        assert!(tm.get_task(non_existing_id).is_none());
        assert!(tm.get_finished_task(non_existing_id).is_none());
    }

    #[tokio::test]
    async fn join_called_multiple_times_is_ok() {
        let tm = TaskManager::new(TASK_OUTPUT_BUFFER_CAPACITY);
        let _ = tm.create_task("ls").submit().unwrap();
        tm.join().await;

        let waker = noop_waker();
        let mut ctx = std::task::Context::from_waker(&waker);
        let future = pin!(tm.join());
        assert!(matches!(future.poll(&mut ctx), Poll::Ready(())));
    }

    #[tokio::test]
    #[should_panic(expected = "without calling join")]
    async fn panics_if_join_was_not_called() {
        let _tm = TaskManager::new(TASK_OUTPUT_BUFFER_CAPACITY);
    }

    #[tokio::test]
    #[should_panic(expected = "custom panic")]
    async fn drop_doesnt_panic_if_already_panicking() {
        let _tm = TaskManager::new(TASK_OUTPUT_BUFFER_CAPACITY);
        panic!("custom panic");
    }

    #[tokio::test]
    async fn output_buffer_capacity_passed_to_task() {
        let tm = TaskManager::new(TASK_OUTPUT_BUFFER_CAPACITY);
        let builder = tm.create_task("cat");
        let task_id = builder.task_id();
        let _ = builder.submit().unwrap();
        let task = tm.get_task(task_id).unwrap();
        assert_eq!(task.output_buffer().capacity(), TASK_OUTPUT_BUFFER_CAPACITY);
        task.send_signal(Signal::TERM).unwrap();
        tm.join().await;
    }
}
