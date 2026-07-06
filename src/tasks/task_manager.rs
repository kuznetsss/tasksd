use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    env::current_dir,
    path::PathBuf,
    sync::{Arc, Mutex, RwLock, atomic::AtomicUsize},
};

use crate::tasks::{
    finished_task::FinishedTask, info::TaskInfo, recent_finished_tasks::RecentFinishedTasks,
    task::TaskReadingGate, task_error::TaskError,
};
use crate::utils::tracker::{PanicHandler, WrappedTaskTracker};

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

    pub fn create_task(
        self: &Arc<Self>,
        executable: impl Into<String>,
        args: Vec<String>,
        working_dir: Option<String>,
    ) -> Result<(Arc<Task>, TaskId, TaskReadingGate), TaskError> {
        let lock = self.completion_coroutines.lock().unwrap();
        let completion_coroutines = lock.as_ref().ok_or(TaskError::AlreadyExited)?;

        let task_id = TaskId(
            self.next_id
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        );
        let task_info = TaskInfo {
            executable: executable.into(),
            args,
            working_dir: working_dir
                .map(PathBuf::from)
                .unwrap_or(current_dir().map_err(|_| TaskError::InvalidDirectory)?),
        };
        let (task, reading_gate) = Task::new(task_info, self.task_output_buffer_capacity)?;
        let task = Arc::new(task);
        self.spawn_task_completion(completion_coroutines, task.clone(), task_id);
        self.tasks
            .write()
            .expect("RwLock is poisoned")
            .insert(task_id, task.clone());
        Ok((task, task_id, reading_gate))
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
        assert_matches, collections::HashSet, env::current_dir, os::unix::process::ExitStatusExt,
        pin::pin, task::Poll, time::Duration,
    };

    use futures::task::noop_waker;
    use rustix::{path::Arg, process::Signal};

    use crate::tasks::sender::TaskEvent;

    use super::*;

    const TASK_OUTPUT_BUFFER_CAPACITY: usize = 10;

    impl TaskManager {
        fn spawn(
            self: &Arc<Self>,
            exe: &str,
            args: &[&str],
            working_dir: Option<String>,
        ) -> Result<(Arc<Task>, TaskId, TaskReadingGate), TaskError> {
            self.create_task(
                exe,
                args.iter().map(|s| s.to_string()).collect(),
                working_dir,
            )
        }
    }

    #[tokio::test]
    async fn create_task_fails_if_task_couldnt_be_started() {
        let tm = TaskManager::new(TASK_OUTPUT_BUFFER_CAPACITY);
        let err = tm.spawn("non_existing", &[], None).unwrap_err();
        assert!(matches!(err, TaskError::StartingChildProcessError(_)));
        tm.join().await;
    }

    #[tokio::test]
    async fn create_task_success() {
        let tm = TaskManager::new(TASK_OUTPUT_BUFFER_CAPACITY);
        let (task, _, gate) = tm.spawn("echo", &["-n", "hello\nworld"], None).unwrap();
        let mut events = task.events_stream().unwrap();
        drop(gate);
        tm.join().await;

        let events: Vec<_> = std::iter::from_fn(|| events.try_recv().ok()).collect();
        assert_eq!(events.len(), 3);
        assert_matches!(&events[0], TaskEvent::Output(s) if s.as_str() == "hello\n");
        assert_matches!(&events[1], TaskEvent::Output(s) if s.as_str() == "world");
        assert_matches!(&events[2], TaskEvent::Exit(e) if e.code().unwrap() == 0);
    }

    #[tokio::test]
    async fn create_task_after_join_returns_error() {
        let tm = TaskManager::new(TASK_OUTPUT_BUFFER_CAPACITY);
        tm.join().await;
        let err = tm.spawn("ls", &[], None).unwrap_err();
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

        let (task, _, gate) = tm.spawn("pwd", &[], Some(dir.clone())).unwrap();
        let mut events = task.events_stream().unwrap();
        drop(gate);

        tm.join().await;
        let event = events.recv().await.unwrap();
        assert_matches!(event, TaskEvent::Output(s) if s.as_str() == format!("{dir}\n"));
    }

    #[tokio::test]
    async fn create_multiple_tasks() {
        let tm = TaskManager::new(TASK_OUTPUT_BUFFER_CAPACITY);
        let mut task_ids = HashSet::new();
        for _ in 0..3 {
            let (_, task_id, _) = tm.spawn("ls", &[], None).unwrap();
            assert!(task_ids.insert(task_id));
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
        let (task, task_id, _) = tm.spawn(executable, &[], None).unwrap();

        assert!(Arc::ptr_eq(&task, &tm.get_task(task_id).unwrap()));
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
        let _ = tm.spawn("ls", &[], None).unwrap();
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
        let (_, task_id, _) = tm.spawn("cat", &[], None).unwrap();
        let task = tm.get_task(task_id).unwrap();
        assert_eq!(task.output_buffer().capacity(), TASK_OUTPUT_BUFFER_CAPACITY);
        task.send_signal(Signal::TERM).unwrap();
        tm.join().await;
    }
}
