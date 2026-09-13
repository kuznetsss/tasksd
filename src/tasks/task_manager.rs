use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    env::current_dir,
    path::PathBuf,
    sync::{Arc, Mutex, RwLock, atomic::AtomicUsize},
};

use crate::utils::tracker::{PanicHandler, WrappedTaskTracker};
use crate::{
    api::TaskExitStatus,
    tasks::{
        finished_task::FinishedTask, info::TaskInfo, recent_finished_tasks::RecentFinishedTasks,
        task::TaskReadingGate, task_error::TaskError,
    },
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
struct TaskRegistry {
    running: HashMap<TaskId, Arc<Task>>,
    finished: RecentFinishedTasks,
}

impl TaskRegistry {
    fn new(finished_tasks_capacity: usize) -> Self {
        Self {
            running: HashMap::new(),
            finished: RecentFinishedTasks::new(finished_tasks_capacity),
        }
    }
}

#[derive(Debug)]
pub struct TaskManager {
    task_output_buffer_capacity: usize,
    tasks: RwLock<TaskRegistry>,
    next_id: AtomicUsize,
    completion_coroutines: Mutex<Option<WrappedTaskTracker>>,
}

#[derive(Debug)]
pub enum AnyTask {
    Running(Arc<Task>),
    Finished(Arc<FinishedTask>),
}

impl TaskManager {
    pub fn new(task_output_buffer_capacity: usize) -> Arc<Self> {
        const FINISHED_TASKS_CAPACITY: usize = 100;
        Arc::new(Self {
            task_output_buffer_capacity,
            tasks: RwLock::new(TaskRegistry::new(FINISHED_TASKS_CAPACITY)),
            next_id: AtomicUsize::new(0),
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
        // TODO: should be shutdown error
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
            .running
            .insert(task_id, task.clone());
        Ok((task, task_id, reading_gate))
    }

    pub fn get_running_task(&self, id: TaskId) -> Result<Arc<Task>, TaskError> {
        let tasks = self.tasks.read().unwrap();
        if let Some(t) = tasks.running.get(&id) {
            return Ok(t.clone());
        }
        if tasks.finished.get(id).is_some() {
            Err(TaskError::AlreadyExited)
        } else {
            Err(TaskError::NotFound)
        }
    }

    pub fn find_task(&self, id: TaskId) -> Option<AnyTask> {
        let tasks = self.tasks.read().unwrap();
        if let Some(t) = tasks.running.get(&id) {
            return Some(AnyTask::Running(t.clone()));
        }
        if let Some(t) = tasks.finished.get(id) {
            return Some(AnyTask::Finished(t.clone()));
        }
        None
    }

    pub async fn join(&self) {
        let completion_coroutines = match self.completion_coroutines.lock().unwrap().take() {
            Some(c) => c,
            None => return, // Already joined
        };
        completion_coroutines.join().await;
    }

    pub fn send_signal_to_all_tasks(&self, signal: rustix::process::Signal) {
        let tasks = self.tasks.read().unwrap();
        for task in tasks.running.values() {
            let _ = task.send_signal(signal);
        }
    }

    pub fn task_list(&self) -> TaskList {
        let tasks = self.tasks.read().unwrap();
        let mut list = TaskList {
            tasks: Vec::with_capacity(tasks.running.len() + tasks.finished.len()),
        };
        for (&task_id, task) in tasks.running.iter() {
            let entry = TaskEntry {
                info: task.info(),
                task_id,
                status: TaskStatus::Running,
            };
            list.tasks.push(entry);
        }
        for (&task_id, task) in tasks.finished.iter() {
            let entry = TaskEntry {
                info: task.info.clone(),
                task_id,
                status: TaskStatus::Finished(task.exit_status.into()),
            };
            list.tasks.push(entry);
        }
        list
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
                    let mut tasks = this.tasks.write().unwrap();
                    tasks.finished.insert(task_id, Arc::new(finished_task));
                    tasks
                        .running
                        .remove(&task_id)
                        .expect("Task should still be in the running map");
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

#[derive(Debug, Serialize, Clone)]
#[serde(tag = "status", rename_all = "lowercase")]
pub enum TaskStatus {
    Running,
    Finished(TaskExitStatus),
}

#[derive(Debug, Serialize)]
pub struct TaskEntry {
    pub info: Arc<TaskInfo>,
    pub task_id: TaskId,
    #[serde(flatten)]
    pub status: TaskStatus,
}

#[derive(Debug, Serialize, Default)]
pub struct TaskList {
    pub tasks: Vec<TaskEntry>,
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::{
        assert_matches, collections::HashSet, env::current_dir, os::unix::process::ExitStatusExt,
        pin::pin, sync::Arc, task::Poll, time::Duration,
    };

    use futures::task::noop_waker;
    use rustix::{path::Arg, process::Signal};
    use serde_json::json;

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
        assert_matches!(&events[0], TaskEvent::Output(s) if s.content == "hello\n");
        assert_matches!(&events[1], TaskEvent::Output(s) if s.content == "world");
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
        assert_matches!(event, TaskEvent::Output(s) if s.content == format!("{dir}\n"));
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
            assert_matches!(tm.find_task(*id).unwrap(), AnyTask::Finished(_));
        }
    }

    // TODO: split into tests for get_running_task and find_task
    #[tokio::test]
    async fn get_methods_return_task() {
        let tm = TaskManager::new(TASK_OUTPUT_BUFFER_CAPACITY);
        let executable = "cat";
        let (task, task_id, _) = tm.spawn(executable, &[], None).unwrap();

        assert!(Arc::ptr_eq(&task, &tm.get_running_task(task_id).unwrap()));
        assert!(Arc::ptr_eq(&task, &tm.get_running_task(task_id).unwrap()));
        assert!(tm.find_task(task_id).is_none());

        let non_existing_id = TaskId(task_id.0 + 123);
        assert_matches!(
            tm.get_running_task(non_existing_id),
            Err(TaskError::NotFound)
        );
        assert!(tm.get_running_task_deprecated(non_existing_id).is_none());
        assert!(tm.get_finished_task_deprecated(non_existing_id).is_none());

        let signal = Signal::TERM;
        task.send_signal(signal).unwrap();
        tokio::time::timeout(Duration::from_secs(1), tm.join())
            .await
            .unwrap();

        assert_matches!(tm.get_running_task(task_id), Err(TaskError::AlreadyExited));
        assert!(tm.get_running_task_deprecated(task_id).is_none());
        let finished_task = tm.get_finished_task_deprecated(task_id).unwrap();
        assert_eq!(&finished_task.info.executable, executable);
        assert_eq!(&finished_task.info.working_dir, &current_dir().unwrap());
        assert_eq!(finished_task.exit_status.signal().unwrap(), signal.as_raw());

        assert_matches!(
            tm.get_running_task(non_existing_id),
            Err(TaskError::NotFound)
        );
        assert!(tm.get_finished_task_deprecated(non_existing_id).is_none());
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
        let task = tm.get_running_task(task_id).unwrap();
        assert_eq!(task.output_buffer().capacity(), TASK_OUTPUT_BUFFER_CAPACITY);
        task.send_signal(Signal::TERM).unwrap();
        tm.join().await;
    }

    #[tokio::test]
    async fn task_list_returns_list_of_tasks() {
        let tm = TaskManager::new(TASK_OUTPUT_BUFFER_CAPACITY);
        let (task, task_id, _) = tm.spawn("cat", &[], None).unwrap();
        let list = tm.task_list();
        assert_eq!(list.tasks.len(), 1);
        assert_eq!(list.tasks[0].task_id, task_id);
        assert_matches!(list.tasks[0].status, TaskStatus::Running);
        let signal = Signal::KILL;
        task.send_signal(signal).unwrap();
        task.wait().await;
        tokio::time::timeout(Duration::from_secs(5), tm.join())
            .await
            .unwrap();

        let list = tm.task_list();
        assert_eq!(list.tasks.len(), 1);
        assert_eq!(list.tasks[0].task_id, task_id);
        let expected_exit_status = TaskExitStatus {
            exit_code: None,
            signal: Some(signal.as_raw()),
        };
        assert_matches!(
            &list.tasks[0].status,
            TaskStatus::Finished(e) if e == &expected_exit_status
        );
    }

    #[test]
    fn task_entry_serialization() {
        let info = Arc::new(TaskInfo {
            executable: "some_executable".to_string(),
            args: vec!["some".to_string(), "args".to_string()],
            working_dir: current_dir().unwrap(),
        });
        let task_id = TaskId(123);
        let status = TaskStatus::Running;

        let task_entry = TaskEntry {
            info: info.clone(),
            task_id,
            status: status.clone(),
        };
        let json_str = serde_json::to_string(&task_entry).unwrap();
        let expected_json = json!({
            "task_id": task_id,
            "info": {
                "executable": &info.executable,
                "args": &info.args,
                "working_dir": &&info.working_dir
            },
            "status": "running"
        });
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&json_str).unwrap(),
            expected_json
        );
    }

    #[test]
    fn task_entry_serialization_finished_task() {
        let info = Arc::new(TaskInfo {
            executable: "some_executable".to_string(),
            args: vec!["some".to_string(), "args".to_string()],
            working_dir: current_dir().unwrap(),
        });
        let task_id = TaskId(123);
        let task_exit_status = TaskExitStatus {
            exit_code: Some(123),
            signal: None,
        };
        let status = TaskStatus::Finished(task_exit_status.clone());

        let task_entry = TaskEntry {
            info: info.clone(),
            task_id,
            status: status.clone(),
        };
        let json_str = serde_json::to_string(&task_entry).unwrap();
        let expected_json = json!({
            "task_id": task_id,
            "info": {
                "executable": &info.executable,
                "args": &info.args,
                "working_dir": &&info.working_dir
            },
            "status": "finished",
            "exit_code": task_exit_status.exit_code,
            "signal": Option::<i32>::None
        });
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&json_str).unwrap(),
            expected_json
        );
    }
}
