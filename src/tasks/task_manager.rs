use anyhow::Result;
use std::{
    collections::HashMap,
    env::current_dir,
    path::PathBuf,
    sync::{Arc, Mutex, RwLock, atomic::AtomicUsize},
};

use crate::tasks::{
    events::{TaskExitCallback, TaskOutputCallback},
    finished_task::FinishedTask,
    task_builder::TaskBuilder,
    task_error::TaskError,
    tracker::{PanicHandler, WrappedTaskTracker},
};

use super::task::Task;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct TaskId(usize);

pub struct TaskManager {
    tasks: RwLock<HashMap<TaskId, Arc<Task>>>,
    next_id: AtomicUsize,
    finished_tasks: RwLock<HashMap<TaskId, Arc<FinishedTask>>>,
    completion_coroutines: Mutex<Option<WrappedTaskTracker>>,
}

impl TaskManager {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            tasks: Default::default(),
            next_id: AtomicUsize::new(0),
            finished_tasks: Default::default(),
            completion_coroutines: Mutex::new(Some(WrappedTaskTracker::new(
                PanicHandler::new_aborting(),
            ))),
        })
    }

    pub fn create_task(
        self: &Arc<Self>,
        executable: String,
        args: Vec<String>,
        working_dir: Option<PathBuf>,
        on_output: Option<impl TaskOutputCallback>,
        on_exit: Option<impl TaskExitCallback>,
    ) -> Result<TaskId, TaskError> {
        let lock = self.completion_coroutines.lock().unwrap();
        let completion_coroutines = lock.as_ref().ok_or(TaskError::AlreadyExited)?;

        let mut task_builder = TaskBuilder::new(executable);
        task_builder.args(args).working_dir(
            working_dir
                .unwrap_or_else(|| current_dir().expect("Unable to get the current directory")),
        );
        if let Some(o) = on_output {
            task_builder.on_output(o);
        }
        if let Some(e) = on_exit {
            task_builder.on_exit(e);
        }
        let task = task_builder.start_task()?;
        let task = Arc::new(task);
        let task_id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let task_id = TaskId(task_id);
        self.spawn_task_completion(completion_coroutines, task.clone(), task_id);
        self.tasks
            .write()
            .expect("RwLock is poisoned")
            .insert(task_id, task);
        Ok(task_id)
    }

    pub fn get_task(&self, id: TaskId) -> Option<Arc<Task>> {
        self.tasks
            .read()
            .expect("RwLock is poisoned")
            .get(&id)
            .map(|t| t.clone())
    }

    pub fn get_finished_task(&self, id: TaskId) -> Option<Arc<FinishedTask>> {
        self.finished_tasks
            .read()
            .unwrap()
            .get(&id)
            .map(|f| f.clone())
    }

    pub async fn join(&self) {
        let completion_coroutines = match self.completion_coroutines.lock().unwrap().take() {
            Some(c) => c,
            None => return, // Already joined
        };
        completion_coroutines.join().await;
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
        assert!(
            self.completion_coroutines.get_mut().unwrap().is_none(),
            "TaskManager is dropped without calling join()"
        );
    }
}
