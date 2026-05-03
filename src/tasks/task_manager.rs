use anyhow::Result;
use std::{
    collections::HashMap,
    env::current_dir,
    path::PathBuf,
    sync::{Arc, Mutex, RwLock, atomic::AtomicUsize},
};
use tokio::task::JoinSet;

use crate::tasks::{
    events::TaskOutputCallback, finished_task::FinishedTask, task_builder::TaskBuilder,
    task_error::TaskError,
};

use super::task::Task;

// TODO: this should probably become uuid
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct TaskId(usize);

pub struct TaskManager {
    tasks: RwLock<HashMap<TaskId, Arc<Task>>>,
    next_id: AtomicUsize,
    finished_tasks: RwLock<HashMap<TaskId, Arc<FinishedTask>>>,
    completion_coroutines: Mutex<JoinSet<()>>,
}

impl TaskManager {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            tasks: Default::default(),
            next_id: AtomicUsize::new(0),
            finished_tasks: Default::default(),
            completion_coroutines: Mutex::new(JoinSet::new()),
        })
    }

    pub fn create_task(
        self: &Arc<Self>,
        executable: String,
        args: Vec<String>,
        working_dir: Option<PathBuf>,
        on_output: Option<impl TaskOutputCallback>,
    ) -> Result<TaskId, TaskError> {
        let mut task_builder = TaskBuilder::new(executable);
        task_builder.args(args).working_dir(
            working_dir
                .unwrap_or_else(|| current_dir().expect("Unable to get the current directory")),
        );
        if let Some(o) = on_output {
            task_builder.on_output(o);
        }
        let task = task_builder.start_task()?;
        let task = Arc::new(task);
        let task_id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let task_id = TaskId(task_id);
        self.spawn_task_completion(task.clone(), task_id);
        self.tasks
            .write()
            .expect("RwLock is poisoned")
            .insert(task_id, task);
        Ok(task_id)
    }

    pub fn get_task(&self, id: TaskId) -> Result<Arc<Task>> {
        match self.tasks.read().expect("RwLock is poisoned").get(&id) {
            Some(t) => Ok(t.clone()),
            None => Err(anyhow::anyhow!("Not found")),
        }
    }
}
