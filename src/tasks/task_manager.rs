use anyhow::Result;
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, RwLock, atomic::AtomicUsize},
};

use super::task::Task;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct TaskId(usize);

pub struct TaskManager {
    tasks: RwLock<HashMap<TaskId, Arc<Task>>>,
    next_id: AtomicUsize,
}

impl TaskManager {
    pub fn new() -> TaskManager {
        Self {
            tasks: Default::default(),
            next_id: AtomicUsize::new(0),
        }
    }

    pub fn create_task(
        &self,
        executable: String,
        args: Vec<String>,
        working_dir: Option<PathBuf>,
    ) -> Result<TaskId> {
        let task = Task::new(executable, args, working_dir)?;
        let task_id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let task_id = TaskId(task_id);
        self.tasks
            .write()
            .expect("RwLock is poisoned")
            .insert(task_id, Arc::new(task));
        // TODO: task should be removed from the map after it is finished
        Ok(task_id)
    }

    pub fn get_task(&self, id: TaskId) -> Result<Arc<Task>> {
        match self.tasks.read().expect("RwLock is poisoned").get(&id) {
            Some(t) => Ok(t.clone()),
            None => Err(anyhow::anyhow!("Not found")),
        }
    }
}
