use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use crate::tasks::{finished_task::FinishedTask, task_manager::TaskId};

#[derive(Debug)]
pub(in crate::tasks) struct RecentFinishedTasks {
    id_to_task: HashMap<TaskId, Arc<FinishedTask>>,
    recent_tasks: VecDeque<TaskId>,
    capacity: usize,
}

impl RecentFinishedTasks {
    pub(in crate::tasks) fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "Capacity should be positive");
        let recent_tasks = VecDeque::with_capacity(capacity);
        Self {
            id_to_task: HashMap::new(),
            recent_tasks,
            capacity,
        }
    }

    pub(in crate::tasks) fn insert(&mut self, id: TaskId, task: Arc<FinishedTask>) {
        if self.recent_tasks.len() == self.capacity {
            self.remove_last();
        }
        let inserted = self.id_to_task.insert(id, task).is_none();
        assert!(
            inserted,
            "Task with id {id:?} is already in LruFinishedTasks"
        );
        self.recent_tasks.push_back(id);
    }

    pub(in crate::tasks) fn get(&self, id: TaskId) -> Option<Arc<FinishedTask>> {
        self.id_to_task.get(&id).map(Arc::clone)
    }

    fn remove_last(&mut self) {
        let id = self
            .recent_tasks
            .pop_front()
            .expect("recent_tasks couldn't be empty");
        self.id_to_task
            .remove(&id)
            .expect("Task should be in id_to_task");
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::{env::current_dir, process::ExitStatus};

    use crate::tasks::info::TaskInfo;

    use super::*;

    fn make_finished_task(id: usize) -> (TaskId, Arc<FinishedTask>) {
        let info = TaskInfo {
            executable: id.to_string(),
            args: Vec::new(),
            working_dir: current_dir().unwrap(),
        };
        let ft = FinishedTask {
            info: Arc::new(info),
            exit_status: ExitStatus::default(),
        };
        (TaskId(id), Arc::new(ft))
    }

    #[test]
    #[should_panic(expected = "is already in")]
    fn insert_same_task_id_twice_panics() {
        let mut ft = RecentFinishedTasks::new(10);
        let (id, task) = make_finished_task(1);
        ft.insert(id, task);
        let (id, task) = make_finished_task(1);
        ft.insert(id, task);
    }

    #[test]
    fn insert_multiple_tasks() {
        let mut ft = RecentFinishedTasks::new(10);
        let (id1, task) = make_finished_task(1);
        ft.insert(id1, task);
        let (id2, task) = make_finished_task(2);
        ft.insert(id2, task);

        let task = ft.get(id1).unwrap();
        assert_eq!(task.info.executable, id1.0.to_string());

        let task = ft.get(id2).unwrap();
        assert_eq!(task.info.executable, id2.0.to_string());

        assert!(ft.get(TaskId(3)).is_none());
    }

    #[test]
    fn insert_more_tasks_then_capacity() {
        let mut ft = RecentFinishedTasks::new(2);

        let (id1, task) = make_finished_task(1);
        ft.insert(id1, task);
        let task = ft.get(id1).unwrap();
        assert_eq!(task.info.executable, id1.0.to_string());

        let (id2, task) = make_finished_task(2);
        ft.insert(id2, task);
        let task = ft.get(id1).unwrap();
        assert_eq!(task.info.executable, id1.0.to_string());
        let task = ft.get(id2).unwrap();
        assert_eq!(task.info.executable, id2.0.to_string());

        let (id3, task) = make_finished_task(3);
        ft.insert(id3, task);
        assert!(ft.get(id1).is_none());
        let task = ft.get(id2).unwrap();
        assert_eq!(task.info.executable, id2.0.to_string());
        let task = ft.get(id3).unwrap();
        assert_eq!(task.info.executable, id3.0.to_string());
    }
}
