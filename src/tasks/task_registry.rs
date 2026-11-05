use std::{collections::HashMap, sync::Arc};

use crate::tasks::{
    TaskId,
    finished_task::FinishedTask,
    gate::{Gate, GateGuard},
    recent_finished_tasks::RecentFinishedTasks,
    task::Task,
};

#[derive(Debug)]
pub struct TaskExitNotificationTag;

pub type TaskExitNotificationGate = Gate<TaskExitNotificationTag>;
type TaskExitNotificationGateGuard = GateGuard<TaskExitNotificationTag>;

#[derive(Debug)]
pub(in crate::tasks) struct RunningTaskEntry {
    pub(in crate::tasks) task: Arc<Task>,
    pub(in crate::tasks) exit_notification_gate_lock: TaskExitNotificationGateGuard,
}

#[derive(Debug)]
pub(in crate::tasks) enum TaskRef<'a> {
    Running(&'a RunningTaskEntry),
    Finished(&'a Arc<FinishedTask>),
}

#[derive(Debug)]
pub(in crate::tasks) struct TaskRegistry {
    running: HashMap<TaskId, RunningTaskEntry>,
    finished: RecentFinishedTasks,
}

impl TaskRegistry {
    pub(in crate::tasks) fn new(finished_tasks_capacity: usize) -> Self {
        Self {
            running: HashMap::new(),
            finished: RecentFinishedTasks::new(finished_tasks_capacity),
        }
    }

    pub(in crate::tasks) fn add_running(
        &mut self,
        task_id: TaskId,
        task: Arc<Task>,
        exit_notification_gate_lock: TaskExitNotificationGateGuard,
    ) {
        self.running.insert(
            task_id,
            RunningTaskEntry {
                task,
                exit_notification_gate_lock,
            },
        );
    }

    pub(in crate::tasks) fn iter_running(
        &self,
    ) -> impl ExactSizeIterator<Item = (&TaskId, &RunningTaskEntry)> {
        self.running.iter()
    }

    pub(in crate::tasks) fn iter_finished(
        &self,
    ) -> impl ExactSizeIterator<Item = (&TaskId, &Arc<FinishedTask>)> {
        self.finished.iter()
    }

    pub(in crate::tasks) fn mark_finished(&mut self, task_id: TaskId, finished_task: FinishedTask) {
        self.finished.insert(task_id, Arc::new(finished_task));
        self.running
            .remove(&task_id)
            .expect("Task should still be in the running map");
    }

    pub(in crate::tasks) fn get(&self, task_id: &TaskId) -> Option<TaskRef<'_>> {
        if let Some(task_data) = self.running.get(task_id) {
            return Some(TaskRef::Running(task_data));
        }
        self.finished.get(task_id).map(TaskRef::Finished)
    }
}
