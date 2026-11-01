use std::{collections::HashMap, sync::Arc};

use crate::tasks::{
    TaskId,
    finished_task::FinishedTask,
    gate::{Gate, GateLock},
    recent_finished_tasks::RecentFinishedTasks,
    task::Task,
};

#[derive(Debug)]
pub struct TaskExitNotificationTag;

pub type TaskExitNotificationGate = Gate<TaskExitNotificationTag>;
type TaskExitNotificationGateLock = GateLock<TaskExitNotificationTag>;

#[derive(Debug)]
pub(in crate::tasks) struct RunningTaskEntry {
    pub(in crate::tasks) task: Arc<Task>,
    pub(in crate::tasks) exit_notification_gate_lock: TaskExitNotificationGateLock,
}

#[derive(Debug)]
pub(in crate::tasks) enum SomeTask<'a> {
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

    pub(in crate::tasks) fn add(
        &mut self,
        task_id: TaskId,
        task: Arc<Task>,
        exit_notification_gate_lock: TaskExitNotificationGateLock,
    ) {
        self.running.insert(
            task_id,
            RunningTaskEntry {
                task,
                exit_notification_gate_lock,
            },
        );
    }

    pub(in crate::tasks) fn running(&self) -> &HashMap<TaskId, RunningTaskEntry> {
        &self.running
    }

    pub(in crate::tasks) fn finished(&self) -> &RecentFinishedTasks {
        &self.finished
    }

    pub(in crate::tasks) fn move_task_to_finished(
        &mut self,
        task_id: TaskId,
        finished_task: FinishedTask,
    ) {
        self.finished.insert(task_id, Arc::new(finished_task));
        self.running
            .remove(&task_id)
            .expect("Task should still be in the running map");
    }

    pub(in crate::tasks) fn get_task(&self, task_id: &TaskId) -> Option<SomeTask<'_>> {
        if let Some(task_data) = self.running.get(task_id) {
            return Some(SomeTask::Running(task_data));
        }
        self.finished.get(task_id).map(SomeTask::Finished)
    }
}
