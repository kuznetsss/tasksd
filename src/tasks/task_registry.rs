use std::{collections::HashMap, sync::Arc};

use crate::tasks::{
    TaskId,
    finished_task::FinishedTask,
    gate::{Gate, GateGuard},
    recent_finished_tasks::RecentFinishedTasks,
    task::Task,
    task_manager::RunningTaskHandle,
};

#[derive(Debug)]
pub struct TaskExitNotificationTag;

pub type TaskExitNotificationGate = Gate<TaskExitNotificationTag>;
type TaskExitNotificationGateGuard = GateGuard<TaskExitNotificationTag>;

#[derive(Debug)]
pub(in crate::tasks) struct RunningTaskEntry {
    pub(in crate::tasks) task: Arc<Task>,
    pub(in crate::tasks) exit_notification_gate_guard: TaskExitNotificationGateGuard,
}

impl RunningTaskEntry {
    pub(in crate::tasks) fn handle(&self) -> RunningTaskHandle {
        RunningTaskHandle {
            task: self.task.clone(),
            exit_notification_gate: self.exit_notification_gate_guard.gate(),
        }
    }
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
        exit_notification_gate_guard: TaskExitNotificationGateGuard,
    ) {
        self.running.insert(
            task_id,
            RunningTaskEntry {
                task,
                exit_notification_gate_guard,
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
        if let Some(task_entry) = self.running.get(task_id) {
            return Some(TaskRef::Running(task_entry));
        }
        self.finished.get(task_id).map(TaskRef::Finished)
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::{assert_matches, env::current_dir, time::Duration};

    use super::*;
    use crate::tasks::{TaskInfo, gate::gate};

    fn make_task() -> (Arc<Task>, TaskExitNotificationGateGuard) {
        let task_info = TaskInfo {
            executable: "ls".into(),
            args: Vec::new(),
            working_dir: current_dir().unwrap(),
        };
        let (task, _) = Task::new(task_info, 1000).unwrap();
        let (_, exit_notification_gate_guard) = gate();
        (Arc::new(task), exit_notification_gate_guard)
    }

    #[tokio::test]
    async fn add_running_adds_task_to_running() {
        let mut tr = TaskRegistry::new(10);
        let (task, gate_guard) = make_task();
        let task_id = TaskId(123);

        tr.add_running(task_id, task.clone(), gate_guard);

        assert_eq!(tr.iter_running().len(), 1);
        assert_eq!(tr.iter_finished().len(), 0);
        assert_matches!(tr.get(&task_id), Some(TaskRef::Running(td)) if Arc::ptr_eq(&task, &td.task));
        task.join().await;
    }

    #[tokio::test]
    async fn mark_finished_moves_task_to_finished() {
        let mut tr = TaskRegistry::new(10);
        let (task, gate_guard) = make_task();
        let task_id = TaskId(123);

        tr.add_running(task_id, task.clone(), gate_guard);
        let finished_task = task.join().await;
        tr.mark_finished(task_id, finished_task);

        assert_eq!(tr.iter_running().len(), 0);
        assert_eq!(tr.iter_finished().len(), 1);
        assert_matches!(tr.get(&task_id), Some(TaskRef::Finished(_)));
    }

    #[tokio::test]
    async fn mark_finished_opens_task_gate() {
        let mut tr = TaskRegistry::new(10);
        let (task, gate_guard) = make_task();
        let task_id = TaskId(123);

        let mut gate_future = tokio_test::task::spawn({
            let mut gate = gate_guard.gate();
            async move {
                gate.wait().await;
            }
        });

        assert!(gate_future.poll().is_pending());

        tr.add_running(task_id, task.clone(), gate_guard);
        assert!(gate_future.poll().is_pending());

        let finished_task = tokio::time::timeout(Duration::from_secs(1), task.join())
            .await
            .unwrap();
        assert!(gate_future.poll().is_pending());

        tr.mark_finished(task_id, finished_task);
        assert!(gate_future.is_woken());
        assert!(gate_future.poll().is_ready());
    }

    #[test]
    fn empty_task_registry() {
        let tr = TaskRegistry::new(10);
        assert_eq!(tr.iter_running().len(), 0);
        assert_eq!(tr.iter_finished().len(), 0);
        assert_matches!(tr.get(&TaskId(123)), None);
    }
}
