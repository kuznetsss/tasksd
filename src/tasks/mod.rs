mod finished_task;
mod info;
mod output_buffer;
// TODO bring back when pty task is added
//mod pty;
//mod pty_reader;
mod recent_finished_tasks;
mod sender;
mod task;
mod task_error;
mod task_manager;
mod task_registry;

pub use info::TaskInfo;
pub use sender::{CHANNEL_CAPACITY, OutputLine, TaskEvent, TaskEventsStream};
pub use task_error::TaskError;
pub use task_manager::{AnyTask, TaskEntry, TaskId, TaskList, TaskManager, TaskStatus};
pub use task_registry::TaskExitNotificationGate;
