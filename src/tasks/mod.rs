mod finished_task;
mod info;
mod output_buffer;
mod pty;
mod pty_reader;
mod recent_finished_tasks;
mod sender;
mod task;
mod task_error;
mod task_manager;

pub use sender::{TaskEvent, TaskEventsStream};
pub use task::TaskReadingGate;
pub use task_error::TaskError;
pub use task_manager::{TaskId, TaskManager};
