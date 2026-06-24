mod events;
mod finished_task;
mod info;
mod output_buffer;
mod pty;
mod pty_reader;
mod recent_finished_tasks;
mod senders;
mod task;
mod task_builder;
mod task_error;
mod task_manager;
mod tracker;

#[cfg(test)]
mod test_subscribers;

pub use events::{TaskEventsSubscriber, TaskSubscriberError};
pub use task_error::TaskError;
pub use task_manager::{TaskId, TaskManager};
