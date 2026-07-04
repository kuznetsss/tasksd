use std::fmt::Display;

use crate::tasks::TaskError;

#[derive(Debug)]
pub enum ApplicationError {
    Shutdown,
    TaskError(TaskError),
}

impl Display for ApplicationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ApplicationError::Shutdown => write!(f, "application is shutting down"),
            ApplicationError::TaskError(task_error) => write!(f, "{task_error}"),
        }
    }
}

impl std::error::Error for ApplicationError {}

impl From<TaskError> for ApplicationError {
    fn from(value: TaskError) -> Self {
        Self::TaskError(value)
    }
}
