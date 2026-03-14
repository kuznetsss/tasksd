use std::{
    error::Error,
    fmt::{Display, write},
};

#[derive(Debug)]
pub enum TaskError {
    InvalidDirectory,
    PtyCreationError(String),
    StartingChildProcessError(String),
}

impl TaskError {
    pub fn pty_creation_error(e: impl ToString) -> TaskError {
        TaskError::PtyCreationError(e.to_string())
    }

    pub fn starting_child_process_error(e: impl ToString) -> TaskError {
        TaskError::StartingChildProcessError(e.to_string())
    }
}

impl Display for TaskError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskError::InvalidDirectory => write!(
                f,
                "Current working directory doesn't exist or there is not enough permissions to use it"
            ),
            TaskError::PtyCreationError(details) => write!(f, "Error creating pty: {}", details),
            TaskError::StartingChildProcessError(details) => {
                write!(f, "Error starting child process: {}", details)
            }
        }
    }
}

impl Error for TaskError {}
