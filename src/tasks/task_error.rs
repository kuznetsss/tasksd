use std::{error::Error, fmt::Display};

#[derive(Debug)]
pub enum TaskError {
    InvalidDirectory,
    PtyCreationError(String),
    StartingChildProcessError(String),
    WriteError(String),
    AlreadyFinished,
    SendSignalError(String),
}

impl TaskError {
    pub fn pty_creation_error(e: impl ToString) -> TaskError {
        TaskError::PtyCreationError(e.to_string())
    }

    pub fn starting_child_process_error(e: impl ToString) -> TaskError {
        TaskError::StartingChildProcessError(e.to_string())
    }

    pub fn write_error(e: impl ToString) -> TaskError {
        TaskError::WriteError(e.to_string())
    }

    pub fn send_signal_error(e: impl ToString) -> TaskError {
        TaskError::SendSignalError(e.to_string())
    }
}

impl Display for TaskError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskError::InvalidDirectory => write!(
                f,
                "Current working directory doesn't exist or there is not enough permissions to use it"
            ),
            TaskError::PtyCreationError(details) => write!(f, "Error creating pty: {details}"),
            TaskError::StartingChildProcessError(details) => {
                write!(f, "Error starting child process: {details}")
            }
            TaskError::WriteError(details) => write!(f, "Error writing message: {details}"),
            TaskError::AlreadyFinished => write!(f, "Error: the task has already finished"),
            TaskError::SendSignalError(details) => write!(f, "Error sending signal: {details}"),
        }
    }
}

impl Error for TaskError {}
