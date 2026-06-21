use std::{fmt::Display, path::PathBuf};

use rustix::path::Arg;

#[derive(Debug)]
pub enum TransportError {
    Eof,
    UnexpectedSymbols(String),
    IoError(tokio::io::Error),
    UnixSocketError(tokio::io::Error, PathBuf),
    WriteError(String),
    HeaderParseError(String),
}

impl Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransportError::Eof => write!(f, "EOF"),
            TransportError::UnexpectedSymbols(msg) => write!(f, "Unexpected symbols: {msg}"),
            TransportError::IoError(error) => write!(f, "IO error: {error}"),
            TransportError::WriteError(details) => write!(f, "Write error: {details}"),
            TransportError::HeaderParseError(details) => {
                write!(f, "Error parsing header: {details}")
            }
            TransportError::UnixSocketError(error, path_buf) => write!(
                f,
                "Error opening unix socket '{}': {error}",
                path_buf.as_str().unwrap_or("invalid")
            ),
        }
    }
}

impl std::error::Error for TransportError {}

impl From<tokio::io::Error> for TransportError {
    fn from(value: tokio::io::Error) -> Self {
        match value.kind() {
            std::io::ErrorKind::UnexpectedEof => Self::Eof,
            _ => Self::IoError(value),
        }
    }
}

impl From<std::str::Utf8Error> for TransportError {
    fn from(value: std::str::Utf8Error) -> Self {
        Self::UnexpectedSymbols(value.to_string())
    }
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for TransportError {
    fn from(value: tokio::sync::mpsc::error::SendError<T>) -> Self {
        Self::WriteError(value.to_string())
    }
}
