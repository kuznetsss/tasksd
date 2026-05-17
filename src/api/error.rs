use std::{error::Error, fmt::Display};

#[derive(Debug)]
pub enum ApiError {
    InvalidRpcJsonVersion(String),
}

impl Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ApiError::InvalidRpcJsonVersion(s) => {
                write!(f, "Invalid jsonrpc version '{s}'. Only '2.0' is supported.")
            }
        }
    }
}

impl Error for ApiError {}

