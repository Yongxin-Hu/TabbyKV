use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error,Serialize, Deserialize)]
pub enum OpsError{
    #[error("Command not found, try 'get' | 'set' | 'remove' ")]
    CommandNotFound,
    #[error("Please enter command.")]
    EmptyCommand,
    #[error("Command argument number error, expect {0} but got {1}")]
    ArgumentNumError(usize, usize),
    #[error("key not found")]
    KeyNotFound,
    #[error("serde_json error")]
    SerdeError,
    #[error("io error")]
    IOError,
}

impl From<serde_json::Error> for OpsError {
    fn from(_err: serde_json::Error) -> OpsError {
        OpsError::SerdeError
    }
}

impl From<std::io::Error> for OpsError {
    fn from(_err: std::io::Error) -> OpsError {
        OpsError::IOError
    }
}