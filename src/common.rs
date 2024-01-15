use std::time::Duration;
use log::error;
use thiserror::Error;
use serde::{Serialize, Deserialize};


/// Request
#[derive(Debug,Serialize, Deserialize)]
pub struct Request{
    pub command: Command,
    pub meta: MetaData
}

/// Response
#[derive(Debug,Serialize, Deserialize)]
pub struct Response{
    pub message: String,
    pub value: Result<ValueObject, OpsError>
}

/// Client command
#[derive(Debug,Serialize, Deserialize, PartialEq,Clone)]
pub enum Command {
    Get(String),
    Set(String, String),
    Remove(String),
    Exit
}

#[derive(Debug,Serialize, Deserialize)]
pub enum Value{
    VString(String),
    VList(Vec<String>),
}

#[derive(Debug,Serialize, Deserialize)]
pub struct MetaData{
    pub timestamp: u128,
    pub expire: Option<Duration>,
}

#[derive(Debug,Serialize, Deserialize)]
pub struct ValueObject{
    pub value: Value,
    pub meta: MetaData
}

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

impl From<serde_json::Error> for OpsError{
    fn from(_err: serde_json::Error) -> OpsError {
        OpsError::SerdeError
    }
}

impl From<std::io::Error> for OpsError{
    fn from(_err: std::io::Error) -> OpsError {
        OpsError::IOError
    }
}