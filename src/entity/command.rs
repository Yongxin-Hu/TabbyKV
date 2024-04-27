use serde::{Deserialize, Serialize};

#[derive(Debug,Serialize, Deserialize, PartialEq,Clone)]
pub enum Command {
    Get(String),
    Set(String, String),
    Remove(String),
    Exit
}