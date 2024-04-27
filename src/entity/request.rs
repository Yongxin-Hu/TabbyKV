use serde::{Deserialize, Serialize};
use crate::entity::command::Command;


#[derive(Debug,Serialize, Deserialize)]
pub struct Request{
    pub command: Command
}