use serde::{Deserialize, Serialize};
use crate::entity::commons::Code;

#[derive(Debug,Serialize, Deserialize)]
pub struct Response{
    pub code: Code,
    pub message: String,
    pub value: ValueObject,
}

#[derive(Debug,Serialize, Deserialize)]
pub enum Value{
    Empty,
    VString(String),
    VList(Vec<String>),
}

#[derive(Debug,Serialize, Deserialize)]
pub struct ValueObject{
    pub value: Value,
}