use std::cell::RefCell;
use std::sync::{Arc, Mutex};
use tonic::{Request, Response, Status};
use tabbykv::tabbykv_rpc_server::{TabbykvRpc, TabbykvRpcServer};
use tabbykv::{Req, Res, Command};
use crate::engines::Engine;

pub mod tabbykv {
    tonic::include_proto!("tabbykv");
}

#[derive(Debug,Clone)]
pub struct GrpcServer<E: Engine>{
    engine: Arc<Mutex<E>>,
}

impl<E: Clone+Engine+Send + 'static> GrpcServer<E>{
    pub fn new(engine: Arc<Mutex<E>>) -> Self{
        GrpcServer{
            engine
        }
    }
}

#[tonic::async_trait]
impl<E:Clone+Engine+Send + 'static> TabbykvRpc for GrpcServer<E>{
    async fn service(
        &self,
        request: Request<Req>
    ) -> Result<Response<Res>, Status> {

        let command = request.into_inner().command.unwrap();

        match command.code {
            1 /* get */ => {
                let key = command.key;
                let engine = self.engine.lock().unwrap();
                match engine.get(&key) {
                    Ok(v) => {
                        match v {
                            Some(value) => {
                                let response = Res {
                                    code: 1,
                                    message: value
                                };
                                Ok(Response::new(response))
                            },
                            None => {
                                let response = Res{
                                    code: 2,
                                    message: "key not found!".to_string()
                                };
                                Ok(Response::new(response))
                            }
                        }
                    },
                    Err(e) => {
                        let response = Res{
                            code: 3,
                            message: "fail to get!".to_string()
                        };
                        Ok(Response::new(response))
                    }
                }
            },
            2 /* put */ => {
                let key = command.key;
                let value = command.value;
                let mut engine = self.engine.lock().unwrap();
                match engine.put(key, value){
                    Ok(()) => {
                        let response = Res {
                            code: 1,
                            message: "Ok".to_string(),
                        };
                        Ok(Response::new(response))
                    },
                    Err(e) => {
                        let response = Res {
                            code: 3,
                            message: "fail to put!".to_string(),
                        };
                        Ok(Response::new(response))
                    }
                }
            },
            3 /* delete */ => {
                let key = command.key;
                let mut engine = self.engine.lock().unwrap();
                match engine.delete(&key){
                    Ok(()) => {
                        let response = Res {
                            code: 1,
                            message: "Ok".to_string(),
                        };
                        Ok(Response::new(response))
                    },
                    Err(e) => {
                        let response = Res {
                            code: 3,
                            message: "fail to delete!".to_string(),
                        };
                        Ok(Response::new(response))
                    }
                }
            },
            _ => unreachable!()
        }
    }
}



