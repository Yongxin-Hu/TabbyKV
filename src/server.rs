use std::sync::{Arc};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use crate::common::{Code, Command, MetaData, OpsError, Request, Response, Value, ValueObject};
use crate::engines::Engine;
use anyhow::{Error, Result};

#[derive(Clone)]
pub struct Server<E: Engine>{
    engine: E,
}

impl<E: Clone+Engine+Send + 'static> Server<E> {
    pub fn new(engine: E) -> Self{
        Server{
            engine
        }
    }

    pub async fn run(&mut self, host: &String, port: &String) -> Result<Response>{
        let listener = TcpListener::bind(format!("{}:{}", host, port))
            .await.unwrap();
        let server = Arc::new(Mutex::new(self.clone()));
        loop {
            let (mut socket, socket_addr) = listener.accept().await.unwrap();
            let server_clone = Arc::clone(&server);
            tokio::spawn(async move {
                println!("server count:{}", Arc::strong_count(&server_clone));
                loop{
                    let (reader, writer) = socket.split();
                    let mut reader = BufReader::new(reader);
                    let mut writer = BufWriter::new(writer);
                    let mut result = "".into();
                    reader.read_line(&mut result).await.expect("TODO");
                    let request = serde_json::from_str::<Request>(&result).unwrap();
                    let command = request.command;
                    if command == Command::Exit {
                        break;
                    }
                    println!("{socket_addr} : {command:?}");
                    let mut inner_server = server_clone.lock().await;

                    match inner_server.process(command) {
                        Ok((message, value)) => {
                            let response = Response{
                                code: Code::OK,
                                message,
                                value: ValueObject{
                                    value,
                                    meta: MetaData {
                                        timestamp: 0,
                                        expire: None,
                                    },
                                },
                            };
                            let response = serde_json::to_string(&response).expect("Serde json error!");
                            let _ = writer.write(format!("{response}\n").as_bytes()).await;
                            let _ = writer.flush().await;
                        }
                        Err(e) => {
                            let response = Response {
                                code: Code::Err,
                                message: e.to_string(),
                                value: ValueObject{
                                    value: Value::Empty,
                                    meta: MetaData {
                                        timestamp: 0,
                                        expire: None,
                                    },
                                },
                            };
                            let response = serde_json::to_string(&response).expect("Serde json error!");
                            let _ = writer.write(format!("{response}\n").as_bytes()).await;
                            let _ = writer.flush().await;
                        }
                    };
                }
            });

        }
    }

    fn process(&mut self, command: Command) -> Result<(String, Value)>{
        match command{
            Command::Get(key) => {
                let value = self.engine.get(&key)?;
                match value{
                    Some(s) => Ok((s.to_owned(), Value::VString(s.clone()))),
                    None => Err(OpsError::KeyNotFound.into()),
                }
            },
            Command::Set(key,value) => {
                self.engine.put(key,value)?;
                Ok(("Ok".to_string(), Value::Empty))
            },
            Command::Remove(key) => {
                self.engine.delete(&key)?;
                Ok(("Ok".to_string(), Value::Empty))
            },
            _ => {Err(OpsError::EmptyCommand.into())}
        }
    }

    pub fn close(&self) -> Result<()> {
        self.engine.close()
    }
}