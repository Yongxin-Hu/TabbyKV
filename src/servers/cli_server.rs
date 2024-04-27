use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use crate::engines::Engine;
use crate::entity::command::Command;
use crate::entity::commons::Code;
use crate::entity::errors::OpsError;
use crate::entity::request::Request;
use crate::entity::response::{Response, Value, ValueObject};


#[derive(Clone)]
pub struct CliServer<E: Engine>{
    engine: E,
}

impl<E: Clone+Engine+Send + 'static> CliServer<E> {
    pub fn new(engine: E) -> Self{
        CliServer{
            engine
        }
    }

    pub async fn run(&mut self, host: &String, port: &String) -> anyhow::Result<Response> {
        let listener = TcpListener::bind(format!("{}:{}", host, port))
            .await.unwrap();
        let server = Arc::new(Mutex::new(self.clone()));
        loop {
            let (mut socket, socket_addr) = listener.accept().await.unwrap();
            let server_clone = Arc::clone(&server);
            tokio::spawn(async move {
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
                                    value
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
                                    value: Value::Empty
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

    fn process(&mut self, command: Command) -> anyhow::Result<(String, Value)> {
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
}
