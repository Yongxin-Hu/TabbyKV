use std::io;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::{TcpStream, ToSocketAddrs};
use crate::common::{Command, MetaData, OpsError, Request, Response};
use std::time::{SystemTime, UNIX_EPOCH};

pub struct Client{
    reader: BufReader<TcpStream>,
    writer: BufWriter<TcpStream>,
}

impl Client{
    pub fn connect<A: ToSocketAddrs>(addr: A) -> Result<Self, io::Error> {
        let tcp_reader = TcpStream::connect(addr)?;
        let tcp_writer = tcp_reader.try_clone()?;
        Ok(Client {
            reader: BufReader::new(tcp_reader),
            writer: BufWriter::new(tcp_writer),
        })
    }

    pub fn send(&mut self, command: Command) -> Result<(), OpsError>{
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let request = Request{
            command,
            meta: MetaData {
                timestamp,
                expire: None
            },
        };
        let request = serde_json::to_string(&request)?;
        self.writer.write(format!("{request}\n").as_bytes())?;
        self.writer.flush()?;
        Ok(())
    }

    pub fn read(&mut self) -> Result<Response, OpsError>{
        let mut response = String::new();
        self.reader.read_line(&mut response)?;
        let response = serde_json::from_str::<Response>(&response)?;
        Ok(response)
    }
}