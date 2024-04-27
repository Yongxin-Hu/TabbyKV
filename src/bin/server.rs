use std::cell::RefCell;
use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use structopt::clap::arg_enum;
use structopt::StructOpt;
use tonic::transport::Server;

use tabby_kv::engines::LsmStore;
use tabby_kv::servers::cli_server::CliServer;
use tabby_kv::servers::grpc_server::GrpcServer;
use tabby_kv::servers::grpc_server::tabbykv::tabbykv_rpc_server::{TabbykvRpc, TabbykvRpcServer};

arg_enum! {
    #[derive(Debug, Copy, Clone, PartialEq, Eq)]
    enum EngineType {
        LsmStore
    }
}
arg_enum! {
    #[derive(Debug, Copy, Clone, PartialEq, Eq)]
    enum ModeType {
        Cli,
        Grpc,
        Http
    }
}

#[derive(Debug, StructOpt)]
#[structopt(name = "server", about = "Running server")]
struct Arg{
    #[structopt(short, default_value = "127.0.0.1")]
    host: String,
    #[structopt(short, default_value = "6381")]
    port: String,
    #[structopt(long,
    possible_values = &ModeType::variants(),
    case_insensitive = true,
    default_value = "Cli")]
    mode: ModeType,
    #[structopt(long,
    possible_values = &EngineType::variants(),
    case_insensitive = true,
    default_value = "LsmStore")]
    engine: EngineType
}

#[tokio::main]
async fn main() -> Result<()>{
    let arg = Arg::from_args();
    // TODO temp
    let dir = Path::new(r"D:\temp\db3");
    let engine = match arg.engine{
        EngineType::LsmStore => LsmStore::new(&dir)?
    };
    match arg.mode{
        ModeType::Cli => {
            let mut cli_server = CliServer::new(engine);
            cli_server.run(&arg.host, &arg.port).await.expect("Error");
        }
        ModeType::Grpc => {
            let addr = format!("{}:{}", &arg.host, &arg.port).parse()?;
            let grpc_server = GrpcServer::new(Arc::new(Mutex::new(engine)));
            Server::builder()
                .add_service(TabbykvRpcServer::new(grpc_server))
                .serve(addr)
                .await?;
        }
        ModeType::Http => unreachable!()
    }
    Ok(())
}