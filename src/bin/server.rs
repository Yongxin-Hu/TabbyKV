use std::cell::RefCell;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use structopt::clap::arg_enum;
use structopt::StructOpt;
use tonic::transport::Server;

use tabby_kv::engines::LsmStore;
use tabby_kv::servers::cli_server::CliServer;
use tabby_kv::servers::grpc_server::GrpcServer;
use tabby_kv::servers::grpc_server::tabbykv::tabbykv_rpc_server::{TabbykvRpc, TabbykvRpcServer};
use tabby_kv::engines::lsm::storage::option::LsmStorageOptions;

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

#[derive(Debug, Serialize, Deserialize)]
struct Option{
    #[cfg(target_os = "linux")]
    #[serde(rename(deserialize = "linux"))]
    path: String,
    #[cfg(target_os = "windows")]
    #[serde(rename(deserialize = "windows"))]
    path: String,
    lsm: LsmStorageOptions
}

fn init_config() -> Option {
    let config_path = Path::new("config.json");

    let mut file = File::open(config_path).expect("Config file not exist!");
    let mut content = String::new();
    file.read_to_string(&mut content).expect("Read config file fail!");
    let option:Option = serde_json::from_str(&content).expect("Parse Option fail!");

    option
}

#[tokio::main]
async fn main() -> Result<()>{
    let arg = Arg::from_args();
    let option = init_config();

    let engine = match arg.engine{
        EngineType::LsmStore => LsmStore::with_options(option.path, option.lsm)?
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