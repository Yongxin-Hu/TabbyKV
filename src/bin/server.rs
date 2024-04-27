use std::path::Path;

use anyhow::Result;
use structopt::clap::arg_enum;
use structopt::StructOpt;

use tabby_kv::engines::LsmStore;
use tabby_kv::servers::cli_server::CliServer;

arg_enum! {
    #[derive(Debug, Copy, Clone, PartialEq, Eq)]
    enum EngineType {
        LsmStore
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
    possible_values = &EngineType::variants(),
    case_insensitive = true,
    default_value = "LsmStore")]
    engine: EngineType
}

#[tokio::main]
async fn main() -> Result<()>{
    let arg = Arg::from_args();
    // TODO temp
    let dir = Path::new(r"D:\temp\db2");

    let mut cli_server = CliServer::new(match arg.engine{
        EngineType::LsmStore => LsmStore::new(&dir)?
    });
    cli_server.run(&arg.host, &arg.port).await.expect("Error");
    Ok(())
}