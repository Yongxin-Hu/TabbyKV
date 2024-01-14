use structopt::clap::arg_enum;
use structopt::StructOpt;
use tabby_kv::engines::KvStore;
use tabby_kv::server::Server;


arg_enum! {
    #[derive(Debug, Copy, Clone, PartialEq, Eq)]
    enum EngineType {
        KvStore
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
    default_value = "KvStore")]
    engine: EngineType
}

#[tokio::main]
async fn main() {
    let arg = Arg::from_args();

    let mut server = Server::new(match arg.engine{
        EngineType::KvStore => KvStore::new()
    });

    server.run(&arg.host, &arg.port).await.expect("Error");
}




