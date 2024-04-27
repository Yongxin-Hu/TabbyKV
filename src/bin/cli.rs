use dialoguer::Input;
use structopt::StructOpt;

use tabby_kv::client::Client;
use tabby_kv::entity::command::Command;
use tabby_kv::entity::errors::OpsError;
use tabby_kv::parse::Parse;

#[derive(Debug, StructOpt)]
#[structopt(name = "server", about = "Running server")]
struct Arg{
    #[structopt(short, default_value = "127.0.0.1")]
    host: String,
    #[structopt(short, default_value = "6381")]
    port: String
}


fn main() -> Result<(), OpsError>{
    draw_logo();
    let arg = Arg::from_args();
    let mut client = Client::connect(format!("{}:{}", arg.host, arg.port))?;
    loop{
        let command: String = Input::new().with_prompt("Enter command").interact().expect("Interact error");
        match Parse::parse_command(command){
            Ok(ops) if ops == Command::Exit => {
                client.send(ops)?;
                break;
            },
            Ok(ops) => {
                client.send(ops)?;
            },
            Err(e) => {
                println!("{e:?}");
                continue;
            }
        }
        let resp = client.read()?;
        println!("{}", resp.message);
    }
    Ok(())
}

fn draw_logo(){

    let logo = r#"
     _______      _      _              _  ____      __
    |__   __|    | |    | |            | |/ /\ \    / /
       | |  __ _ | |__  | |__   _   _  | ' /  \ \  / /
       | | / _` || '_ \ | '_ \ | | | | |  <    \ \/ /
       | || (_| || |_) || |_) || |_| | | . \    \  /
       |_| \__,_||_.__/ |_.__/  \__, | |_|\_\    \/
                                 __/ |
                                |___/
    "#;
    println!("{logo}");
}