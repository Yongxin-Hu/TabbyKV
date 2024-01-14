use crate::common::{OpsError, Command};

pub struct Parse;

impl Parse {
    pub fn parse_command(command: String) -> Result<Command, OpsError>{
        /// 检查参数数量
        fn check_subcommand(subcommand: &str, command: &Vec<&str>) -> Result<(), OpsError>{
            match subcommand {
                "get" => {
                    if command.len() != 2 {
                        return Err(OpsError::ArgumentNumError(1, command.len()-1));
                    }
                    Ok(())
                },
                "set" => {
                    if command.len() != 3 {
                        return Err(OpsError::ArgumentNumError(2, command.len()-1));
                    }
                    Ok(())
                },
                "remove" | "rm" => {
                    if command.len() != 2{
                        return Err(OpsError::ArgumentNumError(1, command.len()-1));
                    }
                    Ok(())
                },
                _ => Err(OpsError::CommandNotFound)
            }
        }
        let command = command.trim().split(" ").collect::<Vec<&str>>();
        if command.len() == 0 { return Err(OpsError::EmptyCommand);}
        match command[0] {
            "get" => {
                check_subcommand(command[0], &command)?;
                Ok(Command::Get(command[1].to_string()))
            },
            "set" => {
                check_subcommand(command[0], &command)?;
                Ok(Command::Set(command[1].to_string(), command[2].to_string()))
            },
            "remove" | "rm" => {
                check_subcommand(command[0], &command)?;
                Ok(Command::Remove(command[1].to_string()))
            },
            "Exit" | "exit" => {
                Ok(Command::Exit)
            }
            _ => Err(OpsError::CommandNotFound),
        }
    }
}