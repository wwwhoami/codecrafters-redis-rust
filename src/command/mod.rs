mod echo;

mod ping;
use ping::Ping;

mod set;
use set::Set;

mod get;
use get::Get;

mod info;
use info::Info;

pub mod replconf;
use replconf::ReplConf;

pub mod psync;
use psync::Psync;

use crate::{frame::Frame, parse::Parse, server, Db};
use echo::Echo;

#[derive(Debug)]
pub enum Command {
    Echo(Echo),
    Ping(Ping),
    Set(Set),
    Get(Get),
    ReplConf(ReplConf),
    Info(Info),
    Psync(Psync),
}

impl Command {
    pub fn from_frame(frame: Frame) -> crate::Result<Command> {
        let mut frames = Parse::new(frame)?;

        let command = match frames.next_string()?.to_uppercase().as_str() {
            "ECHO" => Command::Echo(Echo::parse_frames(&mut frames)?),
            "PING" => Command::Ping(Ping::parse_frames(&mut frames)?),
            "SET" => Command::Set(Set::parse_frames(&mut frames)?),
            "GET" => Command::Get(Get::parse_frames(&mut frames)?),
            "INFO" => Command::Info(Info::parse_frames(&mut frames)?),
            "REPLCONF" => Command::ReplConf(ReplConf::parse_frames(&mut frames)?),
            "PSYNC" => Command::Psync(Psync::parse_frames(&mut frames)?),
            cmd => return Err(format!("Protocol error: unknown command {:?}", cmd).into()),
        };

        frames.finish()?;

        Ok(command)
    }

    pub fn to_frame(&self) -> Frame {
        match self {
            Command::Echo(echo) => echo.to_frame(),
            Command::Ping(ping) => ping.to_frame(),
            Command::Set(set) => set.to_frame(),
            Command::Get(get) => get.to_frame(),
            Command::Info(info) => info.to_frame(),
            Command::ReplConf(replconf) => replconf.to_frame(),
            Command::Psync(psync) => psync.to_frame(),
        }
    }

    pub fn execute(&self, db: &Db, server_info: &server::Info) -> Frame {
        match self {
            Command::Echo(echo) => echo.execute(),
            Command::Ping(ping) => ping.execute(),
            Command::Set(set) => set.execute(db),
            Command::Get(get) => get.execute(db),
            Command::Info(info) => info.execute(server_info),
            Command::ReplConf(replconf) => replconf.execute(),
            Command::Psync(psync) => psync.execute(),
        }
    }
}
