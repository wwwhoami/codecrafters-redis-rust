mod echo;
use echo::Echo;

mod ping;
use ping::Ping;

mod set;
use set::Set;

mod get;
use get::Get;

mod info;
use info::Info;

use crate::{frame::Frame, parse::Parse};

#[derive(Debug)]
pub enum Command {
    Echo(Echo),
    Ping(Ping),
    Set(Set),
    Get(Get),
    Info(()),
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
            cmd => return Err(format!("Protocol error: unknown command {:?}", cmd).into()),
        };

        frames.finish()?;

        Ok(command)
    }
}
