mod echo;

mod ping;
pub use ping::Ping;

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
pub struct Command;

impl Command {
    /// Parse the frame into a command
    ///
    /// # Errors
    ///
    /// This function will return an error if the frame is not a valid command
    pub fn from_frame(frame: Frame) -> crate::Result<Box<dyn CommandTrait>> {
        let mut frames = Parse::new(frame)?;

        let command: Box<dyn CommandTrait> = match frames.next_string()?.to_uppercase().as_str() {
            "ECHO" => Box::new(Echo::parse_frames(&mut frames)?),
            "PING" => Box::new(Ping::parse_frames(&mut frames)?),
            "SET" => Box::new(Set::parse_frames(&mut frames)?),
            "GET" => Box::new(Get::parse_frames(&mut frames)?),
            "INFO" => Box::new(Info::parse_frames(&mut frames)?),
            "REPLCONF" => Box::new(ReplConf::parse_frames(&mut frames)?),
            "PSYNC" => Box::new(Psync::parse_frames(&mut frames)?),
            cmd => return Err(format!("Protocol error: unknown command {:?}", cmd).into()),
        };

        frames.finish()?;

        Ok(command)
    }

    /// Execute the command from the given frame
    /// Returns the result as a Frame
    pub fn execute(frame: Frame, db: &Db, server_info: &server::Info) -> Frame {
        match Command::from_frame(frame) {
            Ok(command) => command.execute(db, server_info),
            Err(err) => Frame::Error(err.to_string()),
        }
    }

    pub fn to_frame(command: &dyn CommandTrait) -> Frame {
        command.to_frame()
    }
}

pub trait CommandTrait {
    /// Parse the frames into a command
    ///
    /// # Errors
    ///
    /// This function will return an error if the frame is not a valid command
    fn parse_frames(&self, frames: &mut Parse) -> crate::Result<Box<dyn CommandTrait>>;

    /// Execute the command
    /// Returns the result as a Frame
    fn execute(&self, db: &Db, server_info: &server::Info) -> Frame;

    /// Convert the command to a Frame
    /// Returns the command as a Frame
    fn to_frame(&self) -> Frame;
}
