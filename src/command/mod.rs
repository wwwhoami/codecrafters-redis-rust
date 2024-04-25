use crate::{connection::Connection, frame::Frame, parse::Parse, Db, Info as ServerInfo};

mod echo;
use echo::Echo;

mod ping;
pub use ping::Ping;

mod set;
use set::Set;

mod get;
use get::Get;

mod keys;
pub use keys::Keys;

mod info;
use info::Info;

pub mod replconf;
use replconf::ReplConf;

pub mod psync;
use psync::Psync;

pub mod wait;
use wait::Wait;

pub mod config;
use config::Config;

pub mod get_type;
use get_type::Type;

mod xadd;
use xadd::XAdd;
pub use xadd::XAddId;

#[derive(Debug)]
pub struct Command;

impl Command {
    /// Parse the frame into a command
    ///
    /// # Errors
    ///
    /// This function will return an error if the frame is not a valid command
    pub fn from_frame(frame: Frame) -> crate::Result<Box<dyn CommandTrait + Send>> {
        let mut frames = Parse::new(frame)?;

        let command: Box<dyn CommandTrait + Send> =
            match frames.next_string()?.to_uppercase().as_str() {
                "ECHO" => Box::new(Echo::parse_frames(&mut frames)?),
                "PING" => Box::new(Ping::parse_frames(&mut frames)?),
                "SET" => Box::new(Set::parse_frames(&mut frames)?),
                "GET" => Box::new(Get::parse_frames(&mut frames)?),
                "KEYS" => Box::new(Keys::parse_frames(&mut frames)?),
                "INFO" => Box::new(Info::parse_frames(&mut frames)?),
                "REPLCONF" => Box::new(ReplConf::parse_frames(&mut frames)?),
                "PSYNC" => Box::new(Psync::parse_frames(&mut frames)?),
                "WAIT" => Box::new(Wait::parse_frames(&mut frames)?),
                "CONFIG" => Box::new(Config::parse_frames(&mut frames)?),
                "TYPE" => Box::new(Type::parse_frames(&mut frames)?),
                "XADD" => Box::new(XAdd::parse_frames(&mut frames)?),
                cmd => return Err(format!("Protocol error: unknown command {:?}", cmd).into()),
            };

        frames.finish()?;

        Ok(command)
    }

    /// Parse the frame into a command
    /// Used for replica commands parsing
    ///
    /// # Errors
    ///
    /// This function will return an error if the frame is not a valid command
    pub fn from_frame_writes(frame: Frame) -> crate::Result<Box<dyn CommandTrait + Send>> {
        let mut frames = Parse::new(frame)?;

        let command: Box<dyn CommandTrait + Send> =
            match frames.next_string()?.to_uppercase().as_str() {
                "SET" => Box::new(Set::parse_frames(&mut frames)?),
                "REPLCONF" => Box::new(ReplConf::parse_frames(&mut frames)?),
                "PING" => Box::new(Ping::parse_frames(&mut frames)?),
                cmd => {
                    return Err(format!("Protocol error: not a 'write' command {:?}", cmd).into())
                }
            };

        frames.finish()?;

        Ok(command)
    }

    /// Parse the frame into a command
    /// Then execute the command
    ///
    /// # Returns
    ///
    /// Returns response to the command as a Frame
    /// And the byte length of the parsed frame
    ///
    /// # Errors
    ///
    /// Returns an error if the frame is not a valid command
    pub async fn execute(
        frame: Frame,
        db: &Db,
        server_info: &mut ServerInfo,
        connection: Connection,
    ) -> (Frame, usize) {
        match Command::from_frame(frame) {
            Ok(command) => {
                match command.as_any().downcast_ref::<Wait>() {
                    Some(wait_command) => {
                        // let mut parse_frame = Parse::new(frame.clone()).unwrap();
                        // let command = command.parse_frames(&mut parse_frame).unwrap();
                        let count = server_info
                            .count_sync_repl(wait_command.replica_count, wait_command.timeout)
                            .await;

                        (
                            Frame::Integer(count),
                            command.to_frame().encode().bytes().len(),
                        )
                    }
                    None => (
                        command.execute(db, server_info, connection),
                        command.to_frame().encode().bytes().len(),
                    ),
                }
            }
            Err(err) => (Frame::Error(err.to_string()), 0),
        }
    }

    /// Parse the frame into a command for replica
    /// Then execute the command as a replica
    ///
    /// # Returns
    ///
    /// Returns response to the command as a Frame
    /// And the byte length of the parsed frame
    ///
    /// # Errors
    ///
    /// Returns an error if the frame is not a valid command
    pub fn execute_replica(
        frame: Frame,
        db: &Db,
        server_info: &mut ServerInfo,
        connection: Connection,
    ) -> (Frame, usize) {
        match Command::from_frame_writes(frame) {
            Ok(command) => (
                command.execute_replica(db, server_info, connection),
                command.to_frame().encode().bytes().len(),
            ),
            Err(err) => (Frame::Error(err.to_string()), 0),
        }
    }

    pub fn is_propagatable(frame: Frame) -> crate::Result<bool> {
        let mut frames = Parse::new(frame)?;

        match frames.next_string()?.to_uppercase().as_str() {
            "SET" => Ok(true),
            _ => Ok(false),
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
    fn execute(&self, db: &Db, server_info: &mut ServerInfo, connection: Connection) -> Frame;

    fn execute_replica(
        &self,
        db: &Db,
        server_info: &mut ServerInfo,
        connection: Connection,
    ) -> Frame;

    /// Convert the command to a Frame
    /// Returns the command as a Frame
    fn to_frame(&self) -> Frame;

    fn as_any(&self) -> &dyn std::any::Any;
}
