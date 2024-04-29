use async_trait::async_trait;

use crate::{connection::Connection, Db, Frame, Info as ServerInfo, Parse};

use super::CommandTrait;

#[derive(Debug)]
pub enum Config {
    Dir,
    DbFilename,
}

impl Config {
    pub fn parse_frames(frames: &mut Parse) -> crate::Result<Config> {
        match frames.next_string() {
            Ok(section) => match section.as_str().to_lowercase().as_str() {
                "get" => Config::parse_get(frames),
                _ => Err(format!("Protocol error: unsupported Config section: {}", section).into()),
            },
            Err(err) => Err(err.into()),
        }
    }

    fn parse_get(frames: &mut Parse) -> crate::Result<Config> {
        let get_arg = frames.next_string()?.as_str().to_lowercase();

        match get_arg.as_str() {
            "dir" => Ok(Config::Dir),
            "dbfilename" => Ok(Config::DbFilename),
            _ => Err("Protocol error: expected command: Config get".into()),
        }
    }

    pub fn to_frame(&self) -> Frame {
        Frame::Simple("Config".into())
    }

    pub fn execute(&self, server_info: &mut ServerInfo) -> Frame {
        let (key, value) = match self {
            Config::Dir => ("dir", server_info.dir().to_string()),
            Config::DbFilename => ("dbfilename", server_info.dbfilename().to_string()),
        };

        Frame::Array(vec![Frame::Bulk(key.into()), Frame::Bulk(value.into())])
    }
}

#[async_trait]
impl CommandTrait for Config {
    fn parse_frames(&self, frames: &mut Parse) -> crate::Result<Box<dyn CommandTrait>> {
        Ok(Box::new(Config::parse_frames(frames)?))
    }

    async fn execute(
        &self,
        _db: &Db,
        server_info: &mut ServerInfo,
        _connection: Connection,
    ) -> Frame {
        self.execute(server_info)
    }

    fn execute_replica(
        &self,
        _db: &Db,
        server_info: &mut ServerInfo,
        _connection: Connection,
    ) -> Frame {
        self.execute(server_info)
    }

    fn to_frame(&self) -> Frame {
        self.to_frame()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
