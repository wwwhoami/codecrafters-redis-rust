use crate::{connection::Connection, parse::Error, Db, Frame, Info as ServerInfo, Parse};

use super::CommandTrait;

#[derive(Debug, Default)]
pub struct Info {}

impl Info {
    pub fn parse_frames(frames: &mut Parse) -> crate::Result<Info> {
        match frames.next_string() {
            Ok(section) => match section.as_str().to_lowercase().as_str() {
                "replication" => Ok(Info {}),
                _ => Err(format!("Protocol error: unsupported INFO section: {}", section).into()),
            },
            Err(Error::EndOfStream) => Ok(Info {}),
            Err(err) => Err(err.into()),
        }
    }

    pub fn to_frame(&self) -> Frame {
        Frame::Simple("INFO".into())
    }

    pub fn execute(&self, server_info: &mut ServerInfo) -> Frame {
        Frame::Bulk(bytes::Bytes::from(server_info.to_string()))
    }
}

impl CommandTrait for Info {
    fn parse_frames(&self, frames: &mut Parse) -> crate::Result<Box<dyn CommandTrait>> {
        Ok(Box::new(Info::parse_frames(frames)?))
    }

    fn execute(&self, _db: &Db, server_info: &mut ServerInfo, _connection: Connection) -> Frame {
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
