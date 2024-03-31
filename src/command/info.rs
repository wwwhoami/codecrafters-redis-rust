use crate::{connection::Connection, parse::Error, server, Db, Frame, Parse};

use super::CommandTrait;

#[derive(Debug, Default)]
pub struct Info {}

impl Info {
    pub fn parse_frames(frames: &mut Parse) -> crate::Result<Info> {
        match frames.next_string() {
            Ok(section) if section == "replication" => Ok(Info {}),
            Ok(section) => {
                Err(format!("Protocol error: unsupported INFO section: {}", section).into())
            }
            Err(Error::EndOfStream) => Ok(Info {}),
            Err(err) => Err(err.into()),
        }
    }

    pub fn to_frame(&self) -> Frame {
        Frame::Simple("INFO".into())
    }

    pub fn execute(&self, server_info: &mut server::Info) -> Frame {
        Frame::Bulk(bytes::Bytes::from(server_info.to_string()))
    }
}

impl CommandTrait for Info {
    fn parse_frames(&self, frames: &mut Parse) -> crate::Result<Box<dyn CommandTrait>> {
        Ok(Box::new(Info::parse_frames(frames)?))
    }

    fn execute(&self, _db: &Db, server_info: &mut server::Info, _connection: Connection) -> Frame {
        self.execute(server_info)
    }

    fn to_frame(&self) -> Frame {
        self.to_frame()
    }
}
