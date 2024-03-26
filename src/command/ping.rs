use crate::{server, Db, Frame, Parse};

use super::CommandTrait;

#[derive(Debug)]
pub struct Ping {
    message: String,
}

impl Default for Ping {
    fn default() -> Self {
        Ping::new()
    }
}

impl Ping {
    pub fn new() -> Ping {
        Ping {
            message: "PONG".to_string(),
        }
    }

    pub fn execute(&self) -> Frame {
        Frame::Simple(self.message.clone())
    }

    pub fn parse_frames(_frames: &mut Parse) -> crate::Result<Ping> {
        Ok(Ping::new())
    }

    pub fn to_frame(&self) -> Frame {
        Frame::Array(vec![Frame::Bulk("PING".into())])
    }
}

impl CommandTrait for Ping {
    fn parse_frames(&self, _frames: &mut Parse) -> crate::Result<Box<dyn CommandTrait>> {
        Ok(Box::new(Ping::parse_frames(_frames)?))
    }

    fn execute(&self, _db: &Db, _server_info: &server::Info) -> Frame {
        self.execute()
    }

    fn to_frame(&self) -> Frame {
        self.to_frame()
    }
}
