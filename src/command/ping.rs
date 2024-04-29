use async_trait::async_trait;

use crate::{connection::Connection, Db, Frame, Info, Parse};

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

#[async_trait]
impl CommandTrait for Ping {
    fn parse_frames(&self, _frames: &mut Parse) -> crate::Result<Box<dyn CommandTrait>> {
        Ok(Box::new(Ping::parse_frames(_frames)?))
    }

    async fn execute(&self, _db: &Db, _server_info: &mut Info, _connection: Connection) -> Frame {
        self.execute()
    }

    fn execute_replica(&self, _db: &Db, _server_info: &mut Info, _connection: Connection) -> Frame {
        Frame::Null
    }

    fn to_frame(&self) -> Frame {
        self.to_frame()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
