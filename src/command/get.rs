use crate::{connection::Connection, server, Db, Frame, Parse};

use super::CommandTrait;

#[derive(Debug, Default)]
pub struct Get {
    key: String,
}

impl Get {
    pub fn new(key: String) -> Self {
        Self { key }
    }

    pub fn execute(&self, db: &Db) -> Frame {
        match db.get(&self.key) {
            Some(value) => Frame::Bulk(value),
            None => Frame::Null,
        }
    }

    pub fn parse_frames(frames: &mut Parse) -> crate::Result<Get> {
        let key = frames.next_string()?;
        Ok(Get::new(key))
    }

    pub fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::Bulk("GET".into()),
            Frame::Bulk(self.key.clone().into()),
        ])
    }
}

impl CommandTrait for Get {
    fn parse_frames(&self, frames: &mut Parse) -> crate::Result<Box<dyn CommandTrait>> {
        Ok(Box::new(Get::parse_frames(frames)?))
    }

    fn execute(&self, db: &Db, _server_info: &mut server::Info, _connection: Connection) -> Frame {
        self.execute(db)
    }

    fn execute_replica(
        &self,
        db: &Db,
        _server_info: &mut server::Info,
        _connection: Connection,
    ) -> Frame {
        self.execute(db)
    }

    fn to_frame(&self) -> Frame {
        self.to_frame()
    }
}
