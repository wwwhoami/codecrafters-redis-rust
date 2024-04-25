use crate::{connection::Connection, db::Entry, Db, Frame, Info, Parse};

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
            Some(Entry::String(entry)) => Frame::Bulk(entry.value().clone()),
            Some(_) => Frame::Null,
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

    fn execute(&self, db: &Db, _server_info: &mut Info, _connection: Connection) -> Frame {
        self.execute(db)
    }

    fn execute_replica(&self, db: &Db, _server_info: &mut Info, _connection: Connection) -> Frame {
        self.execute(db)
    }

    fn to_frame(&self) -> Frame {
        self.to_frame()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
