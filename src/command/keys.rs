use async_trait::async_trait;
use bytes::Bytes;

use crate::{connection::Connection, Db, Frame, Info, Parse};

use super::CommandTrait;

#[derive(Debug, Default)]
pub struct Keys {}

impl Keys {
    pub fn execute(&self, db: &Db) -> Frame {
        Frame::Array(
            db.keys()
                .iter()
                .map(|k| Frame::Bulk(Bytes::from(k.clone())))
                .collect(),
        )
    }

    pub fn parse_frames(frames: &mut Parse) -> crate::Result<Keys> {
        let key = frames.next_string()?;

        match key.as_str() {
            "*" => Ok(Keys {}),
            _ => Err("Protocol error: expected *".into()),
        }
    }

    pub fn to_frame(&self) -> Frame {
        Frame::Array(vec![Frame::Bulk("KEYS".into()), Frame::Bulk("*".into())])
    }
}

#[async_trait]
impl CommandTrait for Keys {
    fn parse_frames(&self, frames: &mut Parse) -> crate::Result<Box<dyn CommandTrait>> {
        Ok(Box::new(Keys::parse_frames(frames)?))
    }

    async fn execute(&self, db: &Db, _server_info: &mut Info, _connection: Connection) -> Frame {
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
