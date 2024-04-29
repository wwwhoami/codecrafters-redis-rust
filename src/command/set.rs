use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;

use crate::{connection::Connection, db::Db, parse, Frame, Info, Parse};

use super::CommandTrait;

#[derive(Debug, Default)]
pub struct Set {
    key: String,
    value: Bytes,
    expire: Option<Duration>,
}

impl Set {
    pub fn new(key: impl ToString, value: Bytes, expire: Option<Duration>) -> Self {
        Self {
            key: key.to_string(),
            value,
            expire,
        }
    }

    pub fn execute(&self, db: &Db) -> Frame {
        db.set(self.key.clone(), self.value.clone(), self.expire);
        Frame::Simple("OK".to_string())
    }

    pub fn parse_frames(frames: &mut Parse) -> crate::Result<Set> {
        let key = frames.next_string()?;
        let value = frames.next_bytes()?;

        let expire = match frames.next_string() {
            // Parse the EX option for seconds
            Ok(s) if s.to_uppercase() == "EX" => {
                let expire = frames.next_uint()?;
                Some(Duration::from_secs(expire))
            }
            // Parse the PX option for milliseconds
            Ok(s) if s.to_uppercase() == "PX" => {
                let expire = frames.next_uint()?;
                Some(Duration::from_millis(expire))
            }
            Ok(_) => return Err("Protocol error: expected EX or PX for expiration".into()),
            // No expiration if end of stream is reached
            Err(parse::Error::EndOfStream) => None,
            Err(err) => return Err(err.into()),
        };

        Ok(Set::new(key, value, expire))
    }

    pub fn to_frame(&self) -> Frame {
        let mut frame = vec![
            Frame::Bulk("SET".into()),
            Frame::Bulk(self.key.clone().into()),
            Frame::Bulk(self.value.clone()),
        ];

        if let Some(expire) = self.expire {
            frame.push(Frame::Bulk("EX".into()));
            frame.push(Frame::Bulk(expire.as_secs().to_string().into()));
        }

        Frame::Array(frame)
    }
}

#[async_trait]
impl CommandTrait for Set {
    fn parse_frames(&self, frames: &mut Parse) -> crate::Result<Box<dyn CommandTrait>> {
        Ok(Box::new(Set::parse_frames(frames)?))
    }

    async fn execute(&self, db: &Db, _server_info: &mut Info, _connection: Connection) -> Frame {
        self.execute(db)
    }

    fn execute_replica(&self, db: &Db, _server_info: &mut Info, _connection: Connection) -> Frame {
        self.execute(db);
        Frame::Null
    }

    fn to_frame(&self) -> Frame {
        self.to_frame()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
