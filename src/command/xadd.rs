use async_trait::async_trait;
use bytes::Bytes;

use crate::{connection::Connection, db::StreamEntryId, Db, Frame, Info, Parse};

use super::CommandTrait;

#[derive(Debug)]
pub struct XAdd {
    stream_key: String,
    id: XAddId,
    key_value: Vec<(String, Bytes)>,
}

#[derive(Debug, Clone, Copy)]
pub enum XAddId {
    Auto,
    AutoSeq(u128),
    Explicit(StreamEntryId),
}

impl XAdd {
    pub fn new(stream_key: String, id: XAddId, key_value: Vec<(String, Bytes)>) -> XAdd {
        XAdd {
            stream_key,
            id,
            key_value,
        }
    }

    pub async fn execute(&self, db: &Db) -> Frame {
        let id = db
            .xadd(self.stream_key.clone(), self.id, self.key_value.clone())
            .await;

        match id {
            Ok(id) => Frame::Bulk(id.into()),
            Err(err) => {
                eprintln!("XAdd error: {:?}", err);
                Frame::Error(
                "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                    .into(),
            )
            }
        }
    }

    pub fn parse_frames(frames: &mut Parse) -> crate::Result<XAdd> {
        let stream_key = frames.next_string()?;
        let id = frames.next_string()?;
        let id = XAdd::parse_id(id.as_str())?;
        let mut key_value = Vec::new();

        while let Ok(key) = frames.next_string() {
            let value = frames.next_string()?;
            let value = Bytes::from(value);
            key_value.push((key, value));
        }

        Ok(XAdd::new(stream_key, id, key_value))
    }

    pub fn parse_id(id: &str) -> crate::Result<XAddId> {
        if id == "*" {
            return Ok(XAddId::Auto);
        }

        let parts: Vec<&str> = id.split('-').collect();
        let timestamp = parts[0].parse()?;
        let idx = parts[1];

        if idx == "*" {
            return Ok(XAddId::AutoSeq(parts[0].parse()?));
        }

        let idx = idx.parse()?;
        if (timestamp, idx) == (0, 0) {
            Err("ERR The ID specified in XADD must be greater than 0-0".into())
        } else {
            Ok(XAddId::Explicit(StreamEntryId::new(timestamp, idx)))
        }
    }

    pub fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::Bulk("XADD".into()),
            Frame::Bulk(self.stream_key.clone().into()),
        ];

        match self.id {
            XAddId::Auto => frames.push(Frame::Bulk("*".into())),
            XAddId::AutoSeq(timestamp) => {
                frames.push(Frame::Bulk(format!("{}-*", timestamp).into()))
            }
            XAddId::Explicit(id) => frames.push(Frame::Bulk(id.to_string().into())),
        }

        for (key, value) in &self.key_value {
            frames.push(Frame::Bulk(key.clone().into()));
            frames.push(Frame::Bulk(value.clone()));
        }

        Frame::Array(frames)
    }
}

#[async_trait]
impl CommandTrait for XAdd {
    fn parse_frames(&self, _frames: &mut Parse) -> crate::Result<Box<dyn CommandTrait>> {
        Ok(Box::new(XAdd::parse_frames(_frames)?))
    }

    async fn execute(&self, db: &Db, _server_info: &mut Info, _connection: Connection) -> Frame {
        self.execute(db).await
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
