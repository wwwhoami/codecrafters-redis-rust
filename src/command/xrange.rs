use async_trait::async_trait;

use crate::{connection::Connection, db::StreamEntryId, Db, Frame, Info, Parse};

use super::CommandTrait;

#[derive(Debug)]
pub struct XRange {
    stream_key: String,
    start: Option<StreamEntryId>,
    end: Option<StreamEntryId>,
}

impl XRange {
    pub fn new(
        stream_key: String,
        start: Option<StreamEntryId>,
        end: Option<StreamEntryId>,
    ) -> XRange {
        XRange {
            stream_key,
            start,
            end,
        }
    }

    pub fn execute(&self, db: &Db) -> Frame {
        let entries = db.xrange(&self.stream_key, self.start, self.end);

        let mut frames = Vec::new();
        for entry in entries {
            let id = entry.id();
            let key_value = entry.key_value();

            let mut frame = Vec::new();
            frame.push(Frame::Bulk(id.to_string().into()));

            let mut entry = Vec::new();
            for (key, value) in key_value {
                entry.push(Frame::Bulk(key.clone().into()));
                entry.push(Frame::Bulk(value.clone()));
            }

            frame.push(Frame::Array(entry));
            frames.push(Frame::Array(frame));
        }

        Frame::Array(frames)
    }

    pub fn parse_frames(frames: &mut Parse) -> crate::Result<XRange> {
        let stream_key = frames.next_string()?;
        let start = frames.next_string()?;
        let end = frames.next_string()?;
        let start = if start == "-" {
            None
        } else {
            Some(XRange::parse_id(start.as_str())?)
        };
        let end = if end == "+" {
            None
        } else {
            Some(XRange::parse_id(end.as_str())?)
        };

        Ok(XRange::new(stream_key, start, end))
    }

    pub fn parse_id(id: &str) -> crate::Result<StreamEntryId> {
        let split_id = id.split('-').collect::<Vec<&str>>();
        let timestamp = split_id[0].parse::<u128>()?;
        let sequence = split_id[1].parse::<usize>()?;

        Ok(StreamEntryId::new(timestamp, sequence))
    }

    pub fn to_frame(&self) -> Frame {
        let mut frames = vec![Frame::Bulk("XRANGE".into())];
        frames.push(Frame::Bulk(self.stream_key.clone().into()));

        if let Some(start) = &self.start {
            frames.push(Frame::Bulk(start.to_string().into()));
        } else {
            frames.push(Frame::Bulk("-".into()));
        }

        if let Some(end) = &self.end {
            frames.push(Frame::Bulk(end.to_string().into()));
        } else {
            frames.push(Frame::Bulk("+".into()));
        }

        Frame::Array(frames)
    }
}

#[async_trait]
impl CommandTrait for XRange {
    fn parse_frames(&self, _frames: &mut Parse) -> crate::Result<Box<dyn CommandTrait>> {
        Ok(Box::new(XRange::parse_frames(_frames)?))
    }

    async fn execute(&self, db: &Db, _server_info: &mut Info, _connection: Connection) -> Frame {
        self.execute(db)
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
