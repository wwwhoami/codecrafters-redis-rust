use crate::{
    connection::Connection,
    db::{StreamEntry, StreamEntryId},
    Db, Frame, Info, Parse,
};

use super::CommandTrait;

#[derive(Debug)]
pub struct XRead {
    stream_keys: Vec<String>,
    start_ids: Vec<StreamEntryId>,
}

impl XRead {
    pub fn new(stream_keys: Vec<String>, start_ids: Vec<StreamEntryId>) -> XRead {
        XRead {
            stream_keys,
            start_ids,
        }
    }

    pub fn execute(&self, db: &Db) -> Frame {
        println!("XRead execute");
        println!("stream_keys: {:?}", self.stream_keys);
        println!("start_ids: {:?}", self.start_ids);

        let streams = db.xread(&self.stream_keys, &self.start_ids);

        let mut frames = Vec::new();
        for (stream_key, entries) in streams {
            let mut stream = Vec::new();
            stream.push(Frame::Bulk(stream_key.into()));

            let entries = Self::entries_to_frames(entries);
            stream.push(entries);

            frames.push(Frame::Array(stream));
        }
        // for entry in entries {
        //     let id = entry.id();
        //     let key_value = entry.key_value();
        //
        //     let mut frame = Vec::new();
        //     frame.push(Frame::Bulk(id.to_string().into()));
        //
        //     let mut entry = Vec::new();
        //     for (key, value) in key_value {
        //         entry.push(Frame::Bulk(key.clone().into()));
        //         entry.push(Frame::Bulk(value.clone()));
        //     }
        //
        //     frame.push(Frame::Array(entry));
        //     frames.push(Frame::Array(frame));
        // }

        Frame::Array(frames)
    }

    fn entries_to_frames(entries: Vec<StreamEntry>) -> Frame {
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

    pub fn parse_frames(frames: &mut Parse) -> crate::Result<XRead> {
        match frames.next_string()?.to_uppercase().as_str() {
            "STREAMS" => XRead::parse_streams(frames),
            _ => Err("Protocol error: unsupported XREAD section".into()),
        }
    }

    pub fn parse_streams(frames: &mut Parse) -> crate::Result<XRead> {
        let stream_keys = Self::parse_keys(frames)?;
        let stream_ids = Self::parse_ids(frames)?;

        if stream_keys.len() != stream_ids.len() {
            return Err(
                "Protocol error: STREAMS command should have same keys and ids counts".into(),
            );
        }
        Ok(XRead::new(stream_keys, stream_ids))
    }

    pub fn parse_keys(frames: &mut Parse) -> crate::Result<Vec<String>> {
        let mut keys = Vec::new();

        while let Some(key) = frames.peek_string() {
            // If the key is a valid stream id, then we have reached the end of the keys
            if Self::parse_id(&key).is_ok() {
                break;
            }
            // Otherwise, add the key to the list of keys and proceed frames iterator
            keys.push(frames.next_string()?);
        }

        Ok(keys)
    }

    pub fn parse_ids(frames: &mut Parse) -> crate::Result<Vec<StreamEntryId>> {
        let mut ids = Vec::new();

        while let Ok(id) = frames.next_string() {
            ids.push(Self::parse_id(&id)?);
        }

        Ok(ids)
    }

    pub fn parse_id(id: &str) -> crate::Result<StreamEntryId> {
        let split_id = id.split('-').collect::<Vec<&str>>();
        let timestamp = split_id[0].parse::<u128>()?;
        let sequence = split_id[1].parse::<usize>()?;

        Ok(StreamEntryId::new(timestamp, sequence))
    }

    pub fn to_frame(&self) -> Frame {
        Frame::Array(vec![Frame::Bulk("PING".into())])
    }
}

impl CommandTrait for XRead {
    fn parse_frames(&self, _frames: &mut Parse) -> crate::Result<Box<dyn CommandTrait>> {
        Ok(Box::new(XRead::parse_frames(_frames)?))
    }

    fn execute(&self, db: &Db, _server_info: &mut Info, _connection: Connection) -> Frame {
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
