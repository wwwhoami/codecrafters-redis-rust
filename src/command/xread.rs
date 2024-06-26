use std::str;

use async_trait::async_trait;

use crate::{
    connection::Connection,
    db::{StreamEntry, StreamEntryId},
    Db, Frame, Info, Parse,
};

use super::CommandTrait;

#[derive(Debug)]
pub enum StartIds {
    Explicit(Vec<StreamEntryId>),
    Min,
}

impl ToString for StartIds {
    fn to_string(&self) -> String {
        match self {
            StartIds::Explicit(ids) => {
                let mut ids_str = String::new();
                for id in ids {
                    ids_str.push_str(&id.to_string());
                    ids_str.push(' ');
                }
                ids_str
            }
            StartIds::Min => "$".to_string(),
        }
    }
}

#[derive(Debug)]
pub struct XRead {
    stream_keys: Vec<String>,
    start_ids: StartIds,
    block: Option<u64>,
}

impl XRead {
    pub fn new(stream_keys: Vec<String>, start_ids: StartIds, block: Option<u64>) -> XRead {
        XRead {
            stream_keys,
            start_ids,
            block,
        }
    }

    pub async fn execute(&self, db: &Db) -> Frame {
        let stream_ids = match &self.start_ids {
            StartIds::Explicit(ids) => ids.clone(),
            StartIds::Min => db.get_streams_last_ids(&self.stream_keys),
        };
        let streams = db.xread(&self.stream_keys, &stream_ids, self.block).await;

        if streams.is_empty() {
            return Frame::Null;
        }

        let mut frames = Vec::new();
        for (stream_key, entries) in streams {
            let mut stream = Vec::new();
            stream.push(Frame::Bulk(stream_key.into()));

            let entries = Self::entries_to_frames(entries);
            stream.push(entries);

            frames.push(Frame::Array(stream));
        }

        Frame::Array(frames)
    }

    fn entries_to_frames(entries: Vec<StreamEntry>) -> Frame {
        if entries.is_empty() {
            return Frame::Null;
        }
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
            "BLOCK" => {
                let block = frames.next_uint()?;
                // Consume the "STREAMS" string
                frames.next_string()?;
                XRead::parse_streams(frames, Some(block))
            }
            "STREAMS" => XRead::parse_streams(frames, None),
            _ => Err("Protocol error: unsupported XREAD section".into()),
        }
    }

    fn parse_streams(frames: &mut Parse, block: Option<u64>) -> crate::Result<XRead> {
        let stream_keys = Self::parse_keys(frames)?;
        let start_ids = Self::parse_start_ids(frames)?;

        // Validate the start ids if explicit
        if let StartIds::Explicit(ids) = &start_ids {
            if ids.is_empty() {
                return Err(
                    "Protocol error: ids section in STREAMS command should not be empty".into(),
                );
            }
            if stream_keys.len() != ids.len() {
                return Err(
                    "Protocol error: STREAMS command should have same keys and ids counts".into(),
                );
            }
        }

        Ok(XRead::new(stream_keys, start_ids, block))
    }

    fn parse_keys(frames: &mut Parse) -> crate::Result<Vec<String>> {
        let mut keys = Vec::new();

        while let Some(key) = frames.peek_string() {
            // If the key is a valid stream id or if the key is "$",
            // then we have reached the end of the keys
            if Self::parse_id(&key).is_ok() || key == "$" {
                break;
            }
            // Otherwise, add the key to the list of keys and proceed frames iterator
            keys.push(frames.next_string()?);
        }

        Ok(keys)
    }

    fn parse_start_ids(frames: &mut Parse) -> crate::Result<StartIds> {
        match frames.peek_string() {
            Some(str) if str == "$" => {
                // Consume the "$" string
                frames.next_string()?;
                Ok(StartIds::Min)
            }
            Some(_) => Ok(StartIds::Explicit(Self::parse_ids(frames)?)),
            _ => Err("Protocol error: missing ids section in STREAMS command".into()),
        }
    }

    pub fn parse_ids(frames: &mut Parse) -> crate::Result<Vec<StreamEntryId>> {
        let mut ids = Vec::new();

        while let Ok(id) = frames.next_string() {
            ids.push(Self::parse_id(&id)?);
        }

        Ok(ids)
    }

    fn parse_id(id: &str) -> crate::Result<StreamEntryId> {
        let split_id = id.split('-').collect::<Vec<&str>>();
        let timestamp = split_id[0].parse::<u128>()?;
        let sequence = split_id[1].parse::<usize>()?;

        Ok(StreamEntryId::new(timestamp, sequence))
    }

    pub fn to_frame(&self) -> Frame {
        let mut frames = vec![Frame::Bulk("XREAD".into())];

        if let Some(block) = self.block {
            frames.push(Frame::Bulk("BLOCK".into()));
            frames.push(Frame::Bulk(block.to_string().into()));
        }

        frames.push(Frame::Bulk("STREAMS".into()));

        for key in &self.stream_keys {
            frames.push(Frame::Bulk(key.clone().into()));
        }

        frames.push(Frame::Bulk(self.start_ids.to_string().into()));

        Frame::Array(frames)
    }
}

#[async_trait]
impl CommandTrait for XRead {
    fn parse_frames(&self, frames: &mut Parse) -> crate::Result<Box<dyn CommandTrait>> {
        Ok(Box::new(XRead::parse_frames(frames)?))
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
