use crate::{Db, Frame, Parse};

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
