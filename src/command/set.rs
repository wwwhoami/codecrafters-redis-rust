use bytes::Bytes;

use crate::{db::Db, Frame, Parse};

#[derive(Debug)]
pub struct Set {
    key: String,
    value: Bytes,
}

impl Set {
    pub fn new(key: impl ToString, value: Bytes) -> Self {
        Self {
            key: key.to_string(),
            value,
        }
    }

    pub fn execute(&self, db: &Db) -> Frame {
        db.set(self.key.clone(), self.value.clone());
        Frame::Simple("OK".to_string())
    }

    pub fn parse_frames(frames: &mut Parse) -> crate::Result<Set> {
        let key = frames.next_string()?;
        let value = frames.next_bytes()?;

        Ok(Set::new(key, value))
    }
}
