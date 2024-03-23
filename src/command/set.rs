use std::time::Duration;

use bytes::Bytes;

use crate::{db::Db, parse, Frame, Parse};

#[derive(Debug)]
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
}
