use bytes::Bytes;

use crate::{parse::Error, Frame, Parse};

#[derive(Debug)]
pub struct Info {
    info: String,
}

impl Info {
    pub fn new() -> Self {
        Self {
            info: "role:master".to_string(),
        }
    }

    pub fn execute(&self) -> Frame {
        Frame::Bulk(Bytes::from(self.info.as_bytes().to_vec()))
    }

    pub fn parse_frames(frames: &mut Parse) -> crate::Result<Info> {
        match frames.next_string() {
            Ok(section) if section == "replication" => return Ok(Info::new()),
            Ok(section) => {
                return Err(format!("Protocol error: unsupported INFO section: {}", section).into())
            }
            Err(Error::EndOfStream) => return Ok(Info::new()),
            Err(err) => return Err(err.into()),
        }
    }
}
