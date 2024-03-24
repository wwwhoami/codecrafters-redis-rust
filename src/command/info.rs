use crate::{parse::Error, Parse};

#[derive(Debug)]
pub struct Info {}

impl Info {
    pub fn parse_frames(frames: &mut Parse) -> crate::Result<()> {
        match frames.next_string() {
            Ok(section) if section == "replication" => Ok(()),
            Ok(section) => {
                Err(format!("Protocol error: unsupported INFO section: {}", section).into())
            }
            Err(Error::EndOfStream) => Ok(()),
            Err(err) => Err(err.into()),
        }
    }
}
