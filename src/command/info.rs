use crate::{parse::Error, server, Frame, Parse};

#[derive(Debug, Default)]
pub struct Info {}

impl Info {
    pub fn parse_frames(frames: &mut Parse) -> crate::Result<Info> {
        match frames.next_string() {
            Ok(section) if section == "replication" => Ok(Info {}),
            Ok(section) => {
                Err(format!("Protocol error: unsupported INFO section: {}", section).into())
            }
            Err(Error::EndOfStream) => Ok(Info {}),
            Err(err) => Err(err.into()),
        }
    }

    pub fn to_frame(&self) -> Frame {
        Frame::Simple("INFO".into())
    }

    pub fn execute(&self, server_info: &server::Info) -> Frame {
        Frame::Bulk(bytes::Bytes::from(server_info.to_string()))
    }
}
