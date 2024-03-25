use crate::{Frame, Parse};

#[derive(Debug)]
pub struct Psync {
    pub offset: i64,
    pub replid: String,
}

impl Psync {
    pub fn new(offset: i64, replid: impl ToString) -> Psync {
        Psync {
            offset,
            replid: replid.to_string(),
        }
    }

    pub fn parse_frames(frames: &mut Parse) -> crate::Result<Psync> {
        let replid = frames.next_string()?;
        let offset = frames.next_int()?;

        Ok(Psync::new(offset, replid))
    }

    pub fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::Bulk("PSYNC".into()),
            Frame::Bulk(self.replid.clone().into()),
            Frame::Bulk(self.offset.to_string().into()),
        ])
    }

    pub fn execute(&self) -> Frame {
        Frame::Simple(format!("FULLRESYNC {} 0", self.replid))
    }
}
