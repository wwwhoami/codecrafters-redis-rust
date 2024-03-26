use crate::{server, Db, Frame, Parse};

use super::CommandTrait;

#[derive(Debug, Default)]
pub struct Echo {
    message: String,
}

impl Echo {
    pub fn new(message: impl ToString) -> Echo {
        Echo {
            message: message.to_string(),
        }
    }

    pub fn execute(&self) -> Frame {
        Frame::Simple(self.message.clone())
    }

    pub fn parse_frames(frames: &mut Parse) -> crate::Result<Echo> {
        let message = frames.next_string()?;
        Ok(Echo::new(message))
    }

    pub fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::Bulk("ECHO".into()),
            Frame::Bulk(self.message.clone().into()),
        ])
    }
}

impl CommandTrait for Echo {
    fn parse_frames(&self, frames: &mut Parse) -> crate::Result<Box<dyn CommandTrait>> {
        Ok(Box::new(Echo::parse_frames(frames)?))
    }

    fn execute(&self, _db: &Db, _server_info: &server::Info) -> Frame {
        self.execute()
    }

    fn to_frame(&self) -> Frame {
        self.to_frame()
    }
}
