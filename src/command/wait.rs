use crate::{connection::Connection, server, Db, Frame, Parse};

use super::CommandTrait;

#[derive(Debug)]
pub struct Wait {
    _message: String,
}

impl Wait {
    pub fn execute(&self, server_info: &server::Info) -> Frame {
        // Frame::Simple(self.message.clone())
        Frame::Integer(server_info.replicas_count() as u64)
    }

    pub fn parse_frames(frames: &mut Parse) -> crate::Result<Wait> {
        let _timeout = frames.next_uint()?;
        let _event = frames.next_uint()?;

        Ok(Wait {
            _message: "WAIT".to_string(),
        })
    }

    pub fn to_frame(&self) -> Frame {
        Frame::Integer(0)
    }
}

impl CommandTrait for Wait {
    fn parse_frames(&self, frames: &mut Parse) -> crate::Result<Box<dyn CommandTrait>> {
        Ok(Box::new(Wait::parse_frames(frames)?))
    }

    fn execute(&self, _db: &Db, server_info: &mut server::Info, _connection: Connection) -> Frame {
        self.execute(server_info)
    }

    fn execute_replica(
        &self,
        _db: &Db,
        _server_info: &mut server::Info,
        _connection: Connection,
    ) -> Frame {
        Frame::Null
    }

    fn to_frame(&self) -> Frame {
        self.to_frame()
    }
}
