use std::time::Duration;

use async_trait::async_trait;

use crate::{connection::Connection, Db, Frame, Info, Parse};

use super::CommandTrait;

#[derive(Debug, Default)]
pub struct Wait {
    pub replica_count: u64,
    pub timeout: Duration,
}

impl Wait {
    pub fn new(replica_count: u64, timeout: Duration) -> Wait {
        Wait {
            replica_count,
            timeout,
        }
    }

    pub fn execute(&self, _server_info: &Info) -> Frame {
        Frame::Null
    }

    pub fn parse_frames(frames: &mut Parse) -> crate::Result<Wait> {
        let replica_count = frames.next_uint()?;
        let timeout = frames.next_uint()?;

        Ok(Wait::new(replica_count, Duration::from_millis(timeout)))
    }

    pub fn to_frame(&self) -> Frame {
        Frame::Null
    }
}

#[async_trait]
impl CommandTrait for Wait {
    fn parse_frames(&self, frames: &mut Parse) -> crate::Result<Box<dyn CommandTrait>> {
        Ok(Box::new(Wait::parse_frames(frames)?))
    }

    async fn execute(&self, _db: &Db, server_info: &mut Info, _connection: Connection) -> Frame {
        self.execute(server_info)
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
