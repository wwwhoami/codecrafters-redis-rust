use async_trait::async_trait;

use crate::{connection::Connection, replicaiton::rdb, Db, Frame, Info, Parse};

use super::CommandTrait;

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
        let _replid = frames.next_string()?;
        let offset = frames.next_int()?;

        Ok(Psync::new(
            offset,
            "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
        ))
    }

    pub fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::Bulk("PSYNC".into()),
            Frame::Bulk(self.replid.clone().into()),
            Frame::Bulk(self.offset.to_string().into()),
        ])
    }

    /// Sent by master to a replica to create a replication stream.
    pub fn execute(&self, server_info: &mut Info) -> Frame {
        // Simple string part of the frame
        let full_resync = format!(
            "FULLRESYNC {} 0",
            server_info.master_replid().unwrap_or_default()
        );
        // RDB part of the frame
        let rdb = rdb::empty_rdb();

        // Frame::Array(vec![Frame::Simple(full_resync.clone()), Frame::Bulk(rdb)])

        Frame::Rdb(full_resync, rdb)
    }
}

#[async_trait]
impl CommandTrait for Psync {
    fn parse_frames(&self, frames: &mut Parse) -> crate::Result<Box<dyn CommandTrait>> {
        Ok(Box::new(Psync::parse_frames(frames)?))
    }

    async fn execute(&self, _db: &Db, server_info: &mut Info, _connection: Connection) -> Frame {
        self.execute(server_info)
    }

    fn execute_replica(&self, _db: &Db, server_info: &mut Info, _connection: Connection) -> Frame {
        self.execute(server_info)
    }

    fn to_frame(&self) -> Frame {
        self.to_frame()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
