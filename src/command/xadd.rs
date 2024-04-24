use crate::{connection::Connection, Db, Frame, Info, Parse};

use super::CommandTrait;

#[derive(Debug)]
pub struct XAdd {
    stream_key: String,
    id: String,
    key_value: Vec<(String, String)>,
}

impl XAdd {
    pub fn new(stream_key: String, id: String, key_value: Vec<(String, String)>) -> XAdd {
        XAdd {
            stream_key,
            id,
            key_value,
        }
    }

    pub fn execute(&self, db: &Db) -> Frame {
        let _id = db.xadd(self.stream_key.clone(), self.key_value.clone());
        Frame::Bulk(self.id.to_string().into())
    }

    pub fn parse_frames(frames: &mut Parse) -> crate::Result<XAdd> {
        let stream_key = frames.next_string()?;
        let id = frames.next_string()?;
        let mut key_value = Vec::new();

        while let Ok(key) = frames.next_string() {
            let value = frames.next_string()?;
            key_value.push((key, value));
        }

        Ok(XAdd::new(stream_key, id, key_value))
    }

    pub fn to_frame(&self) -> Frame {
        Frame::Array(vec![Frame::Bulk("PING".into())])
    }
}

impl CommandTrait for XAdd {
    fn parse_frames(&self, _frames: &mut Parse) -> crate::Result<Box<dyn CommandTrait>> {
        Ok(Box::new(XAdd::parse_frames(_frames)?))
    }

    fn execute(&self, db: &Db, _server_info: &mut Info, _connection: Connection) -> Frame {
        self.execute(db)
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
