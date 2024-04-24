use crate::{connection::Connection, Db, Frame, Info, Parse};

use super::CommandTrait;

#[derive(Debug)]
pub struct Type {
    key: String,
}

impl Type {
    pub fn new(key: String) -> Type {
        Type { key }
    }

    pub fn execute(&self, db: &Db) -> Frame {
        let t = db.get_type(&self.key);
        Frame::Simple(t)
    }

    pub fn parse_frames(frames: &mut Parse) -> crate::Result<Type> {
        match frames.next_string() {
            Ok(key) => Ok(Type::new(key)),
            Err(err) => Err(err.into()),
        }
    }

    pub fn to_frame(&self) -> Frame {
        Frame::Array(vec![Frame::Bulk("PING".into())])
    }
}

impl CommandTrait for Type {
    fn parse_frames(&self, _frames: &mut Parse) -> crate::Result<Box<dyn CommandTrait>> {
        Ok(Box::new(Type::parse_frames(_frames)?))
    }

    fn execute(&self, db: &Db, _server_info: &mut Info, _connection: Connection) -> Frame {
        self.execute(db)
    }

    fn execute_replica(&self, db: &Db, _server_info: &mut Info, _connection: Connection) -> Frame {
        self.execute(db)
    }

    fn to_frame(&self) -> Frame {
        self.to_frame()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
