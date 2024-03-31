use std::vec;

use crate::{connection::Connection, parse, server, Db, Frame, Parse};

use super::CommandTrait;

#[derive(Debug, Default)]
pub struct ReplConf {
    listening_port: Option<u16>,
}

impl ReplConf {
    pub fn new(port: u16) -> Self {
        Self {
            listening_port: Some(port),
        }
    }

    pub fn parse_frames(frames: &mut Parse) -> crate::Result<ReplConf> {
        match frames.next_string() {
            Ok(section) => match section.as_str() {
                "listening-port" => ReplConf::parse_port(frames),
                "capa" => ReplConf::parse_psync2(frames),
                _ => {
                    Err(format!("Protocol error: unsupported REPLCONF section: {}", section).into())
                }
            },
            Err(parse::Error::EndOfStream) => Ok(Default::default()),
            Err(err) => Err(err.into()),
        }
    }

    fn parse_port(frames: &mut Parse) -> crate::Result<ReplConf> {
        let port = frames
            .next_string()?
            .parse::<u16>()
            .map_err(|err| format!("Protocol error: invalid port: {}", err))?;

        Ok(ReplConf::new(port))
    }

    fn parse_psync2(frames: &mut Parse) -> crate::Result<ReplConf> {
        let psync2 = frames.next_string()?;

        if psync2 == "psync2" {
            Ok(Default::default())
        } else {
            Err("Protocol error: expected command: REPLCONF capa psync2".into())
        }
    }

    pub fn to_frame(&self) -> Frame {
        let frame_first = Frame::Array(vec![
            Frame::Bulk(bytes::Bytes::from("REPLCONF".to_string())),
            Frame::Bulk(bytes::Bytes::from("listening-port".to_string())),
            Frame::Bulk(bytes::Bytes::from(
                self.listening_port
                    .map(|port| port.to_string())
                    .unwrap_or("".to_string()),
            )),
        ]);
        let frame_second = Frame::Array(vec![
            Frame::Bulk(bytes::Bytes::from("REPLCONF".to_string())),
            Frame::Bulk(bytes::Bytes::from("capa".to_string())),
            Frame::Bulk(bytes::Bytes::from("psync2".to_string())),
        ]);

        Frame::Array(vec![frame_first, frame_second])
    }

    pub fn execute(&self, server_info: &mut server::Info, connection: Connection) -> Frame {
        if let Some(port) = self.listening_port {
            server_info.add_slave(("localhost".to_string(), port), connection);
        }

        Frame::Simple("OK".into())
    }
}

impl CommandTrait for ReplConf {
    fn parse_frames(&self, frames: &mut Parse) -> crate::Result<Box<dyn CommandTrait>> {
        Ok(Box::new(ReplConf::parse_frames(frames)?))
    }

    fn execute(&self, _db: &Db, server_info: &mut server::Info, connection: Connection) -> Frame {
        self.execute(server_info, connection)
    }

    fn to_frame(&self) -> Frame {
        self.to_frame()
    }
}
