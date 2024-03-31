use std::vec;

use bytes::Bytes;

use crate::{connection::Connection, parse, server, Db, Frame, Parse};

use super::CommandTrait;

#[derive(Debug, Default)]
pub struct ReplConfListeningPort(pub u16);

#[derive(Debug)]
pub enum ReplConf {
    ListeningPort(ReplConfListeningPort),
    Capa,
    GetAck,
}

impl ReplConf {
    pub fn parse_frames(frames: &mut Parse) -> crate::Result<ReplConf> {
        match frames.next_string() {
            Ok(section) => match section.as_str().to_lowercase().as_str() {
                "listening-port" => ReplConf::parse_port(frames),
                "capa" => ReplConf::parse_psync2(frames),
                "getack" => ReplConf::parse_ack(frames),
                _ => {
                    Err(format!("Protocol error: unsupported REPLCONF section: {}", section).into())
                }
            },
            Err(parse::Error::EndOfStream) => Ok(ReplConf::Capa),
            Err(err) => Err(err.into()),
        }
    }

    fn parse_port(frames: &mut Parse) -> crate::Result<ReplConf> {
        let port = frames
            .next_string()?
            .parse::<u16>()
            .map_err(|err| format!("Protocol error: invalid port: {}", err))?;

        Ok(ReplConf::ListeningPort(ReplConfListeningPort(port)))
    }

    fn parse_psync2(frames: &mut Parse) -> crate::Result<ReplConf> {
        let psync2 = frames.next_string()?.to_lowercase();

        if psync2 == "psync2" {
            Ok(ReplConf::Capa)
        } else {
            Err("Protocol error: expected command: REPLCONF capa psync2".into())
        }
    }

    fn parse_ack(frames: &mut Parse) -> crate::Result<ReplConf> {
        let ack_arg = frames.next_string()?;

        if ack_arg == "*" {
            Ok(ReplConf::GetAck)
        } else {
            Err("Protocol error: expected command: REPLCONF getack *".into())
        }
    }

    pub fn to_frame(&self) -> Frame {
        match self {
            ReplConf::ListeningPort(listening_port) => {
                let frame_first = Frame::Array(vec![
                    Frame::Bulk(Bytes::from("REPLCONF".to_string())),
                    Frame::Bulk(Bytes::from("listening-port".to_string())),
                    Frame::Bulk(Bytes::from(listening_port.0.to_string())),
                ]);
                let frame_second = Frame::Array(vec![
                    Frame::Bulk(Bytes::from("REPLCONF".to_string())),
                    Frame::Bulk(Bytes::from("capa".to_string())),
                    Frame::Bulk(Bytes::from("psync2".to_string())),
                ]);

                Frame::Array(vec![frame_first, frame_second])
            }
            ReplConf::Capa => {
                let frame_first = Frame::Array(vec![
                    Frame::Bulk(Bytes::from("REPLCONF".to_string())),
                    Frame::Bulk(Bytes::from("listening-port".to_string())),
                    Frame::Bulk(Bytes::from("".to_string())),
                ]);
                let frame_second = Frame::Array(vec![
                    Frame::Bulk(Bytes::from("REPLCONF".to_string())),
                    Frame::Bulk(Bytes::from("capa".to_string())),
                    Frame::Bulk(Bytes::from("psync2".to_string())),
                ]);

                Frame::Array(vec![frame_first, frame_second])
            }
            ReplConf::GetAck => Frame::Array(vec![
                Frame::Bulk(Bytes::from("REPLCONF".to_string())),
                Frame::Bulk(Bytes::from("getack".to_string())),
                Frame::Bulk(Bytes::from("*".to_string())),
            ]),
        }
    }

    pub fn execute(&self, server_info: &mut server::Info, connection: Connection) -> Frame {
        match self {
            ReplConf::ListeningPort(listening_port) => {
                server_info.add_slave(("localhost".to_string(), listening_port.0), connection);
                Frame::Simple("OK".into())
            }
            ReplConf::Capa => Frame::Simple("OK".into()),
            ReplConf::GetAck => Frame::Array(vec![
                Frame::Bulk(Bytes::from("REPLCONF".to_string())),
                Frame::Bulk(Bytes::from("ACK".to_string())),
                Frame::Bulk(Bytes::from(
                    server_info.parsed_command_bytes().unwrap().to_string(),
                )),
            ]),
        }
    }
}

impl CommandTrait for ReplConf {
    fn parse_frames(&self, frames: &mut Parse) -> crate::Result<Box<dyn CommandTrait>> {
        Ok(Box::new(ReplConf::parse_frames(frames)?))
    }

    fn execute(&self, _db: &Db, server_info: &mut server::Info, connection: Connection) -> Frame {
        self.execute(server_info, connection)
    }

    fn execute_replica(
        &self,
        _db: &Db,
        server_info: &mut server::Info,
        connection: Connection,
    ) -> Frame {
        self.execute(server_info, connection)
    }

    fn to_frame(&self) -> Frame {
        self.to_frame()
    }
}
