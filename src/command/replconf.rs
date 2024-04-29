use std::vec;

use async_trait::async_trait;
use bytes::Bytes;

use crate::{connection::Connection, parse, Db, Frame, Info, Parse};

use super::CommandTrait;

#[derive(Debug, Default)]
pub struct ReplConfListeningPort(pub u16);

#[derive(Debug)]
pub enum ReplConf {
    /// REPLCONF listening-port \<port\>
    ListeningPort(ReplConfListeningPort),
    /// REPLCONF capa psync2
    Capa,
    /// REPLCONF getack *
    GetAck,
    /// REPLCONF ack \<offset\>
    Ack(u64),
}

impl ReplConf {
    pub fn parse_frames(frames: &mut Parse) -> crate::Result<ReplConf> {
        match frames.next_string() {
            Ok(section) => match section.as_str().to_lowercase().as_str() {
                "listening-port" => ReplConf::parse_port(frames),
                "capa" => ReplConf::parse_psync2(frames),
                "getack" => ReplConf::parse_get_ack(frames),
                "ack" => ReplConf::parse_ack(frames),
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

    fn parse_get_ack(frames: &mut Parse) -> crate::Result<ReplConf> {
        let ack_arg = frames.next_string()?;

        if ack_arg == "*" {
            Ok(ReplConf::GetAck)
        } else {
            Err("Protocol error: expected command: REPLCONF getack *".into())
        }
    }

    fn parse_ack(frames: &mut Parse) -> crate::Result<ReplConf> {
        let ack_offset = frames.next_uint()?;

        Ok(ReplConf::Ack(ack_offset))
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
                Frame::Bulk(Bytes::from("GETACK".to_string())),
                Frame::Bulk(Bytes::from("*".to_string())),
            ]),
            ReplConf::Ack(ack_offset) => Frame::Array(vec![
                Frame::Bulk(Bytes::from("REPLCONF".to_string())),
                Frame::Bulk(Bytes::from("ACK".to_string())),
                Frame::Bulk(Bytes::from(ack_offset.to_string())),
            ]),
        }
    }

    pub fn execute(&self, server_info: &mut Info, connection: Connection) -> Frame {
        match self {
            ReplConf::ListeningPort(listening_port) => {
                server_info.add_slave(("127.0.0.1".to_string(), listening_port.0), connection);
                Frame::Simple("OK".into())
            }
            ReplConf::Capa => Frame::Simple("OK".into()),
            ReplConf::GetAck => Frame::Array(vec![
                Frame::Bulk(Bytes::from("REPLCONF".to_string())),
                Frame::Bulk(Bytes::from("ACK".to_string())),
                Frame::Bulk(Bytes::from(
                    // server_info.parsed_command_bytes().unwrap().to_string(),
                    server_info.offset().to_string(),
                )),
            ]),
            ReplConf::Ack(ack_offset) => {
                let tx_repl_got_ack = server_info.tx_repl_got_ack().unwrap();
                tx_repl_got_ack
                    .send((connection.addr(), *ack_offset))
                    .unwrap();

                server_info.update_replica_offset(connection.addr(), *ack_offset);

                // Frame::Array(vec![Frame::Bulk("PING".into())])
                Frame::NoSend
            }
        }
    }
}

#[async_trait]
impl CommandTrait for ReplConf {
    fn parse_frames(&self, frames: &mut Parse) -> crate::Result<Box<dyn CommandTrait>> {
        Ok(Box::new(ReplConf::parse_frames(frames)?))
    }

    async fn execute(&self, _db: &Db, server_info: &mut Info, connection: Connection) -> Frame {
        self.execute(server_info, connection)
    }

    fn execute_replica(&self, _db: &Db, server_info: &mut Info, connection: Connection) -> Frame {
        self.execute(server_info, connection)
    }

    fn to_frame(&self) -> Frame {
        self.to_frame()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
