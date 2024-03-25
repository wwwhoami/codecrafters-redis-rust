use std::vec;

use crate::{parse, Frame, Parse};

#[derive(Debug, Default)]
pub struct ReplConf {
    listening_port: u16,
    _parsed_port: Option<u16>,
}

impl ReplConf {
    pub fn new(port: u16) -> Self {
        Self {
            listening_port: port,
            _parsed_port: None,
        }
    }

    pub fn parse_frames(frames: &mut Parse) -> crate::Result<ReplConf> {
        match frames.next_string() {
            Ok(section) if section == "listening-port" => ReplConf::parse_port(frames),
            Ok(section) if section == "capa" => {
                if frames.next_string()? == "psync2" {
                    Ok(Default::default())
                } else {
                    Err("Protocol error: expected psync2".into())
                }
            }
            Ok(section) => {
                Err(format!("Protocol error: unsupported REPLCONF section: {}", section).into())
            }
            Err(parse::Error::EndOfStream) => Ok(Default::default()),
            Err(err) => Err(err.into()),
        }
    }

    fn parse_port(frames: &mut Parse) -> crate::Result<ReplConf> {
        let port = frames
            .next_string()?
            .parse::<u16>()
            .map_err(|err| format!("Protocol error: invalid port: {}", err))?;

        Ok(ReplConf {
            listening_port: port,
            _parsed_port: Some(port),
        })
    }

    pub fn to_frame(&self) -> Frame {
        let frame_first = Frame::Array(vec![
            Frame::Bulk(bytes::Bytes::from("REPLCONF".to_string())),
            Frame::Bulk(bytes::Bytes::from("listening-port".to_string())),
            Frame::Bulk(bytes::Bytes::from(self.listening_port.to_string())),
        ]);
        let frame_second = Frame::Array(vec![
            Frame::Bulk(bytes::Bytes::from("REPLCONF".to_string())),
            Frame::Bulk(bytes::Bytes::from("capa".to_string())),
            Frame::Bulk(bytes::Bytes::from("psync2".to_string())),
        ]);

        Frame::Array(vec![frame_first, frame_second])
    }

    pub fn execute(&self) -> Frame {
        Frame::Simple("OK".into())
    }
}
