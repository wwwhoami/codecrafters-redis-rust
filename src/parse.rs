use std::{fmt, str, vec};

use bytes::Bytes;

use crate::frame::Frame;

pub struct Parse {
    frame_iter: vec::IntoIter<Frame>,
}

impl Parse {
    pub fn new(frame: Frame) -> Result<Parse, Error> {
        let frames = match frame {
            Frame::Array(frames) => frames,
            frame => {
                return Err(format!(
                    "Protocol error: expected frame to be an array, got {:?}",
                    frame
                )
                .into())
            }
        };

        Ok(Parse {
            frame_iter: frames.into_iter(),
        })
    }

    pub fn next_frame(&mut self) -> Result<Frame, Error> {
        self.frame_iter.next().ok_or(Error::EndOfStream)
    }

    pub fn next_string(&mut self) -> Result<String, Error> {
        match self.next_frame()? {
            Frame::Simple(s) => Ok(s),
            Frame::Bulk(s) => str::from_utf8(&s)
                .map(|s| s.to_string())
                .map_err(|_| "Protocol error: invalid string".into()),
            frame => Err(format!(
                "Protocol error: expected simple or bulk string frame, got {:?}",
                frame
            )
            .into()),
        }
    }

    pub fn next_bytes(&mut self) -> Result<Bytes, Error> {
        match self.next_frame()? {
            Frame::Simple(s) => Ok(s.into_bytes().into()),
            Frame::Bulk(s) => Ok(s),
            frame => Err(format!(
                "Protocol error: expected simple or bulk string frame, got {:?}",
                frame
            )
            .into()),
        }
    }

    pub fn next_uint(&mut self) -> Result<u64, Error> {
        const ERROR_MSG: &str = "Protocol error: expected integer frame";
        match self.next_frame()? {
            Frame::Integer(n) => Ok(n),
            Frame::Simple(s) => s.parse().map_err(|_| ERROR_MSG.into()),
            Frame::Bulk(s) => {
                let s = str::from_utf8(&s).map_err(|_| ERROR_MSG)?;
                s.parse().map_err(|_| ERROR_MSG.into())
            }
            frame => Err(format!("Protocol error: expected integer frame, got {:?}", frame).into()),
        }
    }

    pub(crate) fn next_int(&mut self) -> Result<i64, Error> {
        const ERROR_MSG: &str = "Protocol error: expected integer frame";
        match self.next_frame()? {
            Frame::Integer(n) => Ok(n.try_into().unwrap()),
            Frame::Simple(s) => s.parse().map_err(|_| ERROR_MSG.into()),
            Frame::Bulk(s) => {
                let s = str::from_utf8(&s).map_err(|_| ERROR_MSG)?;
                s.parse().map_err(|_| ERROR_MSG.into())
            }
            frame => Err(format!("Protocol error: expected integer frame, got {:?}", frame).into()),
        }
    }

    pub fn finish(&mut self) -> Result<(), Error> {
        if self.frame_iter.next().is_none() {
            Ok(())
        } else {
            Err("Protocol error: end of frame expecred".into())
        }
    }
}

#[derive(Debug)]
pub enum Error {
    EndOfStream,
    Other(crate::Error),
}

impl From<&str> for Error {
    fn from(value: &str) -> Self {
        value.to_string().into()
    }
}

impl From<String> for Error {
    fn from(value: String) -> Self {
        Error::Other(value.into())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::EndOfStream => "Protocol error: unexpected end of stream".fmt(f),
            Error::Other(err) => err.fmt(f),
        }
    }
}

impl std::error::Error for Error {}
