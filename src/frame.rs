use std::{fmt, io::Cursor};

use bytes::{Buf, Bytes};

#[derive(Clone, Debug, PartialEq)]
pub enum Frame {
    Simple(String),
    Error(String),
    Integer(u64),
    Bulk(Bytes),
    Array(Vec<Frame>),
    Null,
    /// RDB is a special frame that contains a simple string and a rdb payload
    Rdb(String, Bytes),
    RawBytes(Bytes),
    /// NoSend is a special frame that indicates that the frame should not be sent to the client
    NoSend,
}

impl Frame {
    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Frame, Error> {
        match get_u8(src)? {
            // Simple string
            b'+' => {
                let line = get_line(src)?.to_vec();
                let string = String::from_utf8(line)?;

                Ok(Frame::Simple(string))
            }
            // Simple error
            b'-' => {
                let line = get_line(src)?.to_vec();
                let string = String::from_utf8(line)?;

                Ok(Frame::Error(string))
            }
            // Integer
            b':' => {
                let val = get_decimal(src)?;
                Ok(Frame::Integer(val))
            }
            // Bulk string
            b'$' => {
                if b'-' == peek_u8(src)? {
                    let line = get_line(src)?;
                    if line != b"-1\r\n" {
                        return Err("Protocol error: invalid frame format".into());
                    }

                    Ok(Frame::Null)
                } else {
                    let len = get_decimal(src)? as usize;
                    let n = len + 2;

                    if src.remaining() < n {
                        return Err(Error::Incomplete);
                    }

                    let bulk = Bytes::copy_from_slice(&src.chunk()[..len]);
                    // skip remaining butes "\r\n"
                    skip(src, n)?;

                    Ok(Frame::Bulk(bulk))
                }
            }
            // Array
            b'*' => {
                let len = get_decimal(src)? as usize;
                let mut vec = Vec::with_capacity(len);

                for _ in 0..len {
                    vec.push(Frame::parse(src)?)
                }
                Ok(Frame::Array(vec))
            }
            actual => Err(format!("Protocol error: invalid frame type byte `{}`", actual).into()),
        }
    }
    /// Checks if an entire message can be decoded from `src`
    pub fn check(src: &mut Cursor<&[u8]>) -> Result<(), Error> {
        match get_u8(src)? {
            // Simple string
            b'+' => {
                get_line(src)?;
                Ok(())
            }
            // Simple error
            b'-' => {
                get_line(src)?;
                Ok(())
            }
            // Integer
            b':' => {
                get_decimal(src)?;
                Ok(())
            }
            // Bulk string
            b'$' => {
                if b'-' == peek_u8(src)? {
                    // skip "-1\r\n"
                    skip(src, 4)
                } else {
                    let len = get_decimal(src)? as usize;
                    // skip len + "\r\n"
                    skip(src, len + 2)
                }
            }
            // Array
            b'*' => {
                let len = get_decimal(src)?;

                // check each frame in range
                for _ in 0..len {
                    Frame::check(src)?
                }
                Ok(())
            }
            actual => Err(format!("Protocol error: invalid frame type byte `{}`", actual).into()),
        }
    }

    pub fn check_rdb(src: &mut Cursor<&[u8]>) -> Result<(), Error> {
        match get_u8(src)? {
            // // Simple string
            // b'+' => {
            //     get_line(src)?;
            //     Ok(())
            // }
            // RDB
            b'$' => {
                let len = get_decimal(src)? as usize;
                // skip len
                skip(src, len)
            }
            actual => Err(format!("Protocol error: invalid frame type byte `{}`", actual).into()),
        }
    }

    pub fn parse_rdb(src: &mut Cursor<&[u8]>) -> Result<Frame, Error> {
        match get_u8(src)? {
            // // Simple string
            // b'+' => {
            //     let line = get_line(src)?.to_vec();
            //     let string = String::from_utf8(line)?;
            //
            //     Ok(Frame::Simple(string))
            // }
            // RDB
            b'$' => {
                let len = get_decimal(src)? as usize;
                let n = len;

                if src.remaining() < n {
                    return Err(Error::Incomplete);
                }

                let rdb = Bytes::copy_from_slice(&src.chunk()[..len]);
                // skip remaining bytes
                skip(src, n)?;

                Ok(Frame::RawBytes(rdb))
            }
            actual => Err(format!("Protocol error: invalid frame type byte `{}`", actual).into()),
        }
    }

    pub fn into_array(self) -> Result<Vec<Frame>, Error> {
        match self {
            Frame::Array(vec) => Ok(vec),
            _ => Err("Protocol error: expected array".into()),
        }
    }

    pub fn into_bytes(self) -> Bytes {
        Bytes::from(self.encode())
    }

    pub fn encode(&self) -> String {
        return match self {
            Frame::Simple(string) => encode_simple_string(string),
            Frame::Error(error) => encode_simple_error(error),
            Frame::Integer(integer) => encode_integer(*integer as i64),
            Frame::Bulk(bytes) => encode_bulk_string(Some(std::str::from_utf8(bytes).unwrap())),
            Frame::Array(array) => encode_array(array),
            Frame::Null => encode_null(),
            Frame::Rdb(string, bytes) => {
                let rdb_string = encode_simple_string(string);
                let rdb_bytes = encode_bulk_string(Some(std::str::from_utf8(bytes).unwrap()));
                rdb_string + &rdb_bytes
            }
            Frame::RawBytes(bytes) => {
                let length = bytes.len();
                let bytes = std::str::from_utf8(bytes).unwrap();
                format!("${length}\r\n{bytes}")
            }
            Frame::NoSend => "".to_string(),
        };
    }
}

#[derive(Debug)]
pub enum Error {
    Incomplete,
    Anyhow(crate::Error),
}

impl From<String> for Error {
    fn from(src: String) -> Error {
        Error::Anyhow(src.into())
    }
}

impl From<&str> for Error {
    fn from(src: &str) -> Error {
        src.to_string().into()
    }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(_src: std::string::FromUtf8Error) -> Error {
        "Protocol error: invalid frame format".into()
    }
}

impl From<std::num::TryFromIntError> for Error {
    fn from(_src: std::num::TryFromIntError) -> Error {
        "Protocol error: invalid frame format".into()
    }
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Incomplete => "stream ended early".fmt(fmt),
            Error::Anyhow(err) => err.fmt(fmt),
        }
    }
}

fn get_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !src.has_remaining() {
        Err(Error::Incomplete)
    } else {
        Ok(src.get_u8())
    }
}

fn peek_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !src.has_remaining() {
        Err(Error::Incomplete)
    } else {
        Ok(src.chunk()[0])
    }
}

fn skip(src: &mut Cursor<&[u8]>, n: usize) -> Result<(), Error> {
    if src.remaining() < n {
        Err(Error::Incomplete)
    } else {
        src.advance(n);
        Ok(())
    }
}

fn get_line<'a>(src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], Error> {
    let start = src.position() as usize;
    let end = src.get_ref().len() - 1;

    for i in start..end {
        if src.get_ref()[i] == b'\r' && src.get_ref()[i + 1] == b'\n' {
            src.set_position((i + 2) as u64);

            return Ok(&src.get_ref()[start..i]);
        }
    }

    Err(Error::Incomplete)
}

fn get_decimal(src: &mut Cursor<&[u8]>) -> Result<u64, Error> {
    let line = get_line(src)?.to_vec();
    String::from_utf8(line)?
        .parse()
        .map_err(|e| format!("Invalid frame format: failed to get_decimal: {}", e).into())
}

pub fn encode_simple_string(string: &str) -> String {
    format!("+{string}\r\n")
}

pub fn encode_simple_error(error: &str) -> String {
    format!("-{error}\r\n")
}

pub fn encode_integer(integer: i64) -> String {
    format!(":{integer}\r\n")
}

pub fn encode_bulk_string(string_option: Option<&str>) -> String {
    if string_option.is_none() {
        return "$-1\r\n".to_owned(); // null bulk string
    }
    let string = string_option.unwrap();
    let length = string.len();
    format!("${length}\r\n{string}\r\n")
}

pub fn encode_array(array: &Vec<Frame>) -> String {
    let length = array.len();
    let mut result = format!("*{length}\r\n");

    for item in array {
        result += &item.encode();
    }

    result
}

pub fn encode_null() -> String {
    "_\r\n".to_string()
}

// pub fn encode_boolean(boolean: bool) -> String {
//     let boolean_char = if boolean { "t" } else { "f" };
//     return format!("#{boolean_char}\r\n");
// }

// pub fn encode_double(double: f64) -> String {
//     return format!(",{double}\r\n");
// }

// pub fn encode_bulk_error(bulk_error: &str) -> String {
//     let length = bulk_error.len();
//     return format!("!{length}\r\n{bulk_error}\r\n");
// }

// pub fn encode_verbatim_string(string: &str) -> String {
//     let length = string.len();
//     return format!("={length}\r\n{string}\r\n");
// }

// pub fn encode_push(push: &Vec<Frame>) -> String {
//     let length = push.len();
//     let mut result = format!("*{length}\r\n");
//     for item in push {
//         result += &item.encode();
//     }
//     return result;
// }
