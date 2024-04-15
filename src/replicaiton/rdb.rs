use std::{
    collections::HashMap,
    fmt::Display,
    time::{Duration, SystemTime},
};

use base64::{self, Engine};
use bytes::Bytes;
use tokio::{fs::File, io::AsyncReadExt};

const EMPTY_RDB_BASE64: &[u8] = b"UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";

pub fn empty_rdb() -> Bytes {
    let decoded_bytes = base64::prelude::BASE64_STANDARD
        .decode(EMPTY_RDB_BASE64)
        .unwrap();

    Bytes::from(decoded_bytes)
}

enum RdbOpCode {
    Eof,
    /// Databese selector
    SelectDB,
    ExpireTime,
    ExpireTimeMs,
    /// Hash table sizes for main and expires keyspaces
    ResizeDB,
    /// Auxiliary fields
    Aux,
}

impl RdbOpCode {
    fn from_u8(value: &u8) -> crate::Result<RdbOpCode> {
        match value {
            0xFF => Ok(RdbOpCode::Eof),
            0xFE => Ok(RdbOpCode::SelectDB),
            0xFD => Ok(RdbOpCode::ExpireTime),
            0xFC => Ok(RdbOpCode::ExpireTimeMs),
            0xFB => Ok(RdbOpCode::ResizeDB),
            0xFA => Ok(RdbOpCode::Aux),
            _ => Err(format!("Invalid RDB opcode {}", value).into()),
        }
    }

    #[allow(dead_code)]
    fn to_u8(&self) -> u8 {
        match self {
            RdbOpCode::Eof => 0xFF,
            RdbOpCode::SelectDB => 0xFE,
            RdbOpCode::ExpireTime => 0xFD,
            RdbOpCode::ExpireTimeMs => 0xFC,
            RdbOpCode::ResizeDB => 0xFB,
            RdbOpCode::Aux => 0xFA,
        }
    }
}

enum RdbEncodingLen {
    Bit6(u64),
    Bit14(u64),
    Bit64(u64),
    SpecialEncoding(u32),
}

impl RdbEncodingLen {
    fn from_u8(bytes: &mut impl Iterator<Item = u8>) -> crate::Result<RdbEncodingLen> {
        let first_byte = bytes.next().ok_or("Iter reached end")?;
        let first_2_bytes = first_byte & 192;

        match first_2_bytes {
            0 => Ok(RdbEncodingLen::Bit6(first_byte as u64)),
            64 => {
                let first_6_bits = first_byte & 63;
                let next_byte = bytes.next().ok_or("Iter reached end")?;
                let value = ((first_6_bits as u16) << 8) | next_byte as u16;
                Ok(RdbEncodingLen::Bit14(value as u64))
            }
            128 => {
                let mut val: u64 = 0;
                for _ in 0..4 {
                    let next_byte = bytes.next().ok_or("Iter reached end")?;
                    val = (val << 8) | next_byte as u64;
                }
                Ok(RdbEncodingLen::Bit64(val))
            }
            192 => {
                let last_6_bits = first_byte & 63;

                if last_6_bits == 0 {
                    let next_byte = bytes.next().ok_or("Iter reached end")?;
                    return Ok(RdbEncodingLen::SpecialEncoding(next_byte as u32));
                } else if last_6_bits < 3 {
                    let mut val: u32 = 0;
                    for _ in 0..last_6_bits {
                        let next_byte = bytes.next().ok_or("Iter reached end")?;
                        val = (val << 8) | next_byte as u32;
                    }
                    return Ok(RdbEncodingLen::SpecialEncoding(val));
                }

                Err(format!("Special encoding: {}", last_6_bits).into())
            }
            _ => Err("Invalid RDB length encoding".into()),
        }
    }
}

impl Display for RdbEncodingLen {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RdbEncodingLen::Bit6(num) => write!(f, "{}", num),
            RdbEncodingLen::Bit14(num) => write!(f, "{}", num),
            RdbEncodingLen::Bit64(num) => write!(f, "{}", num),
            RdbEncodingLen::SpecialEncoding(num) => write!(f, "{}", num),
        }
    }
}

enum RdbEncodingType {
    String,
    // List,
    // Set,
    // SortedSet,
    // Hash,
    // ZipMap,
    // ZipList,
    // IntSet,
    // SortedSetZipList,
    // HashMapZipList,
    // ListQuickList,
}

impl RdbEncodingType {
    fn from_u8(value: &u8) -> crate::Result<RdbEncodingType> {
        match value {
            0 => Ok(RdbEncodingType::String),
            e => Err(format!("Invalid RDB value encoding {}", e).into()),
        }
    }
}

enum StringEncoding {
    Int32(u32),
    LenPrefixed(LenPrefixedString),
    #[allow(dead_code)]
    Lzf,
}

struct LenPrefixedString {
    #[allow(dead_code)]
    len: u32,
    value: String,
}

impl StringEncoding {
    fn from_u8(bytes: &mut impl Iterator<Item = u8>) -> crate::Result<StringEncoding> {
        let len_encoding = RdbEncodingLen::from_u8(bytes)?;
        match len_encoding {
            RdbEncodingLen::Bit6(num) | RdbEncodingLen::Bit14(num) | RdbEncodingLen::Bit64(num) => {
                let mut val: Vec<u8> = Vec::new();
                for _ in 0..num {
                    let byte = bytes.next().ok_or("Iter reached end")?;
                    val.push(byte);
                }
                let lps = LenPrefixedString {
                    len: num as u32,
                    value: String::from_utf8(val)?,
                };
                Ok(StringEncoding::LenPrefixed(lps))
            }
            RdbEncodingLen::SpecialEncoding(num) => Ok(StringEncoding::Int32(num)),
        }
    }
}

impl Display for StringEncoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StringEncoding::Int32(num) => write!(f, "{}", num),
            StringEncoding::LenPrefixed(lps) => write!(f, "{}", lps.value.clone()),
            StringEncoding::Lzf => write!(f, "LZF"),
        }
    }
}

pub struct RedisDB {
    filename: String,
}

impl RedisDB {
    pub fn new(filename: String) -> Self {
        Self { filename }
    }

    fn get_next_opcode(&self, bite: &u8) -> crate::Result<RdbOpCode> {
        RdbOpCode::from_u8(bite)
    }

    async fn get_rbd_bytes(&self) -> crate::Result<Vec<u8>> {
        let mut file = File::open(self.filename.as_str())
            .await
            .map_err(|e| format!("Error opening RDB file: {}", e))?;
        let mut buffer = Vec::new();

        file.read_to_end(&mut buffer).await?;

        Ok(buffer)
    }

    pub async fn read_rdb(
        &mut self,
    ) -> crate::Result<HashMap<String, (String, Option<SystemTime>)>> {
        let mut bytes = self.get_rbd_bytes().await?;

        let magic_string = bytes.drain(0..5).collect::<Vec<u8>>();
        if magic_string != b"REDIS" {
            return Err("Invalid RDB file".into());
        }

        let _version = bytes.drain(0..4).collect::<Vec<u8>>();
        let mut byte_iter = bytes.into_iter().peekable();
        let mut next_byte = byte_iter.next().ok_or("Iter reached end")?;

        let mut db = HashMap::new();

        loop {
            let opcode = self.get_next_opcode(&next_byte)?;

            match opcode {
                // End of rdb reached
                RdbOpCode::Eof => {
                    return Ok(db);
                }
                RdbOpCode::SelectDB => {
                    let _db_number = RdbEncodingLen::from_u8(&mut byte_iter)?;
                    let _opcode =
                        self.get_next_opcode(&byte_iter.next().ok_or("Iter reached end")?)?;
                    let _db_size = RdbEncodingLen::from_u8(&mut byte_iter)?;
                    let _exp_size = RdbEncodingLen::from_u8(&mut byte_iter)?;

                    loop {
                        let peeked_byte = *byte_iter.peek().ok_or("Iter reached end")?;
                        let expiry = self.get_expiry(peeked_byte, &mut byte_iter)?;

                        let (k, v) = self.load_key_val(&mut byte_iter)?;
                        db.insert(k, (v, expiry));

                        if let Some(next_byte) = byte_iter.peek() {
                            match self.get_next_opcode(next_byte) {
                                // proceed to the next key-value pair till we reach RdbOpCode
                                Ok(opcode) => match opcode {
                                    RdbOpCode::SelectDB
                                    | RdbOpCode::Aux
                                    | RdbOpCode::ResizeDB
                                    | RdbOpCode::Eof => break,
                                    _ => continue,
                                },
                                Err(_) => continue,
                            }
                        }
                    }
                }
                RdbOpCode::Aux => loop {
                    let _key = StringEncoding::from_u8(&mut byte_iter)?.to_string();
                    let _val = StringEncoding::from_u8(&mut byte_iter)?.to_string();

                    let nb = byte_iter.peek().ok_or("Iter reached end")?;

                    // if next opcode is SelectDB, break, so we can process it
                    if let RdbOpCode::SelectDB = self.get_next_opcode(nb).unwrap_or(RdbOpCode::Aux)
                    {
                        break;
                    }
                    // if next opcode is Aux, continue to next key-val pair
                    if let RdbOpCode::Aux = self.get_next_opcode(nb).unwrap_or(RdbOpCode::SelectDB)
                    {
                        byte_iter.next().ok_or("Iter reached end")?;
                        continue;
                    }
                },
                RdbOpCode::ResizeDB => panic!("ResizeDB should come after select DB"),
                RdbOpCode::ExpireTime => panic!("ExpireTime should come after select DB"),
                RdbOpCode::ExpireTimeMs => panic!("ExpireTimeMs should come after select DB"),
            };

            next_byte = byte_iter.next().ok_or("Iter reached end")?;
        }
    }

    fn get_expiry(
        &self,
        next_byte: u8,
        byte_iter: &mut impl Iterator<Item = u8>,
    ) -> crate::Result<Option<SystemTime>> {
        let expiry = match self.get_next_opcode(&next_byte) {
            Err(_) => None,
            Ok(opcode) => match opcode {
                RdbOpCode::ExpireTime => {
                    let _ = byte_iter.next().ok_or("Iter reached end")?;

                    let arr = byte_iter.take(4).collect::<Vec<u8>>();
                    let expiry = u64::from_le_bytes(arr.try_into().unwrap());

                    SystemTime::UNIX_EPOCH.checked_add(Duration::from_secs(expiry))
                }
                RdbOpCode::ExpireTimeMs => {
                    let _ = byte_iter.next().ok_or("Iter reached end")?;

                    let arr = byte_iter.take(8).collect::<Vec<u8>>();
                    let expiry = u64::from_le_bytes(arr.try_into().unwrap());

                    SystemTime::UNIX_EPOCH.checked_add(Duration::from_millis(expiry))
                }
                _ => None,
            },
        };

        Ok(expiry)
    }

    fn load_key_val(
        &mut self,
        bytes: &mut impl Iterator<Item = u8>,
    ) -> crate::Result<(String, String)> {
        let val_type_byte = bytes.next().ok_or("Iter reached end")?;
        let key = StringEncoding::from_u8(bytes)?.to_string();

        let val_encoding = RdbEncodingType::from_u8(&val_type_byte)?;
        match val_encoding {
            RdbEncodingType::String => {
                let val_string_encoding = StringEncoding::from_u8(bytes)?;
                let val = val_string_encoding.to_string();

                Ok((key, val))
            }
        }
    }
}
