use std::{collections::HashMap, fmt::Display};

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

enum RDBOpCode {
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

impl RDBOpCode {
    fn from_u8(value: &u8) -> crate::Result<RDBOpCode> {
        match value {
            0xFF => Ok(RDBOpCode::Eof),
            0xFE => Ok(RDBOpCode::SelectDB),
            0xFD => Ok(RDBOpCode::ExpireTime),
            0xFC => Ok(RDBOpCode::ExpireTimeMs),
            0xFB => Ok(RDBOpCode::ResizeDB),
            0xFA => Ok(RDBOpCode::Aux),
            _ => Err(format!("Invalid RDB opcode {}", value).into()),
        }
    }

    #[allow(dead_code)]
    fn to_u8(&self) -> u8 {
        match self {
            RDBOpCode::Eof => 0xFF,
            RDBOpCode::SelectDB => 0xFE,
            RDBOpCode::ExpireTime => 0xFD,
            RDBOpCode::ExpireTimeMs => 0xFC,
            RDBOpCode::ResizeDB => 0xFB,
            RDBOpCode::Aux => 0xFA,
        }
    }
}

enum RDBEncodingLen {
    Bit6(u64),
    Bit14(u64),
    Bit64(u64),
    SpecialEncoding(u32),
}

impl RDBEncodingLen {
    fn from_u8(bytes: &mut impl Iterator<Item = u8>) -> crate::Result<RDBEncodingLen> {
        let first_byte = bytes.next().ok_or("Iter reached end")?;
        let first_2_bytes = first_byte & 192;

        match first_2_bytes {
            0 => Ok(RDBEncodingLen::Bit6(first_byte as u64)),
            64 => {
                let first_6_bits = first_byte & 63;
                let next_byte = bytes.next().ok_or("Iter reached end")?;
                let value = ((first_6_bits as u16) << 8) | next_byte as u16;
                Ok(RDBEncodingLen::Bit14(value as u64))
            }
            128 => {
                let mut val: u64 = 0;
                for _ in 0..4 {
                    let next_byte = bytes.next().ok_or("Iter reached end")?;
                    val = (val << 8) | next_byte as u64;
                }
                Ok(RDBEncodingLen::Bit64(val))
            }
            192 => {
                let last_6_bits = first_byte & 63;

                if last_6_bits == 0 {
                    let next_byte = bytes.next().ok_or("Iter reached end")?;
                    return Ok(RDBEncodingLen::SpecialEncoding(next_byte as u32));
                } else if last_6_bits < 3 {
                    let mut val: u32 = 0;
                    for _ in 0..last_6_bits {
                        let next_byte = bytes.next().ok_or("Iter reached end")?;
                        val = (val << 8) | next_byte as u32;
                    }
                    return Ok(RDBEncodingLen::SpecialEncoding(val));
                }

                Err(format!("Special encoding: {}", last_6_bits).into())
            }
            _ => Err("Invalid RDB length encoding".into()),
        }
    }
}

impl Display for RDBEncodingLen {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RDBEncodingLen::Bit6(num) => write!(f, "{}", num),
            RDBEncodingLen::Bit14(num) => write!(f, "{}", num),
            RDBEncodingLen::Bit64(num) => write!(f, "{}", num),
            RDBEncodingLen::SpecialEncoding(num) => write!(f, "{}", num),
        }
    }
}

enum RDBEncodingType {
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

impl RDBEncodingType {
    fn from_u8(value: &u8) -> crate::Result<RDBEncodingType> {
        match value {
            0 => Ok(RDBEncodingType::String),
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
        let len_encoding = RDBEncodingLen::from_u8(bytes)?;
        match len_encoding {
            RDBEncodingLen::Bit6(num) | RDBEncodingLen::Bit14(num) | RDBEncodingLen::Bit64(num) => {
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
            RDBEncodingLen::SpecialEncoding(num) => Ok(StringEncoding::Int32(num)),
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

    fn get_next_opcode(&mut self, bite: &u8) -> crate::Result<RDBOpCode> {
        RDBOpCode::from_u8(bite)
    }

    async fn get_rbd_bytes(&self) -> crate::Result<Vec<u8>> {
        let mut file = File::open(self.filename.as_str())
            .await
            .map_err(|e| format!("Error opening RDB file: {}", e))?;
        let mut buffer = Vec::new();

        file.read_to_end(&mut buffer).await?;

        Ok(buffer)
    }

    pub async fn read_rdb(&mut self) -> crate::Result<HashMap<String, String>> {
        let mut bytes = self.get_rbd_bytes().await?;

        let magic_string = bytes.drain(0..5).collect::<Vec<u8>>();
        if magic_string != b"REDIS" {
            return Err("Invalid RDB file".into());
        }

        let _version = bytes.drain(0..4).collect::<Vec<u8>>();
        let mut byte_iter = bytes.into_iter().peekable();
        let mut next_byte = byte_iter.next().ok_or("Iter reached end")?;

        let mut db: HashMap<String, String> = HashMap::new();

        loop {
            let opcode = self.get_next_opcode(&next_byte)?;

            match opcode {
                // End of rdb reached
                RDBOpCode::Eof => {
                    return Ok(db);
                }
                RDBOpCode::SelectDB => {
                    let _db_number = RDBEncodingLen::from_u8(&mut byte_iter)?;
                    let _opcode =
                        self.get_next_opcode(&byte_iter.next().ok_or("Iter reached end")?)?;
                    let _db_size = RDBEncodingLen::from_u8(&mut byte_iter)?;
                    let _exp_size = RDBEncodingLen::from_u8(&mut byte_iter)?;

                    loop {
                        let (k, v) = self.load_key_val(&mut byte_iter)?;
                        db.insert(k, v);

                        if let Some(next_byte) = byte_iter.peek() {
                            // If next byte is not a valid opcode, skip it
                            if let Err(_e) = self.get_next_opcode(next_byte) {
                                continue;
                            } else {
                                break;
                            }
                        }
                    }
                }
                RDBOpCode::Aux => loop {
                    let _key = StringEncoding::from_u8(&mut byte_iter)?.to_string();
                    let _val = StringEncoding::from_u8(&mut byte_iter)?.to_string();

                    let nb = byte_iter.peek().ok_or("Iter reached end")?;

                    // if next opcode is SelectDB, break, so we can process it
                    if let RDBOpCode::SelectDB = self.get_next_opcode(nb).unwrap_or(RDBOpCode::Aux)
                    {
                        break;
                    }
                    // if next opcode is Aux, continue to next key-val pair
                    if let RDBOpCode::Aux = self.get_next_opcode(nb).unwrap_or(RDBOpCode::SelectDB)
                    {
                        byte_iter.next().ok_or("Iter reached end")?;
                        continue;
                    }
                },
                RDBOpCode::ResizeDB => panic!("ResizeDB should come after select DB"),
                RDBOpCode::ExpireTime => todo!(),
                RDBOpCode::ExpireTimeMs => todo!(),
            };

            next_byte = byte_iter.next().ok_or("Iter reached end")?;
        }
    }

    fn load_key_val(
        &mut self,
        bytes: &mut impl Iterator<Item = u8>,
    ) -> crate::Result<(String, String)> {
        let val_type_byte = bytes.next().ok_or("Iter reached end")?;
        let key = StringEncoding::from_u8(bytes)?.to_string();

        let val_encoding = RDBEncodingType::from_u8(&val_type_byte)?;
        match val_encoding {
            RDBEncodingType::String => {
                let val_string_encoding = StringEncoding::from_u8(bytes)?;
                let val = val_string_encoding.to_string();

                Ok((key, val))
            }
        }
    }
}
