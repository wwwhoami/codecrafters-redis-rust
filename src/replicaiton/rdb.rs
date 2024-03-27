use base64::{self, Engine};
use bytes::Bytes;

const EMPTY_RDB_BASE64: &[u8] = b"UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";

pub fn empty_rdb() -> Bytes {
    let decoded_bytes = base64::prelude::BASE64_STANDARD
        .decode(EMPTY_RDB_BASE64)
        .unwrap();

    Bytes::from(decoded_bytes)
}
