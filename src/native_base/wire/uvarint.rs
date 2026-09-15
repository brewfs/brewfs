//! Canonical uvarint (shortest unsigned LEB128) and the scalar reader/writer
//! used by every length-prefixed structure in the native packed base formats.
//!
//! Spec 02 §1: `bytes = canonical_uvarint(length) || payload`. At most 10
//! bytes; the last byte may only use the bits of u64 that remain. Overlong,
//! truncated, and non-shortest encodings are all rejected. `Option<T>` is a
//! u8 tag (0/1) followed by `T` when 1; vec is canonical_uvarint(count)
//! followed by per-item encoding; bool is exactly 0 or 1; i64 is two's
//! complement little-endian.

use super::error::{WireError, WireResult};

/// Maximum encoded length of a canonical uvarint (u64 uses at most 10 bytes).
pub const UVARINT_MAX_LEN: usize = 10;

/// Encode `value` as the shortest unsigned LEB128.
pub fn encode_uvarint(value: u64, out: &mut Vec<u8>) {
    let mut v = value;
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

/// Return the canonical encoding of `value`.
pub fn uvarint_bytes(value: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(UVARINT_MAX_LEN);
    encode_uvarint(value, &mut out);
    out
}

/// Decode a canonical uvarint from the front of `buf`.
///
/// Rejects: empty input (truncated), an unterminated 10-byte sequence
/// (truncated), a 10th byte using bits beyond u64, and any non-shortest
/// encoding (e.g. `80 00` for zero).
pub fn decode_uvarint(buf: &[u8]) -> WireResult<(u64, usize)> {
    let mut value: u64 = 0;
    let mut shift: u32 = 0;
    for (i, &byte) in buf.iter().enumerate() {
        if i >= UVARINT_MAX_LEN {
            return Err(WireError::invalid(
                "uvarint",
                format!("longer than {UVARINT_MAX_LEN} bytes"),
            ));
        }
        let payload = u64::from(byte & 0x7f);
        if i == UVARINT_MAX_LEN - 1 {
            // Final byte of a 10-byte sequence: only bit 0 of the 64th bit
            // position is representable (shift 63).
            if payload > 1 {
                return Err(WireError::invalid(
                    "uvarint",
                    format!("last byte 0x{byte:02x} exceeds u64 range"),
                ));
            }
        }
        // Checked shift+or: a set bit at shift >= 64 is impossible because
        // i < 10 keeps shift <= 63.
        value |= payload << shift;
        if byte & 0x80 == 0 {
            // Terminated: reject non-shortest encodings. Any earlier
            // terminator with a zero payload byte means the same value had a
            // shorter encoding.
            if i + 1 < UVARINT_MAX_LEN && payload == 0 && value >> shift == 0 && i > 0 {
                // e.g. `81 00`: the terminating byte contributes nothing, so
                // the value was fully representable in fewer bytes.
                // (shift>0 guarantees at least one preceding byte.)
                return Err(WireError::invalid(
                    "uvarint",
                    format!("non-shortest encoding: terminator at byte {i} adds no bits"),
                ));
            }
            return Ok((value, i + 1));
        }
        shift += 7;
    }
    Err(WireError::Truncated {
        what: "uvarint",
        need: UVARINT_MAX_LEN,
        have: buf.len(),
    })
}

/// Bounds-checked sequential reader. Every take is length-validated before
/// the slice is materialized, so a malformed length never allocates.
pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    pub fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    pub fn take(&mut self, len: usize, what: &'static str) -> WireResult<&'a [u8]> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or_else(|| WireError::invalid(what, "length overflow"))?;
        if end > self.buf.len() {
            return Err(WireError::Truncated {
                what,
                need: len,
                have: self.buf.len() - self.pos,
            });
        }
        let out = &self.buf[self.pos..end];
        self.pos = end;
        Ok(out)
    }

    pub fn u8(&mut self, what: &'static str) -> WireResult<u8> {
        Ok(self.take(1, what)?[0])
    }

    pub fn u16(&mut self, what: &'static str) -> WireResult<u16> {
        let b = self.take(2, what)?;
        Ok(u16::from_le_bytes([b[0], b[1]]))
    }

    pub fn u32(&mut self, what: &'static str) -> WireResult<u32> {
        let b = self.take(4, what)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    pub fn u64(&mut self, what: &'static str) -> WireResult<u64> {
        let b = self.take(8, what)?;
        Ok(u64::from_le_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }

    /// i64 in two's complement little-endian (spec 02 §1).
    pub fn i64(&mut self, what: &'static str) -> WireResult<i64> {
        Ok(self.u64(what)? as i64)
    }

    pub fn uvarint(&mut self, _what: &'static str) -> WireResult<u64> {
        let (v, used) = decode_uvarint(&self.buf[self.pos..])?;
        self.pos += used;
        Ok(v)
    }

    pub fn bool(&mut self, what: &'static str) -> WireResult<bool> {
        match self.u8(what)? {
            0 => Ok(false),
            1 => Ok(true),
            other => Err(WireError::invalid(
                what,
                format!("bool byte 0x{other:02x} is not 0/1"),
            )),
        }
    }

    /// `bytes = canonical_uvarint(length) || payload`, with the announced
    /// length validated against the remaining input before any use.
    pub fn bytes(&mut self, what: &'static str) -> WireResult<&'a [u8]> {
        let len = self.uvarint(what)?;
        let len = usize::try_from(len).map_err(|_| {
            WireError::invalid(what, "length does not fit in usize on this platform")
        })?;
        self.take(len, what)
    }

    /// `Option<T>` = u8 tag (0/1) + [T]; returns `None` for tag 0 after
    /// consuming exactly one byte.
    pub fn option_tag(&mut self, what: &'static str) -> WireResult<bool> {
        match self.u8(what)? {
            0 => Ok(false),
            1 => Ok(true),
            other => Err(WireError::invalid(
                what,
                format!("option tag 0x{other:02x} is not 0/1"),
            )),
        }
    }
}

/// Sequential writer mirroring [`Reader`].
#[derive(Default)]
pub struct Writer {
    buf: Vec<u8>,
}

impl Writer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.buf
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.buf
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn put(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    pub fn u8(&mut self, v: u8) {
        self.buf.push(v);
    }

    pub fn u16(&mut self, v: u16) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    pub fn u32(&mut self, v: u32) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    pub fn u64(&mut self, v: u64) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    pub fn i64(&mut self, v: i64) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    pub fn uvarint(&mut self, v: u64) {
        encode_uvarint(v, &mut self.buf);
    }

    pub fn bool(&mut self, v: bool) {
        self.buf.push(u8::from(v));
    }

    pub fn bytes(&mut self, bytes: &[u8]) {
        self.uvarint(bytes.len() as u64);
        self.buf.extend_from_slice(bytes);
    }

    pub fn option_tag(&mut self, some: bool) {
        self.buf.push(u8::from(some));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Goldens from examples/wire-vectors.json (uvarint_positive).
    #[test]
    fn positive_vectors_match_goldens() {
        let cases: &[(u64, &str)] = &[
            (0, "00"),
            (1, "01"),
            (127, "7f"),
            (128, "8001"),
            (255, "ff01"),
            (16384, "808001"),
            (4_294_967_296, "8080808010"),
            (u64::MAX, "ffffffffffffffffff01"),
        ];
        for &(value, hex) in cases {
            assert_eq!(hex::encode(uvarint_bytes(value)), hex, "value {value}");
            let decoded = decode_uvarint(&hex::decode(hex).unwrap()).unwrap();
            assert_eq!(decoded, (value, hex.len() / 2));
        }
    }

    // Goldens from examples/wire-vectors.json (uvarint_negative_hex).
    #[test]
    fn negative_vectors_rejected() {
        let cases: &[&str] = &[
            "",                       // empty: truncated
            "80",                     // unterminated: truncated
            "8000",                   // non-shortest zero
            "8100",                   // non-shortest one
            "ffffffffffffffffff02",   // 10th byte exceeds u64 range
            "8080808080808080808080", // 11 bytes
        ];
        for &hex in cases {
            let bytes = hex::decode(hex).unwrap();
            assert!(
                decode_uvarint(&bytes).is_err(),
                "expected rejection of {hex}"
            );
        }
    }

    #[test]
    fn roundtrip_boundaries() {
        for &v in &[
            0u64,
            1,
            0x7f,
            0x80,
            0x3fff,
            0x4000,
            u32::MAX as u64,
            u32::MAX as u64 + 1,
            u64::MAX - 1,
            u64::MAX,
        ] {
            let enc = uvarint_bytes(v);
            assert_eq!(decode_uvarint(&enc).unwrap(), (v, enc.len()));
        }
    }

    #[test]
    fn reader_bytes_validates_length_before_use() {
        // Announced length larger than input must be Truncated, not panic.
        let mut w = Writer::new();
        w.uvarint(1000);
        w.put(&[0u8; 4]);
        let mut r = Reader::new(w.as_slice());
        assert!(matches!(r.bytes("test"), Err(WireError::Truncated { .. })));
    }

    #[test]
    fn reader_rejects_invalid_bool_and_option_tags() {
        let mut r = Reader::new(&[2u8]);
        assert!(r.bool("test").is_err());
        let mut r = Reader::new(&[7u8]);
        assert!(r.option_tag("test").is_err());
    }
}
