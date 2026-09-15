//! FrameHeader and frame payload codecs (spec 03 §3–§4).
//!
//! A frame is `FrameHeader[80] + stored payload + zero padding to 8-byte
//! alignment`. `raw_digest` is SHA-256 of the payload after undoing the
//! outer codec. The Data Seal's `stored_digest` (SHA-256 of
//! `FrameHeader || stored payload`) is computed here as well for the seal
//! writer, but the pack itself only persists `raw_digest`.

use sha2::{Digest, Sha256};

use super::container::{Codec, features};
use super::error::{WireError, WireResult};

pub const FRAME_HEADER_LEN: usize = 80;
pub const FRAME_MAGIC: &[u8; 4] = b"BNFR";
pub const FRAME_VERSION: u8 = 1;
pub const ALIGNMENT: u64 = 8;

/// Hard payload limits (spec 02 §8, 03 §4).
pub const MAX_PLAIN_PAYLOAD: u32 = 64 * 1024 * 1024;
pub const MAX_NATIVE_OUTER_PAYLOAD: u32 = 65 * 1024 * 1024;
pub const MAX_ENCODED_FRAME_PAYLOAD: u32 = 65 * 1024 * 1024;

/// payload_format (spec 03 §3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadFormat {
    PlainBytes,
    NativeBlockV1,
}

impl PayloadFormat {
    pub fn as_u8(self) -> u8 {
        match self {
            PayloadFormat::PlainBytes => 0,
            PayloadFormat::NativeBlockV1 => 1,
        }
    }

    pub fn from_u8(v: u8) -> WireResult<PayloadFormat> {
        match v {
            0 => Ok(PayloadFormat::PlainBytes),
            1 => Ok(PayloadFormat::NativeBlockV1),
            other => Err(WireError::UnsupportedFormat(format!(
                "frame payload_format {other} (expected 0=PlainBytes or 1=NativeBlockV1)"
            ))),
        }
    }

    pub fn max_raw_payload(self) -> u32 {
        match self {
            PayloadFormat::PlainBytes => MAX_PLAIN_PAYLOAD,
            PayloadFormat::NativeBlockV1 => MAX_NATIVE_OUTER_PAYLOAD,
        }
    }

    /// The required_features bit a writer must declare for this format.
    pub fn feature_bit(self) -> u64 {
        match self {
            PayloadFormat::PlainBytes => features::PLAIN_BYTES_FRAMES,
            PayloadFormat::NativeBlockV1 => features::NATIVE_BLOCK_V1_INNER,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameHeader {
    pub payload_format: PayloadFormat,
    pub codec: Codec,
    /// Pack-internal ordinal, contiguous from 0.
    pub ordinal: u64,
    pub stored_len: u32,
    pub raw_len: u32,
    /// SHA-256 of the payload after undoing the outer codec.
    pub raw_digest: [u8; 32],
}

impl FrameHeader {
    pub fn encode(&self) -> [u8; FRAME_HEADER_LEN] {
        let mut out = [0u8; FRAME_HEADER_LEN];
        out[0..4].copy_from_slice(FRAME_MAGIC);
        out[4] = FRAME_VERSION;
        out[5] = self.payload_format.as_u8();
        out[6] = self.codec.as_u8();
        out[7] = 0; // flags
        out[8..16].copy_from_slice(&self.ordinal.to_le_bytes());
        out[16..20].copy_from_slice(&self.stored_len.to_le_bytes());
        out[20..24].copy_from_slice(&self.raw_len.to_le_bytes());
        out[24..56].copy_from_slice(&self.raw_digest);
        // reserved [56..76] stays zero
        let crc = super::container::crc32c(&out[0..76]);
        out[76..80].copy_from_slice(&crc.to_le_bytes());
        out
    }

    /// Parse and validate a frame header from the front of `bytes`. Enforces
    /// magic/version/format/codec/flags/reserved, per-format payload limits,
    /// the PlainBytes `stored_len == raw_len` invariant for codec None, and
    /// the CRC. Checks nothing about surrounding object bounds.
    pub fn parse(bytes: &[u8]) -> WireResult<FrameHeader> {
        let what = "frame header";
        if bytes.len() < FRAME_HEADER_LEN {
            return Err(WireError::Truncated {
                what,
                need: FRAME_HEADER_LEN,
                have: bytes.len(),
            });
        }
        if &bytes[0..4] != FRAME_MAGIC {
            return Err(WireError::invalid(
                what,
                format!("magic {:02x?} is not {FRAME_MAGIC:02x?}", &bytes[0..4]),
            ));
        }
        if bytes[4] != FRAME_VERSION {
            return Err(WireError::UnsupportedFormat(format!(
                "frame version {} (expected {FRAME_VERSION})",
                bytes[4]
            )));
        }
        let payload_format = PayloadFormat::from_u8(bytes[5])?;
        let codec = Codec::from_u8(bytes[6])?;
        if bytes[7] != 0 {
            return Err(WireError::invalid(what, "flags must be zero"));
        }
        let ordinal = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        let stored_len = u32::from_le_bytes(bytes[16..20].try_into().unwrap());
        let raw_len = u32::from_le_bytes(bytes[20..24].try_into().unwrap());
        let raw_digest: [u8; 32] = bytes[24..56].try_into().unwrap();
        if bytes[56..76].iter().any(|&b| b != 0) {
            return Err(WireError::invalid(what, "reserved bytes not zero"));
        }
        let stored_crc = u32::from_le_bytes(bytes[76..80].try_into().unwrap());
        let computed = super::container::crc32c(&bytes[0..76]);
        if stored_crc != computed {
            return Err(WireError::CrcMismatch {
                what,
                stored: stored_crc,
                computed,
            });
        }
        if stored_len > MAX_ENCODED_FRAME_PAYLOAD {
            return Err(WireError::LimitExceeded(format!(
                "frame stored_len {stored_len} exceeds encoded payload limit {MAX_ENCODED_FRAME_PAYLOAD}"
            )));
        }
        if raw_len > payload_format.max_raw_payload() {
            return Err(WireError::LimitExceeded(format!(
                "frame raw_len {raw_len} exceeds {:?} payload limit {}",
                payload_format,
                payload_format.max_raw_payload()
            )));
        }
        // NativeBlockV1 never carries an outer codec (spec 03 §4.2).
        if payload_format == PayloadFormat::NativeBlockV1 && codec != Codec::None {
            return Err(WireError::invalid(
                what,
                "NativeBlockV1 frames must use outer codec None",
            ));
        }
        // PlainBytes with codec None has stored_len == raw_len (spec 03 §4.1).
        if codec == Codec::None && stored_len != raw_len {
            return Err(WireError::invalid(
                what,
                format!("codec None requires stored_len {stored_len} == raw_len {raw_len}"),
            ));
        }
        Ok(FrameHeader {
            payload_format,
            codec,
            ordinal,
            stored_len,
            raw_len,
            raw_digest,
        })
    }

    /// Decode the frame payload: undo the outer codec and verify the exact
    /// output length and the `raw_digest` (spec 03 §4.1: Zstd output must be
    /// exactly `raw_len`). The returned buffer is bounded by the validated
    /// `raw_len`, never by anything the bytes claim beyond the header limits.
    pub fn decode_payload(&self, stored: &[u8]) -> WireResult<Vec<u8>> {
        if stored.len() as u64 != self.stored_len as u64 {
            return Err(WireError::invalid(
                "frame payload",
                format!(
                    "stored payload is {} bytes, header announces {}",
                    stored.len(),
                    self.stored_len
                ),
            ));
        }
        let raw = match self.codec {
            Codec::None => stored.to_vec(),
            Codec::Zstd => {
                let expected = self.raw_len as usize;
                let out = zstd::bulk::decompress(stored, expected)
                    .map_err(|e| WireError::Codec(format!("zstd: {e}")))?;
                if out.len() != expected {
                    return Err(WireError::invalid(
                        "frame payload",
                        format!(
                            "zstd output {} bytes, header announces {expected}",
                            out.len()
                        ),
                    ));
                }
                out
            }
        };
        let digest = Sha256::digest(&raw);
        if digest.as_slice() != self.raw_digest {
            return Err(WireError::HashMismatch {
                what: "frame raw payload",
                stored: hex::encode(self.raw_digest),
                computed: hex::encode(digest),
            });
        }
        Ok(raw)
    }

    /// The Data Seal digest: SHA-256(FrameHeader || stored payload),
    /// excluding alignment padding (spec 03 §3).
    pub fn stored_digest(&self, stored: &[u8]) -> [u8; 32] {
        let mut h = Sha256::new();
        h.update(self.encode());
        h.update(stored);
        h.finalize().into()
    }

    /// Total on-object footprint: header + payload + zero padding to the
    /// 8-byte alignment.
    pub fn frame_span(&self) -> u64 {
        let total = FRAME_HEADER_LEN as u64 + self.stored_len as u64;
        total.div_ceil(ALIGNMENT) * ALIGNMENT
    }
}

/// Compress `raw` with a single independent Zstd frame (no dictionary) and
/// return the stored bytes; `level` only affects construction (spec 03 §4.1).
pub fn zstd_encode(raw: &[u8], level: i32) -> WireResult<Vec<u8>> {
    zstd::bulk::compress(raw, level).map_err(|e| WireError::Codec(format!("zstd: {e}")))
}

/// Read one frame record out of an object at `offset`, returning the header
/// and the stored payload slice. Validates that the frame (including
/// padding) fits before `end` and that the padding is zero.
pub(crate) fn read_frame_at(
    object: &[u8],
    offset: usize,
    end: usize,
) -> WireResult<(FrameHeader, &[u8])> {
    let what = "frame";
    if offset >= end {
        return Err(WireError::invalid(
            what,
            format!("offset {offset} past frame region end {end}"),
        ));
    }
    if object.len() < offset || object.len() - offset < FRAME_HEADER_LEN {
        return Err(WireError::Truncated {
            what,
            need: FRAME_HEADER_LEN,
            have: object.len().saturating_sub(offset),
        });
    }
    let header = FrameHeader::parse(&object[offset..offset + FRAME_HEADER_LEN])?;
    let payload_start = offset + FRAME_HEADER_LEN;
    let span = header.frame_span() as usize;
    let payload_end = payload_start
        .checked_add(header.stored_len as usize)
        .ok_or_else(|| WireError::invalid(what, "payload end overflow"))?;
    let frame_end = offset
        .checked_add(span)
        .ok_or_else(|| WireError::invalid(what, "frame end overflow"))?;
    if frame_end > end {
        return Err(WireError::invalid(
            what,
            format!("frame spans past region end ({frame_end} > {end})"),
        ));
    }
    let padding = &object[payload_end..frame_end];
    if padding.iter().any(|&b| b != 0) {
        return Err(WireError::invalid(what, "alignment padding not zero"));
    }
    Ok((header, &object[payload_start..payload_end]))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> FrameHeader {
        FrameHeader {
            payload_format: PayloadFormat::PlainBytes,
            codec: Codec::None,
            ordinal: 0,
            stored_len: 16,
            raw_len: 16,
            raw_digest: Sha256::digest(b"abcdefghijklmnop").into(),
        }
    }

    #[test]
    fn frame_header_roundtrip() {
        let h = sample();
        assert_eq!(FrameHeader::parse(&h.encode()).unwrap(), h);
    }

    #[test]
    fn frame_header_rejects_unknown_version_format_codec_flags() {
        for (at, bad, why) in [
            (4usize, 2u8, "version"),
            (5, 2, "payload_format"),
            (6, 1, "codec"),
            (7, 1, "flags"),
        ] {
            let mut enc = sample().encode();
            enc[at] = bad;
            // Version/format/codec are UnsupportedFormat; flags is Invalid —
            // both must be hard rejections, never fallbacks.
            assert!(FrameHeader::parse(&enc).is_err(), "{why}");
        }
    }

    #[test]
    fn frame_header_rejects_crc_and_reserved() {
        let mut enc = sample().encode();
        enc[9] ^= 0xff;
        assert!(matches!(
            FrameHeader::parse(&enc),
            Err(WireError::CrcMismatch { .. })
        ));
        let mut enc = sample().encode();
        enc[60] = 1;
        let crc = super::super::container::crc32c(&enc[0..76]);
        enc[76..80].copy_from_slice(&crc.to_le_bytes());
        assert!(FrameHeader::parse(&enc).is_err());
    }

    #[test]
    fn frame_header_enforces_payload_limits() {
        let mut h = sample();
        h.raw_len = MAX_PLAIN_PAYLOAD + 1;
        h.stored_len = h.raw_len;
        assert!(matches!(
            FrameHeader::parse(&h.encode()),
            Err(WireError::LimitExceeded(_))
        ));
        let mut h = sample();
        h.stored_len = MAX_ENCODED_FRAME_PAYLOAD + 1;
        assert!(matches!(
            FrameHeader::parse(&h.encode()),
            Err(WireError::LimitExceeded(_))
        ));
    }

    #[test]
    fn native_block_v1_requires_outer_codec_none() {
        let mut h = sample();
        h.payload_format = PayloadFormat::NativeBlockV1;
        // With codec None the combination is legal; the violation is carrying
        // an outer codec on a NativeBlockV1 frame (spec 03 §4.2).
        h.codec = Codec::Zstd;
        assert!(FrameHeader::parse(&h.encode()).is_err());
    }

    #[test]
    fn codec_none_requires_stored_eq_raw() {
        let mut h = sample();
        h.stored_len = 15;
        assert!(FrameHeader::parse(&h.encode()).is_err());
    }

    #[test]
    fn zstd_roundtrip_and_exact_length_enforced() {
        let raw = b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let stored = zstd_encode(raw, 3).unwrap();
        let h = FrameHeader {
            payload_format: PayloadFormat::PlainBytes,
            codec: Codec::Zstd,
            ordinal: 0,
            stored_len: stored.len() as u32,
            raw_len: raw.len() as u32,
            raw_digest: Sha256::digest(raw).into(),
        };
        assert_eq!(h.decode_payload(&stored).unwrap(), raw);

        // Announced raw_len one byte too large: decompression must be
        // rejected even though zstd itself would succeed.
        let mut bad = h.clone();
        bad.raw_len += 1;
        assert!(bad.decode_payload(&stored).is_err());

        // Corrupted digest must be rejected.
        let mut bad = h.clone();
        bad.raw_digest[0] ^= 1;
        assert!(matches!(
            bad.decode_payload(&stored),
            Err(WireError::HashMismatch { .. })
        ));
    }

    #[test]
    fn frame_span_pads_to_alignment() {
        let h = FrameHeader {
            payload_format: PayloadFormat::PlainBytes,
            codec: Codec::None,
            ordinal: 0,
            stored_len: 9,
            raw_len: 9,
            raw_digest: [0u8; 32],
        };
        assert_eq!(h.frame_span(), 80 + 16); // 89 -> 96
        let h = FrameHeader {
            stored_len: 16,
            ..h
        };
        assert_eq!(h.frame_span(), 80 + 16); // already aligned
    }
}
