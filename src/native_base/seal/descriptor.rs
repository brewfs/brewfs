//! FrameDescriptor: the 96-byte Data Seal frame table value (spec 04 §5).
//!
//! `object_offset` points at the 80-byte FrameHeader; the Range GET is
//! `[object_offset, object_offset + 80 + stored_len)`. The descriptor must
//! agree exactly with the actual frame header on ordinal, lengths, codec,
//! format and raw_digest — a mismatch is reference corruption.

use crate::native_base::wire::container::Codec;
use crate::native_base::wire::datapack::ScrubbedFrame;
use crate::native_base::wire::error::{WireError, WireResult};
use crate::native_base::wire::frame::{FRAME_HEADER_LEN, FrameHeader, PayloadFormat};
use crate::native_base::wire::uvarint::{Reader, Writer};

pub const FRAME_DESCRIPTOR_LEN: usize = 96;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameDescriptor {
    pub object_ordinal: u32,
    /// The pack-internal frame ordinal (contiguous from 0 inside the pack).
    pub frame_ordinal: u64,
    /// Object-relative offset of the 80-byte FrameHeader.
    pub object_offset: u64,
    /// Stored payload length, header excluded.
    pub stored_len: u32,
    pub raw_len: u32,
    pub payload_format: PayloadFormat,
    pub codec: Codec,
    /// SHA-256(FrameHeader || stored payload), excluding alignment padding.
    pub stored_digest: [u8; 32],
    /// SHA-256 of the payload after undoing the outer codec.
    pub raw_digest: [u8; 32],
}

impl FrameDescriptor {
    pub fn encode_into(&self, w: &mut Writer) {
        w.u32(self.object_ordinal);
        w.u64(self.frame_ordinal);
        w.u64(self.object_offset);
        w.u32(self.stored_len);
        w.u32(self.raw_len);
        w.u8(self.payload_format.as_u8());
        w.u8(self.codec.as_u8());
        w.u16(0); // reserved
        w.put(&self.stored_digest);
        w.put(&self.raw_digest);
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        self.encode_into(&mut w);
        debug_assert_eq!(w.len(), FRAME_DESCRIPTOR_LEN);
        w.into_bytes()
    }

    pub fn decode(r: &mut Reader<'_>) -> WireResult<FrameDescriptor> {
        let what = "frame descriptor";
        let object_ordinal = r.u32(what)?;
        let frame_ordinal = r.u64(what)?;
        let object_offset = r.u64(what)?;
        let stored_len = r.u32(what)?;
        let raw_len = r.u32(what)?;
        let payload_format = PayloadFormat::from_u8(r.u8(what)?)?;
        let codec = Codec::from_u8(r.u8(what)?)?;
        let reserved = r.u16(what)?;
        if reserved != 0 {
            return Err(WireError::invalid(what, "reserved must be zero"));
        }
        let stored_digest: [u8; 32] = r.take(32, what)?.try_into().unwrap();
        let raw_digest: [u8; 32] = r.take(32, what)?.try_into().unwrap();
        Ok(FrameDescriptor {
            object_ordinal,
            frame_ordinal,
            object_offset,
            stored_len,
            raw_len,
            payload_format,
            codec,
            stored_digest,
            raw_digest,
        })
    }

    /// The Range GET extent for this frame: `[start, end)` covering the
    /// FrameHeader plus the stored payload, alignment padding excluded.
    pub fn range_get(&self) -> (u64, u64) {
        let start = self.object_offset;
        let end = start + FRAME_HEADER_LEN as u64 + self.stored_len as u64;
        (start, end)
    }

    /// Check the descriptor against the actual frame header fetched from the
    /// object (spec 04 §5: ordinal, lengths, codec, format and raw_digest
    /// must be identical).
    pub fn verify_against_header(&self, header: &FrameHeader) -> Result<(), String> {
        let mut mismatches = Vec::new();
        if self.frame_ordinal != header.ordinal {
            mismatches.push(format!(
                "frame_ordinal {} vs header {}",
                self.frame_ordinal, header.ordinal
            ));
        }
        if self.stored_len != header.stored_len {
            mismatches.push(format!(
                "stored_len {} vs header {}",
                self.stored_len, header.stored_len
            ));
        }
        if self.raw_len != header.raw_len {
            mismatches.push(format!(
                "raw_len {} vs header {}",
                self.raw_len, header.raw_len
            ));
        }
        if self.payload_format != header.payload_format {
            mismatches.push(format!(
                "payload_format {:?} vs header {:?}",
                self.payload_format, header.payload_format
            ));
        }
        if self.codec != header.codec {
            mismatches.push(format!(
                "codec {:?} vs header {:?}",
                self.codec, header.codec
            ));
        }
        if self.raw_digest != header.raw_digest {
            mismatches.push("raw_digest".to_string());
        }
        if mismatches.is_empty() {
            Ok(())
        } else {
            Err(mismatches.join(", "))
        }
    }

    /// Derive the descriptor from a scrubbed frame (the pack builder's own
    /// output, deterministically re-scrubbed to learn offsets and digests).
    pub fn from_scrubbed(frame: &ScrubbedFrame, object_ordinal: u32) -> FrameDescriptor {
        FrameDescriptor {
            object_ordinal,
            frame_ordinal: frame.header.ordinal,
            object_offset: frame.object_offset,
            stored_len: frame.header.stored_len,
            raw_len: frame.header.raw_len,
            payload_format: frame.header.payload_format,
            codec: frame.header.codec,
            stored_digest: frame.header.stored_digest(&frame.stored),
            raw_digest: frame.header.raw_digest,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> FrameDescriptor {
        FrameDescriptor {
            object_ordinal: 1,
            frame_ordinal: 0,
            object_offset: 64,
            stored_len: 16,
            raw_len: 16,
            payload_format: PayloadFormat::PlainBytes,
            codec: Codec::None,
            stored_digest: [1u8; 32],
            raw_digest: [2u8; 32],
        }
    }

    #[test]
    fn descriptor_is_exactly_96_bytes_and_roundtrips() {
        let d = sample();
        assert_eq!(d.encode().len(), FRAME_DESCRIPTOR_LEN);
        let encoded = d.encode();
        let mut r = Reader::new(&encoded);
        assert_eq!(FrameDescriptor::decode(&mut r).unwrap(), d);
        assert!(r.is_empty());
    }

    #[test]
    fn reserved_must_be_zero() {
        let mut enc = sample().encode();
        enc[30] = 1; // reserved u16 at [30..32] (after format/codec bytes)
        assert!(FrameDescriptor::decode(&mut Reader::new(&enc)).is_err());
    }

    #[test]
    fn range_get_covers_header_and_payload() {
        let d = sample();
        assert_eq!(d.range_get(), (64, 64 + 80 + 16));
    }

    #[test]
    fn header_mismatch_detected_on_every_field() {
        let header = FrameHeader {
            payload_format: PayloadFormat::PlainBytes,
            codec: Codec::None,
            ordinal: 0,
            stored_len: 16,
            raw_len: 16,
            raw_digest: [2u8; 32],
        };
        assert!(sample().verify_against_header(&header).is_ok());

        // Each single-field flip must be reported.
        let mut d = sample();
        d.frame_ordinal = 1;
        assert!(d.verify_against_header(&header).is_err());
        let mut d = sample();
        d.stored_len = 15;
        assert!(d.verify_against_header(&header).is_err());
        let mut d = sample();
        d.raw_len = 17;
        assert!(d.verify_against_header(&header).is_err());
        let mut d = sample();
        d.payload_format = PayloadFormat::NativeBlockV1;
        assert!(d.verify_against_header(&header).is_err());
        let mut d = sample();
        d.codec = Codec::Zstd;
        assert!(d.verify_against_header(&header).is_err());
        let mut d = sample();
        d.raw_digest[0] ^= 1;
        assert!(d.verify_against_header(&header).is_err());
    }
}
