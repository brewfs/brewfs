//! DataPack container: sequential scrub parsing and deterministic building
//! (spec 03 §2–§3).
//!
//! A pack is `Header[64] (FrameHeader[80] + payload + padding-to-8)*
//! Footer[64]` with all root fields zero. The authoritative frame inventory
//! lives in the Data Seal; the parser here is the offline-scrub /
//! recovery-diagnostics path that walks frames from offset 64 to the footer.

use sha2::{Digest, Sha256};

use super::container::{
    Codec, ContainerFooter, ContainerHeader, FeatureClosure, MIN_OBJECT_LEN, ObjectKind, features,
};
use super::error::{WireError, WireResult};
use super::frame::{
    FRAME_HEADER_LEN, FrameHeader, MAX_ENCODED_FRAME_PAYLOAD, PayloadFormat, read_frame_at,
};

/// One scrubbed frame with its decoded (outer-codec-undone) payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScrubbedFrame {
    pub header: FrameHeader,
    /// Object-relative offset of the FrameHeader.
    pub object_offset: u64,
    /// Stored payload (after the outer codec is applied).
    pub stored: Vec<u8>,
    /// Payload with the outer codec undone; digest-verified against
    /// `header.raw_digest`.
    pub raw: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScrubbedPack {
    pub header: ContainerHeader,
    pub frames: Vec<ScrubbedFrame>,
    pub footer: ContainerFooter,
    /// SHA-256 of the entire object (header through footer).
    pub full_hash: [u8; 32],
    /// The capability closure of this pack: the declared
    /// `required_features` unioned with what the frames actually require
    /// (spec 02 §3, INV-12/INV-20). A pack only scrubs when the header
    /// already declares everything the content needs.
    pub feature_closure: FeatureClosure,
}

impl ScrubbedPack {
    /// Full scrub: parse header, walk frames verifying CRCs, ordinals,
    /// padding, digests and limits, then parse the footer. Rejects
    /// unsupported required features up front, and rejects a header whose
    /// declaration does not cover what the frames actually require (fail
    /// closed, spec 02 §3).
    pub fn scrub(object: &[u8]) -> WireResult<ScrubbedPack> {
        let header = ContainerHeader::parse(object)?;
        if header.kind != ObjectKind::DataPack {
            return Err(WireError::invalid(
                "data pack",
                format!("container kind {:?} is not DataPack", header.kind),
            ));
        }
        if header.object_len as usize > object.len() {
            // Object ends before the header announces its end.
            return Err(WireError::Truncated {
                what: "data pack",
                need: header.object_len as usize,
                have: object.len(),
            });
        }
        if header.object_len as usize != object.len() {
            return Err(WireError::invalid(
                "data pack",
                format!(
                    "header object_len {} != actual {}",
                    header.object_len,
                    object.len()
                ),
            ));
        }
        // DataPack root fields are all zero (spec 03 §2).
        if header.root_offset != 0 || header.root_stored_len != 0 || header.root_raw_len != 0 {
            return Err(WireError::invalid(
                "data pack header",
                "root fields must be zero for DataPack",
            ));
        }
        if header.required_features & !features::KNOWN != 0 {
            return Err(WireError::UnsupportedFormat(
                "required features beyond this build".to_string(),
            ));
        }
        header.ensure_supported_features()?;

        let footer = super::container::parse_footer(object)?;
        if footer.root_stored_digest != [0u8; 32] {
            return Err(WireError::invalid(
                "data pack footer",
                "root digest must be zero for DataPack",
            ));
        }

        let body_end = object.len() - super::container::FOOTER_LEN;
        let mut frames = Vec::new();
        let mut offset = super::container::HEADER_LEN;
        let mut expected_ordinal: u64 = 0;
        let mut content_features: u64 = 0;
        while offset < body_end {
            let (fh, stored) = read_frame_at(object, offset, body_end)?;
            if fh.ordinal != expected_ordinal {
                return Err(WireError::invalid(
                    "frame ordinal",
                    format!(
                        "ordinal {} at offset {offset}, expected {expected_ordinal} \
                         (ordinals are contiguous from 0)",
                        fh.ordinal
                    ),
                ));
            }
            content_features |= fh.payload_format.feature_bit();
            if fh.codec == Codec::Zstd {
                content_features |= features::ZSTD_FRAME_OR_PAGE;
            }
            let raw = fh.decode_payload(stored)?;
            frames.push(ScrubbedFrame {
                header: fh,
                object_offset: offset as u64,
                stored: stored.to_vec(),
                raw,
            });
            expected_ordinal += 1;
            offset += frames.last().unwrap().header.frame_span() as usize;
        }
        if offset != body_end {
            return Err(WireError::invalid(
                "data pack",
                format!("frame region ends at {offset}, footer starts at {body_end}"),
            ));
        }
        let feature_closure = FeatureClosure::of(header.required_features, content_features);
        feature_closure.ensure_declared()?;
        let full_hash: [u8; 32] = Sha256::digest(object).into();
        Ok(ScrubbedPack {
            header,
            frames,
            footer,
            full_hash,
            feature_closure,
        })
    }
}

/// A frame to be written by [`PackBuilder`].
#[derive(Debug, Clone)]
pub struct PackFrame {
    pub payload_format: PayloadFormat,
    pub codec: Codec,
    /// Payload with the outer codec already applied (i.e. the bytes that
    /// will be stored on the object).
    pub stored: Vec<u8>,
    /// Length of the payload after the outer codec is undone.
    pub raw_len: u32,
}

impl PackFrame {
    /// Build an uncompressed PlainBytes frame from raw bytes.
    pub fn plain_bytes(raw: &[u8]) -> WireResult<PackFrame> {
        if raw.len() as u64 > super::frame::MAX_PLAIN_PAYLOAD as u64 {
            return Err(WireError::LimitExceeded(format!(
                "PlainBytes payload {} exceeds {}",
                raw.len(),
                super::frame::MAX_PLAIN_PAYLOAD
            )));
        }
        Ok(PackFrame {
            payload_format: PayloadFormat::PlainBytes,
            codec: Codec::None,
            stored: raw.to_vec(),
            raw_len: raw.len() as u32,
        })
    }

    /// Build a NativeBlockV1 frame from already-encoded block bytes. The
    /// outer codec must be None and the payload is the inner encoded bytes
    /// (spec 03 §4.2: `raw` describes the inner encoded block bytes).
    pub fn native_block_v1(encoded: &[u8]) -> WireResult<PackFrame> {
        if encoded.len() as u64 > super::frame::MAX_NATIVE_OUTER_PAYLOAD as u64 {
            return Err(WireError::LimitExceeded(format!(
                "NativeBlockV1 encoded payload {} exceeds {}",
                encoded.len(),
                super::frame::MAX_NATIVE_OUTER_PAYLOAD
            )));
        }
        Ok(PackFrame {
            payload_format: PayloadFormat::NativeBlockV1,
            codec: Codec::None,
            stored: encoded.to_vec(),
            raw_len: encoded.len() as u32,
        })
    }

    /// Build a Zstd-compressed PlainBytes frame.
    pub fn plain_bytes_zstd(raw: &[u8], level: i32) -> WireResult<PackFrame> {
        if raw.len() as u64 > super::frame::MAX_PLAIN_PAYLOAD as u64 {
            return Err(WireError::LimitExceeded(format!(
                "PlainBytes payload {} exceeds {}",
                raw.len(),
                super::frame::MAX_PLAIN_PAYLOAD
            )));
        }
        let stored = super::frame::zstd_encode(raw, level)?;
        if stored.len() as u64 > MAX_ENCODED_FRAME_PAYLOAD as u64 {
            return Err(WireError::LimitExceeded(format!(
                "encoded payload {} exceeds {MAX_ENCODED_FRAME_PAYLOAD}",
                stored.len()
            )));
        }
        Ok(PackFrame {
            payload_format: PayloadFormat::PlainBytes,
            codec: Codec::Zstd,
            stored,
            raw_len: raw.len() as u32,
        })
    }

    fn frame_header(&self, ordinal: u64) -> WireResult<FrameHeader> {
        let raw_digest: [u8; 32] = match self.codec {
            Codec::None => Sha256::digest(&self.stored).into(),
            Codec::Zstd => {
                // Decompress to verify; the caller handed us stored bytes,
                // but the header must carry the digest of the raw bytes.
                let expected = self.raw_len as usize;
                let out = zstd::bulk::decompress(&self.stored, expected)
                    .map_err(|e| WireError::Codec(format!("zstd: {e}")))?;
                if out.len() != expected {
                    return Err(WireError::invalid(
                        "pack frame",
                        format!("zstd output {} != announced {expected}", out.len()),
                    ));
                }
                Sha256::digest(&out).into()
            }
        };
        Ok(FrameHeader {
            payload_format: self.payload_format,
            codec: self.codec,
            ordinal,
            stored_len: self.stored.len() as u32,
            raw_len: self.raw_len,
            raw_digest,
        })
    }
}

/// Deterministic DataPack builder. Frames are appended in order with
/// contiguous ordinals; `required_features` is derived from the frames so a
/// writer only declares capabilities it actually uses (spec 02 §3).
#[derive(Debug, Default)]
pub struct PackBuilder {
    frames: Vec<PackFrame>,
}

impl PackBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, frame: PackFrame) {
        self.frames.push(frame);
    }

    pub fn frame_count(&self) -> usize {
        self.frames.len()
    }

    /// Assemble the complete object bytes.
    pub fn build(&self) -> WireResult<Vec<u8>> {
        let mut required_features = 0u64;
        let mut body_len: u64 = 0;
        for frame in &self.frames {
            required_features |= frame.payload_format.feature_bit();
            if frame.codec == Codec::Zstd {
                required_features |= features::ZSTD_FRAME_OR_PAGE;
            }
            // Validate each header now so build fails before producing
            // bytes for an invalid frame.
            body_len = body_len
                .checked_add(FRAME_HEADER_LEN as u64)
                .and_then(|v| v.checked_add(frame.stored.len() as u64))
                .ok_or_else(|| WireError::invalid("pack builder", "body length overflow"))?;
            body_len = body_len.div_ceil(super::frame::ALIGNMENT) * super::frame::ALIGNMENT;
        }
        let object_len = (super::container::HEADER_LEN as u64)
            .checked_add(body_len)
            .and_then(|v| v.checked_add(super::container::FOOTER_LEN as u64))
            .ok_or_else(|| WireError::invalid("pack builder", "object length overflow"))?;
        if object_len < MIN_OBJECT_LEN {
            // header + footer alone is 128, so this cannot happen; kept for
            // arithmetic completeness.
            return Err(WireError::invalid(
                "pack builder",
                "object below minimum length",
            ));
        }

        let header = ContainerHeader {
            kind: ObjectKind::DataPack,
            required_features,
            object_len,
            root_offset: 0,
            root_stored_len: 0,
            root_raw_len: 0,
            hash_id: 1,
            root_codec: Codec::None,
        };
        let footer = ContainerFooter {
            object_len,
            root_stored_digest: [0u8; 32],
        };

        let mut out = Vec::with_capacity(object_len as usize);
        out.extend_from_slice(&header.encode());
        for (ordinal, frame) in self.frames.iter().enumerate() {
            let fh = frame.frame_header(ordinal as u64)?;
            out.extend_from_slice(&fh.encode());
            out.extend_from_slice(&frame.stored);
            let padded = fh.frame_span() - FRAME_HEADER_LEN as u64 - frame.stored.len() as u64;
            out.resize(out.len() + padded as usize, 0);
        }
        out.extend_from_slice(&footer.encode());
        debug_assert_eq!(out.len() as u64, object_len);
        Ok(out)
    }
}

/// Convenience: full SHA-256 of an object, for ObjectRef.full_hash.
pub fn full_object_hash(object: &[u8]) -> [u8; 32] {
    Sha256::digest(object).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Goldens from examples/fixtures (copied verbatim into testdata).
    const EMPTY: &[u8] = include_bytes!("../testdata/empty.brfdp");
    const TWO_PLAIN: &[u8] = include_bytes!("../testdata/two_plain_frames.brfdp");
    const NATIVE_NONE: &[u8] = include_bytes!("../testdata/native_none.brfdp");
    const PLAIN_MAGIC: &[u8] = include_bytes!("../testdata/plain_magic_prefix.brfdp");

    /// GATE-002: a producer that only claims None/plain capabilities cannot
    /// smuggle a Zstd frame past capability admission — the header CRC and
    /// the frame digests are all valid, the declaration is what lies.
    #[test]
    fn declared_plain_header_cannot_hide_a_zstd_frame() {
        let mut builder = PackBuilder::new();
        builder.push(PackFrame::plain_bytes_zstd(b"capability smoke payload", 3).unwrap());
        let honest = builder.build().unwrap();
        let scrubbed = ScrubbedPack::scrub(&honest).unwrap();
        assert_eq!(
            scrubbed.feature_closure.declared,
            features::PLAIN_BYTES_FRAMES | features::ZSTD_FRAME_OR_PAGE
        );
        assert_eq!(scrubbed.feature_closure.undeclared(), 0);

        // Same bytes, header rewritten to a None-only declaration with a
        // repaired CRC: capability admission must refuse it.
        let mut lying = honest.clone();
        crate::native_base::wire::container::set_declared_features(
            &mut lying,
            features::PLAIN_BYTES_FRAMES,
        );
        let err = ScrubbedPack::scrub(&lying).unwrap_err();
        assert!(matches!(err, WireError::UnsupportedFormat(_)), "{err}");
        assert!(err.to_string().contains("0x2"), "{err}");
    }

    /// GATE-003: the closure is summarized and every shipped golden still
    /// declares exactly what its content requires.
    #[test]
    fn shipped_goldens_declare_their_content_feature_closure() {
        let cases: [(&[u8], u64, &str); 4] = [
            (EMPTY, 0, "empty"),
            (TWO_PLAIN, features::PLAIN_BYTES_FRAMES, "two plain frames"),
            (
                PLAIN_MAGIC,
                features::PLAIN_BYTES_FRAMES,
                "plain magic prefix",
            ),
            (
                NATIVE_NONE,
                features::NATIVE_BLOCK_V1_INNER,
                "native block v1",
            ),
        ];
        for (bytes, expected, what) in cases {
            let pack = ScrubbedPack::scrub(bytes).unwrap_or_else(|e| panic!("{what}: {e}"));
            assert_eq!(pack.feature_closure.declared, expected, "{what}");
            assert_eq!(pack.feature_closure.content, expected, "{what}");
            assert_eq!(pack.feature_closure.closure(), expected, "{what}");
            assert_eq!(pack.feature_closure.undeclared(), 0, "{what}");
        }
        // A pack with no frames needs no capability at all.
        assert_eq!(ScrubbedPack::scrub(EMPTY).unwrap().frames.len(), 0);
    }

    #[test]
    fn golden_empty_pack_scrubs_and_rebuilds_byte_identical() {
        let pack = ScrubbedPack::scrub(EMPTY).unwrap();
        assert!(pack.frames.is_empty());
        assert_eq!(pack.header.object_len, 128);
        assert_eq!(pack.header.required_features, 0);
        assert_eq!(
            pack.full_hash[..],
            hex::decode("9c4e03e7a8a08ca8013717f91182b75b9f1b5f7f2c30cf61d6e5e2258fffee8f")
                .unwrap()[..]
        );

        let rebuilt = PackBuilder::new().build().unwrap();
        assert_eq!(rebuilt, EMPTY);
    }

    #[test]
    fn golden_two_plain_frames_scrubs_and_rebuilds_byte_identical() {
        let pack = ScrubbedPack::scrub(TWO_PLAIN).unwrap();
        assert_eq!(pack.frames.len(), 2);
        assert_eq!(pack.frames[0].raw, b"abcdefghijklmnop");
        assert_eq!(pack.frames[1].raw, b"1234");
        assert_eq!(pack.frames[0].object_offset, 64);
        assert_eq!(pack.frames[1].object_offset, 160);
        assert_eq!(pack.header.required_features, features::PLAIN_BYTES_FRAMES);
        assert_eq!(
            pack.frames[0].header.raw_digest[..],
            hex::decode("f39dac6cbaba535e2c207cd0cd8f154974223c848f727f98b3564cea569b41cf")
                .unwrap()[..]
        );
        assert_eq!(
            pack.frames[1].header.raw_digest[..],
            hex::decode("03ac674216f3e15c761ee1a5e255f067953623c8b388b4459e13f978d7c846f4")
                .unwrap()[..]
        );

        let mut b = PackBuilder::new();
        b.push(PackFrame::plain_bytes(b"abcdefghijklmnop").unwrap());
        b.push(PackFrame::plain_bytes(b"1234").unwrap());
        assert_eq!(b.build().unwrap(), TWO_PLAIN);
    }

    #[test]
    fn golden_native_none_scrubs_and_rebuilds_byte_identical() {
        let pack = ScrubbedPack::scrub(NATIVE_NONE).unwrap();
        assert_eq!(pack.frames.len(), 1);
        assert_eq!(
            pack.frames[0].header.payload_format,
            PayloadFormat::NativeBlockV1
        );
        // The inner SF header is payload, never re-interpreted by the pack
        // layer (spec 03 §4.2).
        assert_eq!(pack.frames[0].raw, b"SF\x00\x00hello");
        assert_eq!(
            pack.header.required_features,
            features::NATIVE_BLOCK_V1_INNER
        );

        let mut b = PackBuilder::new();
        b.push(PackFrame::native_block_v1(b"SF\x00\x00hello").unwrap());
        assert_eq!(b.build().unwrap(), NATIVE_NONE);
    }

    #[test]
    fn golden_plain_magic_prefix_keeps_raw_bytes() {
        // WIRE-007: a PlainBytes frame whose payload starts with the native
        // "SF" magic must stay raw data — no magic guessing.
        let pack = ScrubbedPack::scrub(PLAIN_MAGIC).unwrap();
        assert_eq!(
            pack.frames[0].header.payload_format,
            PayloadFormat::PlainBytes
        );
        assert_eq!(pack.frames[0].raw, b"SF\x00\x00hello");
        assert_eq!(pack.header.required_features, features::PLAIN_BYTES_FRAMES);

        let mut b = PackBuilder::new();
        b.push(PackFrame::plain_bytes(b"SF\x00\x00hello").unwrap());
        assert_eq!(b.build().unwrap(), PLAIN_MAGIC);
    }

    #[test]
    fn scrub_rejects_corrupt_raw_digest() {
        // WIRE-005: flip one payload byte; CRC of the frame header still
        // passes, the raw digest must catch it.
        let mut broken = TWO_PLAIN.to_vec();
        let payload_at = 64 + FRAME_HEADER_LEN;
        broken[payload_at] ^= 0x01;
        assert!(matches!(
            ScrubbedPack::scrub(&broken),
            Err(WireError::HashMismatch { .. })
        ));
    }

    #[test]
    fn scrub_rejects_header_crc_error() {
        // WIRE-004: validation happens on the fixed 64-byte header, and no
        // data buffer is allocated for the payload.
        let mut broken = TWO_PLAIN.to_vec();
        broken[16] ^= 0xff;
        assert!(matches!(
            ScrubbedPack::scrub(&broken),
            Err(WireError::CrcMismatch { .. })
        ));
    }

    #[test]
    fn scrub_rejects_nonzero_padding() {
        let mut broken = NATIVE_NONE.to_vec();
        // 64 + 80 + 9 = 153; padding occupies [153..160).
        broken[153] = 1;
        assert!(ScrubbedPack::scrub(&broken).is_err());
    }

    #[test]
    fn scrub_rejects_ordinal_gap() {
        let mut broken = TWO_PLAIN.to_vec();
        // Second frame starts at 160; its ordinal is at 160+8.
        broken[168] = 5;
        let crc = super::super::container::crc32c(&broken[160..160 + 76]);
        broken[160 + 76..160 + 80].copy_from_slice(&crc.to_le_bytes());
        let err = ScrubbedPack::scrub(&broken).unwrap_err();
        assert!(err.to_string().contains("ordinal"), "{err}");
    }

    #[test]
    fn scrub_rejects_truncated_object() {
        assert!(matches!(
            ScrubbedPack::scrub(&TWO_PLAIN[..TWO_PLAIN.len() - 1]),
            Err(WireError::Truncated { .. })
        ));
        assert!(ScrubbedPack::scrub(&TWO_PLAIN[..64]).is_err());
    }

    #[test]
    fn scrub_rejects_unsupported_required_features() {
        // WIRE-006: a feature bit this build does not know must fail closed.
        let mut b = PackBuilder::new();
        b.push(PackFrame::plain_bytes(b"data").unwrap());
        let mut object = b.build().unwrap();
        // Flip an unknown feature bit and fix the header CRC.
        let mut header = ContainerHeader::parse(&object).unwrap();
        header.required_features = 1 << 7;
        object[0..64].copy_from_slice(&header.encode());
        assert!(matches!(
            ScrubbedPack::scrub(&object),
            Err(WireError::UnsupportedFormat(_))
        ));
    }

    #[test]
    fn zero_length_payloads_are_never_frames() {
        // WIRE-012: a zero-length file contributes no data frame; the empty
        // pack (header+footer only) is the golden for that.
        let b = PackBuilder::new();
        let object = b.build().unwrap();
        let pack = ScrubbedPack::scrub(&object).unwrap();
        assert!(pack.frames.is_empty());
    }

    #[test]
    fn zstd_frames_roundtrip_through_scrub() {
        let raw = vec![0x5au8; 100_000];
        let mut b = PackBuilder::new();
        b.push(PackFrame::plain_bytes_zstd(&raw, 3).unwrap());
        let object = b.build().unwrap();
        let pack = ScrubbedPack::scrub(&object).unwrap();
        assert_eq!(pack.frames[0].raw, raw);
        assert!(pack.header.required_features & features::ZSTD_FRAME_OR_PAGE != 0);
    }

    #[test]
    fn builder_rejects_oversized_payloads() {
        let too_big = vec![0u8; super::super::frame::MAX_PLAIN_PAYLOAD as usize + 1];
        assert!(matches!(
            PackFrame::plain_bytes(&too_big),
            Err(WireError::LimitExceeded(_))
        ));
    }
}
