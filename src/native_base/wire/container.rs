//! Container header/footer (spec 02 §3–§4) and the object kind/magic table
//! (spec 02 §2).
//!
//! The header is exactly 64 bytes, the footer exactly 64 bytes at
//! `object_len - 64`. CRC32C is reflected Castagnoli (poly 0x82F63B78,
//! init/xorout 0xffffffff); the standard vector `123456789 -> e3069283` is
//! asserted in the tests.

use super::error::{WireError, WireResult};

/// Reflected Castagnoli CRC-32C as required by spec 02 §4.
pub fn crc32c(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
}

/// Minimum container length: header + footer.
pub const MIN_OBJECT_LEN: u64 = 128;

pub const HEADER_LEN: usize = 64;
pub const FOOTER_LEN: usize = 64;

pub const FOOTER_MAGIC: &[u8; 8] = b"BRFEND03";

/// Object kinds with a dedicated container magic (spec 02 §2).
/// Kind 6 (native Loose object) has no container header and is not
/// represented here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectKind {
    DataPack,
    DataSeal,
    FrozenMetadata,
    SnapshotManifest,
    PagedInventory,
}

impl ObjectKind {
    pub fn as_u8(self) -> u8 {
        match self {
            ObjectKind::DataPack => 1,
            ObjectKind::DataSeal => 2,
            ObjectKind::FrozenMetadata => 3,
            ObjectKind::SnapshotManifest => 4,
            ObjectKind::PagedInventory => 5,
        }
    }

    pub fn magic(self) -> &'static [u8; 8] {
        match self {
            ObjectKind::DataPack => b"BRFDP003",
            ObjectKind::DataSeal => b"BRFDS003",
            ObjectKind::FrozenMetadata => b"BRFCL003",
            ObjectKind::SnapshotManifest => b"BRFSM003",
            ObjectKind::PagedInventory => b"BRFIN003",
        }
    }

    pub fn from_magic(magic: &[u8]) -> WireResult<ObjectKind> {
        for kind in [
            ObjectKind::DataPack,
            ObjectKind::DataSeal,
            ObjectKind::FrozenMetadata,
            ObjectKind::SnapshotManifest,
            ObjectKind::PagedInventory,
        ] {
            if magic == kind.magic() {
                return Ok(kind);
            }
        }
        Err(WireError::UnsupportedFormat(format!(
            "unknown container magic {:02x?}",
            magic
        )))
    }
}

/// `required_features` bits (spec 02 §3). Other bits must not be set by a
/// writer of this version; a reader that does not support a required bit
/// must reject the object.
pub mod features {
    pub const PLAIN_BYTES_FRAMES: u64 = 1 << 0;
    pub const ZSTD_FRAME_OR_PAGE: u64 = 1 << 1;
    pub const EXTERNAL_INDEX_CHILDREN: u64 = 1 << 2;
    pub const NATIVE_BLOCK_V1_INNER: u64 = 1 << 3;

    /// Bits this build understands.
    pub const KNOWN: u64 =
        PLAIN_BYTES_FRAMES | ZSTD_FRAME_OR_PAGE | EXTERNAL_INDEX_CHILDREN | NATIVE_BLOCK_V1_INNER;
}

/// The capability closure of a container's `required_features`
/// (spec 02 §3, INV-12/INV-20).
///
/// `declared` is what the container header promises; `content` is what the
/// bytes actually need — the frame formats and codecs a DataPack carries,
/// the root codec and external index children a Data Seal carries. A
/// writer must declare the closure. A reader that trusted a declaration
/// which omits a content requirement would honour an object whose real
/// dependencies it was told not to expect, so every full verify refuses
/// such an object instead of probing deeper.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeatureClosure {
    pub declared: u64,
    pub content: u64,
}

impl FeatureClosure {
    pub fn of(declared: u64, content: u64) -> Self {
        Self { declared, content }
    }

    /// `declared ∪ content`: everything a reader of this object needs.
    pub fn closure(self) -> u64 {
        self.declared | self.content
    }

    /// The content requirements the header does not declare.
    pub fn undeclared(self) -> u64 {
        self.content & !self.declared
    }

    /// Refuse a declaration that does not cover the content (fail closed).
    pub fn ensure_declared(self) -> WireResult<()> {
        let missing = self.undeclared();
        if missing != 0 {
            return Err(WireError::UnsupportedFormat(format!(
                "content requires feature bits {missing:#x} that required_features {:#x} does not declare",
                self.declared
            )));
        }
        Ok(())
    }
}

/// Compression codec identifiers shared by header/root, frames, and pages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Codec {
    None,
    Zstd,
}

impl Codec {
    pub fn as_u8(self) -> u8 {
        match self {
            Codec::None => 0,
            Codec::Zstd => 2,
        }
    }

    pub fn from_u8(v: u8) -> WireResult<Codec> {
        match v {
            0 => Ok(Codec::None),
            2 => Ok(Codec::Zstd),
            other => Err(WireError::UnsupportedFormat(format!(
                "codec id {other} (expected 0=None or 2=Zstd)"
            ))),
        }
    }
}

/// 64-byte container header (spec 02 §3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerHeader {
    pub kind: ObjectKind,
    pub required_features: u64,
    pub object_len: u64,
    /// Non-zero only for containers with a root payload (DataPack is 0).
    pub root_offset: u64,
    pub root_stored_len: u32,
    pub root_raw_len: u32,
    /// 1 = SHA-256 (the only value in this version).
    pub hash_id: u8,
    pub root_codec: Codec,
}

impl ContainerHeader {
    pub fn encode(&self) -> [u8; HEADER_LEN] {
        let mut out = [0u8; HEADER_LEN];
        out[0..8].copy_from_slice(self.kind.magic());
        out[8..10].copy_from_slice(&WIRE_MAJOR.to_le_bytes());
        out[10..12].copy_from_slice(&WIRE_MINOR.to_le_bytes());
        out[12..16].copy_from_slice(&(HEADER_LEN as u32).to_le_bytes());
        out[16..24].copy_from_slice(&self.required_features.to_le_bytes());
        out[24..32].copy_from_slice(&self.object_len.to_le_bytes());
        out[32..40].copy_from_slice(&self.root_offset.to_le_bytes());
        out[40..44].copy_from_slice(&self.root_stored_len.to_le_bytes());
        out[44..48].copy_from_slice(&self.root_raw_len.to_le_bytes());
        out[48] = self.hash_id;
        out[49] = self.root_codec.as_u8();
        // reserved bytes [50..60] stay zero
        let crc = crc32c(&out[0..60]);
        out[60..64].copy_from_slice(&crc.to_le_bytes());
        out
    }

    /// Parse and fully validate a 64-byte header. Fails closed on unknown
    /// magic, wrong wire major/minor, non-canonical lengths, non-zero
    /// reserved bytes, and CRC mismatch. No payload is read or allocated.
    pub fn parse(bytes: &[u8]) -> WireResult<ContainerHeader> {
        let what = "container header";
        if bytes.len() < HEADER_LEN {
            return Err(WireError::Truncated {
                what,
                need: HEADER_LEN,
                have: bytes.len(),
            });
        }
        let kind = ObjectKind::from_magic(&bytes[0..8])?;
        let major = u16::from_le_bytes([bytes[8], bytes[9]]);
        let minor = u16::from_le_bytes([bytes[10], bytes[11]]);
        if major != WIRE_MAJOR {
            return Err(WireError::UnsupportedFormat(format!(
                "wire major {major} (this build reads {WIRE_MAJOR} only)"
            )));
        }
        if minor != WIRE_MINOR {
            return Err(WireError::UnsupportedFormat(format!(
                "wire minor {minor} (this build reads {WIRE_MINOR} only)"
            )));
        }
        let header_len = u32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]);
        if header_len as usize != HEADER_LEN {
            return Err(WireError::invalid(what, format!("header_len {header_len}")));
        }
        let required_features = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
        let object_len = u64::from_le_bytes(bytes[24..32].try_into().unwrap());
        if object_len < MIN_OBJECT_LEN {
            return Err(WireError::invalid(
                what,
                format!("object_len {object_len} below minimum {MIN_OBJECT_LEN}"),
            ));
        }
        let root_offset = u64::from_le_bytes(bytes[32..40].try_into().unwrap());
        let root_stored_len = u32::from_le_bytes(bytes[40..44].try_into().unwrap());
        let root_raw_len = u32::from_le_bytes(bytes[44..48].try_into().unwrap());
        let hash_id = bytes[48];
        if hash_id != 1 {
            return Err(WireError::UnsupportedFormat(format!(
                "hash id {hash_id} (expected 1 = SHA-256)"
            )));
        }
        let root_codec = Codec::from_u8(bytes[49])?;
        if bytes[50..60].iter().any(|&b| b != 0) {
            return Err(WireError::invalid(what, "reserved bytes not zero"));
        }
        let stored_crc = u32::from_le_bytes(bytes[60..64].try_into().unwrap());
        let computed = crc32c(&bytes[0..60]);
        if stored_crc != computed {
            return Err(WireError::CrcMismatch {
                what,
                stored: stored_crc,
                computed,
            });
        }
        Ok(ContainerHeader {
            kind,
            required_features,
            object_len,
            root_offset,
            root_stored_len,
            root_raw_len,
            hash_id,
            root_codec,
        })
    }

    /// Reject objects whose `required_features` bits this build does not
    /// implement (spec 02 §3).
    pub fn ensure_supported_features(&self) -> WireResult<()> {
        let unknown = self.required_features & !features::KNOWN;
        if unknown != 0 {
            return Err(WireError::UnsupportedFormat(format!(
                "required features {unknown:#x} not supported by this build"
            )));
        }
        Ok(())
    }
}

/// Wire version this build reads and writes (read-only 3/0).
pub const WIRE_MAJOR: u16 = 3;
pub const WIRE_MINOR: u16 = 0;

/// 64-byte container footer (spec 02 §4), located at `object_len - 64`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerFooter {
    pub object_len: u64,
    /// SHA-256 of the root stored payload; all zero for DataPack.
    pub root_stored_digest: [u8; 32],
}

impl ContainerFooter {
    pub fn encode(&self) -> [u8; FOOTER_LEN] {
        let mut out = [0u8; FOOTER_LEN];
        out[0..8].copy_from_slice(FOOTER_MAGIC);
        out[8..16].copy_from_slice(&self.object_len.to_le_bytes());
        out[16..48].copy_from_slice(&self.root_stored_digest);
        // reserved [48..60] stay zero
        let crc = crc32c(&out[0..60]);
        out[60..64].copy_from_slice(&crc.to_le_bytes());
        out
    }

    pub fn parse(bytes: &[u8]) -> WireResult<ContainerFooter> {
        let what = "container footer";
        if bytes.len() < FOOTER_LEN {
            return Err(WireError::Truncated {
                what,
                need: FOOTER_LEN,
                have: bytes.len(),
            });
        }
        if &bytes[0..8] != FOOTER_MAGIC {
            return Err(WireError::invalid(
                what,
                format!("magic {:02x?} is not {FOOTER_MAGIC:02x?}", &bytes[0..8]),
            ));
        }
        let object_len = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        if bytes[48..60].iter().any(|&b| b != 0) {
            return Err(WireError::invalid(what, "reserved bytes not zero"));
        }
        let stored_crc = u32::from_le_bytes(bytes[60..64].try_into().unwrap());
        let computed = crc32c(&bytes[0..60]);
        if stored_crc != computed {
            return Err(WireError::CrcMismatch {
                what,
                stored: stored_crc,
                computed,
            });
        }
        Ok(ContainerFooter {
            object_len,
            root_stored_digest: bytes[16..48].try_into().unwrap(),
        })
    }
}

/// Read the footer of a complete container: the last [`FOOTER_LEN`] bytes.
/// Cross-checks `object_len` with the header when both are available.
pub fn parse_footer(object: &[u8]) -> WireResult<ContainerFooter> {
    let what = "container footer";
    if (object.len() as u64) < MIN_OBJECT_LEN {
        return Err(WireError::Truncated {
            what,
            need: MIN_OBJECT_LEN as usize,
            have: object.len(),
        });
    }
    let footer = ContainerFooter::parse(&object[object.len() - FOOTER_LEN..])?;
    if footer.object_len as usize != object.len() {
        return Err(WireError::invalid(
            what,
            format!(
                "footer object_len {} != actual {}",
                footer.object_len,
                object.len()
            ),
        ));
    }
    Ok(footer)
}

/// Rewrite a container header's declared `required_features` and repair the
/// header CRC. Test-only: it exists so a lying producer can be simulated
/// byte for byte (the frames/footer stay valid).
#[cfg(test)]
pub(crate) fn set_declared_features(object: &mut [u8], bits: u64) {
    object[16..24].copy_from_slice(&bits.to_le_bytes());
    let crc = crc32c(&object[0..60]);
    object[60..64].copy_from_slice(&crc.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32c_standard_vector() {
        // spec 02 §4: 123456789 -> e3069283
        assert_eq!(crc32c(b"123456789"), 0xe3069283);
    }

    #[test]
    fn header_roundtrip_all_kinds() {
        for kind in [
            ObjectKind::DataPack,
            ObjectKind::DataSeal,
            ObjectKind::FrozenMetadata,
            ObjectKind::SnapshotManifest,
            ObjectKind::PagedInventory,
        ] {
            let h = ContainerHeader {
                kind,
                required_features: features::PLAIN_BYTES_FRAMES,
                object_len: 4096,
                root_offset: 0,
                root_stored_len: 0,
                root_raw_len: 0,
                hash_id: 1,
                root_codec: Codec::None,
            };
            let enc = h.encode();
            assert_eq!(ContainerHeader::parse(&enc).unwrap(), h);
        }
    }

    #[test]
    fn header_rejects_unknown_major() {
        let h = ContainerHeader {
            kind: ObjectKind::DataPack,
            required_features: 0,
            object_len: 128,
            root_offset: 0,
            root_stored_len: 0,
            root_raw_len: 0,
            hash_id: 1,
            root_codec: Codec::None,
        };
        let mut enc = h.encode();
        // V3 must not read BRFDP002-era bytes (spec 02 §2).
        enc[8..10].copy_from_slice(&2u16.to_le_bytes());
        let err = ContainerHeader::parse(&enc).unwrap_err();
        assert!(matches!(err, WireError::UnsupportedFormat(_)), "{err}");
    }

    #[test]
    fn header_rejects_crc_mismatch() {
        let h = ContainerHeader {
            kind: ObjectKind::DataPack,
            required_features: 0,
            object_len: 128,
            root_offset: 0,
            root_stored_len: 0,
            root_raw_len: 0,
            hash_id: 1,
            root_codec: Codec::None,
        };
        let mut enc = h.encode();
        enc[16] ^= 0xff; // corrupt required_features without fixing CRC
        assert!(matches!(
            ContainerHeader::parse(&enc),
            Err(WireError::CrcMismatch { .. })
        ));
    }

    #[test]
    fn header_rejects_nonzero_reserved_and_short_object() {
        let mk = |object_len: u64| ContainerHeader {
            kind: ObjectKind::DataPack,
            required_features: 0,
            object_len,
            root_offset: 0,
            root_stored_len: 0,
            root_raw_len: 0,
            hash_id: 1,
            root_codec: Codec::None,
        };
        let mut enc = mk(128).encode();
        enc[55] = 1;
        assert!(ContainerHeader::parse(&enc).is_err());
        let mut enc = mk(64).encode(); // header+footer cannot fit
        let crc = crc32c(&enc[0..60]);
        enc[60..64].copy_from_slice(&crc.to_le_bytes());
        assert!(ContainerHeader::parse(&enc).is_err());
    }

    #[test]
    fn header_rejects_unknown_magic_codec_and_hash() {
        let h = ContainerHeader {
            kind: ObjectKind::DataPack,
            required_features: 0,
            object_len: 128,
            root_offset: 0,
            root_stored_len: 0,
            root_raw_len: 0,
            hash_id: 1,
            root_codec: Codec::None,
        };
        let mut enc = h.encode();
        enc[0..8].copy_from_slice(b"BRFDP002");
        assert!(matches!(
            ContainerHeader::parse(&enc),
            Err(WireError::UnsupportedFormat(_))
        ));
        let mut enc = h.encode();
        enc[49] = 1; // root_codec 1
        let crc = crc32c(&enc[0..60]);
        enc[60..64].copy_from_slice(&crc.to_le_bytes());
        assert!(matches!(
            ContainerHeader::parse(&enc),
            Err(WireError::UnsupportedFormat(_))
        ));
        let mut enc = h.encode();
        enc[48] = 2; // hash_id 2
        let crc = crc32c(&enc[0..60]);
        enc[60..64].copy_from_slice(&crc.to_le_bytes());
        assert!(matches!(
            ContainerHeader::parse(&enc),
            Err(WireError::UnsupportedFormat(_))
        ));
    }

    #[test]
    fn feature_closure_requires_the_declaration_to_cover_the_content() {
        // A declaration that already covers the content is admitted and
        // summarizes as the union (GATE-003).
        let covered = FeatureClosure::of(
            features::PLAIN_BYTES_FRAMES | features::ZSTD_FRAME_OR_PAGE,
            features::PLAIN_BYTES_FRAMES,
        );
        assert_eq!(
            covered.closure(),
            features::PLAIN_BYTES_FRAMES | features::ZSTD_FRAME_OR_PAGE
        );
        assert_eq!(covered.undeclared(), 0);
        assert!(covered.ensure_declared().is_ok());

        // A None-only declaration that hides a Zstd dependency is refused
        // with the exact missing bits (GATE-002).
        let lying = FeatureClosure::of(features::PLAIN_BYTES_FRAMES, features::ZSTD_FRAME_OR_PAGE);
        assert_eq!(lying.undeclared(), features::ZSTD_FRAME_OR_PAGE);
        let err = lying.ensure_declared().unwrap_err();
        assert!(matches!(err, WireError::UnsupportedFormat(_)));
        assert!(err.to_string().contains("0x2"), "{err}");
    }

    #[test]
    fn set_declared_features_rewrites_the_header_and_repairs_its_crc() {
        let mut object = ContainerHeader {
            kind: ObjectKind::DataPack,
            required_features: features::PLAIN_BYTES_FRAMES | features::ZSTD_FRAME_OR_PAGE,
            object_len: 4096,
            root_offset: 0,
            root_stored_len: 0,
            root_raw_len: 0,
            hash_id: 1,
            root_codec: Codec::None,
        }
        .encode()
        .to_vec();
        set_declared_features(&mut object, features::PLAIN_BYTES_FRAMES);
        let reparsed = ContainerHeader::parse(&object).unwrap();
        assert_eq!(reparsed.required_features, features::PLAIN_BYTES_FRAMES);
    }

    #[test]
    fn unsupported_required_features_rejected() {
        let h = ContainerHeader {
            kind: ObjectKind::DataPack,
            required_features: 1 << 9,
            object_len: 128,
            root_offset: 0,
            root_stored_len: 0,
            root_raw_len: 0,
            hash_id: 1,
            root_codec: Codec::None,
        };
        assert!(h.ensure_supported_features().is_err());
    }

    #[test]
    fn footer_roundtrip_and_validation() {
        let f = ContainerFooter {
            object_len: 128,
            root_stored_digest: [7u8; 32],
        };
        assert_eq!(ContainerFooter::parse(&f.encode()).unwrap(), f);

        let mut enc = f.encode();
        enc[20] ^= 1;
        assert!(matches!(
            ContainerFooter::parse(&enc),
            Err(WireError::CrcMismatch { .. })
        ));

        let mut enc = f.encode();
        enc[0..8].copy_from_slice(b"BRFEND02");
        let crc = crc32c(&enc[0..60]);
        enc[60..64].copy_from_slice(&crc.to_le_bytes());
        assert!(ContainerFooter::parse(&enc).is_err());

        let mut enc = f.encode();
        enc[50] = 1;
        let crc = crc32c(&enc[0..60]);
        enc[60..64].copy_from_slice(&crc.to_le_bytes());
        assert!(ContainerFooter::parse(&enc).is_err());
    }

    #[test]
    fn parse_footer_checks_object_len() {
        let f = ContainerFooter {
            object_len: 256,
            root_stored_digest: [0u8; 32],
        };
        let mut object = vec![0u8; 128];
        object[64..].copy_from_slice(&f.encode());
        assert!(parse_footer(&object).is_err());
    }
}
