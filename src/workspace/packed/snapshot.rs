//! Authenticated snapshot boundary. Unsupported wire revisions are rejected.

mod budget;
mod build_policy;
pub use build_policy::{
    SnapshotBuildPolicy, SnapshotBuildProvenance, SnapshotFramePolicy, SnapshotP90Policy,
};
mod cold;
pub(crate) use budget::SnapshotOwned;
pub use budget::{
    SnapshotBudgetLimits, SnapshotBudgetPool, SnapshotMountBudget, SnapshotOwnedBytes,
    SnapshotOwnedPermit,
};
mod container;
mod directory;
pub use cold::{SnapshotColdAttributes, SnapshotXattr};
pub(crate) use directory::SnapshotDirectoryNames;
mod frame_directory;
pub(crate) use container::SnapshotDirectoryMetadata;
pub use container::{
    SnapshotBuiltContainer, SnapshotGroupRef, build_snapshot_container,
    build_snapshot_container_with_policy,
};
mod index;
mod inode;
pub use inode::SnapshotInodeLocation;
mod index_builder;
pub use index_builder::SnapshotIndexBuilder;
mod large_chunk;
mod manifest;
#[cfg(test)]
mod metadata_cache_tests;
mod native_placement;
mod pipeline;
#[cfg(test)]
pub(crate) fn verify_memo_reservation_bytes() -> PackedResult<u64> {
    pipeline::verify_memo_reservation_bytes()
}
mod placement;
mod producer;
mod publication;
mod read_cache;
pub use placement::{SnapshotLargeExtent, SnapshotPlacement};
#[cfg(target_os = "linux")]
pub(crate) use publication::native_effective::NativeCaptureLimits;
#[cfg(target_os = "linux")]
pub(crate) use publication::native_effective::{
    FrozenNativeArtifact, NativePromotionFailure, VerifiedFrozenNativeView,
    VerifiedHashedNativeView,
};
pub use publication::{
    SnapshotAuthenticatedPayload, SnapshotIndexAuditCounts, SnapshotIndexAuditLimits,
    SnapshotIndexContextAudit, SnapshotPayloadLimits, SnapshotPayloadSummary,
    SnapshotPhysicalAuditCounts, SnapshotPhysicalAuditLimits, SnapshotPhysicalDependencyAudit,
    audit_snapshot_index_contexts, audit_snapshot_physical_dependencies,
    authenticate_snapshot_payload,
};
pub(crate) use publication::{SnapshotStagedObjectVerifier, audit_snapshot_staged_index_contexts};
mod source_stat;
pub use source_stat::{SnapshotRootAttributes, SnapshotSourceAttributes};
#[cfg(target_os = "linux")]
mod source;
#[cfg(target_os = "linux")]
mod source_file;
#[cfg(target_os = "linux")]
pub use source::CapturedSource;
#[cfg(target_os = "linux")]
mod source_layout;
#[cfg(target_os = "linux")]
mod source_namespace;
#[cfg(target_os = "linux")]
mod source_root;
#[cfg(target_os = "linux")]
pub use source_layout::CapturedSourceLayout;
#[cfg(target_os = "linux")]
pub use source_root::SnapshotSourceProvenance;
mod spool;
pub use frame_directory::SnapshotFrameDirectoryPage;
pub(crate) use index::SnapshotIndexCacheStats;
pub use index::{
    SnapshotIndexPage, SnapshotIndexReader, SnapshotIndexRecord, SnapshotIndexRecordHandle,
    SnapshotIndexRows, SnapshotIndexValue, SnapshotIndexValueHandle,
};
pub use manifest::{AuthenticatedSnapshot, SnapshotManifest, SnapshotReadRange, SnapshotRootKind};
pub use native_placement::{NativePackedPlacementProvider, NativePlacementRepository};
pub use producer::{SnapshotProducer, SnapshotProducerOptions};
#[cfg(target_os = "linux")]
pub use source_file::{CapturedSourceFile, CapturedSourceRoot, SnapshotSourceFileLimits};
#[cfg(target_os = "linux")]
pub(crate) use source_namespace::SnapshotFinalSourceProof;
#[cfg(target_os = "linux")]
pub use source_namespace::{
    SnapshotSourceConsistency, SnapshotSourceHardlinkPolicy, SnapshotSourceNamespaceInventory,
    SnapshotSourceNamespaceOptions, SnapshotSourceNamespaceReport, SnapshotSourceNamespaceSnapshot,
};
pub use spool::SnapshotIndexSpool;

use sha2::{Digest, Sha256};

use super::wire::{PackedResult, PackedWireError};

pub const SNAPSHOT_HEADER_LEN: usize = 4096;
pub const SNAPSHOT_FOOTER_LEN: usize = 64;
pub const SNAPSHOT_MAX_BODY_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[repr(u8)]
pub enum SnapshotObjectKind {
    Manifest = 0,
    GroupContainer = 1,
    ColdAttributes = 2,
    FrameDirectory = 3,
    LargeData = 4,
    GroupIndex = 5,
    InodeIndex = 6,
    ReverseIndex = 7,
    ContainerIndex = 8,
    FrameIndex = 9,
    ColdIndex = 10,
    LargeIndex = 11,
    SourceStatsIndex = 12,
}

impl SnapshotObjectKind {
    fn from_u8(value: u8) -> PackedResult<Self> {
        match value {
            0 => Ok(Self::Manifest),
            1 => Ok(Self::GroupContainer),
            2 => Ok(Self::ColdAttributes),
            3 => Ok(Self::FrameDirectory),
            4 => Ok(Self::LargeData),
            5 => Ok(Self::GroupIndex),
            6 => Ok(Self::InodeIndex),
            7 => Ok(Self::ReverseIndex),
            8 => Ok(Self::ContainerIndex),
            9 => Ok(Self::FrameIndex),
            10 => Ok(Self::ColdIndex),
            11 => Ok(Self::LargeIndex),
            12 => Ok(Self::SourceStatsIndex),
            _ => Err(PackedWireError::UnsupportedFormat(
                "unknown wire 005 object kind".into(),
            )),
        }
    }

    fn magic(self) -> &'static [u8; 8] {
        match self {
            Self::Manifest => b"BRFPM005",
            Self::GroupContainer => b"BRFGC005",
            Self::ColdAttributes => b"BRFCA005",
            Self::FrameDirectory => b"BRFFD005",
            Self::LargeData => b"BRFLD005",
            Self::GroupIndex => b"BRFGI005",
            Self::InodeIndex => b"BRFII005",
            Self::ReverseIndex => b"BRFRI005",
            Self::ContainerIndex => b"BRFCI005",
            Self::FrameIndex => b"BRFFI005",
            Self::ColdIndex => b"BRFAI005",
            Self::LargeIndex => b"BRFLI005",
            Self::SourceStatsIndex => b"BRFSI005",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotObjectRef {
    pub key: String,
    pub kind: SnapshotObjectKind,
    pub object_len: u64,
    pub digest: [u8; 32],
}

impl SnapshotObjectRef {
    /// Canonical leaf value for an object reference inside a PM07 index.
    pub fn encode_value(&self) -> PackedResult<Vec<u8>> {
        validate_key(&self.key)?;
        if self.digest == [0; 32]
            || self.object_len < (SNAPSHOT_HEADER_LEN + SNAPSHOT_FOOTER_LEN) as u64
            || self.object_len
                > (SNAPSHOT_HEADER_LEN + SNAPSHOT_MAX_BODY_BYTES + SNAPSHOT_FOOTER_LEN) as u64
        {
            return Err(PackedWireError::Invalid(
                "wire 005 object ref identity/length is invalid".into(),
            ));
        }
        let mut writer = super::wire::Writer::default();
        writer.bytes(b"OR05");
        writer.u8(self.kind as u8);
        writer.u8(0);
        writer.u16(self.key.len() as u16);
        writer.u64(self.object_len);
        writer.bytes(&self.digest);
        writer.bytes(self.key.as_bytes());
        Ok(writer.finish())
    }

    pub fn decode_value(bytes: &[u8]) -> PackedResult<Self> {
        let mut reader = super::wire::Reader::new(bytes);
        if reader.take(4)? != b"OR05" {
            return Err(PackedWireError::UnsupportedFormat(
                "wire 005 object ref payload mismatch".into(),
            ));
        }
        let kind = SnapshotObjectKind::from_u8(reader.u8()?)?;
        reader.skip_zeroes(1)?;
        let length = reader.u16()? as usize;
        if length == 0 || length > 4096 {
            return Err(PackedWireError::LimitExceeded(
                "wire 005 ref key exceeds budget".into(),
            ));
        }
        let object_len = reader.u64()?;
        let digest = reader.array::<32>()?;
        let key = std::str::from_utf8(reader.take(length)?)
            .map_err(|_| PackedWireError::Invalid("wire 005 ref key is not UTF-8".into()))?
            .to_owned();
        if !reader.is_empty() {
            return Err(PackedWireError::Invalid(
                "wire 005 ref value has trailing bytes".into(),
            ));
        }
        let reference = Self {
            key,
            kind,
            object_len,
            digest,
        };
        reference.encode_value()?;
        Ok(reference)
    }

    pub fn from_bytes(key: String, kind: SnapshotObjectKind, bytes: &[u8]) -> PackedResult<Self> {
        validate_key(&key)?;
        Ok(Self {
            key,
            kind,
            object_len: bytes.len() as u64,
            digest: Sha256::digest(bytes).into(),
        })
    }

    pub fn verify<'a>(&self, bytes: &'a [u8], body_limit: usize) -> PackedResult<&'a [u8]> {
        validate_key(&self.key)?;
        if self.object_len
            > (SNAPSHOT_HEADER_LEN + body_limit.min(SNAPSHOT_MAX_BODY_BYTES) + SNAPSHOT_FOOTER_LEN)
                as u64
        {
            return Err(PackedWireError::LimitExceeded(
                "wire 005 authenticated object exceeds budget".into(),
            ));
        }
        if bytes.len() as u64 != self.object_len {
            return Err(PackedWireError::Invalid(
                "wire 005 object length disagrees with authenticated ref".into(),
            ));
        }
        let computed: [u8; 32] = Sha256::digest(bytes).into();
        if computed != self.digest {
            return Err(PackedWireError::HashMismatch {
                what: "wire 005 authenticated object",
                expected: hex::encode(self.digest),
                computed: hex::encode(computed),
            });
        }
        decode_snapshot_object(bytes, self.kind, body_limit)
    }
}

fn validate_key(key: &str) -> PackedResult<()> {
    if key.is_empty()
        || key.len() > 4096
        || key.contains('\0')
        || key.starts_with('/')
        || key
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(PackedWireError::Invalid(
            "wire 005 object key is empty, too long or contains NUL".into(),
        ));
    }
    Ok(())
}

pub fn encode_snapshot_object(
    kind: SnapshotObjectKind,
    body: &[u8],
    body_limit: usize,
) -> PackedResult<Vec<u8>> {
    if body.len() > body_limit.min(SNAPSHOT_MAX_BODY_BYTES) {
        return Err(PackedWireError::LimitExceeded(
            "wire 005 body exceeds budget".into(),
        ));
    }
    let object_len = SNAPSHOT_HEADER_LEN + body.len() + SNAPSHOT_FOOTER_LEN;
    let mut header = [0u8; SNAPSHOT_HEADER_LEN];
    header[..8].copy_from_slice(kind.magic());
    header[8..10].copy_from_slice(&5u16.to_le_bytes());
    header[12..16].copy_from_slice(&(SNAPSHOT_HEADER_LEN as u32).to_le_bytes());
    header[24..32].copy_from_slice(&(object_len as u64).to_le_bytes());
    header[32..40].copy_from_slice(&(SNAPSHOT_HEADER_LEN as u64).to_le_bytes());
    header[40..44].copy_from_slice(&(body.len() as u32).to_le_bytes());
    header[44..48].copy_from_slice(&(body.len() as u32).to_le_bytes());
    header[48] = 1; // SHA-256; body codec 0, independent sub-block codecs only.
    let crc = crc32c::crc32c(&header[..60]);
    header[60..64].copy_from_slice(&crc.to_le_bytes());
    let mut footer = [0u8; SNAPSHOT_FOOTER_LEN];
    footer[..8].copy_from_slice(b"BRFEND05");
    footer[8..16].copy_from_slice(&(object_len as u64).to_le_bytes());
    footer[16..48].copy_from_slice(&Sha256::digest(body));
    footer[48..64].copy_from_slice(&Sha256::digest(header)[..16]);
    let mut bytes = Vec::with_capacity(object_len);
    bytes.extend_from_slice(&header);
    bytes.extend_from_slice(body);
    bytes.extend_from_slice(&footer);
    Ok(bytes)
}

pub fn decode_snapshot_object(
    bytes: &[u8],
    kind: SnapshotObjectKind,
    body_limit: usize,
) -> PackedResult<&[u8]> {
    if bytes.len() < SNAPSHOT_HEADER_LEN + SNAPSHOT_FOOTER_LEN {
        return Err(PackedWireError::Truncated {
            what: "wire 005 envelope",
            need: SNAPSHOT_HEADER_LEN + SNAPSHOT_FOOTER_LEN,
            have: bytes.len(),
        });
    }
    let header = &bytes[..SNAPSHOT_HEADER_LEN];
    let length = decode_snapshot_envelope_header(header, kind, bytes.len() as u64, body_limit)?;
    let body = &bytes[SNAPSHOT_HEADER_LEN..SNAPSHOT_HEADER_LEN + length];
    let footer = &bytes[SNAPSHOT_HEADER_LEN + length..];
    let body_digest: [u8; 32] = Sha256::digest(body).into();
    validate_snapshot_envelope_footer(header, footer, bytes.len() as u64, &body_digest)?;
    Ok(body)
}

/// Shared buffered/streaming envelope rules. The caller must independently
/// establish physical length; a prefix range cannot exclude an added suffix.
pub(super) fn decode_snapshot_envelope_header(
    header: &[u8],
    kind: SnapshotObjectKind,
    actual_object_len: u64,
    body_limit: usize,
) -> PackedResult<usize> {
    if header.len() != SNAPSHOT_HEADER_LEN {
        return Err(PackedWireError::Invalid(
            "wire 005 envelope header has a noncanonical length".into(),
        ));
    }
    if &header[..8] != kind.magic()
        || u16::from_le_bytes(header[8..10].try_into().unwrap()) != 5
        || header[10..12] != [0; 2]
    {
        return Err(PackedWireError::UnsupportedFormat(
            "wire 005 magic, kind or version mismatch".into(),
        ));
    }
    if header[16..24] != [0; 8] || header[48] != 1 || header[49] != 0 {
        return Err(PackedWireError::UnsupportedFormat(
            "unknown wire 005 required features, hash or body codec".into(),
        ));
    }
    if u32::from_le_bytes(header[12..16].try_into().unwrap()) as usize != SNAPSHOT_HEADER_LEN
        || u64::from_le_bytes(header[32..40].try_into().unwrap()) != SNAPSHOT_HEADER_LEN as u64
        || header[50..60] != [0; 10]
        || header[64..].iter().any(|byte| *byte != 0)
    {
        return Err(PackedWireError::Invalid(
            "wire 005 header length, offset or reserved padding mismatch".into(),
        ));
    }
    if u32::from_le_bytes(header[60..64].try_into().unwrap()) != crc32c::crc32c(&header[..60]) {
        return Err(PackedWireError::Invalid(
            "wire 005 header CRC mismatch".into(),
        ));
    }
    let length = u32::from_le_bytes(header[40..44].try_into().unwrap()) as usize;
    if length > body_limit.min(SNAPSHOT_MAX_BODY_BYTES) {
        return Err(PackedWireError::LimitExceeded(
            "wire 005 body exceeds budget".into(),
        ));
    }
    if header[40..44] != header[44..48] {
        return Err(PackedWireError::Invalid(
            "wire 005 body raw/stored length mismatch".into(),
        ));
    }
    let object_len = SNAPSHOT_HEADER_LEN + length + SNAPSHOT_FOOTER_LEN;
    if actual_object_len != object_len as u64
        || u64::from_le_bytes(header[24..32].try_into().unwrap()) != object_len as u64
    {
        return Err(PackedWireError::Invalid(
            "wire 005 declared/actual object length mismatch".into(),
        ));
    }
    Ok(length)
}

pub(super) fn validate_snapshot_envelope_footer(
    header: &[u8],
    footer: &[u8],
    object_len: u64,
    body_digest: &[u8; 32],
) -> PackedResult<()> {
    if header.len() != SNAPSHOT_HEADER_LEN || footer.len() != SNAPSHOT_FOOTER_LEN {
        return Err(PackedWireError::Invalid(
            "wire 005 envelope header/footer has a noncanonical length".into(),
        ));
    }
    if &footer[..8] != b"BRFEND05"
        || u64::from_le_bytes(footer[8..16].try_into().unwrap()) != object_len
    {
        return Err(PackedWireError::Invalid(
            "wire 005 footer identity mismatch".into(),
        ));
    }
    if footer[16..48] != body_digest[..] || footer[48..64] != Sha256::digest(header)[..16] {
        return Err(PackedWireError::Invalid(
            "wire 005 footer body/header hash mismatch".into(),
        ));
    }
    Ok(())
}

/// Fetch only a bounded authenticated metadata object. Large payload objects
/// must use independently authenticated ranges, not this whole-page helper.
pub async fn read_snapshot_page<B: crate::cadapter::client::ObjectBackend + Clone>(
    client: &crate::cadapter::client::ObjectClient<B>,
    reference: &SnapshotObjectRef,
    body_limit: usize,
) -> PackedResult<Vec<u8>> {
    read_snapshot_page_validated(client, reference, body_limit, |bytes| {
        reference.verify(&bytes, body_limit)?;
        Ok(bytes)
    })
    .await
}

pub async fn read_snapshot_page_validated<B, T, Verify>(
    client: &crate::cadapter::client::ObjectClient<B>,
    reference: &SnapshotObjectRef,
    body_limit: usize,
    verify: Verify,
) -> PackedResult<T>
where
    B: crate::cadapter::client::ObjectBackend + Clone,
    Verify: FnOnce(Vec<u8>) -> PackedResult<T>,
{
    validate_key(&reference.key)?;
    let maximum = SNAPSHOT_HEADER_LEN + body_limit.min(512 * 1024) + SNAPSHOT_FOOTER_LEN;
    if body_limit > 512 * 1024 || reference.object_len > maximum as u64 {
        return Err(PackedWireError::LimitExceeded(
            "wire 005 page exceeds range/decode budget".into(),
        ));
    }
    let class = page_read_class(reference.kind)?;
    client
        .typed_exact(
            class,
            &reference.key,
            0,
            reference.object_len,
            maximum as u64,
            |bytes| verify(bytes).map_err(observer_validation_error),
        )
        .await
        .map_err(observer_backend_error)
}

pub(crate) fn page_read_class(
    kind: SnapshotObjectKind,
) -> PackedResult<crate::cadapter::read_observer::ReadClass> {
    use crate::cadapter::read_observer::ReadClass;
    Ok(match kind {
        SnapshotObjectKind::Manifest => ReadClass::Manifest,
        SnapshotObjectKind::ColdAttributes => ReadClass::ColdAttributes,
        SnapshotObjectKind::FrameDirectory => ReadClass::FrameDirectory,
        SnapshotObjectKind::GroupIndex => ReadClass::GroupIndex,
        SnapshotObjectKind::InodeIndex => ReadClass::InodeIndex,
        SnapshotObjectKind::ReverseIndex => ReadClass::ReverseIndex,
        SnapshotObjectKind::ContainerIndex => ReadClass::ContainerIndex,
        SnapshotObjectKind::FrameIndex => ReadClass::FrameIndex,
        SnapshotObjectKind::ColdIndex => ReadClass::ColdIndex,
        SnapshotObjectKind::LargeIndex => ReadClass::LargeIndex,
        SnapshotObjectKind::SourceStatsIndex => ReadClass::SourceStatsIndex,
        SnapshotObjectKind::GroupContainer | SnapshotObjectKind::LargeData => {
            return Err(PackedWireError::Invalid(
                "container requires a typed metadata/frame subrange".into(),
            ));
        }
    })
}

pub(crate) fn observer_validation_error(
    error: PackedWireError,
) -> (crate::cadapter::read_observer::FailureClass, anyhow::Error) {
    use crate::cadapter::read_observer::FailureClass;
    let class = match &error {
        PackedWireError::HashMismatch { .. } => FailureClass::Authentication,
        PackedWireError::Truncated { .. } => FailureClass::ShortBody,
        PackedWireError::LimitExceeded(_) => FailureClass::Admission,
        PackedWireError::Backend(_) => FailureClass::Backend,
        PackedWireError::ReadViewChanged => FailureClass::Generation,
        PackedWireError::UnsupportedFormat(_) | PackedWireError::Invalid(_) => FailureClass::Schema,
    };
    let error = match error {
        PackedWireError::ReadViewChanged => {
            anyhow::Error::new(crate::chunk::read_plan::ReadViewChanged)
        }
        error => error.into(),
    };
    (class, error)
}

pub(crate) fn observer_backend_error(error: anyhow::Error) -> PackedWireError {
    match error.downcast::<PackedWireError>() {
        Ok(error) => error,
        Err(error) => match error.downcast::<crate::cadapter::read_observer::ReadBoundaryError>() {
            Ok(crate::cadapter::read_observer::ReadBoundaryError::Admission) => {
                PackedWireError::LimitExceeded("typed range allocation/end boundary".into())
            }
            Ok(crate::cadapter::read_observer::ReadBoundaryError::Short { expected, received }) => {
                PackedWireError::Truncated {
                    what: "packed streamed range",
                    need: usize::try_from(expected).unwrap_or(usize::MAX),
                    have: usize::try_from(received).unwrap_or(usize::MAX),
                }
            }
            Ok(crate::cadapter::read_observer::ReadBoundaryError::Excess) => {
                PackedWireError::Invalid("typed range returned more bytes than requested".into())
            }
            Err(error) => PackedWireError::Backend(error.to_string()),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn authenticated_page_read_rejects_replaced_object_and_oversized_ref() {
        use crate::cadapter::{client::ObjectClient, localfs::LocalFsBackend};
        let root = tempfile::tempdir().unwrap();
        let client = ObjectClient::new(LocalFsBackend::new(root.path()));
        let first =
            encode_snapshot_object(SnapshotObjectKind::FrameDirectory, b"first", 1024).unwrap();
        let reference = SnapshotObjectRef::from_bytes(
            "directory".into(),
            SnapshotObjectKind::FrameDirectory,
            &first,
        )
        .unwrap();
        client.put_object(&reference.key, &first).await.unwrap();
        assert_eq!(
            read_snapshot_page(&client, &reference, 1024).await.unwrap(),
            first
        );
        let replacement =
            encode_snapshot_object(SnapshotObjectKind::FrameDirectory, b"other", 1024).unwrap();
        client
            .put_object(&reference.key, &replacement)
            .await
            .unwrap();
        assert!(matches!(
            read_snapshot_page(&client, &reference, 1024).await,
            Err(PackedWireError::HashMismatch { .. })
        ));
        let huge = SnapshotObjectRef {
            object_len: u64::MAX,
            ..reference
        };
        assert!(matches!(
            read_snapshot_page(&client, &huge, 1024).await,
            Err(PackedWireError::LimitExceeded(_))
        ));
    }

    #[test]
    fn snapshot_roundtrips_all_independent_object_kinds() {
        for kind in [
            SnapshotObjectKind::Manifest,
            SnapshotObjectKind::GroupContainer,
            SnapshotObjectKind::ColdAttributes,
            SnapshotObjectKind::FrameDirectory,
            SnapshotObjectKind::LargeData,
            SnapshotObjectKind::GroupIndex,
            SnapshotObjectKind::InodeIndex,
            SnapshotObjectKind::ReverseIndex,
            SnapshotObjectKind::SourceStatsIndex,
        ] {
            let bytes = encode_snapshot_object(kind, b"payload", 1024).unwrap();
            assert_eq!(&bytes[..8], kind.magic());
            assert_eq!(bytes.len(), SNAPSHOT_HEADER_LEN + 7 + SNAPSHOT_FOOTER_LEN);
            let reference =
                SnapshotObjectRef::from_bytes("objects/test".into(), kind, &bytes).unwrap();
            assert_eq!(reference.verify(&bytes, 1024).unwrap(), b"payload");
            assert!(super::super::wire::PackedEnvelope::parse(bytes).is_err());
        }
    }

    #[test]
    fn snapshot_rejects_mutated_header_padding_payload_and_footer() {
        let bytes = encode_snapshot_object(SnapshotObjectKind::Manifest, b"payload", 1024).unwrap();
        for index in [
            8usize,
            12,
            16,
            24,
            32,
            40,
            44,
            48,
            49,
            50,
            60,
            64,
            SNAPSHOT_HEADER_LEN,
            SNAPSHOT_HEADER_LEN + 7,
            SNAPSHOT_HEADER_LEN + 7 + 16,
            SNAPSHOT_HEADER_LEN + 7 + 48,
        ] {
            let mut bad = bytes.clone();
            bad[index] ^= 1;
            assert!(
                decode_snapshot_object(&bad, SnapshotObjectKind::Manifest, 1024).is_err(),
                "index {index}"
            );
        }
        assert!(decode_snapshot_object(&bytes, SnapshotObjectKind::ColdAttributes, 1024).is_err());
        assert!(
            decode_snapshot_object(
                &bytes[..bytes.len() - 1],
                SnapshotObjectKind::Manifest,
                1024
            )
            .is_err()
        );
        let mut tail = bytes.clone();
        tail.push(0);
        assert!(decode_snapshot_object(&tail, SnapshotObjectKind::Manifest, 1024).is_err());
    }

    #[test]
    fn independently_pinned_ref_rejects_recomputed_internal_hashes() {
        let first =
            encode_snapshot_object(SnapshotObjectKind::FrameDirectory, b"first", 1024).unwrap();
        let replacement =
            encode_snapshot_object(SnapshotObjectKind::FrameDirectory, b"other", 1024).unwrap();
        assert!(
            decode_snapshot_object(&replacement, SnapshotObjectKind::FrameDirectory, 1024).is_ok()
        );
        let reference = SnapshotObjectRef::from_bytes(
            "objects/directory".into(),
            SnapshotObjectKind::FrameDirectory,
            &first,
        )
        .unwrap();
        assert!(matches!(
            reference.verify(&replacement, 1024),
            Err(PackedWireError::HashMismatch { .. })
        ));
        assert!(matches!(
            reference.verify(&first, 4),
            Err(PackedWireError::LimitExceeded(_))
        ));
        assert!(
            SnapshotObjectRef::from_bytes("bad\0key".into(), SnapshotObjectKind::Manifest, &first)
                .is_err()
        );
    }
}
