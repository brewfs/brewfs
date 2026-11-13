use sha2::{Digest, Sha256};
use thiserror::Error;

use super::layout::{AccessProfile, LayoutError, SizeClassTable};

pub const MANIFEST_MAGIC: &[u8; 8] = b"BRFPM004";
pub const GROUP_CONTAINER_MAGIC: &[u8; 8] = b"BRFGC004";
pub const COLD_ATTRIBUTE_MAGIC: &[u8; 8] = b"BRFCA004";
pub const GROUP_INDEX_MAGIC: &[u8; 8] = b"BRFGI004";
pub const INODE_INDEX_MAGIC: &[u8; 8] = b"BRFII004";
pub const PACKED_FOOTER_MAGIC: &[u8; 8] = b"BRFEND04";

pub const PACKED_HEADER_LEN: usize = 64;
pub const PACKED_FOOTER_LEN: usize = 64;
const WIRE_MAJOR: u16 = 4;
const WIRE_MINOR: u16 = 0;
const HEADER_HASH_ID_SHA256: u8 = 1;
const CODEC_NONE: u8 = 0;
pub(crate) const MAX_OBJECT_BODY: u64 = 64 * 1024 * 1024;
const MAX_MANIFEST_GROUPS: u32 = 16 * 1024 * 1024;
const MAX_MANIFEST_CONTAINERS: u32 = 1024 * 1024;
const MAX_NAME_RANGE_BYTES: u32 = 1024 * 1024;
const MAX_INDEX_FENCE_NAME_BYTES: u32 = 1024;
const MAX_OBJECT_KEY_BYTES: u32 = 4 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PackedObjectKind {
    Manifest,
    GroupContainer,
    ColdAttributes,
    GroupIndex,
    InodeIndex,
}

impl PackedObjectKind {
    pub const fn magic(self) -> &'static [u8; 8] {
        match self {
            Self::Manifest => MANIFEST_MAGIC,
            Self::GroupContainer => GROUP_CONTAINER_MAGIC,
            Self::ColdAttributes => COLD_ATTRIBUTE_MAGIC,
            Self::GroupIndex => GROUP_INDEX_MAGIC,
            Self::InodeIndex => INODE_INDEX_MAGIC,
        }
    }

    fn from_magic(magic: &[u8]) -> Result<Self, PackedWireError> {
        if magic == MANIFEST_MAGIC.as_slice() {
            Ok(Self::Manifest)
        } else if magic == GROUP_CONTAINER_MAGIC.as_slice() {
            Ok(Self::GroupContainer)
        } else if magic == COLD_ATTRIBUTE_MAGIC.as_slice() {
            Ok(Self::ColdAttributes)
        } else if magic == GROUP_INDEX_MAGIC.as_slice() {
            Ok(Self::GroupIndex)
        } else if magic == INODE_INDEX_MAGIC.as_slice() {
            Ok(Self::InodeIndex)
        } else {
            Err(PackedWireError::UnsupportedFormat(
                "unknown packed v3 object magic".into(),
            ))
        }
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum PackedWireError {
    #[error("unsupported packed v3 format: {0}")]
    UnsupportedFormat(String),
    #[error("invalid packed v3 object: {0}")]
    Invalid(String),
    #[error("truncated packed v3 object while reading {what}: need {need}, have {have}")]
    Truncated {
        what: &'static str,
        need: usize,
        have: usize,
    },
    #[error("packed v3 object limit exceeded: {0}")]
    LimitExceeded(String),
    #[error("packed v3 hash mismatch for {what}: expected {expected}, computed {computed}")]
    HashMismatch {
        what: &'static str,
        expected: String,
        computed: String,
    },
    #[error("packed v3 object backend error: {0}")]
    Backend(String),
    #[error("packed v3 read view changed")]
    ReadViewChanged,
}

pub type PackedResult<T> = Result<T, PackedWireError>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackedHeader {
    pub kind: PackedObjectKind,
    pub flags: u64,
    pub object_len: u64,
    pub body_offset: u64,
    pub body_stored_len: u32,
    pub body_raw_len: u32,
}

impl PackedHeader {
    pub fn encode(&self) -> [u8; PACKED_HEADER_LEN] {
        let mut bytes = [0u8; PACKED_HEADER_LEN];
        bytes[0..8].copy_from_slice(self.kind.magic());
        bytes[8..10].copy_from_slice(&WIRE_MAJOR.to_le_bytes());
        bytes[10..12].copy_from_slice(&WIRE_MINOR.to_le_bytes());
        bytes[12..16].copy_from_slice(&(PACKED_HEADER_LEN as u32).to_le_bytes());
        bytes[16..24].copy_from_slice(&self.flags.to_le_bytes());
        bytes[24..32].copy_from_slice(&self.object_len.to_le_bytes());
        bytes[32..40].copy_from_slice(&self.body_offset.to_le_bytes());
        bytes[40..44].copy_from_slice(&self.body_stored_len.to_le_bytes());
        bytes[44..48].copy_from_slice(&self.body_raw_len.to_le_bytes());
        bytes[48] = HEADER_HASH_ID_SHA256;
        bytes[49] = CODEC_NONE;
        let crc = crc32c::crc32c(&bytes[..60]);
        bytes[60..64].copy_from_slice(&crc.to_le_bytes());
        bytes
    }

    pub fn parse(bytes: &[u8]) -> PackedResult<Self> {
        if bytes.len() < PACKED_HEADER_LEN {
            return Err(PackedWireError::Truncated {
                what: "packed v3 header",
                need: PACKED_HEADER_LEN,
                have: bytes.len(),
            });
        }
        let kind = PackedObjectKind::from_magic(&bytes[..8])?;
        let major = u16::from_le_bytes([bytes[8], bytes[9]]);
        let minor = u16::from_le_bytes([bytes[10], bytes[11]]);
        if major != WIRE_MAJOR || minor != WIRE_MINOR {
            return Err(PackedWireError::UnsupportedFormat(format!(
                "wire version {major}.{minor}, expected {WIRE_MAJOR}.{WIRE_MINOR}"
            )));
        }
        let header_len = u32::from_le_bytes(bytes[12..16].try_into().unwrap());
        if header_len as usize != PACKED_HEADER_LEN {
            return Err(PackedWireError::Invalid(format!(
                "header length {header_len} is not {PACKED_HEADER_LEN}"
            )));
        }
        let flags = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
        if flags != 0 {
            return Err(PackedWireError::UnsupportedFormat(format!(
                "unknown packed v3 flags {flags:#x}"
            )));
        }
        let object_len = u64::from_le_bytes(bytes[24..32].try_into().unwrap());
        let body_offset = u64::from_le_bytes(bytes[32..40].try_into().unwrap());
        let body_stored_len = u32::from_le_bytes(bytes[40..44].try_into().unwrap());
        let body_raw_len = u32::from_le_bytes(bytes[44..48].try_into().unwrap());
        if bytes[48] != HEADER_HASH_ID_SHA256 || bytes[49] != CODEC_NONE {
            return Err(PackedWireError::UnsupportedFormat(
                "packed v3 requires SHA-256 and uncompressed body".into(),
            ));
        }
        if body_raw_len != body_stored_len {
            return Err(PackedWireError::Invalid(
                "uncompressed packed v3 body has different raw and stored lengths".into(),
            ));
        }
        if bytes[50..60].iter().any(|byte| *byte != 0) {
            return Err(PackedWireError::Invalid(
                "packed v3 header reserved bytes are non-zero".into(),
            ));
        }
        let stored_crc = u32::from_le_bytes(bytes[60..64].try_into().unwrap());
        let computed_crc = crc32c::crc32c(&bytes[..60]);
        if stored_crc != computed_crc {
            return Err(PackedWireError::Invalid(format!(
                "packed v3 header CRC mismatch: stored {stored_crc:08x}, computed {computed_crc:08x}"
            )));
        }
        if object_len < (PACKED_HEADER_LEN + PACKED_FOOTER_LEN) as u64 {
            return Err(PackedWireError::Invalid(
                "packed v3 object is shorter than header and footer".into(),
            ));
        }
        if body_offset != PACKED_HEADER_LEN as u64 {
            return Err(PackedWireError::Invalid(format!(
                "packed v3 body offset {body_offset} is not {PACKED_HEADER_LEN}"
            )));
        }
        if u64::from(body_raw_len) > MAX_OBJECT_BODY || u64::from(body_stored_len) > MAX_OBJECT_BODY
        {
            return Err(PackedWireError::LimitExceeded(
                "packed v3 body exceeds 64 MiB".into(),
            ));
        }
        let expected_len = body_offset
            .checked_add(u64::from(body_stored_len))
            .and_then(|value| value.checked_add(PACKED_FOOTER_LEN as u64))
            .ok_or_else(|| PackedWireError::Invalid("packed v3 object length overflows".into()))?;
        if object_len != expected_len {
            return Err(PackedWireError::Invalid(format!(
                "packed v3 object length {object_len} does not match body {body_stored_len}"
            )));
        }
        Ok(Self {
            kind,
            flags,
            object_len,
            body_offset,
            body_stored_len,
            body_raw_len,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PackedFooter {
    object_len: u64,
    body_digest: [u8; 32],
}

impl PackedFooter {
    fn encode(&self) -> [u8; PACKED_FOOTER_LEN] {
        let mut bytes = [0u8; PACKED_FOOTER_LEN];
        bytes[..8].copy_from_slice(PACKED_FOOTER_MAGIC);
        bytes[8..16].copy_from_slice(&self.object_len.to_le_bytes());
        bytes[16..48].copy_from_slice(&self.body_digest);
        let crc = crc32c::crc32c(&bytes[..60]);
        bytes[60..64].copy_from_slice(&crc.to_le_bytes());
        bytes
    }

    fn parse(bytes: &[u8]) -> PackedResult<Self> {
        if bytes.len() < PACKED_FOOTER_LEN {
            return Err(PackedWireError::Truncated {
                what: "packed v3 footer",
                need: PACKED_FOOTER_LEN,
                have: bytes.len(),
            });
        }
        if &bytes[..8] != PACKED_FOOTER_MAGIC {
            return Err(PackedWireError::UnsupportedFormat(
                "packed v3 footer magic mismatch".into(),
            ));
        }
        if bytes[48..60].iter().any(|byte| *byte != 0) {
            return Err(PackedWireError::Invalid(
                "packed v3 footer reserved bytes are non-zero".into(),
            ));
        }
        let stored_crc = u32::from_le_bytes(bytes[60..64].try_into().unwrap());
        let computed_crc = crc32c::crc32c(&bytes[..60]);
        if stored_crc != computed_crc {
            return Err(PackedWireError::Invalid(format!(
                "packed v3 footer CRC mismatch: stored {stored_crc:08x}, computed {computed_crc:08x}"
            )));
        }
        Ok(Self {
            object_len: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
            body_digest: bytes[16..48].try_into().unwrap(),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackedEnvelope {
    pub header: PackedHeader,
    body: Vec<u8>,
}

impl PackedEnvelope {
    pub fn build(kind: PackedObjectKind, body: Vec<u8>) -> PackedResult<Vec<u8>> {
        if body.len() as u64 > MAX_OBJECT_BODY {
            return Err(PackedWireError::LimitExceeded(
                "packed v3 body exceeds 64 MiB".into(),
            ));
        }
        let object_len = (PACKED_HEADER_LEN as u64)
            .checked_add(body.len() as u64)
            .and_then(|value| value.checked_add(PACKED_FOOTER_LEN as u64))
            .ok_or_else(|| {
                PackedWireError::LimitExceeded("packed v3 object is too large".into())
            })?;
        let header = PackedHeader {
            kind,
            flags: 0,
            object_len,
            body_offset: PACKED_HEADER_LEN as u64,
            body_stored_len: u32::try_from(body.len())
                .map_err(|_| PackedWireError::LimitExceeded("body exceeds u32".into()))?,
            body_raw_len: u32::try_from(body.len())
                .map_err(|_| PackedWireError::LimitExceeded("body exceeds u32".into()))?,
        };
        let digest: [u8; 32] = Sha256::digest(&body).into();
        let footer = PackedFooter {
            object_len,
            body_digest: digest,
        };
        let mut object = Vec::with_capacity(object_len as usize);
        object.extend_from_slice(&header.encode());
        object.extend_from_slice(&body);
        object.extend_from_slice(&footer.encode());
        Ok(object)
    }

    pub fn parse(bytes: Vec<u8>) -> PackedResult<Self> {
        let header = PackedHeader::parse(&bytes)?;
        let object_len = usize::try_from(header.object_len).map_err(|_| {
            PackedWireError::LimitExceeded("packed v3 object length exceeds usize".into())
        })?;
        if object_len != bytes.len() {
            return Err(PackedWireError::Truncated {
                what: "packed v3 object",
                need: object_len,
                have: bytes.len(),
            });
        }
        let footer_start = bytes.len() - PACKED_FOOTER_LEN;
        let footer = PackedFooter::parse(&bytes[footer_start..])?;
        if footer.object_len != header.object_len {
            return Err(PackedWireError::Invalid(
                "packed v3 header/footer object lengths disagree".into(),
            ));
        }
        let body = bytes[PACKED_HEADER_LEN..footer_start].to_vec();
        let digest: [u8; 32] = Sha256::digest(&body).into();
        if digest != footer.body_digest {
            return Err(PackedWireError::HashMismatch {
                what: "packed v3 body",
                expected: hex::encode(footer.body_digest),
                computed: hex::encode(digest),
            });
        }
        Ok(Self { header, body })
    }

    pub fn body(&self) -> &[u8] {
        &self.body
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackedGroupRef {
    pub group_id: u64,
    pub container_ordinal: u32,
    pub parent_dir_key: [u8; 32],
    pub first_name: Vec<u8>,
    pub last_name: Vec<u8>,
    pub meta_offset: u64,
    pub meta_len: u32,
    pub data_offset: u64,
    pub data_len: u32,
    pub entry_count: u32,
    pub file_count: u32,
    pub frame_count: u32,
    pub layout_profile: AccessProfile,
    pub metadata_digest: [u8; 32],
    pub data_digest: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackedContainerRef {
    pub object_key: Vec<u8>,
    pub object_len: u64,
    pub object_digest: [u8; 32],
}

/// Authenticated routing fence for one pageable group-index object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackedGroupIndexPageRef {
    pub object: PackedContainerRef,
    pub first_parent_dir_key: [u8; 32],
    pub first_name: Vec<u8>,
    pub last_parent_dir_key: [u8; 32],
    pub last_name: Vec<u8>,
}

impl PackedGroupIndexPageRef {
    fn validate(&self) -> PackedResult<()> {
        validate_object_ref(&self.object, "group index page")?;
        validate_fence_name(&self.first_name)?;
        validate_fence_name(&self.last_name)?;
        if compare_group_key(
            self.first_parent_dir_key,
            &self.first_name,
            self.last_parent_dir_key,
            &self.last_name,
        ) == std::cmp::Ordering::Greater
        {
            return Err(PackedWireError::Invalid(
                "group index page fence is inverted".into(),
            ));
        }
        Ok(())
    }
}

/// Authenticated inclusive inode range for one pageable inode-index object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackedInodeIndexPageRef {
    pub object: PackedContainerRef,
    pub first_inode: u64,
    pub last_inode: u64,
}

impl PackedInodeIndexPageRef {
    fn validate(&self) -> PackedResult<()> {
        validate_object_ref(&self.object, "inode index page")?;
        if self.first_inode > self.last_inode {
            return Err(PackedWireError::Invalid(
                "inode index page range is inverted".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackedSnapshotManifest {
    pub snapshot_id: [u8; 32],
    /// Stable namespace identity used as the parent key for the root
    /// directory.  It is required even when no group is currently attached
    /// to the root so a read-only mount can resolve `/` without a lookup.
    pub root_dir_key: [u8; 32],
    /// Snapshot-local inode number of the root directory.
    pub root_inode: u64,
    pub layout_profile: AccessProfile,
    pub size_classes: SizeClassTable,
    pub groups: Vec<PackedGroupRef>,
    pub containers: Vec<PackedContainerRef>,
    /// CAS references to pageable group and inode indexes.  Small snapshots
    /// may leave these empty and keep the group refs inline.
    pub group_index_pages: Vec<PackedGroupIndexPageRef>,
    pub inode_index_pages: Vec<PackedInodeIndexPageRef>,
}

impl PackedSnapshotManifest {
    pub fn encode(&self) -> PackedResult<Vec<u8>> {
        self.validate()?;
        let mut writer = Writer::default();
        writer.bytes(b"PM06");
        writer.bytes(&self.snapshot_id);
        writer.bytes(&self.root_dir_key);
        writer.u64(self.root_inode);
        writer.u8(self.layout_profile as u8);
        writer.bytes(&[0, 0, 0]);
        writer.u64(self.size_classes.min_frame_raw_bytes);
        writer.u64(self.size_classes.max_random_frame_raw_bytes);
        writer.u64(self.size_classes.max_sequential_frame_raw_bytes);
        writer.u32(self.groups.len() as u32);
        writer.u32(self.containers.len() as u32);
        for group in &self.groups {
            writer.u64(group.group_id);
            writer.u32(group.container_ordinal);
            writer.u8(group.layout_profile as u8);
            writer.bytes(&[0, 0, 0]);
            writer.bytes(&group.parent_dir_key);
            writer.u64(group.meta_offset);
            writer.u32(group.meta_len);
            writer.u64(group.data_offset);
            writer.u32(group.data_len);
            writer.u32(group.entry_count);
            writer.u32(group.file_count);
            writer.u32(group.frame_count);
            writer.u32(group.first_name.len() as u32);
            writer.u32(group.last_name.len() as u32);
            writer.bytes(&group.metadata_digest);
            writer.bytes(&group.data_digest);
            writer.bytes(&group.first_name);
            writer.bytes(&group.last_name);
        }
        for container in &self.containers {
            writer.u32(container.object_key.len() as u32);
            writer.u64(container.object_len);
            writer.bytes(&container.object_digest);
            writer.bytes(&container.object_key);
        }
        encode_group_page_refs(&mut writer, &self.group_index_pages)?;
        encode_inode_page_refs(&mut writer, &self.inode_index_pages)?;
        PackedEnvelope::build(PackedObjectKind::Manifest, writer.finish())
    }

    pub fn decode(object: Vec<u8>) -> PackedResult<Self> {
        let envelope = PackedEnvelope::parse(object)?;
        if envelope.header.kind != PackedObjectKind::Manifest {
            return Err(PackedWireError::Invalid(
                "packed object is not a manifest".into(),
            ));
        }
        let mut reader = Reader::new(envelope.body());
        if reader.take(4)? != b"PM06" {
            return Err(PackedWireError::UnsupportedFormat(
                "packed manifest payload version mismatch".into(),
            ));
        }
        let snapshot_id = reader.array::<32>()?;
        let root_dir_key = reader.array::<32>()?;
        let root_inode = reader.u64()?;
        let layout_profile = AccessProfile::from_u8(reader.u8()?)
            .map_err(|error| PackedWireError::Invalid(error.to_string()))?;
        reader.skip_zeroes(3)?;
        let size_classes = SizeClassTable {
            min_frame_raw_bytes: reader.u64()?,
            max_random_frame_raw_bytes: reader.u64()?,
            max_sequential_frame_raw_bytes: reader.u64()?,
        }
        .validate()
        .map_err(layout_error)?;
        let group_count = reader.u32()?;
        let container_count = reader.u32()?;
        if group_count > MAX_MANIFEST_GROUPS || container_count > MAX_MANIFEST_CONTAINERS {
            return Err(PackedWireError::LimitExceeded(
                "manifest group/container count exceeds limit".into(),
            ));
        }
        let body_len = envelope.body().len();
        if usize::try_from(group_count)
            .ok()
            .and_then(|count| count.checked_mul(160))
            .is_none_or(|minimum| minimum > body_len)
        {
            return Err(PackedWireError::Invalid(
                "manifest group count exceeds the payload budget".into(),
            ));
        }
        if usize::try_from(container_count)
            .ok()
            .and_then(|count| count.checked_mul(45))
            .is_none_or(|minimum| minimum > body_len)
        {
            return Err(PackedWireError::Invalid(
                "manifest container count exceeds the payload budget".into(),
            ));
        }
        let mut groups = Vec::with_capacity(group_count as usize);
        for _ in 0..group_count {
            let group_id = reader.u64()?;
            let container_ordinal = reader.u32()?;
            let layout_profile = AccessProfile::from_u8(reader.u8()?)
                .map_err(|error| PackedWireError::Invalid(error.to_string()))?;
            reader.skip_zeroes(3)?;
            let parent_dir_key = reader.array::<32>()?;
            let meta_offset = reader.u64()?;
            let meta_len = reader.u32()?;
            let data_offset = reader.u64()?;
            let data_len = reader.u32()?;
            let entry_count = reader.u32()?;
            let file_count = reader.u32()?;
            let frame_count = reader.u32()?;
            let first_name_len = reader.u32()?;
            let last_name_len = reader.u32()?;
            validate_name_len(first_name_len)?;
            validate_name_len(last_name_len)?;
            let metadata_digest = reader.array::<32>()?;
            let data_digest = reader.array::<32>()?;
            let first_name = reader.bytes(first_name_len as usize)?.to_vec();
            let last_name = reader.bytes(last_name_len as usize)?.to_vec();
            groups.push(PackedGroupRef {
                group_id,
                container_ordinal,
                parent_dir_key,
                first_name,
                last_name,
                meta_offset,
                meta_len,
                data_offset,
                data_len,
                entry_count,
                file_count,
                frame_count,
                layout_profile,
                metadata_digest,
                data_digest,
            });
        }
        let mut containers = Vec::with_capacity(container_count as usize);
        for _ in 0..container_count {
            let object_key_len = reader.u32()?;
            if object_key_len == 0 || object_key_len > MAX_OBJECT_KEY_BYTES {
                return Err(PackedWireError::LimitExceeded(
                    "manifest object key is empty or too long".into(),
                ));
            }
            let object_len = reader.u64()?;
            let object_digest = reader.array::<32>()?;
            let object_key = reader.bytes(object_key_len as usize)?.to_vec();
            if object_key.contains(&0) {
                return Err(PackedWireError::Invalid(
                    "manifest object key contains NUL".into(),
                ));
            }
            containers.push(PackedContainerRef {
                object_key,
                object_len,
                object_digest,
            });
        }
        let group_index_pages = decode_group_page_refs(&mut reader)?;
        let inode_index_pages = decode_inode_page_refs(&mut reader)?;
        if !reader.is_empty() {
            return Err(PackedWireError::Invalid(
                "packed manifest has trailing bytes".into(),
            ));
        }
        let manifest = Self {
            snapshot_id,
            root_dir_key,
            root_inode,
            layout_profile,
            size_classes,
            groups,
            containers,
            group_index_pages,
            inode_index_pages,
        };
        manifest.validate()?;
        Ok(manifest)
    }

    fn validate(&self) -> PackedResult<()> {
        if self.root_inode == 0 {
            return Err(PackedWireError::Invalid(
                "manifest root inode must be non-zero".into(),
            ));
        }
        self.size_classes.validate().map_err(layout_error)?;
        if self.groups.len() as u64 > u64::from(MAX_MANIFEST_GROUPS)
            || self.containers.len() as u64 > u64::from(MAX_MANIFEST_CONTAINERS)
        {
            return Err(PackedWireError::LimitExceeded(
                "manifest group/container count exceeds limit".into(),
            ));
        }
        // A large snapshot must use the authenticated pageable group index
        // instead of carrying the same table inline.  Keeping both copies
        // would make the catalog retain the complete group table even when a
        // caller intends to use one-page lookups, defeating the v3 memory
        // bound.  Small snapshots may keep inline groups, but then they must
        // leave the group index roots empty.
        if !self.groups.is_empty() && !self.group_index_pages.is_empty() {
            return Err(PackedWireError::Invalid(
                "manifest cannot contain inline groups and group index pages together".into(),
            ));
        }
        let mut group_ids = std::collections::HashSet::with_capacity(self.groups.len());
        for group in &self.groups {
            if !group_ids.insert(group.group_id) {
                return Err(PackedWireError::Invalid(
                    "manifest contains duplicate group id".into(),
                ));
            }
            if usize::try_from(group.container_ordinal)
                .ok()
                .filter(|ordinal| *ordinal < self.containers.len())
                .is_none()
            {
                return Err(PackedWireError::Invalid(
                    "group references a missing container".into(),
                ));
            }
            validate_name_bytes(&group.first_name)?;
            validate_name_bytes(&group.last_name)?;
        }
        for container in &self.containers {
            if container.object_key.is_empty()
                || container.object_key.len() as u32 > MAX_OBJECT_KEY_BYTES
                || container.object_key.contains(&0)
            {
                return Err(PackedWireError::Invalid(
                    "container object key is invalid".into(),
                ));
            }
        }
        validate_group_page_refs(&self.group_index_pages)?;
        validate_inode_page_refs(&self.inode_index_pages)?;
        Ok(())
    }

    /// Return the page whose authenticated fence can contain a dentry.
    pub fn group_index_page_for_name(
        &self,
        parent_dir_key: [u8; 32],
        name: &[u8],
    ) -> Option<usize> {
        let ordinal = self.group_index_pages.partition_point(|page| {
            compare_group_key(
                page.last_parent_dir_key,
                &page.last_name,
                parent_dir_key,
                name,
            ) == std::cmp::Ordering::Less
        });
        let page = self.group_index_pages.get(ordinal)?;
        (compare_group_key(
            page.first_parent_dir_key,
            &page.first_name,
            parent_dir_key,
            name,
        ) != std::cmp::Ordering::Greater
            && compare_group_key(
                page.last_parent_dir_key,
                &page.last_name,
                parent_dir_key,
                name,
            ) != std::cmp::Ordering::Less)
            .then_some(ordinal)
    }

    /// Return the page whose inclusive range can contain an inode.
    pub fn inode_index_page_for_inode(&self, inode: u64) -> Option<usize> {
        let page = self
            .inode_index_pages
            .partition_point(|page| page.last_inode < inode);
        let reference = self.inode_index_pages.get(page)?;
        (reference.first_inode <= inode && inode <= reference.last_inode).then_some(page)
    }
}

const MAX_INDEX_PAGES: u32 = 4 * 1024 * 1024;

fn encode_group_page_refs(
    writer: &mut Writer,
    refs: &[PackedGroupIndexPageRef],
) -> PackedResult<()> {
    if refs.len() as u64 > u64::from(MAX_INDEX_PAGES) {
        return Err(PackedWireError::LimitExceeded(
            "packed index page count exceeds limit".into(),
        ));
    }
    writer.u32(refs.len() as u32);
    for reference in refs {
        reference.validate()?;
        encode_object_ref(writer, &reference.object)?;
        writer.bytes(&reference.first_parent_dir_key);
        writer.u32(reference.first_name.len() as u32);
        writer.bytes(&reference.first_name);
        writer.bytes(&reference.last_parent_dir_key);
        writer.u32(reference.last_name.len() as u32);
        writer.bytes(&reference.last_name);
    }
    Ok(())
}

fn decode_group_page_refs(reader: &mut Reader<'_>) -> PackedResult<Vec<PackedGroupIndexPageRef>> {
    let count = reader.u32()?;
    if count > MAX_INDEX_PAGES {
        return Err(PackedWireError::LimitExceeded(
            "packed index page count exceeds limit".into(),
        ));
    }
    let mut refs = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let object = decode_object_ref(reader, "group index page")?;
        let first_parent_dir_key = reader.array::<32>()?;
        let first_name = decode_fence_name(reader)?;
        let last_parent_dir_key = reader.array::<32>()?;
        let last_name = decode_fence_name(reader)?;
        refs.push(PackedGroupIndexPageRef {
            object,
            first_parent_dir_key,
            first_name,
            last_parent_dir_key,
            last_name,
        });
    }
    validate_group_page_refs(&refs)?;
    Ok(refs)
}

fn encode_inode_page_refs(
    writer: &mut Writer,
    refs: &[PackedInodeIndexPageRef],
) -> PackedResult<()> {
    if refs.len() as u64 > u64::from(MAX_INDEX_PAGES) {
        return Err(PackedWireError::LimitExceeded(
            "packed index page count exceeds limit".into(),
        ));
    }
    writer.u32(refs.len() as u32);
    for reference in refs {
        reference.validate()?;
        encode_object_ref(writer, &reference.object)?;
        writer.u64(reference.first_inode);
        writer.u64(reference.last_inode);
    }
    Ok(())
}

fn decode_inode_page_refs(reader: &mut Reader<'_>) -> PackedResult<Vec<PackedInodeIndexPageRef>> {
    let count = reader.u32()?;
    if count > MAX_INDEX_PAGES {
        return Err(PackedWireError::LimitExceeded(
            "packed index page count exceeds limit".into(),
        ));
    }
    let mut refs = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let object = decode_object_ref(reader, "inode index page")?;
        refs.push(PackedInodeIndexPageRef {
            object,
            first_inode: reader.u64()?,
            last_inode: reader.u64()?,
        });
    }
    validate_inode_page_refs(&refs)?;
    Ok(refs)
}

fn encode_object_ref(writer: &mut Writer, reference: &PackedContainerRef) -> PackedResult<()> {
    validate_object_ref(reference, "index page")?;
    writer.u32(reference.object_key.len() as u32);
    writer.u64(reference.object_len);
    writer.bytes(&reference.object_digest);
    writer.bytes(&reference.object_key);
    Ok(())
}

fn decode_object_ref(
    reader: &mut Reader<'_>,
    what: &'static str,
) -> PackedResult<PackedContainerRef> {
    let key_len = reader.u32()?;
    if key_len == 0 || key_len > MAX_OBJECT_KEY_BYTES {
        return Err(PackedWireError::LimitExceeded(format!(
            "packed {what} object key is empty or too long"
        )));
    }
    let object_len = reader.u64()?;
    let object_digest = reader.array::<32>()?;
    let object_key = reader.bytes(key_len as usize)?.to_vec();
    if object_key.contains(&0) {
        return Err(PackedWireError::Invalid(format!(
            "packed {what} object key contains NUL"
        )));
    }
    Ok(PackedContainerRef {
        object_key,
        object_len,
        object_digest,
    })
}

fn validate_object_ref(reference: &PackedContainerRef, what: &str) -> PackedResult<()> {
    if reference.object_key.is_empty()
        || reference.object_key.len() as u32 > MAX_OBJECT_KEY_BYTES
        || reference.object_key.contains(&0)
        || reference.object_len < (PACKED_HEADER_LEN + PACKED_FOOTER_LEN) as u64
    {
        return Err(PackedWireError::Invalid(format!(
            "{what} object reference is invalid"
        )));
    }
    Ok(())
}

fn validate_group_page_refs(refs: &[PackedGroupIndexPageRef]) -> PackedResult<()> {
    if refs.len() as u64 > u64::from(MAX_INDEX_PAGES) {
        return Err(PackedWireError::LimitExceeded(
            "packed index page count exceeds limit".into(),
        ));
    }
    for reference in refs {
        reference.validate()?;
    }
    for pair in refs.windows(2) {
        if compare_group_key(
            pair[0].last_parent_dir_key,
            &pair[0].last_name,
            pair[1].first_parent_dir_key,
            &pair[1].first_name,
        ) != std::cmp::Ordering::Less
        {
            return Err(PackedWireError::Invalid(
                "packed group index page fences are unsorted or overlapping".into(),
            ));
        }
    }
    Ok(())
}

fn validate_inode_page_refs(refs: &[PackedInodeIndexPageRef]) -> PackedResult<()> {
    if refs.len() as u64 > u64::from(MAX_INDEX_PAGES) {
        return Err(PackedWireError::LimitExceeded(
            "packed index page count exceeds limit".into(),
        ));
    }
    for reference in refs {
        reference.validate()?;
    }
    for pair in refs.windows(2) {
        if pair[0].last_inode >= pair[1].first_inode {
            return Err(PackedWireError::Invalid(
                "packed inode index page ranges are unsorted or overlapping".into(),
            ));
        }
    }
    Ok(())
}

fn decode_fence_name(reader: &mut Reader<'_>) -> PackedResult<Vec<u8>> {
    let length = reader.u32()?;
    if length == 0 || length > MAX_INDEX_FENCE_NAME_BYTES {
        return Err(PackedWireError::LimitExceeded(
            "packed index fence name is empty or exceeds 1 KiB".into(),
        ));
    }
    let name = reader.bytes(length as usize)?.to_vec();
    validate_fence_name(&name)?;
    Ok(name)
}

fn validate_fence_name(name: &[u8]) -> PackedResult<()> {
    if name.is_empty()
        || name.len() as u32 > MAX_INDEX_FENCE_NAME_BYTES
        || name.contains(&0)
        || name.contains(&b'/')
        || name == b"."
        || name == b".."
    {
        return Err(PackedWireError::Invalid(
            "packed index fence name is invalid".into(),
        ));
    }
    Ok(())
}

fn validate_name_len(length: u32) -> PackedResult<()> {
    if length > MAX_NAME_RANGE_BYTES {
        return Err(PackedWireError::LimitExceeded(
            "manifest name range exceeds 1 MiB".into(),
        ));
    }
    Ok(())
}

fn validate_name_bytes(bytes: &[u8]) -> PackedResult<()> {
    validate_name_len(
        u32::try_from(bytes.len())
            .map_err(|_| PackedWireError::LimitExceeded("name range exceeds u32".into()))?,
    )?;
    if bytes.contains(&0) || bytes.contains(&b'/') {
        return Err(PackedWireError::Invalid(
            "manifest name range contains NUL or slash".into(),
        ));
    }
    Ok(())
}

pub(crate) fn compare_group_key(
    left_parent: [u8; 32],
    left_name: &[u8],
    right_parent: [u8; 32],
    right_name: &[u8],
) -> std::cmp::Ordering {
    left_parent
        .cmp(&right_parent)
        .then_with(|| left_name.cmp(right_name))
}

fn layout_error(error: LayoutError) -> PackedWireError {
    PackedWireError::Invalid(error.to_string())
}

#[derive(Default)]
pub(crate) struct Writer {
    bytes: Vec<u8>,
}

impl Writer {
    pub(crate) fn bytes(&mut self, bytes: &[u8]) {
        self.bytes.extend_from_slice(bytes);
    }

    pub(crate) fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    pub(crate) fn u16(&mut self, value: u16) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    pub(crate) fn u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    pub(crate) fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    pub(crate) fn i64(&mut self, value: i64) {
        self.u64(value as u64);
    }

    pub(crate) fn finish(self) -> Vec<u8> {
        self.bytes
    }
}

pub(crate) struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    pub(crate) fn take(&mut self, len: usize) -> PackedResult<&'a [u8]> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| PackedWireError::LimitExceeded("manifest offset overflows".into()))?;
        if end > self.bytes.len() {
            return Err(PackedWireError::Truncated {
                what: "packed manifest payload",
                need: end,
                have: self.bytes.len(),
            });
        }
        let result = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(result)
    }

    pub(crate) fn bytes(&mut self, len: usize) -> PackedResult<&'a [u8]> {
        self.take(len)
    }

    pub(crate) fn array<const N: usize>(&mut self) -> PackedResult<[u8; N]> {
        self.take(N)?.try_into().map_err(|_| {
            PackedWireError::Invalid("packed manifest fixed-width field has invalid length".into())
        })
    }

    pub(crate) fn u8(&mut self) -> PackedResult<u8> {
        Ok(self.take(1)?[0])
    }

    pub(crate) fn u16(&mut self) -> PackedResult<u16> {
        Ok(u16::from_le_bytes(self.array()?))
    }

    pub(crate) fn u32(&mut self) -> PackedResult<u32> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    pub(crate) fn u64(&mut self) -> PackedResult<u64> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    pub(crate) fn i64(&mut self) -> PackedResult<i64> {
        Ok(self.u64()? as i64)
    }

    pub(crate) fn skip_zeroes(&mut self, len: usize) -> PackedResult<()> {
        if self.take(len)?.iter().any(|byte| *byte != 0) {
            return Err(PackedWireError::Invalid(
                "packed manifest reserved bytes are non-zero".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest() -> PackedSnapshotManifest {
        PackedSnapshotManifest {
            snapshot_id: [8; 32],
            root_dir_key: [9; 32],
            root_inode: 1,
            layout_profile: AccessProfile::RandomSmallFile,
            size_classes: SizeClassTable::default(),
            groups: vec![PackedGroupRef {
                group_id: 7,
                container_ordinal: 0,
                parent_dir_key: [1; 32],
                first_name: b"a".to_vec(),
                last_name: b"z".to_vec(),
                meta_offset: 128,
                meta_len: 256,
                data_offset: 4096,
                data_len: 4096,
                entry_count: 2,
                file_count: 2,
                frame_count: 2,
                layout_profile: AccessProfile::RandomSmallFile,
                metadata_digest: [2; 32],
                data_digest: [3; 32],
            }],
            containers: vec![PackedContainerRef {
                object_key: b"packed/group-0".to_vec(),
                object_len: 8192,
                object_digest: [4; 32],
            }],
            group_index_pages: Vec::new(),
            inode_index_pages: Vec::new(),
        }
    }

    #[test]
    fn envelope_roundtrip_checks_body_digest() {
        let object =
            PackedEnvelope::build(PackedObjectKind::GroupContainer, b"payload".to_vec()).unwrap();
        let parsed = PackedEnvelope::parse(object.clone()).unwrap();
        assert_eq!(parsed.header.kind, PackedObjectKind::GroupContainer);
        assert_eq!(parsed.body(), b"payload");

        let mut tampered = object;
        tampered[PACKED_HEADER_LEN] ^= 1;
        assert!(matches!(
            PackedEnvelope::parse(tampered),
            Err(PackedWireError::HashMismatch { .. })
        ));
    }

    #[test]
    fn manifest_roundtrip_is_canonical_and_bounded() {
        let encoded = manifest().encode().unwrap();
        let decoded = PackedSnapshotManifest::decode(encoded).unwrap();
        assert_eq!(decoded, manifest());
    }

    #[test]
    fn manifest_rejects_duplicate_group_and_trailing_payload() {
        let mut duplicate = manifest();
        let first_group = duplicate.groups[0].clone();
        duplicate.groups.push(first_group);
        assert!(duplicate.encode().is_err());

        let encoded = manifest().encode().unwrap();
        let mut body = PackedEnvelope::parse(encoded).unwrap().body().to_vec();
        body.push(0);
        let object = PackedEnvelope::build(PackedObjectKind::Manifest, body).unwrap();
        assert!(PackedSnapshotManifest::decode(object).is_err());
    }

    #[test]
    fn manifest_rejects_inline_groups_with_page_roots() {
        let mut invalid = manifest();
        invalid.group_index_pages.push(PackedGroupIndexPageRef {
            object: PackedContainerRef {
                object_key: b"packed/group-index-0".to_vec(),
                object_len: 4096,
                object_digest: [5; 32],
            },
            first_parent_dir_key: [1; 32],
            first_name: b"a".to_vec(),
            last_parent_dir_key: [1; 32],
            last_name: b"z".to_vec(),
        });
        assert!(invalid.encode().is_err());
    }

    #[test]
    fn manifest_requires_root_inode_identity() {
        let mut invalid = manifest();
        invalid.root_inode = 0;
        assert!(invalid.encode().is_err());
    }

    #[test]
    fn manifest_pages_round_trip_and_route_by_fence() {
        let page_ref = |first_name: &[u8], last_name: &[u8]| PackedGroupIndexPageRef {
            object: PackedContainerRef {
                object_key: format!("packed/group-index-{}-{}", first_name[0], last_name[0])
                    .into_bytes(),
                object_len: 4096,
                object_digest: [5; 32],
            },
            first_parent_dir_key: [1; 32],
            first_name: first_name.to_vec(),
            last_parent_dir_key: [1; 32],
            last_name: last_name.to_vec(),
        };
        let inode_ref = |first_inode, last_inode| PackedInodeIndexPageRef {
            object: PackedContainerRef {
                object_key: format!("packed/inode-index-{first_inode}").into_bytes(),
                object_len: 4096,
                object_digest: [6; 32],
            },
            first_inode,
            last_inode,
        };
        let mut value = manifest();
        value.groups.clear();
        value.containers.clear();
        value.group_index_pages = vec![page_ref(b"a", b"m"), page_ref(b"n", b"z")];
        value.inode_index_pages = vec![inode_ref(1, 10), inode_ref(11, 20)];

        let decoded = PackedSnapshotManifest::decode(value.encode().unwrap()).unwrap();
        assert_eq!(decoded, value);
        assert_eq!(decoded.group_index_page_for_name([1; 32], b"a"), Some(0));
        assert_eq!(decoded.group_index_page_for_name([1; 32], b"m"), Some(0));
        assert_eq!(decoded.group_index_page_for_name([1; 32], b"n"), Some(1));
        assert_eq!(decoded.group_index_page_for_name([1; 32], b"0"), None);
        assert_eq!(decoded.inode_index_page_for_inode(1), Some(0));
        assert_eq!(decoded.inode_index_page_for_inode(20), Some(1));
        assert_eq!(decoded.inode_index_page_for_inode(21), None);
    }

    #[test]
    fn manifest_rejects_pre_root_identity_payload() {
        let encoded = manifest().encode().unwrap();
        let parsed = PackedEnvelope::parse(encoded).unwrap();
        let mut body = parsed.body().to_vec();
        body[..4].copy_from_slice(b"PM04");
        let object = PackedEnvelope::build(PackedObjectKind::Manifest, body).unwrap();
        assert!(matches!(
            PackedSnapshotManifest::decode(object),
            Err(PackedWireError::UnsupportedFormat(_))
        ));
    }
}
