use std::sync::Arc;

use sha2::{Digest, Sha256};

use super::layout::{AccessProfile, SizeClass, SizeClassTable};
use super::meta::{
    GROUP_META_HEADER_LEN, GroupMeta, GroupMetaEntry, GroupMetaExtent, INLINE_DATA_FLAG,
    INLINE_FILE_MAX_BYTES, INLINE_GROUP_DATA_BUDGET_BYTES, MAX_GROUP_META_BYTES,
};
use super::wire::{
    MAX_OBJECT_BODY, PackedEnvelope, PackedObjectKind, PackedResult, PackedWireError, Reader,
    Writer,
};

const GROUP_PAYLOAD_MAGIC: &[u8; 4] = b"GC04";
const GROUP_PREFIX_LEN: usize = 24;
const GROUP_RECORD_LEN: usize = 152;
pub(crate) const FRAME_RECORD_LEN: usize = 48;
const MAX_GROUPS: u32 = 65_536;
const MAX_FRAMES: u32 = 1_048_576;
const MAX_FRAME_RAW_BYTES: u64 = 8 * 1024 * 1024;

/// Return the size of the immutable group-container prefix and frame table
/// (measured from the start of the envelope body).  The prefix contains the
/// group and frame counts, while the fixed-width group records precede the
/// frame records.  Callers can use this after a 24-byte bounded prefix read
/// to fetch exactly the descriptor directory without touching metadata or
/// frame payloads.
pub fn frame_directory_body_len(prefix: &[u8]) -> PackedResult<usize> {
    let (group_count, frame_count) = group_container_counts(prefix)?;
    let group_bytes = usize::try_from(group_count)
        .ok()
        .and_then(|count| count.checked_mul(GROUP_RECORD_LEN))
        .ok_or_else(|| {
            PackedWireError::LimitExceeded("group directory length overflows usize".into())
        })?;
    let frame_bytes = usize::try_from(frame_count)
        .ok()
        .and_then(|count| count.checked_mul(FRAME_RECORD_LEN))
        .ok_or_else(|| {
            PackedWireError::LimitExceeded("frame directory length overflows usize".into())
        })?;
    let length = GROUP_PREFIX_LEN
        .checked_add(group_bytes)
        .and_then(|length| length.checked_add(frame_bytes))
        .ok_or_else(|| PackedWireError::LimitExceeded("frame directory length overflows".into()))?;
    if length as u64 > MAX_OBJECT_BODY {
        return Err(PackedWireError::LimitExceeded(
            "frame directory exceeds packed object body budget".into(),
        ));
    }
    Ok(length)
}

/// Return the group/frame counts encoded in a GC04 body prefix.  The caller
/// only needs the first 24 body bytes; no metadata or payload is inspected.
pub fn group_container_counts(prefix: &[u8]) -> PackedResult<(u32, u32)> {
    parse_group_prefix(prefix)
}

/// Return the byte offset of the first frame record, measured from the start
/// of the envelope body.
pub fn frame_table_body_offset(prefix: &[u8]) -> PackedResult<usize> {
    let (group_count, _) = group_container_counts(prefix)?;
    let group_bytes = usize::try_from(group_count)
        .ok()
        .and_then(|count| count.checked_mul(GROUP_RECORD_LEN))
        .ok_or_else(|| {
            PackedWireError::LimitExceeded("group directory length overflows usize".into())
        })?;
    GROUP_PREFIX_LEN
        .checked_add(group_bytes)
        .ok_or_else(|| PackedWireError::LimitExceeded("frame table offset overflows".into()))
}

/// Parse only the GC04 body prefix, group records and frame table.
///
/// This intentionally does not inspect frame payload bytes, group metadata or
/// frame-list records.  It is the bounded-range counterpart to
/// [`PackedGroupContainer::open`]: each descriptor is still checked against
/// the object bounds and canonical ordinal/codec/length rules, so callers can
/// safely build a read plan before issuing a frame range request.
pub fn parse_frame_directory(
    directory: &[u8],
    object_len: u64,
) -> PackedResult<Vec<PackedFrameDescriptor>> {
    let required_len = frame_directory_body_len(directory)?;
    if directory.len() < required_len {
        return Err(PackedWireError::Truncated {
            what: "packed group frame directory",
            need: required_len,
            have: directory.len(),
        });
    }
    let body_len = object_len
        .checked_sub((super::wire::PACKED_HEADER_LEN + super::wire::PACKED_FOOTER_LEN) as u64)
        .ok_or_else(|| {
            PackedWireError::Invalid("object is shorter than header and footer".into())
        })?;
    if required_len as u64 > body_len {
        return Err(PackedWireError::Truncated {
            what: "packed group frame directory",
            need: required_len,
            have: usize::try_from(body_len).unwrap_or(usize::MAX),
        });
    }
    let (_, frame_count) = parse_group_prefix(directory)?;
    let table_offset = frame_table_body_offset(directory)?;
    let table_len = usize::try_from(frame_count)
        .ok()
        .and_then(|count| count.checked_mul(FRAME_RECORD_LEN))
        .ok_or_else(|| {
            PackedWireError::LimitExceeded("frame directory length overflows usize".into())
        })?;
    let table_end = table_offset
        .checked_add(table_len)
        .ok_or_else(|| PackedWireError::LimitExceeded("frame directory range overflows".into()))?;
    parse_frame_descriptor_range(
        directory
            .get(table_offset..table_end)
            .ok_or(PackedWireError::Truncated {
                what: "packed group frame directory",
                need: table_end,
                have: directory.len(),
            })?,
        object_len,
        0,
    )
}

/// Parse a contiguous slice of frame records beginning at `first_ordinal`.
/// The input must contain only complete 48-byte records.  This is used by the
/// remote reader to page a large frame table in bounded object-store ranges.
pub fn parse_frame_descriptor_range(
    records: &[u8],
    object_len: u64,
    first_ordinal: u32,
) -> PackedResult<Vec<PackedFrameDescriptor>> {
    if !records.len().is_multiple_of(FRAME_RECORD_LEN) {
        return Err(PackedWireError::Truncated {
            what: "packed group frame descriptor range",
            need: records.len() + (FRAME_RECORD_LEN - records.len() % FRAME_RECORD_LEN),
            have: records.len(),
        });
    }
    let count = u32::try_from(records.len() / FRAME_RECORD_LEN)
        .map_err(|_| PackedWireError::LimitExceeded("frame descriptor count exceeds u32".into()))?;
    let end_ordinal = first_ordinal
        .checked_add(count)
        .ok_or_else(|| PackedWireError::LimitExceeded("frame ordinal range overflows".into()))?;
    if end_ordinal > MAX_FRAMES {
        return Err(PackedWireError::LimitExceeded(
            "frame descriptor ordinal exceeds limit".into(),
        ));
    }
    let mut reader = Reader::new(records);
    let mut frames = Vec::with_capacity(count as usize);
    let mut previous_end = None;
    for ordinal in first_ordinal..end_ordinal {
        let frame_ordinal = reader.u32()?;
        if frame_ordinal != ordinal {
            return Err(PackedWireError::Invalid(
                "frame ordinals are not canonical".into(),
            ));
        }
        let size_class = SizeClass::from_u8(reader.u8()?)
            .map_err(|error| PackedWireError::Invalid(error.to_string()))?;
        let codec = reader.u8()?;
        reader.skip_zeroes(2)?;
        let object_offset = reader.u64()?;
        let stored_len = reader.u32()?;
        let raw_len = reader.u32()?;
        let first_file_slot = reader.u32()?;
        let last_file_slot = reader.u32()?;
        let frame_digest = reader.array::<16>()?;
        if codec != 0
            || stored_len == 0
            || raw_len == 0
            || stored_len != raw_len
            || u64::from(raw_len) > MAX_FRAME_RAW_BYTES
        {
            return Err(PackedWireError::UnsupportedFormat(
                "group container frame codec or length is unsupported".into(),
            ));
        }
        if first_file_slot > last_file_slot {
            return Err(PackedWireError::Invalid(
                "frame file slot range is inverted".into(),
            ));
        }
        let end = object_offset
            .checked_add(u64::from(stored_len))
            .ok_or_else(|| PackedWireError::LimitExceeded("frame range overflows".into()))?;
        let footer_start = object_len
            .checked_sub(super::wire::PACKED_FOOTER_LEN as u64)
            .ok_or_else(|| PackedWireError::Invalid("object is shorter than footer".into()))?;
        if object_offset < super::wire::PACKED_HEADER_LEN as u64 || end > footer_start {
            return Err(PackedWireError::Invalid(
                "frame payload range is outside object body".into(),
            ));
        }
        if let Some(previous_end) = previous_end
            && object_offset < previous_end
        {
            return Err(PackedWireError::Invalid(
                "frame payload ranges are not canonical or overlap".into(),
            ));
        }
        previous_end = Some(end);
        frames.push(PackedFrameDescriptor {
            frame_ordinal,
            object_offset,
            stored_len,
            raw_len,
            first_file_slot,
            last_file_slot,
            size_class,
            codec,
            frame_digest,
        });
    }
    Ok(frames)
}

fn parse_group_prefix(prefix: &[u8]) -> PackedResult<(u32, u32)> {
    if prefix.len() < GROUP_PREFIX_LEN {
        return Err(PackedWireError::Truncated {
            what: "packed group container prefix",
            need: GROUP_PREFIX_LEN,
            have: prefix.len(),
        });
    }
    let mut reader = Reader::new(prefix);
    if reader.take(4)? != GROUP_PAYLOAD_MAGIC {
        return Err(PackedWireError::UnsupportedFormat(
            "group container payload version mismatch".into(),
        ));
    }
    reader.u64()?;
    AccessProfile::from_u8(reader.u8()?)
        .map_err(|error| PackedWireError::Invalid(error.to_string()))?;
    reader.skip_zeroes(3)?;
    let group_count = reader.u32()?;
    let frame_count = reader.u32()?;
    if group_count > MAX_GROUPS || frame_count > MAX_FRAMES {
        return Err(PackedWireError::LimitExceeded(
            "group container count exceeds limit".into(),
        ));
    }
    Ok((group_count, frame_count))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackedGroupInput {
    pub group_id: u64,
    pub parent_dir_key: [u8; 32],
    pub metadata: Vec<u8>,
    pub frame_ordinals: Vec<u32>,
    pub entry_count: u32,
    pub file_count: u32,
    pub layout_profile: AccessProfile,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackedFrameInput {
    pub raw: Vec<u8>,
    pub size_class: SizeClass,
    pub codec: u8,
    pub first_file_slot: u32,
    pub last_file_slot: u32,
}

/// One source file supplied to the immutable group packer.  `data` is the
/// file's logical contents; an empty buffer represents a metadata-only inode
/// or a fully sparse file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackedFileInput {
    pub name: Vec<u8>,
    pub inode: u64,
    pub kind: u8,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub rdev: u64,
    pub nlink: u32,
    pub atime_ns: i64,
    pub mtime_ns: i64,
    pub ctime_ns: i64,
    pub flags: u8,
    pub data: Vec<u8>,
}

/// Hard and soft limits used when a directory is split into immutable groups.
///
/// `target_logical_bytes` is a packing preference. `max_logical_bytes`,
/// `max_entries` and `max_metadata_bytes` are hard limits for one group. The
/// metadata limit is estimated conservatively before encoding, so a builder
/// never has to discover an over-sized GroupMeta after consuming a directory
/// shard.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GroupPackingLimits {
    pub target_logical_bytes: usize,
    pub max_logical_bytes: usize,
    pub max_entries: usize,
    pub max_metadata_bytes: usize,
}

impl Default for GroupPackingLimits {
    fn default() -> Self {
        Self {
            target_logical_bytes: 8 * 1024 * 1024,
            max_logical_bytes: 16 * 1024 * 1024,
            max_entries: 256,
            max_metadata_bytes: MAX_GROUP_META_BYTES,
        }
    }
}

impl GroupPackingLimits {
    /// Return a packing profile tuned for the expected access shape.
    ///
    /// The default remains conservative for callers that do not know the
    /// workload.  Immutable training-style snapshots usually scan adjacent
    /// files, so the random-small-file profile can use a larger logical
    /// window while the encoded GroupMeta limit still bounds metadata reads.
    pub fn for_profile(profile: AccessProfile) -> Self {
        match profile {
            AccessProfile::RandomSmallFile => Self {
                target_logical_bytes: 16 * 1024 * 1024,
                max_logical_bytes: 32 * 1024 * 1024,
                max_entries: 512,
                ..Self::default()
            },
            AccessProfile::SequentialSmallFile => Self {
                target_logical_bytes: 32 * 1024 * 1024,
                max_logical_bytes: 48 * 1024 * 1024,
                max_entries: 1024,
                ..Self::default()
            },
            AccessProfile::Mixed => Self::default(),
        }
    }

    fn validate(self) -> PackedResult<Self> {
        if self.target_logical_bytes == 0
            || self.max_logical_bytes < self.target_logical_bytes
            || self.max_entries == 0
            || self.max_metadata_bytes < GROUP_META_HEADER_LEN
            || self.max_metadata_bytes > MAX_GROUP_META_BYTES
        {
            return Err(PackedWireError::Invalid(
                "invalid packed group packing limits".into(),
            ));
        }
        Ok(self)
    }
}

/// Limits for immutable container bin-packing. The body limit excludes the
/// fixed envelope header/footer and may not exceed the wire hard maximum.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ContainerPackingLimits {
    pub target_body_bytes: usize,
    pub max_body_bytes: usize,
    pub max_groups: usize,
}

impl Default for ContainerPackingLimits {
    fn default() -> Self {
        Self {
            target_body_bytes: 16 * 1024 * 1024,
            max_body_bytes: MAX_OBJECT_BODY as usize,
            max_groups: 65_536,
        }
    }
}

impl ContainerPackingLimits {
    /// Match container windows to the same access profile as group packing.
    /// The wire hard limit remains 64 MiB for every profile.
    pub fn for_profile(profile: AccessProfile) -> Self {
        match profile {
            AccessProfile::RandomSmallFile => Self {
                target_body_bytes: 32 * 1024 * 1024,
                ..Self::default()
            },
            AccessProfile::SequentialSmallFile => Self {
                target_body_bytes: 48 * 1024 * 1024,
                ..Self::default()
            },
            AccessProfile::Mixed => Self::default(),
        }
    }

    fn validate(self) -> PackedResult<Self> {
        if self.target_body_bytes == 0
            || self.target_body_bytes > self.max_body_bytes
            || self.max_body_bytes > MAX_OBJECT_BODY as usize
            || self.max_groups == 0
        {
            return Err(PackedWireError::Invalid(
                "invalid packed container packing limits".into(),
            ));
        }
        Ok(self)
    }
}

/// Input for one immutable group container after group-level sharding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackedContainerInput {
    pub container_id: u64,
    pub groups: Vec<PackedGroupInput>,
    pub frames: Vec<PackedFrameInput>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackedFrameDescriptor {
    pub frame_ordinal: u32,
    pub object_offset: u64,
    pub stored_len: u32,
    pub raw_len: u32,
    pub first_file_slot: u32,
    pub last_file_slot: u32,
    pub size_class: SizeClass,
    pub codec: u8,
    pub frame_digest: [u8; 16],
}

/// Split one directory into sorted, independently addressable groups and pack
/// each group into dynamic frames. Group ids are assigned consecutively from
/// `group_id_base`; callers can publish each returned group in a separate
/// container or co-pack several groups after rebasing frame ordinals.
pub fn pack_group_file_shards(
    group_id_base: u64,
    parent_dir_key: [u8; 32],
    mut files: Vec<PackedFileInput>,
    profile: AccessProfile,
    size_classes: SizeClassTable,
    p90_requested_range: Option<u64>,
    limits: GroupPackingLimits,
) -> PackedResult<Vec<(PackedGroupInput, Vec<PackedFrameInput>)>> {
    let limits = limits.validate()?;
    files.sort_by(|left, right| left.name.cmp(&right.name));

    let mut shards = Vec::new();
    let mut current = Vec::new();
    let mut current_logical_bytes = 0usize;
    let mut shard_ordinal = 0u64;

    for file in files {
        let file_bytes = file.data.len();
        let should_flush = !current.is_empty()
            && (current.len() >= limits.max_entries
                || current_logical_bytes >= limits.target_logical_bytes
                || current_logical_bytes.saturating_add(file_bytes) > limits.max_logical_bytes
                || !metadata_budget_allows(
                    &current,
                    &file,
                    profile,
                    size_classes,
                    super::wire005::V3BuildPolicy::default(),
                    limits.max_metadata_bytes,
                ));
        if should_flush {
            let group_id = group_id_base.checked_add(shard_ordinal).ok_or_else(|| {
                PackedWireError::LimitExceeded("packed group id exceeds u64".into())
            })?;
            let packed = pack_group_files(
                group_id,
                parent_dir_key,
                std::mem::take(&mut current),
                profile,
                size_classes,
                p90_requested_range,
            )?;
            if packed.0.metadata.len() > limits.max_metadata_bytes {
                return Err(PackedWireError::LimitExceeded(
                    "packed group metadata exceeds configured limit".into(),
                ));
            }
            shards.push(packed);
            shard_ordinal = shard_ordinal.saturating_add(1);
            current_logical_bytes = 0;
        }

        current_logical_bytes = current_logical_bytes.saturating_add(file_bytes);
        current.push(file);
    }

    if !current.is_empty() {
        let group_id = group_id_base
            .checked_add(shard_ordinal)
            .ok_or_else(|| PackedWireError::LimitExceeded("packed group id exceeds u64".into()))?;
        let packed = pack_group_files(
            group_id,
            parent_dir_key,
            current,
            profile,
            size_classes,
            p90_requested_range,
        )?;
        if packed.0.metadata.len() > limits.max_metadata_bytes {
            return Err(PackedWireError::LimitExceeded(
                "packed group metadata exceeds configured limit".into(),
            ));
        }
        shards.push(packed);
    }
    Ok(shards)
}

/// Bin-pack already-sharded groups into bounded containers. Group metadata
/// carries frame ordinals, so ordinals are rebased while groups are appended
/// to a container. No file data is copied during the size decision.
pub fn pack_group_shard_containers(
    container_id_base: u64,
    shards: Vec<(PackedGroupInput, Vec<PackedFrameInput>)>,
    limits: ContainerPackingLimits,
) -> PackedResult<Vec<PackedContainerInput>> {
    let limits = limits.validate()?;
    let mut result = Vec::new();
    let mut groups = Vec::new();
    let mut frames = Vec::new();

    for (group, shard_frames) in shards {
        let candidate_body = container_body_len(&groups, &frames)?
            .checked_add(container_group_delta(&group, &shard_frames)?)
            .ok_or_else(|| {
                PackedWireError::LimitExceeded("packed container size overflows".into())
            })?;
        let must_flush = !groups.is_empty()
            && (groups.len() >= limits.max_groups || candidate_body > limits.target_body_bytes);
        if must_flush {
            push_container(&mut result, container_id_base, &mut groups, &mut frames)?;
        }

        let frame_offset = u32::try_from(frames.len()).map_err(|_| {
            PackedWireError::LimitExceeded("packed container frame ordinal exceeds u32".into())
        })?;
        let rebased = rebase_group_frames(group, frame_offset)?;
        let single_body = container_body_len(std::slice::from_ref(&rebased), &shard_frames)?;
        if single_body > limits.max_body_bytes {
            return Err(PackedWireError::LimitExceeded(
                "one packed group cannot fit in a container".into(),
            ));
        }
        groups.push(rebased);
        frames.extend(shard_frames);
    }

    if !groups.is_empty() {
        push_container(&mut result, container_id_base, &mut groups, &mut frames)?;
    }
    Ok(result)
}

fn rebase_group_frames(
    mut group: PackedGroupInput,
    frame_offset: u32,
) -> PackedResult<PackedGroupInput> {
    if frame_offset == 0 {
        return Ok(group);
    }
    let mut metadata = GroupMeta::decode(&group.metadata)?;
    for entry in metadata.entries_mut() {
        for extent in &mut entry.extents {
            extent.frame_ordinal =
                extent
                    .frame_ordinal
                    .checked_add(frame_offset)
                    .ok_or_else(|| {
                        PackedWireError::LimitExceeded("packed frame ordinal exceeds u32".into())
                    })?;
        }
    }
    group.metadata = metadata.encode()?;
    for ordinal in &mut group.frame_ordinals {
        *ordinal = ordinal.checked_add(frame_offset).ok_or_else(|| {
            PackedWireError::LimitExceeded("packed frame ordinal exceeds u32".into())
        })?;
    }
    Ok(group)
}

fn container_body_len(
    groups: &[PackedGroupInput],
    frames: &[PackedFrameInput],
) -> PackedResult<usize> {
    let frame_lists_len = groups
        .iter()
        .map(|group| group.frame_ordinals.len().saturating_mul(4))
        .sum::<usize>();
    let metadata_len = groups
        .iter()
        .map(|group| group.metadata.len())
        .fold(0usize, usize::saturating_add);
    let payload_len = frames
        .iter()
        .map(|frame| frame.raw.len())
        .fold(0usize, usize::saturating_add);
    GROUP_PREFIX_LEN
        .checked_add(groups.len().saturating_mul(GROUP_RECORD_LEN))
        .and_then(|value| value.checked_add(frames.len().saturating_mul(FRAME_RECORD_LEN)))
        .and_then(|value| value.checked_add(frame_lists_len))
        .and_then(|value| value.checked_add(metadata_len))
        .and_then(|value| value.checked_add(payload_len))
        .ok_or_else(|| PackedWireError::LimitExceeded("packed container size overflows".into()))
}

fn container_group_delta(
    group: &PackedGroupInput,
    frames: &[PackedFrameInput],
) -> PackedResult<usize> {
    GROUP_RECORD_LEN
        .checked_add(frames.len().saturating_mul(FRAME_RECORD_LEN))
        .and_then(|value| value.checked_add(group.frame_ordinals.len().saturating_mul(4)))
        .and_then(|value| value.checked_add(group.metadata.len()))
        .and_then(|value| {
            value.checked_add(
                frames
                    .iter()
                    .map(|frame| frame.raw.len())
                    .fold(0usize, usize::saturating_add),
            )
        })
        .ok_or_else(|| PackedWireError::LimitExceeded("packed container size overflows".into()))
}

fn push_container(
    result: &mut Vec<PackedContainerInput>,
    container_id_base: u64,
    groups: &mut Vec<PackedGroupInput>,
    frames: &mut Vec<PackedFrameInput>,
) -> PackedResult<()> {
    let container_id = container_id_base
        .checked_add(result.len() as u64)
        .ok_or_else(|| PackedWireError::LimitExceeded("packed container id exceeds u64".into()))?;
    result.push(PackedContainerInput {
        container_id,
        groups: std::mem::take(groups),
        frames: std::mem::take(frames),
    });
    Ok(())
}

fn metadata_budget_allows(
    current: &[PackedFileInput],
    next: &PackedFileInput,
    profile: AccessProfile,
    size_classes: SizeClassTable,
    policy: super::wire005::V3BuildPolicy,
    max_metadata_bytes: usize,
) -> bool {
    let fixed_bytes = GROUP_META_HEADER_LEN.saturating_add(
        current
            .iter()
            .chain(std::iter::once(next))
            .map(|file| 96usize.saturating_add(file.name.len()))
            .sum::<usize>(),
    );
    if fixed_bytes > max_metadata_bytes {
        return false;
    }

    // Mirror the packer's deterministic inline admission rule. The fixed
    // record estimate is intentionally larger than the actual GM06 record,
    // so this check errs on the side of an earlier shard boundary.
    let inline_budget =
        INLINE_GROUP_DATA_BUDGET_BYTES.min(max_metadata_bytes.saturating_sub(fixed_bytes));
    let mut inline_bytes = 0usize;
    let mut extent_bytes = 0usize;
    for file in current.iter().chain(std::iter::once(next)) {
        let size = file.data.len();
        let inline = policy.inline_data
            && size != 0
            && file.kind == 1
            && size < INLINE_FILE_MAX_BYTES
            && inline_bytes.saturating_add(size) <= inline_budget;
        if inline {
            inline_bytes = inline_bytes.saturating_add(size);
        } else if size != 0 {
            let frame_bytes = policy
                .select(size as u64, profile, size_classes)
                .map(|decision| decision.frame_raw_bytes.max(1) as usize)
                .unwrap_or(1);
            extent_bytes =
                extent_bytes.saturating_add(size.div_ceil(frame_bytes).saturating_mul(24));
        }
    }
    fixed_bytes
        .saturating_add(inline_bytes)
        .saturating_add(extent_bytes)
        <= max_metadata_bytes
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackedGroupDescriptor {
    pub group_id: u64,
    pub parent_dir_key: [u8; 32],
    pub metadata_offset: u64,
    pub metadata_len: u32,
    pub data_offset: u64,
    pub data_len: u32,
    pub entry_count: u32,
    pub file_count: u32,
    pub frame_ordinals: Vec<u32>,
    pub layout_profile: AccessProfile,
    pub metadata_digest: [u8; 32],
    pub data_digest: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackedGroupContainer {
    container_id: u64,
    layout_profile: AccessProfile,
    groups: Vec<PackedGroupDescriptor>,
    frames: Vec<PackedFrameDescriptor>,
    body: Vec<u8>,
    object_len: u64,
    object_digest: [u8; 32],
}

struct FrameBuilder {
    raw: Vec<u8>,
    target_len: usize,
    size_class: SizeClass,
    first_file_slot: u32,
    last_file_slot: u32,
}

/// Pack sorted directory entries and their independent contents into dynamic
/// frames.  Tiny files share a frame up to the published minimum, while a
/// large file gets multiple extents when it exceeds the profile's frame cap.
pub fn pack_group_files(
    group_id: u64,
    parent_dir_key: [u8; 32],
    files: Vec<PackedFileInput>,
    profile: AccessProfile,
    size_classes: SizeClassTable,
    p90_requested_range: Option<u64>,
) -> PackedResult<(PackedGroupInput, Vec<PackedFrameInput>)> {
    pack_group_files_policy_inner(
        group_id,
        parent_dir_key,
        files,
        profile,
        size_classes,
        p90_requested_range,
        super::wire005::V3BuildPolicy::default(),
    )
}

/// Build deterministic v3 frames with explicit inline admission.
pub fn pack_group_files_with_policy(
    group_id: u64,
    parent_dir_key: [u8; 32],
    files: Vec<PackedFileInput>,
    profile: AccessProfile,
    size_classes: SizeClassTable,
    policy: super::wire005::V3BuildPolicy,
) -> PackedResult<(PackedGroupInput, Vec<PackedFrameInput>)> {
    pack_group_files_policy_inner(
        group_id,
        parent_dir_key,
        files,
        profile,
        size_classes,
        None,
        policy,
    )
}

fn pack_group_files_policy_inner(
    group_id: u64,
    parent_dir_key: [u8; 32],
    mut files: Vec<PackedFileInput>,
    profile: AccessProfile,
    size_classes: SizeClassTable,
    p90_requested_range: Option<u64>,
    policy: super::wire005::V3BuildPolicy,
) -> PackedResult<(PackedGroupInput, Vec<PackedFrameInput>)> {
    if p90_requested_range.is_some() {
        return Err(PackedWireError::UnsupportedFormat(
            "packed p90 policy requires authenticated histogram provenance".into(),
        ));
    }
    files.sort_by(|left, right| left.name.cmp(&right.name));
    let mut entries = Vec::with_capacity(files.len());
    let mut frames = Vec::new();
    let mut frame_ordinals = Vec::new();
    let mut current: Option<FrameBuilder> = None;
    let mut inline_bytes = 0usize;
    // Reserve the fixed record and name bytes before admitting inline data;
    // otherwise a directory with many 1 KiB files could exceed the 256 KiB
    // GroupMeta bound even though its inline payload stayed under budget.
    let fixed_metadata_bytes = 12usize.saturating_add(
        files
            .iter()
            .map(|file| 96usize.saturating_add(file.name.len()))
            .sum::<usize>(),
    );
    let inline_budget = INLINE_GROUP_DATA_BUDGET_BYTES
        .min(MAX_GROUP_META_BYTES.saturating_sub(fixed_metadata_bytes));
    let entry_count = files.len();
    let file_count = files.iter().filter(|file| file.kind == 1).count();

    // Consume the inputs after calculating the metadata budget. Inline data
    // can then move directly into GroupMeta instead of being cloned.
    for (file_slot, file) in files.into_iter().enumerate() {
        let file_slot = u32::try_from(file_slot)
            .map_err(|_| PackedWireError::LimitExceeded("file slot exceeds u32".into()))?;
        let data = file.data;
        let size = u64::try_from(data.len())
            .map_err(|_| PackedWireError::LimitExceeded("file size exceeds u64".into()))?;
        let decision = policy.select(size, profile, size_classes)?;
        let mut drafts = Vec::new();

        let inline = policy.inline_data
            && size != 0
            && file.kind == 1
            && data.len() < INLINE_FILE_MAX_BYTES
            && inline_bytes.saturating_add(data.len()) <= inline_budget;
        if inline {
            inline_bytes = inline_bytes.saturating_add(data.len());
        } else if size != 0 {
            let target_len = usize::try_from(decision.frame_raw_bytes).map_err(|_| {
                PackedWireError::LimitExceeded("dynamic frame size exceeds usize".into())
            })?;
            if decision.frame_count == 1 {
                let can_append = current.as_ref().is_some_and(|frame| {
                    frame.size_class == decision.size_class
                        && frame.target_len == target_len
                        && frame.raw.len().saturating_add(data.len()) <= target_len
                });
                if !can_append {
                    flush_frame(&mut current, &mut frames, &mut frame_ordinals)?;
                    current = Some(FrameBuilder {
                        raw: Vec::with_capacity(target_len),
                        target_len,
                        size_class: decision.size_class,
                        first_file_slot: file_slot,
                        last_file_slot: file_slot,
                    });
                }
                let frame = current.as_mut().expect("single-file frame was created");
                let raw_offset = u32::try_from(frame.raw.len()).map_err(|_| {
                    PackedWireError::LimitExceeded("frame raw offset exceeds u32".into())
                })?;
                frame.raw.extend_from_slice(&data);
                frame.last_file_slot = file_slot;
                drafts.push(GroupMetaExtent {
                    file_offset: 0,
                    logical_len: u32::try_from(data.len()).map_err(|_| {
                        PackedWireError::LimitExceeded("file extent length exceeds u32".into())
                    })?,
                    frame_ordinal: u32::try_from(frames.len()).map_err(|_| {
                        PackedWireError::LimitExceeded("frame ordinal exceeds u32".into())
                    })?,
                    raw_offset,
                    raw_len: 0,
                });
            } else {
                flush_frame(&mut current, &mut frames, &mut frame_ordinals)?;
                let mut file_offset = 0u64;
                for chunk in data.chunks(target_len) {
                    let frame_ordinal = frames.len();
                    let logical_len = u32::try_from(chunk.len()).map_err(|_| {
                        PackedWireError::LimitExceeded("file extent length exceeds u32".into())
                    })?;
                    frames.push(PackedFrameInput {
                        raw: chunk.to_vec(),
                        size_class: decision.size_class,
                        codec: 0,
                        first_file_slot: file_slot,
                        last_file_slot: file_slot,
                    });
                    let frame_ordinal = u32::try_from(frame_ordinal).map_err(|_| {
                        PackedWireError::LimitExceeded("frame ordinal exceeds u32".into())
                    })?;
                    frame_ordinals.push(frame_ordinal);
                    drafts.push(GroupMetaExtent {
                        file_offset,
                        logical_len,
                        frame_ordinal,
                        raw_offset: 0,
                        raw_len: 0,
                    });
                    file_offset = file_offset.saturating_add(u64::from(logical_len));
                }
            }
        }

        entries.push(GroupMetaEntry {
            name: file.name,
            inode: file.inode,
            kind: file.kind,
            mode: file.mode,
            uid: file.uid,
            gid: file.gid,
            rdev: file.rdev,
            nlink: file.nlink,
            atime_ns: file.atime_ns,
            mtime_ns: file.mtime_ns,
            ctime_ns: file.ctime_ns,
            size,
            flags: file.flags | if inline { INLINE_DATA_FLAG } else { 0 },
            inline_data: if inline {
                Arc::from(data)
            } else {
                Arc::from([])
            },
            extents: drafts,
        });
    }
    flush_frame(&mut current, &mut frames, &mut frame_ordinals)?;
    for entry in &mut entries {
        for extent in &mut entry.extents {
            let frame = frames.get(extent.frame_ordinal as usize).ok_or_else(|| {
                PackedWireError::Invalid("packed group extent references a missing frame".into())
            })?;
            extent.raw_len = u32::try_from(frame.raw.len()).map_err(|_| {
                PackedWireError::LimitExceeded("frame raw length exceeds u32".into())
            })?;
        }
    }
    let metadata = GroupMeta::new(entries)?.encode()?;
    Ok((
        PackedGroupInput {
            group_id,
            parent_dir_key,
            metadata,
            frame_ordinals,
            entry_count: u32::try_from(entry_count).map_err(|_| {
                PackedWireError::LimitExceeded("group entry count exceeds u32".into())
            })?,
            file_count: u32::try_from(file_count).map_err(|_| {
                PackedWireError::LimitExceeded("group file count exceeds u32".into())
            })?,
            layout_profile: profile,
        },
        frames,
    ))
}

fn flush_frame(
    current: &mut Option<FrameBuilder>,
    frames: &mut Vec<PackedFrameInput>,
    frame_ordinals: &mut Vec<u32>,
) -> PackedResult<()> {
    let Some(frame) = current.take() else {
        return Ok(());
    };
    let ordinal = u32::try_from(frames.len())
        .map_err(|_| PackedWireError::LimitExceeded("frame ordinal exceeds u32".into()))?;
    frames.push(PackedFrameInput {
        raw: frame.raw,
        size_class: frame.size_class,
        codec: 0,
        first_file_slot: frame.first_file_slot,
        last_file_slot: frame.last_file_slot,
    });
    frame_ordinals.push(ordinal);
    Ok(())
}

impl PackedGroupContainer {
    pub fn build(
        container_id: u64,
        layout_profile: AccessProfile,
        groups: Vec<PackedGroupInput>,
        frames: Vec<PackedFrameInput>,
    ) -> PackedResult<Vec<u8>> {
        validate_inputs(&groups, &frames)?;

        let group_count = groups.len();
        let frame_count = frames.len();
        let frame_lists_len = groups
            .iter()
            .map(|group| group.frame_ordinals.len().saturating_mul(4))
            .sum::<usize>();
        let metadata_len = groups
            .iter()
            .map(|group| group.metadata.len())
            .sum::<usize>();
        let payload_len = frames.iter().map(|frame| frame.raw.len()).sum::<usize>();
        let tables_len = GROUP_PREFIX_LEN
            .checked_add(group_count.saturating_mul(GROUP_RECORD_LEN))
            .and_then(|value| value.checked_add(frame_count.saturating_mul(FRAME_RECORD_LEN)))
            .and_then(|value| value.checked_add(frame_lists_len))
            .and_then(|value| value.checked_add(metadata_len))
            .and_then(|value| value.checked_add(payload_len))
            .ok_or_else(|| {
                PackedWireError::LimitExceeded("group container size overflows".into())
            })?;
        if tables_len as u64 > MAX_OBJECT_BODY {
            return Err(PackedWireError::LimitExceeded(
                "group container body exceeds 64 MiB".into(),
            ));
        }

        let frame_list_start = GROUP_PREFIX_LEN
            .checked_add(group_count * GROUP_RECORD_LEN)
            .and_then(|value| value.checked_add(frame_count * FRAME_RECORD_LEN))
            .ok_or_else(|| PackedWireError::LimitExceeded("group list offset overflows".into()))?;
        let metadata_start = frame_list_start
            .checked_add(frame_lists_len)
            .ok_or_else(|| PackedWireError::LimitExceeded("metadata offset overflows".into()))?;
        let payload_start = metadata_start
            .checked_add(metadata_len)
            .ok_or_else(|| PackedWireError::LimitExceeded("payload offset overflows".into()))?;

        let mut frame_descriptors = Vec::with_capacity(frames.len());
        let mut frame_cursor = payload_start;
        for (ordinal, frame) in frames.iter().enumerate() {
            let frame_ordinal = u32::try_from(ordinal)
                .map_err(|_| PackedWireError::LimitExceeded("frame ordinal exceeds u32".into()))?;
            let object_offset = (super::wire::PACKED_HEADER_LEN as u64)
                .checked_add(frame_cursor as u64)
                .ok_or_else(|| PackedWireError::LimitExceeded("frame offset overflows".into()))?;
            let raw_len = u32::try_from(frame.raw.len()).map_err(|_| {
                PackedWireError::LimitExceeded("frame raw length exceeds u32".into())
            })?;
            let frame_digest: [u8; 16] = Sha256::digest(&frame.raw)[..16]
                .try_into()
                .expect("sha256 prefix has 16 bytes");
            frame_descriptors.push(PackedFrameDescriptor {
                frame_ordinal,
                object_offset,
                stored_len: raw_len,
                raw_len,
                first_file_slot: frame.first_file_slot,
                last_file_slot: frame.last_file_slot,
                size_class: frame.size_class,
                codec: frame.codec,
                frame_digest,
            });
            frame_cursor = frame_cursor.checked_add(frame.raw.len()).ok_or_else(|| {
                PackedWireError::LimitExceeded("frame payload offset overflows".into())
            })?;
        }

        let mut list_cursor = frame_list_start;
        let mut metadata_cursor = metadata_start;
        let mut group_descriptors = Vec::with_capacity(groups.len());
        for group in &groups {
            let frame_list_offset = (super::wire::PACKED_HEADER_LEN as u64)
                .checked_add(list_cursor as u64)
                .ok_or_else(|| {
                    PackedWireError::LimitExceeded("frame list offset overflows".into())
                })?;
            let metadata_offset = (super::wire::PACKED_HEADER_LEN as u64)
                .checked_add(metadata_cursor as u64)
                .ok_or_else(|| {
                    PackedWireError::LimitExceeded("metadata offset overflows".into())
                })?;
            let metadata_len_u32 = u32::try_from(group.metadata.len())
                .map_err(|_| PackedWireError::LimitExceeded("group metadata exceeds u32".into()))?;
            let metadata_digest: [u8; 32] = Sha256::digest(&group.metadata).into();
            let mut data_hasher = Sha256::new();
            let mut data_offset = 0u64;
            let mut data_end = 0u64;
            for (index, ordinal) in group.frame_ordinals.iter().copied().enumerate() {
                let descriptor = &frame_descriptors[ordinal as usize];
                if index == 0 {
                    data_offset = descriptor.object_offset;
                }
                let end = descriptor
                    .object_offset
                    .checked_add(u64::from(descriptor.stored_len))
                    .ok_or_else(|| {
                        PackedWireError::LimitExceeded("group data end overflows".into())
                    })?;
                data_end = data_end.max(end);
                data_hasher.update(&frames[ordinal as usize].raw);
            }
            let data_len = u32::try_from(data_end.saturating_sub(data_offset)).map_err(|_| {
                PackedWireError::LimitExceeded("group data span exceeds u32".into())
            })?;
            group_descriptors.push((
                PackedGroupDescriptor {
                    group_id: group.group_id,
                    parent_dir_key: group.parent_dir_key,
                    metadata_offset,
                    metadata_len: metadata_len_u32,
                    data_offset,
                    data_len,
                    entry_count: group.entry_count,
                    file_count: group.file_count,
                    frame_ordinals: group.frame_ordinals.clone(),
                    layout_profile: group.layout_profile,
                    metadata_digest,
                    data_digest: data_hasher.finalize().into(),
                },
                frame_list_offset,
            ));
            list_cursor = list_cursor
                .checked_add(group.frame_ordinals.len() * 4)
                .ok_or_else(|| {
                    PackedWireError::LimitExceeded("frame list cursor overflows".into())
                })?;
            metadata_cursor = metadata_cursor
                .checked_add(group.metadata.len())
                .ok_or_else(|| {
                    PackedWireError::LimitExceeded("metadata cursor overflows".into())
                })?;
        }

        let mut writer = Writer::default();
        writer.bytes(GROUP_PAYLOAD_MAGIC);
        writer.u64(container_id);
        writer.u8(layout_profile as u8);
        writer.bytes(&[0, 0, 0]);
        writer.u32(u32::try_from(group_count).unwrap());
        writer.u32(u32::try_from(frame_count).unwrap());
        for ((group, frame_list_offset), _input) in group_descriptors.iter().zip(&groups) {
            writer.u64(group.group_id);
            writer.bytes(&group.parent_dir_key);
            writer.u64(group.metadata_offset);
            writer.u32(group.metadata_len);
            writer.u64(group.data_offset);
            writer.u32(group.data_len);
            writer.u32(group.entry_count);
            writer.u32(group.file_count);
            writer.u64(*frame_list_offset);
            writer.u32(group.frame_ordinals.len() as u32);
            writer.u8(group.layout_profile as u8);
            writer.bytes(&[0, 0, 0]);
            writer.bytes(&group.metadata_digest);
            writer.bytes(&group.data_digest);
        }
        for frame in &frame_descriptors {
            writer.u32(frame.frame_ordinal);
            writer.u8(frame.size_class as u8);
            writer.u8(frame.codec);
            writer.bytes(&[0, 0]);
            writer.u64(frame.object_offset);
            writer.u32(frame.stored_len);
            writer.u32(frame.raw_len);
            writer.u32(frame.first_file_slot);
            writer.u32(frame.last_file_slot);
            writer.bytes(&frame.frame_digest);
        }
        for group in &groups {
            for ordinal in &group.frame_ordinals {
                writer.u32(*ordinal);
            }
        }
        for group in &groups {
            writer.bytes(&group.metadata);
        }
        for frame in &frames {
            writer.bytes(&frame.raw);
        }
        PackedEnvelope::build(PackedObjectKind::GroupContainer, writer.finish())
    }

    pub fn open(object: Vec<u8>) -> PackedResult<Self> {
        let object_digest: [u8; 32] = Sha256::digest(&object).into();
        let envelope = PackedEnvelope::parse(object)?;
        if envelope.header.kind != PackedObjectKind::GroupContainer {
            return Err(PackedWireError::Invalid(
                "packed object is not a group container".into(),
            ));
        }
        let mut reader = Reader::new(envelope.body());
        if reader.take(4)? != GROUP_PAYLOAD_MAGIC {
            return Err(PackedWireError::UnsupportedFormat(
                "group container payload version mismatch".into(),
            ));
        }
        let container_id = reader.u64()?;
        let layout_profile = AccessProfile::from_u8(reader.u8()?)
            .map_err(|error| PackedWireError::Invalid(error.to_string()))?;
        reader.skip_zeroes(3)?;
        let group_count = reader.u32()?;
        let frame_count = reader.u32()?;
        if group_count > MAX_GROUPS || frame_count > MAX_FRAMES {
            return Err(PackedWireError::LimitExceeded(
                "group container count exceeds limit".into(),
            ));
        }
        let mut raw_groups = Vec::with_capacity(group_count as usize);
        for _ in 0..group_count {
            let group_id = reader.u64()?;
            let parent_dir_key = reader.array::<32>()?;
            let metadata_offset = reader.u64()?;
            let metadata_len = reader.u32()?;
            let data_offset = reader.u64()?;
            let data_len = reader.u32()?;
            let entry_count = reader.u32()?;
            let file_count = reader.u32()?;
            let frame_list_offset = reader.u64()?;
            let frame_count = reader.u32()?;
            let layout_profile = AccessProfile::from_u8(reader.u8()?)
                .map_err(|error| PackedWireError::Invalid(error.to_string()))?;
            reader.skip_zeroes(3)?;
            let metadata_digest = reader.array::<32>()?;
            let data_digest = reader.array::<32>()?;
            if usize::try_from(metadata_len)
                .is_ok_and(|len| len > super::meta::MAX_GROUP_META_BYTES)
            {
                return Err(PackedWireError::LimitExceeded(
                    "group metadata exceeds 256 KiB".into(),
                ));
            }
            raw_groups.push((
                group_id,
                parent_dir_key,
                metadata_offset,
                metadata_len,
                data_offset,
                data_len,
                entry_count,
                file_count,
                frame_list_offset,
                frame_count,
                layout_profile,
                metadata_digest,
                data_digest,
            ));
        }
        let mut frames = Vec::with_capacity(frame_count as usize);
        for expected_ordinal in 0..frame_count {
            let frame_ordinal = reader.u32()?;
            if frame_ordinal != expected_ordinal {
                return Err(PackedWireError::Invalid(
                    "frame ordinals are not canonical".into(),
                ));
            }
            let size_class = SizeClass::from_u8(reader.u8()?)
                .map_err(|error| PackedWireError::Invalid(error.to_string()))?;
            let codec = reader.u8()?;
            reader.skip_zeroes(2)?;
            let object_offset = reader.u64()?;
            let stored_len = reader.u32()?;
            let raw_len = reader.u32()?;
            let first_file_slot = reader.u32()?;
            let last_file_slot = reader.u32()?;
            let frame_digest = reader.array::<16>()?;
            if codec != 0
                || stored_len == 0
                || raw_len == 0
                || u64::from(raw_len) > MAX_FRAME_RAW_BYTES
            {
                return Err(PackedWireError::UnsupportedFormat(
                    "group container frame codec or length is unsupported".into(),
                ));
            }
            if stored_len != raw_len {
                return Err(PackedWireError::Invalid(
                    "uncompressed frame has different raw and stored lengths".into(),
                ));
            }
            frames.push(PackedFrameDescriptor {
                frame_ordinal,
                object_offset,
                stored_len,
                raw_len,
                first_file_slot,
                last_file_slot,
                size_class,
                codec,
                frame_digest,
            });
        }
        let body = envelope.body().to_vec();
        let body_end = envelope.header.object_len - super::wire::PACKED_FOOTER_LEN as u64;
        validate_frame_ranges(&body, envelope.header.object_len, &frames)?;
        let mut seen = vec![false; frames.len()];
        let mut groups = Vec::with_capacity(raw_groups.len());
        for raw in raw_groups {
            let (
                group_id,
                parent_dir_key,
                metadata_offset,
                metadata_len,
                data_offset,
                data_len,
                entry_count,
                file_count,
                frame_list_offset,
                frame_count,
                layout_profile,
                metadata_digest,
                data_digest,
            ) = raw;
            let list_len = usize::try_from(frame_count)
                .ok()
                .and_then(|count| count.checked_mul(4))
                .ok_or_else(|| {
                    PackedWireError::LimitExceeded("frame list length overflows".into())
                })?;
            let list_range = body_range(frame_list_offset, list_len, envelope.header.object_len)?;
            let mut list_reader = Reader::new(&body[list_range]);
            let mut frame_ordinals = Vec::with_capacity(frame_count as usize);
            for _ in 0..frame_count {
                let ordinal = list_reader.u32()?;
                let index = usize::try_from(ordinal)
                    .map_err(|_| PackedWireError::Invalid("frame ordinal exceeds usize".into()))?;
                if index >= frames.len() || seen[index] {
                    return Err(PackedWireError::Invalid(
                        "frame is missing or referenced by multiple groups".into(),
                    ));
                }
                seen[index] = true;
                frame_ordinals.push(ordinal);
            }
            let metadata_range = body_range(
                metadata_offset,
                usize::try_from(metadata_len).unwrap(),
                envelope.header.object_len,
            )?;
            let metadata = &body[metadata_range];
            let digest: [u8; 32] = Sha256::digest(metadata).into();
            if digest != metadata_digest {
                return Err(PackedWireError::Invalid(
                    "group metadata digest mismatch".into(),
                ));
            }
            let mut data_hasher = Sha256::new();
            for ordinal in &frame_ordinals {
                let descriptor = &frames[*ordinal as usize];
                let range = body_range(
                    descriptor.object_offset,
                    descriptor.stored_len as usize,
                    envelope.header.object_len,
                )?;
                data_hasher.update(&body[range]);
            }
            let computed_data_digest: [u8; 32] = data_hasher.finalize().into();
            if computed_data_digest != data_digest {
                return Err(PackedWireError::Invalid(
                    "group data digest mismatch".into(),
                ));
            }
            if data_len > 0 {
                let data_range =
                    body_range(data_offset, data_len as usize, envelope.header.object_len)?;
                if data_range.end as u64 + super::wire::PACKED_HEADER_LEN as u64 > body_end {
                    return Err(PackedWireError::Invalid(
                        "group data range exceeds object body".into(),
                    ));
                }
            }
            groups.push(PackedGroupDescriptor {
                group_id,
                parent_dir_key,
                metadata_offset,
                metadata_len,
                data_offset,
                data_len,
                entry_count,
                file_count,
                frame_ordinals,
                layout_profile,
                metadata_digest,
                data_digest,
            });
        }
        if seen.iter().any(|value| !value) {
            return Err(PackedWireError::Invalid(
                "group container contains an unowned frame".into(),
            ));
        }
        Ok(Self {
            container_id,
            layout_profile,
            groups,
            frames,
            body,
            object_len: envelope.header.object_len,
            object_digest,
        })
    }

    pub fn container_id(&self) -> u64 {
        self.container_id
    }

    pub fn layout_profile(&self) -> AccessProfile {
        self.layout_profile
    }

    pub fn groups(&self) -> &[PackedGroupDescriptor] {
        &self.groups
    }

    pub fn frames(&self) -> &[PackedFrameDescriptor] {
        &self.frames
    }

    pub fn object_len(&self) -> u64 {
        self.object_len
    }

    pub fn object_digest(&self) -> [u8; 32] {
        self.object_digest
    }

    pub fn group_metadata(&self, group_id: u64) -> PackedResult<&[u8]> {
        let group = self
            .groups
            .iter()
            .find(|group| group.group_id == group_id)
            .ok_or_else(|| PackedWireError::Invalid("group id is missing".into()))?;
        let range = body_range(
            group.metadata_offset,
            group.metadata_len as usize,
            self.object_len,
        )?;
        Ok(&self.body[range])
    }

    /// Decode one canonical group metadata page without touching any frame
    /// payload.  Callers can then use `GroupMeta::page` or `lookup` while the
    /// raw frame ranges remain remote.
    pub fn group_meta(&self, group_id: u64) -> PackedResult<GroupMeta> {
        GroupMeta::decode(self.group_metadata(group_id)?)
    }

    pub fn frame_payload(&self, frame_ordinal: u32) -> PackedResult<&[u8]> {
        let frame = self
            .frames
            .get(frame_ordinal as usize)
            .filter(|frame| frame.frame_ordinal == frame_ordinal)
            .ok_or_else(|| PackedWireError::Invalid("frame ordinal is missing".into()))?;
        let range = body_range(
            frame.object_offset,
            frame.stored_len as usize,
            self.object_len,
        )?;
        Ok(&self.body[range])
    }
}

fn validate_inputs(groups: &[PackedGroupInput], frames: &[PackedFrameInput]) -> PackedResult<()> {
    if groups.len() as u64 > u64::from(MAX_GROUPS) || frames.len() as u64 > u64::from(MAX_FRAMES) {
        return Err(PackedWireError::LimitExceeded(
            "group container count exceeds limit".into(),
        ));
    }
    let mut group_ids = std::collections::HashSet::with_capacity(groups.len());
    for group in groups {
        if !group_ids.insert(group.group_id) {
            return Err(PackedWireError::Invalid("duplicate group id".into()));
        }
        if group.metadata.len() > super::meta::MAX_GROUP_META_BYTES {
            return Err(PackedWireError::LimitExceeded(
                "group metadata exceeds 256 KiB".into(),
            ));
        }
        let metadata = GroupMeta::decode(&group.metadata)?;
        if metadata.len() != usize::try_from(group.entry_count).unwrap_or(usize::MAX) {
            return Err(PackedWireError::Invalid(
                "group metadata entry count disagrees with group descriptor".into(),
            ));
        }
        for entry in metadata.entries() {
            for extent in &entry.extents {
                if !group.frame_ordinals.contains(&extent.frame_ordinal) {
                    return Err(PackedWireError::Invalid(
                        "group metadata entry references a frame outside its group".into(),
                    ));
                }
                let frame = frames.get(extent.frame_ordinal as usize).ok_or_else(|| {
                    PackedWireError::Invalid("group metadata frame is missing".into())
                })?;
                if extent.raw_len as usize != frame.raw.len()
                    || u64::from(extent.raw_offset)
                        .checked_add(u64::from(extent.logical_len))
                        .is_none_or(|end| end > frame.raw.len() as u64)
                {
                    return Err(PackedWireError::Invalid(
                        "group metadata data extent exceeds frame".into(),
                    ));
                }
            }
        }
        for ordinal in &group.frame_ordinals {
            if usize::try_from(*ordinal)
                .ok()
                .filter(|index| *index < frames.len())
                .is_none()
            {
                return Err(PackedWireError::Invalid(
                    "group references a missing frame".into(),
                ));
            }
        }
    }
    let mut owners = vec![false; frames.len()];
    for group in groups {
        for ordinal in &group.frame_ordinals {
            let owner = &mut owners[*ordinal as usize];
            if *owner {
                return Err(PackedWireError::Invalid(
                    "frame is referenced by multiple groups".into(),
                ));
            }
            *owner = true;
        }
    }
    if owners.iter().any(|owner| !owner) {
        return Err(PackedWireError::Invalid(
            "every frame must belong to one group".into(),
        ));
    }
    for frame in frames {
        if frame.raw.is_empty() || frame.raw.len() as u64 > MAX_FRAME_RAW_BYTES || frame.codec != 0
        {
            return Err(PackedWireError::UnsupportedFormat(
                "frame must be a non-empty uncompressed payload <= 8 MiB".into(),
            ));
        }
        if frame.first_file_slot > frame.last_file_slot {
            return Err(PackedWireError::Invalid(
                "frame file slot range is inverted".into(),
            ));
        }
    }
    Ok(())
}

fn validate_frame_ranges(
    body: &[u8],
    object_len: u64,
    frames: &[PackedFrameDescriptor],
) -> PackedResult<()> {
    let mut ranges = Vec::with_capacity(frames.len());
    for frame in frames {
        let range = body_range(frame.object_offset, frame.stored_len as usize, object_len)?;
        let digest: [u8; 16] = Sha256::digest(&body[range.clone()])[..16]
            .try_into()
            .expect("sha256 prefix has 16 bytes");
        if digest != frame.frame_digest {
            return Err(PackedWireError::Invalid("frame digest mismatch".into()));
        }
        ranges.push(range);
    }
    ranges.sort_by_key(|range| range.start);
    for pair in ranges.windows(2) {
        if pair[0].end > pair[1].start {
            return Err(PackedWireError::Invalid(
                "frame payload ranges overlap".into(),
            ));
        }
    }
    Ok(())
}

fn body_range(
    object_offset: u64,
    length: usize,
    object_len: u64,
) -> PackedResult<std::ops::Range<usize>> {
    let footer_start = object_len
        .checked_sub(super::wire::PACKED_FOOTER_LEN as u64)
        .ok_or_else(|| PackedWireError::Invalid("object is shorter than footer".into()))?;
    if object_offset < super::wire::PACKED_HEADER_LEN as u64 || object_offset > footer_start {
        return Err(PackedWireError::Invalid(
            "object range starts outside body".into(),
        ));
    }
    let end = object_offset
        .checked_add(length as u64)
        .ok_or_else(|| PackedWireError::LimitExceeded("object range overflows".into()))?;
    if end > footer_start {
        return Err(PackedWireError::Truncated {
            what: "packed group object range",
            need: length,
            have: footer_start.saturating_sub(object_offset) as usize,
        });
    }
    let start = usize::try_from(object_offset - super::wire::PACKED_HEADER_LEN as u64)
        .map_err(|_| PackedWireError::LimitExceeded("body offset exceeds usize".into()))?;
    let end = start
        .checked_add(length)
        .ok_or_else(|| PackedWireError::LimitExceeded("body range exceeds usize".into()))?;
    Ok(start..end)
}

#[cfg(test)]
mod tests {
    use super::super::meta::GroupMetaEntry;
    use super::*;

    fn metadata() -> Vec<u8> {
        GroupMeta::new(vec![
            GroupMetaEntry {
                name: b"hello".to_vec(),
                inode: 1,
                kind: 1,
                mode: 0o100644,
                uid: 0,
                gid: 0,
                rdev: 0,
                nlink: 1,
                atime_ns: 0,
                mtime_ns: 0,
                ctime_ns: 0,
                size: 5,
                flags: 0,
                inline_data: Arc::from([]),
                extents: vec![GroupMetaExtent {
                    file_offset: 0,
                    logical_len: 5,
                    frame_ordinal: 0,
                    raw_offset: 0,
                    raw_len: 5,
                }],
            },
            GroupMetaEntry {
                name: b"world".to_vec(),
                inode: 2,
                kind: 1,
                mode: 0o100644,
                uid: 0,
                gid: 0,
                rdev: 0,
                nlink: 1,
                atime_ns: 0,
                mtime_ns: 0,
                ctime_ns: 0,
                size: 5,
                flags: 0,
                inline_data: Arc::from([]),
                extents: vec![GroupMetaExtent {
                    file_offset: 0,
                    logical_len: 5,
                    frame_ordinal: 1,
                    raw_offset: 0,
                    raw_len: 5,
                }],
            },
        ])
        .unwrap()
        .encode()
        .unwrap()
    }

    fn inputs() -> (Vec<PackedGroupInput>, Vec<PackedFrameInput>) {
        (
            vec![PackedGroupInput {
                group_id: 9,
                parent_dir_key: [1; 32],
                metadata: metadata(),
                frame_ordinals: vec![0, 1],
                entry_count: 2,
                file_count: 2,
                layout_profile: AccessProfile::RandomSmallFile,
            }],
            vec![
                PackedFrameInput {
                    raw: b"hello".to_vec(),
                    size_class: SizeClass::Tiny,
                    codec: 0,
                    first_file_slot: 0,
                    last_file_slot: 0,
                },
                PackedFrameInput {
                    raw: b"world".to_vec(),
                    size_class: SizeClass::Tiny,
                    codec: 0,
                    first_file_slot: 1,
                    last_file_slot: 1,
                },
            ],
        )
    }

    #[test]
    fn group_container_roundtrip_exposes_bounded_metadata_and_frames() {
        let (groups, frames) = inputs();
        let object =
            PackedGroupContainer::build(44, AccessProfile::RandomSmallFile, groups, frames)
                .unwrap();
        let opened = PackedGroupContainer::open(object).unwrap();
        assert_eq!(opened.container_id(), 44);
        assert_eq!(opened.groups().len(), 1);
        assert_eq!(opened.group_meta(9).unwrap().len(), 2);
        assert_eq!(opened.frame_payload(0).unwrap(), b"hello");
        assert_eq!(opened.frame_payload(1).unwrap(), b"world");
    }

    #[test]
    fn frame_directory_parser_reads_only_prefix_and_table() {
        let (groups, frames) = inputs();
        let object =
            PackedGroupContainer::build(44, AccessProfile::RandomSmallFile, groups, frames)
                .unwrap();
        let body = &object[super::super::wire::PACKED_HEADER_LEN
            ..object.len() - super::super::wire::PACKED_FOOTER_LEN];
        let directory_len = frame_directory_body_len(&body[..GROUP_PREFIX_LEN]).unwrap();
        let parsed = parse_frame_directory(&body[..directory_len], object.len() as u64).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(
            parsed[0],
            PackedGroupContainer::open(object.clone()).unwrap().frames()[0]
        );
        assert_eq!(
            frame_table_body_offset(&body[..GROUP_PREFIX_LEN]).unwrap(),
            GROUP_PREFIX_LEN + GROUP_RECORD_LEN
        );
        assert!(matches!(
            parse_frame_directory(&body[..directory_len - 1], object.len() as u64),
            Err(PackedWireError::Truncated { .. })
        ));
    }

    #[test]
    fn group_container_rejects_unowned_frames() {
        let (mut groups, mut frames) = inputs();
        frames.push(PackedFrameInput {
            raw: b"orphan".to_vec(),
            size_class: SizeClass::Tiny,
            codec: 0,
            first_file_slot: 2,
            last_file_slot: 2,
        });
        assert!(
            PackedGroupContainer::build(1, AccessProfile::Mixed, groups.clone(), frames).is_err()
        );
        groups[0].frame_ordinals = vec![0, 0];
        let (_, frames) = inputs();
        assert!(PackedGroupContainer::build(1, AccessProfile::Mixed, groups, frames).is_err());
    }

    #[test]
    fn container_shards_rebase_frame_ordinals_and_obey_target() {
        let (mut first_groups, first_frames) = inputs();
        let first_group = first_groups.remove(0);
        let (mut second_groups, second_frames) = inputs();
        let mut second_group = second_groups.remove(0);
        second_group.group_id = 10;
        let containers = pack_group_shard_containers(
            100,
            vec![(first_group, first_frames), (second_group, second_frames)],
            ContainerPackingLimits {
                target_body_bytes: 256,
                max_body_bytes: MAX_OBJECT_BODY as usize,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(containers.len(), 2);
        assert_eq!(containers[0].container_id, 100);
        assert_eq!(containers[1].container_id, 101);

        let (mut first_groups, first_frames) = inputs();
        let first_group = first_groups.remove(0);
        let (mut second_groups, second_frames) = inputs();
        let mut second_group = second_groups.remove(0);
        second_group.group_id = 10;
        let containers = pack_group_shard_containers(
            200,
            vec![(first_group, first_frames), (second_group, second_frames)],
            ContainerPackingLimits {
                target_body_bytes: 1024 * 1024,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(containers.len(), 1);
        let container = &containers[0];
        let object = PackedGroupContainer::build(
            container.container_id,
            AccessProfile::RandomSmallFile,
            container.groups.clone(),
            container.frames.clone(),
        )
        .unwrap();
        let opened = PackedGroupContainer::open(object).unwrap();
        let second_meta = opened.group_meta(10).unwrap();
        assert_eq!(second_meta.entries()[0].extents[0].frame_ordinal, 2);
        assert_eq!(second_meta.entries()[1].extents[0].frame_ordinal, 3);
        assert_eq!(opened.frame_payload(2).unwrap(), b"hello");
        assert_eq!(opened.frame_payload(3).unwrap(), b"world");
    }

    #[test]
    fn dynamic_packer_copacks_tiny_files_and_splits_large_files() {
        let tiny_files = (0..3)
            .map(|index| PackedFileInput {
                name: format!("f{index}").into_bytes(),
                inode: index + 1,
                kind: 1,
                mode: 0o100644,
                uid: 0,
                gid: 0,
                rdev: 0,
                nlink: 1,
                atime_ns: 0,
                mtime_ns: 0,
                ctime_ns: 0,
                flags: 0,
                data: vec![index as u8; 100 * 1024],
            })
            .collect();
        let (group, frames) = pack_group_files(
            1,
            [1; 32],
            tiny_files,
            AccessProfile::RandomSmallFile,
            SizeClassTable {
                max_random_frame_raw_bytes: 4 * 1024 * 1024,
                ..SizeClassTable::default()
            },
            None,
        )
        .unwrap();
        assert_eq!(frames.len(), 1);
        let meta = GroupMeta::decode(&group.metadata).unwrap();
        assert_eq!(meta.entries()[0].inline_data.len(), 100 * 1024);
        assert_eq!(meta.entries()[1].inline_data.len(), 100 * 1024);
        assert!(meta.entries()[0].extents.is_empty());
        assert_eq!(meta.entries()[2].extents[0].frame_ordinal, 0);

        let (large_group, large_frames) = pack_group_files(
            2,
            [1; 32],
            vec![PackedFileInput {
                name: b"large".to_vec(),
                inode: 10,
                kind: 1,
                mode: 0o100644,
                uid: 0,
                gid: 0,
                rdev: 0,
                nlink: 1,
                atime_ns: 0,
                mtime_ns: 0,
                ctime_ns: 0,
                flags: 0,
                data: vec![7; 10 * 1024 * 1024],
            }],
            AccessProfile::RandomSmallFile,
            SizeClassTable {
                max_random_frame_raw_bytes: 4 * 1024 * 1024,
                ..SizeClassTable::default()
            },
            None,
        )
        .unwrap();
        assert_eq!(large_frames.len(), 3);
        assert_eq!(
            GroupMeta::decode(&large_group.metadata).unwrap().entries()[0]
                .extents
                .len(),
            3
        );
    }

    #[test]
    fn inline_boundary_uses_a_dynamic_frame() {
        let (group, frames) = pack_group_files(
            3,
            [1; 32],
            vec![PackedFileInput {
                name: b"boundary.bin".to_vec(),
                inode: 11,
                kind: 1,
                mode: 0o100644,
                uid: 0,
                gid: 0,
                rdev: 0,
                nlink: 1,
                atime_ns: 0,
                mtime_ns: 0,
                ctime_ns: 0,
                flags: 0,
                data: vec![3; INLINE_FILE_MAX_BYTES],
            }],
            AccessProfile::RandomSmallFile,
            SizeClassTable::default(),
            None,
        )
        .unwrap();
        let decoded = GroupMeta::decode(&group.metadata).unwrap();
        let entry = &decoded.entries()[0];
        assert!(entry.inline_data.is_empty());
        assert_eq!(entry.extents.len(), 1);
        assert_eq!(frames.len(), 1);
    }

    #[test]
    fn directory_shards_obey_entry_and_logical_limits() {
        let files = (0..100)
            .map(|index| PackedFileInput {
                name: format!("f{index:04}").into_bytes(),
                inode: index + 1,
                kind: 1,
                mode: 0o100644,
                uid: 0,
                gid: 0,
                rdev: 0,
                nlink: 1,
                atime_ns: 0,
                mtime_ns: 0,
                ctime_ns: 0,
                flags: 0,
                data: vec![index as u8; 100 * 1024],
            })
            .collect();
        let limits = GroupPackingLimits {
            target_logical_bytes: 2 * 1024 * 1024,
            max_logical_bytes: 3 * 1024 * 1024,
            max_entries: 32,
            ..Default::default()
        };
        let shards = pack_group_file_shards(
            100,
            [7; 32],
            files,
            AccessProfile::RandomSmallFile,
            SizeClassTable::default(),
            None,
            limits,
        )
        .unwrap();
        assert!(shards.len() >= 5);
        assert_eq!(shards[0].0.group_id, 100);
        assert_eq!(
            shards.last().unwrap().0.group_id,
            100 + shards.len() as u64 - 1
        );

        let mut names = Vec::new();
        for (group, _) in &shards {
            assert!(group.entry_count as usize <= limits.max_entries);
            assert!(group.metadata.len() <= limits.max_metadata_bytes);
            let meta = GroupMeta::decode(&group.metadata).unwrap();
            assert_eq!(meta.len(), group.entry_count as usize);
            names.extend(meta.entries().iter().map(|entry| entry.name.clone()));
        }
        assert_eq!(names.len(), 100);
        assert!(names.windows(2).all(|window| window[0] < window[1]));
    }

    #[test]
    fn metadata_heavy_directory_shards_before_group_meta_overflow() {
        let files = (0..4_000)
            .map(|index| PackedFileInput {
                name: format!("entry-{index:06}-with-a-long-name").into_bytes(),
                inode: index + 1,
                kind: 2,
                mode: 0o040755,
                uid: 0,
                gid: 0,
                rdev: 0,
                nlink: 2,
                atime_ns: 0,
                mtime_ns: 0,
                ctime_ns: 0,
                flags: 0,
                data: Vec::new(),
            })
            .collect();
        let shards = pack_group_file_shards(
            900,
            [8; 32],
            files,
            AccessProfile::RandomSmallFile,
            SizeClassTable::default(),
            None,
            GroupPackingLimits {
                target_logical_bytes: 64 * 1024 * 1024,
                max_logical_bytes: 64 * 1024 * 1024,
                max_entries: 10_000,
                ..Default::default()
            },
        )
        .unwrap();
        assert!(shards.len() >= 2);
        assert!(
            shards
                .iter()
                .all(|(group, _)| group.metadata.len() <= MAX_GROUP_META_BYTES)
        );
    }

    #[test]
    fn access_profiles_use_bounded_but_different_pack_windows() {
        let random = GroupPackingLimits::for_profile(AccessProfile::RandomSmallFile);
        let sequential = GroupPackingLimits::for_profile(AccessProfile::SequentialSmallFile);
        assert!(random.target_logical_bytes < sequential.target_logical_bytes);
        assert!(random.max_logical_bytes < sequential.max_logical_bytes);
        assert_eq!(random.max_metadata_bytes, MAX_GROUP_META_BYTES);
        assert_eq!(
            ContainerPackingLimits::for_profile(AccessProfile::RandomSmallFile).target_body_bytes,
            32 * 1024 * 1024
        );
        assert_eq!(
            ContainerPackingLimits::for_profile(AccessProfile::SequentialSmallFile)
                .target_body_bytes,
            48 * 1024 * 1024
        );
    }
}
