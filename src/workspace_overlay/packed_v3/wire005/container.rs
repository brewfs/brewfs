//! Bounded GC05 construction from the existing deterministic group packer.

use super::{
    V3_FOOTER_LEN, V3_HEADER_LEN, V3_MAX_BODY_BYTES, V3FrameDirectoryPage, V3ObjectKind,
    V3ObjectRef, encode_v3_object,
};
use crate::cadapter::client::{ObjectBackend, ObjectClient};
use crate::workspace_overlay::packed_v3::wire::{PackedResult, PackedWireError, Reader, Writer};
use crate::workspace_overlay::packed_v3::{
    AccessProfile, GroupMeta, PackedCodec, PackedFrameDescriptor, PackedFrameInput,
    PackedGroupInput, SizeClassTable, decode_block, encode_block,
};
use sha2::{Digest, Sha256};
use std::sync::Arc;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V3GroupRef {
    pub group_id: u64,
    pub container_ordinal: u32,
    pub parent_dir_key: [u8; 32],
    pub first_name: Vec<u8>,
    pub last_name: Vec<u8>,
    pub meta_offset: u64,
    pub meta_stored_len: u32,
    pub meta_raw_len: u32,
    pub meta_codec: PackedCodec,
    pub meta_digest: [u8; 32],
    pub entry_count: u32,
    pub first_frame: u32,
    pub frame_count: u32,
}

impl V3GroupRef {
    fn validate(&self) -> PackedResult<()> {
        super::super::meta::validate_name(&self.first_name)?;
        super::super::meta::validate_name(&self.last_name)?;
        if self.meta_digest == [0; 32]
            || (self.meta_codec == PackedCodec::Raw && self.meta_stored_len != self.meta_raw_len)
            || self.first_name > self.last_name
            || self.meta_offset < V3_HEADER_LEN as u64
            || self.meta_stored_len == 0
            || self.meta_stored_len > 256 * 1024
            || self.meta_raw_len == 0
            || self.meta_raw_len > 256 * 1024
            || self.entry_count == 0
            || self.entry_count > 4096
            || self.first_frame.checked_add(self.frame_count).is_none()
        {
            return Err(PackedWireError::Invalid(
                "wire 005 group identity/range/count exceeds limits".into(),
            ));
        }
        Ok(())
    }

    pub fn encode_value(&self) -> PackedResult<Vec<u8>> {
        self.validate()?;
        let mut w = Writer::default();
        w.bytes(b"GR05");
        w.u64(self.group_id);
        w.u32(self.container_ordinal);
        w.bytes(&self.parent_dir_key);
        w.u64(self.meta_offset);
        w.u32(self.meta_stored_len);
        w.u32(self.meta_raw_len);
        w.u8(self.meta_codec as u8);
        w.bytes(&[0; 3]);
        w.bytes(&self.meta_digest);
        w.u32(self.entry_count);
        w.u32(self.first_frame);
        w.u32(self.frame_count);
        w.u16(self.first_name.len() as u16);
        w.u16(self.last_name.len() as u16);
        w.bytes(&self.first_name);
        w.bytes(&self.last_name);
        Ok(w.finish())
    }

    pub fn decode_value(bytes: &[u8]) -> PackedResult<Self> {
        let mut r = Reader::new(bytes);
        if r.take(4)? != b"GR05" {
            return Err(PackedWireError::UnsupportedFormat(
                "wire 005 group ref payload mismatch".into(),
            ));
        }
        let group_id = r.u64()?;
        let container_ordinal = r.u32()?;
        let parent_dir_key = r.array::<32>()?;
        let meta_offset = r.u64()?;
        let meta_stored_len = r.u32()?;
        let meta_raw_len = r.u32()?;
        let meta_codec = PackedCodec::from_u8(r.u8()?)?;
        r.skip_zeroes(3)?;
        let meta_digest = r.array::<32>()?;
        let entry_count = r.u32()?;
        let first_frame = r.u32()?;
        let frame_count = r.u32()?;
        let first_len = r.u16()? as usize;
        let last_len = r.u16()? as usize;
        if first_len > 1024 || last_len > 1024 {
            return Err(PackedWireError::LimitExceeded(
                "wire 005 group name fence exceeds budget".into(),
            ));
        }
        let first_name = r.take(first_len)?.to_vec();
        let last_name = r.take(last_len)?.to_vec();
        if !r.is_empty() {
            return Err(PackedWireError::Invalid(
                "wire 005 group ref has trailing bytes".into(),
            ));
        }
        let reference = Self {
            group_id,
            container_ordinal,
            parent_dir_key,
            first_name,
            last_name,
            meta_offset,
            meta_stored_len,
            meta_raw_len,
            meta_codec,
            meta_digest,
            entry_count,
            first_frame,
            frame_count,
        };
        reference.validate()?;
        Ok(reference)
    }

    pub(super) fn validate_metadata_read(
        &self,
        container: &V3ObjectRef,
        allocation_limit: usize,
    ) -> PackedResult<()> {
        self.validate()?;
        super::validate_key(&container.key)?;
        if container.kind != V3ObjectKind::GroupContainer
            || container.object_len < (V3_HEADER_LEN + V3_FOOTER_LEN) as u64
            || self
                .meta_offset
                .checked_add(u64::from(self.meta_stored_len))
                .is_none_or(|end| end > container.object_len - V3_FOOTER_LEN as u64)
        {
            return Err(PackedWireError::Invalid(
                "wire 005 group metadata lies outside authenticated container".into(),
            ));
        }
        if self.meta_stored_len as usize + self.meta_raw_len as usize > allocation_limit {
            return Err(PackedWireError::LimitExceeded(
                "wire 005 group metadata allocation exceeds budget".into(),
            ));
        }
        Ok(())
    }

    pub async fn read_metadata<B: ObjectBackend + Clone>(
        &self,
        client: &ObjectClient<B>,
        container: &V3ObjectRef,
        allocation_limit: usize,
    ) -> PackedResult<Arc<GroupMeta>> {
        self.validate_metadata_read(container, allocation_limit)?;
        client
            .typed_exact(
                crate::cadapter::read_observer::ReadClass::GroupMetadata,
                &container.key,
                self.meta_offset,
                u64::from(self.meta_stored_len),
                allocation_limit as u64,
                |stored| {
                    self.decode_metadata_stored(&stored)
                        .map_err(super::observer_validation_error)
                },
            )
            .await
            .map_err(super::observer_backend_error)
    }

    /// Decode an authenticated GroupMeta payload that was obtained from a
    /// larger, coalesced container range.  The caller has already validated
    /// the containing object and selected the exact metadata slice; the
    /// digest and all GroupMeta fences are still checked here.
    pub(super) fn decode_metadata_stored(&self, stored: &[u8]) -> PackedResult<Arc<GroupMeta>> {
        if stored.len() != self.meta_stored_len as usize {
            return Err(PackedWireError::Truncated {
                what: "wire 005 GroupMeta range",
                need: self.meta_stored_len as usize,
                have: stored.len(),
            });
        }
        let computed: [u8; 32] = Sha256::digest(stored).into();
        if computed != self.meta_digest {
            return Err(PackedWireError::HashMismatch {
                what: "wire 005 authenticated GroupMeta",
                expected: hex::encode(self.meta_digest),
                computed: hex::encode(computed),
            });
        }
        let raw = decode_block(
            self.meta_codec,
            stored,
            self.meta_raw_len as usize,
            256 * 1024,
            self.meta_raw_len as usize,
        )?;
        let meta = GroupMeta::decode_restart(&raw)?;
        if meta.len() != self.entry_count as usize
            || meta.entries().first().map(|e| e.name.as_slice()) != Some(self.first_name.as_slice())
            || meta.entries().last().map(|e| e.name.as_slice()) != Some(self.last_name.as_slice())
        {
            return Err(PackedWireError::Invalid(
                "wire 005 group count/name fences disagree with metadata".into(),
            ));
        }
        for entry in meta.entries() {
            for extent in &entry.extents {
                if extent.frame_ordinal < self.first_frame
                    || extent.frame_ordinal >= self.first_frame + self.frame_count
                {
                    return Err(PackedWireError::Invalid(
                        "wire 005 entry extent references another group's frame".into(),
                    ));
                }
            }
        }
        Ok(Arc::new(meta))
    }

    /// Decode a GroupMeta slice while retaining the same budget ownership as
    /// the ordinary single-range path.  This is used by the container range
    /// cache after one coalesced GET has supplied several adjacent pages.
    pub(super) fn decode_metadata_owned_from_stored(
        &self,
        stored: &[u8],
        allocation_limit: usize,
        budget: &Arc<super::V3MountBudget>,
    ) -> PackedResult<Arc<super::budget::V3Owned<Arc<GroupMeta>>>> {
        self.validate_metadata_payload(stored, allocation_limit)?;
        let workspace = super::super::codec::decode_workspace_bytes(self.meta_codec)?;
        let mut permit = budget.admit(&[
            (super::V3BudgetPool::Metadata, 6 << 20),
            (
                super::V3BudgetPool::Stored,
                u64::from(self.meta_stored_len) * 2,
            ),
            (super::V3BudgetPool::Raw, u64::from(self.meta_raw_len)),
            (super::V3BudgetPool::Workspace, workspace as u64),
        ])?;
        let metadata = self.decode_metadata_stored(stored)?;
        let weight = metadata.owned_memory_bytes();
        permit.shrink(super::V3BudgetPool::Metadata, weight as u64)?;
        permit.shrink(super::V3BudgetPool::Stored, 0)?;
        permit.shrink(super::V3BudgetPool::Raw, 0)?;
        permit.shrink(super::V3BudgetPool::Workspace, 0)?;
        Ok(Arc::new(super::budget::V3Owned::new(metadata, permit)))
    }

    fn validate_metadata_payload(
        &self,
        stored: &[u8],
        allocation_limit: usize,
    ) -> PackedResult<()> {
        if stored.len() != self.meta_stored_len as usize
            || self.meta_stored_len as usize + self.meta_raw_len as usize > allocation_limit
        {
            return Err(PackedWireError::LimitExceeded(
                "wire 005 group metadata allocation exceeds budget".into(),
            ));
        }
        Ok(())
    }

    pub(super) async fn read_metadata_owned<B: ObjectBackend + Clone>(
        &self,
        client: &ObjectClient<B>,
        container: &V3ObjectRef,
        allocation_limit: usize,
        budget: &Arc<super::V3MountBudget>,
    ) -> PackedResult<Arc<super::budget::V3Owned<Arc<GroupMeta>>>> {
        use super::V3BudgetPool;
        let workspace = super::super::codec::decode_workspace_bytes(self.meta_codec)?;
        let mut permit = budget.admit(&[
            // Raw GM07 cap/count do not bound decoded names: restart prefix
            // expansion can own 3449 names of 1024 bytes. 6MiB covers those,
            // entry Vecs, <=raw/24 extent structs, inline Arc copies, run
            // temporaries and owner bookkeeping before any decode allocation.
            (V3BudgetPool::Metadata, 6 << 20),
            (V3BudgetPool::Stored, u64::from(self.meta_stored_len) * 2),
            (V3BudgetPool::Raw, u64::from(self.meta_raw_len)),
            (V3BudgetPool::Workspace, workspace as u64),
        ])?;
        let metadata = self
            .read_metadata(client, container, allocation_limit)
            .await?;
        let weight = metadata.owned_memory_bytes();
        permit.shrink(V3BudgetPool::Metadata, weight as u64)?;
        permit.shrink(V3BudgetPool::Stored, 0)?;
        permit.shrink(V3BudgetPool::Raw, 0)?;
        permit.shrink(V3BudgetPool::Workspace, 0)?;
        Ok(Arc::new(super::budget::V3Owned::new(metadata, permit)))
    }
}

type OwnedGroupMetadata = Arc<super::budget::V3Owned<Arc<GroupMeta>>>;

struct DirectoryGroup {
    group: V3GroupRef,
    container: V3ObjectRef,
    metadata: tokio::sync::OnceCell<OwnedGroupMetadata>,
}

/// One authenticated group per open directory. Switching groups retires the
/// previous slot; concurrent consumers keep their own charged owners alive.
pub(crate) struct V3DirectoryMetadata {
    current: std::sync::Mutex<Option<Arc<super::budget::V3Owned<DirectoryGroup>>>>,
    budget: Arc<super::V3MountBudget>,
    _permit: super::V3OwnedPermit,
}

impl V3DirectoryMetadata {
    pub(crate) fn new(budget: Arc<super::V3MountBudget>) -> PackedResult<Self> {
        let permit = budget.admit(&[(super::V3BudgetPool::Roots, 256)])?;
        Ok(Self {
            current: std::sync::Mutex::new(None),
            budget,
            _permit: permit,
        })
    }

    pub(super) async fn read<B: ObjectBackend + Clone>(
        &self,
        group: &V3GroupRef,
        client: &ObjectClient<B>,
        container: &V3ObjectRef,
        allocation_limit: usize,
    ) -> PackedResult<OwnedGroupMetadata> {
        // Hits and followers still obey admission and the caller's allocation
        // limit. Match the complete references so count/name/frame claims can
        // never borrow validation from a different occurrence of the bytes.
        let _request = self.budget.admit(&[(super::V3BudgetPool::Control, 512)])?;
        group.validate_metadata_read(container, allocation_limit)?;
        let slot = {
            let mut current = self.current.lock().unwrap();
            if let Some(slot) = current
                .as_ref()
                .filter(|slot| slot.group == *group && slot.container == *container)
            {
                slot.clone()
            } else {
                // Release the old retained group before admitting its successor.
                *current = None;
                let bytes =
                    512 + group.first_name.len() + group.last_name.len() + container.key.len();
                let permit = self
                    .budget
                    .admit(&[(super::V3BudgetPool::Metadata, bytes as u64)])?;
                let slot = Arc::new(super::budget::V3Owned::new(
                    DirectoryGroup {
                        group: group.clone(),
                        container: container.clone(),
                        metadata: tokio::sync::OnceCell::new(),
                    },
                    permit,
                ));
                *current = Some(slot.clone());
                slot
            }
        };
        // No directory-state lock spans I/O. OnceCell shares successful fills
        // and lets a later caller retry after failure or cancellation.
        slot.metadata
            .get_or_try_init(|| {
                group.read_metadata_owned(client, container, allocation_limit, &self.budget)
            })
            .await
            .cloned()
    }
}

pub struct V3BuiltContainer {
    pub bytes: Vec<u8>,
    pub groups: Vec<V3GroupRef>,
    pub frame_pages: Vec<V3FrameDirectoryPage>,
}

pub fn build_v3_container(
    container_ordinal: u32,
    container_id: u64,
    profile: AccessProfile,
    table: SizeClassTable,
    codecs: (PackedCodec, PackedCodec),
    groups: &[PackedGroupInput],
    frames: &[PackedFrameInput],
) -> PackedResult<V3BuiltContainer> {
    build_v3_container_with_policy(
        container_ordinal,
        container_id,
        profile,
        table,
        codecs.0,
        codecs.1,
        groups,
        frames,
        super::V3BuildPolicy::default(),
    )
}

#[allow(clippy::too_many_arguments)]
pub fn build_v3_container_with_policy(
    container_ordinal: u32,
    container_id: u64,
    profile: AccessProfile,
    table: SizeClassTable,
    metadata_codec: PackedCodec,
    data_codec: PackedCodec,
    groups: &[PackedGroupInput],
    frames: &[PackedFrameInput],
    policy: super::V3BuildPolicy,
) -> PackedResult<V3BuiltContainer> {
    policy.select(1, profile, table)?;
    table
        .validate()
        .map_err(|_| PackedWireError::Invalid("wire 005 builder size table is invalid".into()))?;
    if groups.is_empty() || groups.len() > 1024 || frames.len() > 65536 {
        return Err(PackedWireError::LimitExceeded(
            "wire 005 builder group/frame count exceeds budget".into(),
        ));
    }
    let raw_bytes = frames
        .iter()
        .try_fold(0usize, |sum, frame| sum.checked_add(frame.raw.len()))
        .ok_or_else(|| {
            PackedWireError::LimitExceeded("wire 005 frame input bytes overflow".into())
        })?;
    if raw_bytes > V3_MAX_BODY_BYTES {
        return Err(PackedWireError::LimitExceeded(
            "wire 005 raw container exceeds budget".into(),
        ));
    }
    let mut names = Vec::with_capacity(groups.len());
    let mut raw_meta_lengths = Vec::with_capacity(groups.len());
    let mut stored_meta = Vec::with_capacity(groups.len());
    let mut directory_bytes = 24usize;
    let mut accumulated_metadata_bytes = 0usize;
    let mut group_ids = std::collections::HashSet::with_capacity(groups.len());
    let mut used_frames = vec![false; frames.len()];
    for group in groups {
        if !group_ids.insert(group.group_id) || group.layout_profile != profile {
            return Err(PackedWireError::Invalid(
                "wire 005 duplicate group or mixed profile".into(),
            ));
        }
        let metadata = GroupMeta::decode(&group.metadata)?;
        if metadata.is_empty()
            || metadata.len() != group.entry_count as usize
            || metadata
                .entries()
                .iter()
                .filter(|entry| entry.kind == 1)
                .count()
                != group.file_count as usize
            || metadata
                .entries()
                .iter()
                .any(|entry| !(1..=7).contains(&entry.kind))
        {
            return Err(PackedWireError::Invalid(
                "wire 005 input group count/kind disagrees with metadata".into(),
            ));
        }
        let first = metadata.entries()[0].name.clone();
        let last = metadata.entries()[metadata.len() - 1].name.clone();
        let first_frame = group.frame_ordinals.first().copied().unwrap_or(0);
        for (i, ordinal) in group.frame_ordinals.iter().copied().enumerate() {
            if first_frame.checked_add(i as u32) != Some(ordinal)
                || ordinal as usize >= frames.len()
                || used_frames[ordinal as usize]
            {
                return Err(PackedWireError::Invalid(
                    "wire 005 group frames must be unique consecutive ordinals".into(),
                ));
            }
            used_frames[ordinal as usize] = true;
        }
        for entry in metadata.entries() {
            for extent in &entry.extents {
                if !group.frame_ordinals.contains(&extent.frame_ordinal)
                    || frames
                        .get(extent.frame_ordinal as usize)
                        .is_none_or(|frame| frame.raw.len() != extent.raw_len as usize)
                {
                    return Err(PackedWireError::Invalid(
                        "wire 005 placement and frame raw length disagree".into(),
                    ));
                }
            }
        }
        let raw = metadata.encode_restart()?;
        let stored = encode_block(metadata_codec, &raw, 256 * 1024)?;
        if stored.len() > 256 * 1024 {
            return Err(PackedWireError::LimitExceeded(
                "wire 005 compressed metadata exceeds stored budget".into(),
            ));
        }
        directory_bytes = directory_bytes
            .checked_add(108 + first.len() + last.len())
            .ok_or_else(|| {
                PackedWireError::LimitExceeded("wire 005 directory offset overflows".into())
            })?;
        accumulated_metadata_bytes = accumulated_metadata_bytes
            .checked_add(raw.len() + stored.len())
            .ok_or_else(|| {
                PackedWireError::LimitExceeded("wire 005 metadata allocation overflows".into())
            })?;
        if accumulated_metadata_bytes > V3_MAX_BODY_BYTES {
            return Err(PackedWireError::LimitExceeded(
                "wire 005 buffered metadata exceeds builder budget".into(),
            ));
        }
        names.push((first, last));
        raw_meta_lengths.push(raw.len());
        stored_meta.push(stored);
    }
    if used_frames.iter().any(|used| !*used) {
        return Err(PackedWireError::Invalid(
            "wire 005 container has unreferenced input frames".into(),
        ));
    }
    let mut stored_frames = Vec::with_capacity(frames.len());
    let mut frame_codecs = Vec::with_capacity(frames.len());
    for frame in frames {
        if frame.raw.is_empty() || frame.raw.len() > 8 * 1024 * 1024 || frame.codec != 0 {
            return Err(PackedWireError::Invalid(
                "wire 005 builder requires bounded raw frame inputs".into(),
            ));
        }
        let encoded = encode_block(data_codec, &frame.raw, 8 * 1024 * 1024)?;
        if data_codec == PackedCodec::Zstd && encoded.len() >= frame.raw.len() {
            // Compression expansion must not make a valid 8 MiB raw frame
            // exceed the strict range limit. The descriptor records the choice.
            stored_frames.push(frame.raw.clone());
            frame_codecs.push(PackedCodec::Raw);
        } else {
            stored_frames.push(encoded);
            frame_codecs.push(data_codec);
        }
    }
    let body_len = stored_meta
        .iter()
        .chain(stored_frames.iter())
        .try_fold(directory_bytes, |sum, bytes| sum.checked_add(bytes.len()))
        .ok_or_else(|| {
            PackedWireError::LimitExceeded("wire 005 stored container bytes overflow".into())
        })?;
    if body_len > V3_MAX_BODY_BYTES {
        return Err(PackedWireError::LimitExceeded(
            "wire 005 stored container exceeds budget".into(),
        ));
    }
    let mut offset = (V3_HEADER_LEN + directory_bytes) as u64;
    let mut references = Vec::with_capacity(groups.len());
    for (i, group) in groups.iter().enumerate() {
        let (first_name, last_name) = &names[i];
        references.push(V3GroupRef {
            group_id: group.group_id,
            container_ordinal,
            parent_dir_key: group.parent_dir_key,
            first_name: first_name.clone(),
            last_name: last_name.clone(),
            meta_offset: offset,
            meta_stored_len: stored_meta[i].len() as u32,
            meta_raw_len: raw_meta_lengths[i] as u32,
            meta_codec: metadata_codec,
            meta_digest: Sha256::digest(&stored_meta[i]).into(),
            entry_count: group.entry_count,
            first_frame: group.frame_ordinals.first().copied().unwrap_or(0),
            frame_count: group.frame_ordinals.len() as u32,
        });
        offset += stored_meta[i].len() as u64;
    }
    let mut descriptors = Vec::with_capacity(frames.len());
    for (i, frame) in frames.iter().enumerate() {
        descriptors.push(PackedFrameDescriptor {
            frame_ordinal: i as u32,
            object_offset: offset,
            stored_len: stored_frames[i].len() as u32,
            raw_len: frame.raw.len() as u32,
            first_file_slot: frame.first_file_slot,
            last_file_slot: frame.last_file_slot,
            size_class: frame.size_class,
            codec: frame_codecs[i] as u8,
            frame_digest: Sha256::digest(&stored_frames[i])[..16].try_into().unwrap(),
        });
        offset += stored_frames[i].len() as u64;
    }
    let mut writer = Writer::default();
    writer.bytes(b"GC05");
    writer.u64(container_id);
    writer.u8(profile as u8);
    writer.bytes(&[0; 3]);
    writer.u32(groups.len() as u32);
    writer.u32(frames.len() as u32);
    for reference in &references {
        reference.validate()?;
        writer.u64(reference.group_id);
        writer.u64(reference.meta_offset);
        writer.u32(reference.meta_stored_len);
        writer.u32(reference.meta_raw_len);
        writer.u8(reference.meta_codec as u8);
        writer.bytes(&[0; 3]);
        writer.u32(reference.entry_count);
        writer.u32(reference.first_frame);
        writer.u32(reference.frame_count);
        writer.bytes(&reference.meta_digest);
        writer.bytes(&reference.parent_dir_key);
        writer.u16(reference.first_name.len() as u16);
        writer.u16(reference.last_name.len() as u16);
        writer.bytes(&reference.first_name);
        writer.bytes(&reference.last_name);
    }
    for bytes in stored_meta.iter().chain(stored_frames.iter()) {
        writer.bytes(bytes);
    }
    let body = writer.finish();
    if body.len() != body_len {
        return Err(PackedWireError::Invalid(
            "wire 005 container builder directory length mismatch".into(),
        ));
    }
    let bytes = encode_v3_object(V3ObjectKind::GroupContainer, &body, V3_MAX_BODY_BYTES)?;
    let digest: [u8; 32] = Sha256::digest(&bytes).into();
    let mut frame_pages = Vec::with_capacity(descriptors.len().div_ceil(4096));
    for chunk in descriptors.chunks(4096) {
        let page = V3FrameDirectoryPage {
            container_digest: digest,
            container_len: bytes.len() as u64,
            profile,
            size_classes: table,
            frame_policy: policy.frames,
            first_ordinal: chunk[0].frame_ordinal,
            frames: chunk.to_vec(),
        };
        page.encode()?;
        frame_pages.push(page);
    }
    Ok(V3BuiltContainer {
        bytes,
        groups: references,
        frame_pages,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cadapter::localfs::LocalFsBackend;
    use crate::workspace_overlay::packed_v3::{PackedFileInput, pack_group_files};
    fn inputs() -> (PackedGroupInput, Vec<PackedFrameInput>) {
        pack_group_files(
            1,
            [2; 32],
            vec![PackedFileInput {
                name: b"file".to_vec(),
                inode: 3,
                kind: 1,
                mode: 0o100644,
                uid: 1000,
                gid: 1000,
                rdev: 0,
                nlink: 1,
                atime_ns: 1,
                mtime_ns: 2,
                ctime_ns: 3,
                flags: 0,
                data: vec![0x57; 512 * 1024],
            }],
            AccessProfile::RandomSmallFile,
            SizeClassTable::default(),
            None,
        )
        .unwrap()
    }
    #[test]
    fn incompressible_eight_mib_frame_falls_back_to_bounded_raw_codec() {
        use rand::{RngCore, SeedableRng};
        let mut data = vec![0u8; 8 * 1024 * 1024];
        rand::rngs::StdRng::seed_from_u64(20261002).fill_bytes(&mut data);
        let input = PackedFileInput {
            name: b"random".to_vec(),
            inode: 3,
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
            data,
        };
        let (group, frames) = pack_group_files(
            1,
            [2; 32],
            vec![input],
            AccessProfile::SequentialSmallFile,
            SizeClassTable::default(),
            None,
        )
        .unwrap();
        let built = build_v3_container(
            0,
            1,
            AccessProfile::SequentialSmallFile,
            SizeClassTable::default(),
            (PackedCodec::Zstd, PackedCodec::Zstd),
            &[group],
            &frames,
        )
        .unwrap();
        assert_eq!(built.frame_pages[0].frames[0].codec, PackedCodec::Raw as u8);
        assert_eq!(built.frame_pages[0].frames[0].stored_len, 8 * 1024 * 1024);
    }

    #[tokio::test]
    async fn prefix_expanded_metadata_owns_decoded_capacities_and_releases_on_last_arc() {
        let files = (0..2000)
            .map(|i| {
                let mut name = vec![b'a'; 1020];
                name.extend_from_slice(format!("{i:04}").as_bytes());
                PackedFileInput {
                    name,
                    inode: i + 2,
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
                    data: vec![],
                }
            })
            .collect();
        let (group, frames) = pack_group_files(
            1,
            [2; 32],
            files,
            AccessProfile::RandomSmallFile,
            SizeClassTable::default(),
            None,
        )
        .unwrap();
        let built = build_v3_container(
            0,
            1,
            AccessProfile::RandomSmallFile,
            SizeClassTable::default(),
            (PackedCodec::Raw, PackedCodec::Raw),
            &[group],
            &frames,
        )
        .unwrap();
        let container = V3ObjectRef::from_bytes(
            "expanded".into(),
            V3ObjectKind::GroupContainer,
            &built.bytes,
        )
        .unwrap();
        let temp = tempfile::tempdir().unwrap();
        let client = ObjectClient::new(LocalFsBackend::new(temp.path()));
        client
            .put_object(&container.key, &built.bytes)
            .await
            .unwrap();
        let budget = super::super::V3MountBudget::defaults();
        let metadata = built.groups[0]
            .read_metadata_owned(&client, &container, 512 << 10, &budget)
            .await
            .unwrap();
        assert_eq!(metadata.len(), 2000);
        let owned = budget.state().used[super::super::V3BudgetPool::Metadata as usize];
        assert!(owned > 2 << 20);
        assert_eq!(owned, metadata.owned_memory_bytes() as u64);
        let consumer = metadata.clone();
        drop(metadata);
        assert_eq!(
            budget.state().used[super::super::V3BudgetPool::Metadata as usize],
            owned
        );
        drop(consumer);
        assert_eq!(budget.state().used, [0; 8]);
    }

    #[tokio::test]
    async fn directory_metadata_checks_context_retries_failures_and_preserves_owners() {
        let (group, frames) = inputs();
        for codec in [PackedCodec::Raw, PackedCodec::Zstd] {
            let built = build_v3_container(
                0,
                1,
                AccessProfile::RandomSmallFile,
                SizeClassTable::default(),
                (codec, codec),
                std::slice::from_ref(&group),
                &frames,
            )
            .unwrap();
            let container = V3ObjectRef::from_bytes(
                "directory".into(),
                V3ObjectKind::GroupContainer,
                &built.bytes,
            )
            .unwrap();
            let group = &built.groups[0];
            let temp = tempfile::tempdir().unwrap();
            let client = ObjectClient::new(LocalFsBackend::new(temp.path()));
            client
                .put_object(&container.key, &built.bytes)
                .await
                .unwrap();
            let budget = super::super::V3MountBudget::defaults();
            let cache = V3DirectoryMetadata::new(budget.clone()).unwrap();
            let held = cache
                .read(group, &client, &container, 512 << 10)
                .await
                .unwrap();
            assert!(Arc::ptr_eq(
                &held,
                &cache
                    .read(group, &client, &container, 512 << 10)
                    .await
                    .unwrap()
            ));
            assert!(cache.read(group, &client, &container, 1).await.is_err());
            for field in 0..3 {
                let mut wrong = group.clone();
                match field {
                    0 => wrong.entry_count += 1,
                    1 => wrong.first_name = b"fild".to_vec(),
                    _ => wrong.first_frame += 1,
                }
                // These references still point to the same authenticated bytes.
                assert!(
                    cache
                        .read(&wrong, &client, &container, 512 << 10)
                        .await
                        .is_err()
                );
                cache
                    .read(group, &client, &container, 512 << 10)
                    .await
                    .unwrap();
            }
            let mut outside = container.clone();
            outside.object_len = (V3_HEADER_LEN + V3_FOOTER_LEN) as u64;
            assert!(
                cache
                    .read(group, &client, &outside, 512 << 10)
                    .await
                    .is_err()
            );
            let mut missing = container.clone();
            missing.key = "missing".into();
            assert!(
                cache
                    .read(group, &client, &missing, 512 << 10)
                    .await
                    .is_err()
            );
            // A failed authentication must leave the cell retryable.
            let mut corrupt = built.bytes.clone();
            corrupt[group.meta_offset as usize] ^= 1;
            client.put_object(&container.key, &corrupt).await.unwrap();
            assert!(
                cache
                    .read(group, &client, &container, 512 << 10)
                    .await
                    .is_err()
            );
            client
                .put_object(&container.key, &built.bytes)
                .await
                .unwrap();
            let retried = cache
                .read(group, &client, &container, 512 << 10)
                .await
                .unwrap();
            assert_eq!(retried.entries(), held.entries());
            drop(retried);
            budget.close();
            assert!(
                cache
                    .read(group, &client, &container, 512 << 10)
                    .await
                    .is_err()
            );
            drop(cache);
            assert_eq!(
                budget.state().used[super::super::V3BudgetPool::Metadata as usize],
                held.owned_memory_bytes() as u64
            );
            drop(held);
            assert_eq!(budget.state().used, [0; 8]);
        }
    }

    #[tokio::test]
    async fn v3_container_roundtrip_compresses_metadata_and_frames_independently() {
        let (group, frames) = inputs();
        for codec in [PackedCodec::Raw, PackedCodec::Zstd] {
            let built = build_v3_container(
                0,
                1,
                AccessProfile::RandomSmallFile,
                SizeClassTable::default(),
                (codec, codec),
                std::slice::from_ref(&group),
                &frames,
            )
            .unwrap();
            let container = V3ObjectRef::from_bytes(
                "container".into(),
                V3ObjectKind::GroupContainer,
                &built.bytes,
            )
            .unwrap();
            container.verify(&built.bytes, V3_MAX_BODY_BYTES).unwrap();
            let temp = tempfile::tempdir().unwrap();
            let client = ObjectClient::new(LocalFsBackend::new(temp.path()));
            client
                .put_object(&container.key, &built.bytes)
                .await
                .unwrap();
            let meta = built.groups[0]
                .read_metadata(&client, &container, 512 * 1024)
                .await
                .unwrap();
            assert_eq!(meta.entries()[0].name, b"file");
            assert_eq!(
                built.frame_pages[0]
                    .read_frame(&client, &container, 0, 2 * 1024 * 1024)
                    .await
                    .unwrap(),
                vec![0x57; 512 * 1024]
            );
            let encoded = built.groups[0].encode_value().unwrap();
            assert_eq!(V3GroupRef::decode_value(&encoded).unwrap(), built.groups[0]);
        }
    }

    #[tokio::test]
    async fn v3_authenticated_group_rejects_invalid_kind_with_self_consistent_hashes() {
        let (group, frames) = inputs();
        let built = build_v3_container(
            0,
            1,
            AccessProfile::RandomSmallFile,
            SizeClassTable::default(),
            (PackedCodec::Raw, PackedCodec::Raw),
            &[group],
            &frames,
        )
        .unwrap();
        let original = V3ObjectRef::from_bytes(
            "original".into(),
            V3ObjectKind::GroupContainer,
            &built.bytes,
        )
        .unwrap();
        let mut body = original
            .verify(&built.bytes, V3_MAX_BODY_BYTES)
            .unwrap()
            .to_vec();
        let mut group_ref = built.groups[0].clone();
        let start = group_ref.meta_offset as usize - V3_HEADER_LEN;
        let end = start + group_ref.meta_stored_len as usize;
        // Single GM07 run: 24-byte restart table, 12-byte GM06 header,
        // prefix/suffix lengths, then the four-byte name "file" and kind.
        body[start + 24 + 12 + 4 + 4] = 0;
        group_ref.meta_digest = Sha256::digest(&body[start..end]).into();
        let bytes =
            encode_v3_object(V3ObjectKind::GroupContainer, &body, V3_MAX_BODY_BYTES).unwrap();
        let reference =
            V3ObjectRef::from_bytes("invalid".into(), V3ObjectKind::GroupContainer, &bytes)
                .unwrap();
        reference.verify(&bytes, V3_MAX_BODY_BYTES).unwrap();
        let temp = tempfile::tempdir().unwrap();
        let client = ObjectClient::new(LocalFsBackend::new(temp.path()));
        client.put_object(&reference.key, &bytes).await.unwrap();
        assert!(matches!(
            group_ref
                .read_metadata(&client, &reference, 1024 * 1024)
                .await,
            Err(PackedWireError::UnsupportedFormat(_))
        ));
        let mut location = super::super::V3InodeLocation {
            hot: crate::workspace_overlay::packed_v3::PackedInodeIndexEntry {
                inode: 3,
                parent_inode: 1,
                parent_dir_key: [2; 32],
                group_id: group_ref.group_id,
                entry_ordinal: 0,
                name: b"file".to_vec(),
                kind: 1,
                mode: 0o040644,
                uid: 1000,
                gid: 1000,
                rdev: 0,
                nlink: 1,
                atime_ns: 1,
                mtime_ns: 2,
                ctime_ns: 3,
                size: 512 * 1024,
            },
            group: built.groups[0].clone(),
        };
        assert!(location.encode_value().is_err());
        location.hot.mode = 0o100644;
        let mut encoded = location.encode_value().unwrap();
        encoded[68..72].copy_from_slice(&0o040644u32.to_le_bytes());
        assert!(super::super::V3InodeLocation::decode_value(&encoded).is_err());
    }
}
