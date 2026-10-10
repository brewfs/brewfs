//! Streaming producer bridge: bounded packed groups -> objects -> paged roots
//! -> PM10/IP06. Returning a manifest ref is not a workspace-head publication.

use super::{
    V3IndexRecord, V3IndexSpool, V3IndexValue, V3InodeLocation, V3ObjectKind, V3ObjectRef,
    V3RootKind, V3SnapshotManifest, build_v3_container_with_policy,
};
use crate::cadapter::client::{ObjectBackend, ObjectClient};
use crate::cadapter::read_observer::ReadClass;
use crate::workspace_overlay::packed_v3::wire::{PackedResult, PackedWireError};
use crate::workspace_overlay::packed_v3::{
    AccessProfile, GroupMeta, PackedCodec, PackedFrameInput, PackedGroupInput,
    PackedInodeIndexEntry, SizeClassTable,
};
use sha2::{Digest, Sha256};
use std::path::Path;

#[derive(Clone, Debug)]
pub struct V3ProducerOptions {
    pub snapshot_id: [u8; 32],
    pub root_dir_key: [u8; 32],
    pub root_inode: u64,
    pub profile: AccessProfile,
    pub size_classes: SizeClassTable,
    pub build_policy: super::V3BuildPolicy,
    pub metadata_codec: PackedCodec,
    pub data_codec: PackedCodec,
}

pub struct V3SnapshotProducer<B: ObjectBackend + Clone> {
    client: ObjectClient<B>,
    spool: V3IndexSpool,
    prefix: String,
    options: V3ProducerOptions,
    next_container: u32,
    poisoned: bool,
    root_attributes: Option<super::V3RootAttributes>,
    placement_contract: bool,
    build: super::V3BuildProvenance,
}

impl<B: ObjectBackend + Clone + 'static> V3SnapshotProducer<B> {
    #[cfg(target_os = "linux")]
    pub(crate) fn native_spool_pool(&self) -> sea_orm::sqlx::SqlitePool {
        self.spool.pool.clone()
    }

    #[cfg(target_os = "linux")]
    pub(crate) async fn bound_native_spool(&self, max_disk_bytes: u64) -> PackedResult<()> {
        use sea_orm::sqlx::Row;
        let row = sea_orm::sqlx::query("PRAGMA page_size")
            .fetch_one(&self.spool.pool)
            .await
            .map_err(|error| PackedWireError::Backend(error.to_string()))?;
        let page_size: i64 = row
            .try_get(0)
            .map_err(|error| PackedWireError::Backend(error.to_string()))?;
        let page_size = u64::try_from(page_size)
            .map_err(|_| PackedWireError::Invalid("native producer page size".into()))?;
        let pages = max_disk_bytes
            .checked_div(page_size)
            .ok_or_else(|| PackedWireError::Invalid("native producer zero page size".into()))?;
        if pages < 4 || pages >= u64::from(u32::MAX) {
            return Err(PackedWireError::LimitExceeded(
                "native producer disk quota".into(),
            ));
        }
        let statement = format!("PRAGMA max_page_count={pages}");
        let row = sea_orm::sqlx::query(&statement)
            .fetch_one(&self.spool.pool)
            .await
            .map_err(|error| PackedWireError::Backend(error.to_string()))?;
        let actual: i64 = row
            .try_get(0)
            .map_err(|error| PackedWireError::Backend(error.to_string()))?;
        if actual < 0 || actual as u64 > pages {
            return Err(PackedWireError::LimitExceeded(
                "native producer already exceeds disk quota".into(),
            ));
        }
        sea_orm::sqlx::query("PRAGMA journal_mode=OFF")
            .execute(&self.spool.pool)
            .await
            .map_err(|error| PackedWireError::Backend(error.to_string()))?;
        Ok(())
    }

    /// Preserve the genuine workspace inode IDs, attributes and sparse runs.
    /// This consumes the actual frozen scratch artifact, never local tmp-file
    /// stat attributes or the ordinary namespace importer's inode allocator.
    #[cfg(target_os = "linux")]
    pub(crate) async fn add_frozen_native_source<K, S>(
        &mut self,
        source: &mut super::publication::native_effective::FrozenNativeArtifact<K, S>,
    ) -> PackedResult<()>
    where
        K: crate::workspace_overlay::stores::kv_backend::WorkspaceKvBackend + 'static,
        S: crate::chunk::BlockStore + Send + Sync + 'static,
    {
        if self.poisoned || self.options.root_inode != 1 || self.root_attributes.is_some() {
            return Err(PackedWireError::Invalid(
                "native producer requires fresh original-ID root".into(),
            ));
        }
        source.validate().await?;
        let mut after = 0i64;
        while let Some(inode) = source.next_inode_after(after).await? {
            let hot = &inode.0;
            after = hot.inode;
            if hot.inode == 1 {
                self.set_root_attributes(super::V3RootAttributes {
                    inode: 1,
                    size: hot.size,
                    blocks: hot.blocks,
                    mode: hot.mode,
                    uid: hot.uid,
                    gid: hot.gid,
                    nlink: hot.nlink,
                    atime_ns: hot.atime_ns,
                    mtime_ns: hot.mtime_ns,
                    ctime_ns: hot.ctime_ns,
                })?;
            } else {
                self.set_inode_blocks(hot.inode as u64, hot.blocks).await?;
            }
            if hot.kind == 1 {
                self.add_native_external_source(source, hot).await?;
            }
            source.validate().await?;
        }
        let mut parent = 0i64;
        let mut name = Vec::<u8>::new();
        let mut group_id = 0u64;
        while let Some(edge) = source.next_edge_after(parent, &name).await? {
            group_id = group_id.checked_add(1).ok_or_else(|| {
                PackedWireError::LimitExceeded("native group ID exhausted".into())
            })?;
            parent = edge.parent;
            name = edge.name.clone();
            let hot = &edge.hot;
            let metadata =
                GroupMeta::new(vec![crate::workspace_overlay::packed_v3::GroupMetaEntry {
                    name: name.clone(),
                    inode: hot.inode as u64,
                    kind: hot.kind,
                    mode: hot.mode,
                    uid: hot.uid,
                    gid: hot.gid,
                    rdev: u64::from(hot.rdev),
                    nlink: hot.nlink,
                    atime_ns: hot.atime_ns,
                    mtime_ns: hot.mtime_ns,
                    ctime_ns: hot.ctime_ns,
                    size: hot.size,
                    flags: 0,
                    inline_data: std::sync::Arc::from([]),
                    extents: Vec::new(),
                }])?
                .encode()?;
            let group = PackedGroupInput {
                group_id,
                parent_dir_key: if parent == 1 {
                    self.options.root_dir_key
                } else {
                    crate::workspace_overlay::packed_v3::directory_key(
                        self.options.snapshot_id,
                        parent as u64,
                    )
                },
                metadata,
                frame_ordinals: Vec::new(),
                entry_count: 1,
                file_count: u32::from(hot.kind == 1),
                layout_profile: self.options.profile,
            };
            let _permit = source
                .mount_budget()
                .admit(&[(super::V3BudgetPool::Metadata, 64 << 10)])?;
            self.add_container(
                u64::from(self.next_container),
                &[group],
                &[],
                &[parent as u64],
            )
            .await?;
            source.validate().await?;
        }
        // Namespace containers establish the authenticated inode locations.
        // Cold attributes must reference those locations, including symlinks
        // and hardlink aliases. Keep this pass bounded to one captured inode.
        let mut after = 0i64;
        while let Some(inode) = source.next_inode_after(after).await? {
            after = inode.0.inode;
            self.add_cold_attributes(&inode.1).await?;
            source.validate().await?;
        }
        Ok(())
    }

    #[cfg(target_os = "linux")]
    async fn add_native_external_source<K, S>(
        &mut self,
        source: &mut super::publication::native_effective::FrozenNativeArtifact<K, S>,
        hot: &super::publication::native_effective::NativeHot,
    ) -> PackedResult<()>
    where
        K: crate::workspace_overlay::stores::kv_backend::WorkspaceKvBackend + 'static,
        S: crate::chunk::BlockStore + Send + Sync + 'static,
    {
        let inode = hot.inode as u64;
        let size = hot.size;
        if self.poisoned
            || self.root_attributes.is_none()
            || self.placement(inode, size).await?.is_some()
        {
            return Err(PackedWireError::Invalid(
                "native external placement already exists or producer is interrupted".into(),
            ));
        }
        let decision = self.options.build_policy.select(
            size,
            self.options.profile,
            self.options.size_classes,
        )?;
        let target = decision.frame_raw_bytes.max(1);
        let mut cursor = source.frame_cursor(hot.inode, target, decision.size_class)?;
        self.poisoned = true;
        if !self.placement_contract {
            self.spool.enable_placements().await?;
            self.placement_contract = true;
        }
        let mut index = super::V3IndexBuilder::new(
            self.client.clone(),
            V3ObjectKind::LargeIndex,
            format!("{}/indexes", self.prefix),
            256,
            256 * 1024,
        )?;
        let mut frames = Vec::new();
        let mut offsets = Vec::new();
        let mut owners = Vec::new();
        let mut chunk_raw = 0usize;
        let mut content = Sha256::new();
        content.update(size.to_le_bytes());
        let mut run_start: Option<u64> = None;
        let mut run_length = 0u64;
        let mut run = Sha256::new();
        let mut observed_bytes = 0u64;
        let mut observed_frames = 0u64;
        loop {
            // Flush before admitting the next maximum frame. Holding a full
            // chunk plus one new frame would falsely exhaust the default Raw
            // pool even though every legal producer chunk is bounded.
            if !frames.is_empty()
                && (chunk_raw + target as usize > super::large_chunk::V3_LARGE_CHUNK_RAW_LIMIT
                    || frames.len() == super::large_chunk::V3_LARGE_CHUNK_FRAME_LIMIT)
            {
                self.flush_external_chunk(inode, &frames, &offsets, &mut index)
                    .await?;
                frames.clear();
                offsets.clear();
                owners.clear();
                chunk_raw = 0;
            }
            let Some(owned) = source.next_data_frame(&mut cursor).await? else {
                break;
            };
            let offset = owned.offset;
            let frame = owned.frame;
            if run_start.is_some_and(|start| start + run_length != offset) {
                content.update(run_start.unwrap().to_le_bytes());
                content.update(run_length.to_le_bytes());
                content.update(run.finalize());
                run = Sha256::new();
                run_start = None;
                run_length = 0;
            }
            if run_start.is_none() {
                run_start = Some(offset);
            }
            run.update(&frame.raw);
            run_length += frame.raw.len() as u64;
            observed_bytes += frame.raw.len() as u64;
            observed_frames += 1;
            chunk_raw += frame.raw.len();
            offsets.push(offset);
            frames.push(frame);
            owners.push(owned.permit);
        }
        if let Some(start) = run_start {
            content.update(start.to_le_bytes());
            content.update(run_length.to_le_bytes());
            content.update(run.finalize());
        }
        if !frames.is_empty() {
            self.flush_external_chunk(inode, &frames, &offsets, &mut index)
                .await?;
        }
        // The authenticated candidate is still compared with every actual
        // captured span before any native publication authority is issued.
        let placement = super::V3Placement::External {
            inode,
            size,
            data_bytes: observed_bytes,
            extent_count: observed_frames,
            logical_digest: content.finalize().into(),
            extents: index.finish().await?,
        };
        let key = inode.to_be_bytes().to_vec();
        self.spool
            .insert(
                V3RootKind::LargePlacements,
                &V3IndexRecord {
                    first_key: key.clone(),
                    last_key: key,
                    value: V3IndexValue::Leaf(placement.encode()?),
                },
            )
            .await?;
        self.poisoned = false;
        Ok(())
    }

    pub async fn new(
        client: ObjectClient<B>,
        directory: &Path,
        prefix: String,
        options: V3ProducerOptions,
    ) -> PackedResult<Self> {
        let build = Self::validate_options(&prefix, &options)?;
        let spool = V3IndexSpool::create(directory).await?;
        Ok(Self::from_spool(client, prefix, options, build, spool))
    }

    #[cfg(target_os = "linux")]
    pub(crate) async fn new_native(
        client: ObjectClient<B>,
        directory: &Path,
        prefix: String,
        options: V3ProducerOptions,
        owner: std::sync::Arc<super::V3OwnedPermit>,
        max_disk_bytes: u64,
    ) -> PackedResult<Self> {
        let build = Self::validate_options(&prefix, &options)?;
        let spool = V3IndexSpool::create_native(directory, owner, max_disk_bytes).await?;
        Ok(Self::from_spool(client, prefix, options, build, spool))
    }

    fn validate_options(
        prefix: &str,
        options: &V3ProducerOptions,
    ) -> PackedResult<super::V3BuildProvenance> {
        super::validate_key(prefix)?;
        if prefix.len() > 3900
            || options.snapshot_id == [0; 32]
            || options.root_dir_key == [0; 32]
            || options.root_inode == 0
            || options.root_inode > i64::MAX as u64
        {
            return Err(PackedWireError::Invalid(
                "wire 005 producer identity/prefix is invalid".into(),
            ));
        }

        options
            .size_classes
            .validate()
            .map_err(|_| PackedWireError::Invalid("invalid wire 005 producer size table".into()))?;
        options
            .build_policy
            .select(1, options.profile, options.size_classes)?;
        Ok(super::V3BuildProvenance {
            policy: options.build_policy,
            requested_metadata_codec: options.metadata_codec as u8,
            requested_data_codec: options.data_codec as u8,
            ..Default::default()
        })
    }

    fn from_spool(
        client: ObjectClient<B>,
        prefix: String,
        options: V3ProducerOptions,
        build: super::V3BuildProvenance,
        spool: V3IndexSpool,
    ) -> Self {
        Self {
            client,
            spool,
            prefix,
            options,
            next_container: 0,
            poisoned: false,
            root_attributes: None,
            placement_contract: false,
            build,
        }
    }

    /// Opt in to PM08. Every non-root inode must then supply an allocation
    /// record before finish; no attribute can silently revert to PM07.
    pub fn set_root_attributes(&mut self, attributes: super::V3RootAttributes) -> PackedResult<()> {
        if self.poisoned {
            return Err(PackedWireError::Invalid(
                "PM08 producer was interrupted".into(),
            ));
        }
        self.poisoned = true;
        attributes.validate()?;
        if attributes.inode != self.options.root_inode
            || self
                .root_attributes
                .as_ref()
                .is_some_and(|old| old != &attributes)
        {
            self.poisoned = true;
            return Err(PackedWireError::Invalid(
                "PM08 root attributes conflict with producer identity/state".into(),
            ));
        }
        self.root_attributes = Some(attributes);
        self.poisoned = false;
        Ok(())
    }

    pub async fn set_inode_blocks(&mut self, inode: u64, blocks: u64) -> PackedResult<()> {
        self.set_inode_blocks_batch(&[(inode, blocks)]).await
    }

    pub(crate) async fn set_inode_blocks_batch(
        &mut self,
        values: &[(u64, u64)],
    ) -> PackedResult<()> {
        if self.poisoned
            || values
                .iter()
                .any(|(inode, _)| *inode == self.options.root_inode)
        {
            self.poisoned = true;
            return Err(PackedWireError::Invalid(
                "source allocation cannot replace root attributes or interrupted state".into(),
            ));
        }
        self.poisoned = true;
        self.spool.set_allocations(values).await?;
        self.poisoned = false;
        Ok(())
    }

    async fn upload(&self, kind: V3ObjectKind, bytes: &[u8]) -> PackedResult<V3ObjectRef> {
        let digest: [u8; 32] = Sha256::digest(bytes).into();
        let key = format!(
            "{}/objects/{}/{}",
            self.prefix,
            kind as u8,
            hex::encode(digest)
        );
        let reference = V3ObjectRef::from_bytes(key, kind, bytes)?;
        self.client
            .put_object_create_only(&reference.key, bytes)
            .await
            .map_err(|error| PackedWireError::Backend(error.to_string()))?;
        // Verify stored bytes through bounded streams, including backends that
        // fall back from unsupported conditional PUT. Never publish an index
        // ref to a silently truncated or corrupted container.
        let mut offset = 0usize;
        while offset < bytes.len() {
            let take = (bytes.len() - offset).min(8 * 1024 * 1024);
            let expected = &bytes[offset..offset + take];
            self.client
                .typed_exact(
                    ReadClass::PublicationVerification,
                    &reference.key,
                    offset as u64,
                    take as u64,
                    take as u64,
                    |observed| {
                        if observed.as_slice() != expected {
                            return Err(super::observer_validation_error(
                                PackedWireError::HashMismatch {
                                    what: "packed-v3 upload verification",
                                    expected: hex::encode(Sha256::digest(expected)),
                                    computed: hex::encode(Sha256::digest(&observed)),
                                },
                            ));
                        }
                        Ok(())
                    },
                )
                .await
                .map_err(|error| {
                    error
                        .downcast::<PackedWireError>()
                        .unwrap_or_else(|error| PackedWireError::Backend(error.to_string()))
                })?;
            offset += take;
        }
        Ok(reference)
    }

    pub async fn placement(
        &self,
        inode: u64,
        size: u64,
    ) -> PackedResult<Option<super::V3Placement>> {
        self.spool
            .get(V3RootKind::LargePlacements, &inode.to_be_bytes())
            .await?
            .map(|value| super::V3Placement::decode(&value, inode, size))
            .transpose()
    }

    async fn register_payload(
        &self,
        ordinal: u32,
        kind: V3ObjectKind,
        bytes: &[u8],
        pages: &[super::V3FrameDirectoryPage],
    ) -> PackedResult<()> {
        let container = self.upload(kind, bytes).await?;
        let key = ordinal.to_be_bytes().to_vec();
        self.spool
            .insert(
                V3RootKind::Containers,
                &V3IndexRecord {
                    first_key: key.clone(),
                    last_key: key,
                    value: V3IndexValue::Leaf(container.encode_value()?),
                },
            )
            .await?;
        for page in pages {
            let reference = self
                .upload(V3ObjectKind::FrameDirectory, &page.encode()?)
                .await?;
            let mut first = ordinal.to_be_bytes().to_vec();
            first.extend_from_slice(&page.first_ordinal.to_be_bytes());
            let mut last = ordinal.to_be_bytes().to_vec();
            last.extend_from_slice(&page.frames.last().unwrap().frame_ordinal.to_be_bytes());
            self.spool
                .insert(
                    V3RootKind::Frames,
                    &V3IndexRecord {
                        first_key: first,
                        last_key: last,
                        value: V3IndexValue::Leaf(reference.encode_value()?),
                    },
                )
                .await?;
        }
        Ok(())
    }

    /// Streams a pinned file into bounded LD05 chunks and a paged LE09 tree.
    /// Cancellation/failure leaves the producer poisoned; no partial selector
    /// or PM08 downgrade can be published by finish.
    #[cfg(target_os = "linux")]
    pub async fn add_external_source(
        &mut self,
        source: &mut super::CapturedV3SourceLayout,
    ) -> PackedResult<u64> {
        if self.poisoned || self.root_attributes.is_none() {
            return Err(PackedWireError::Invalid(
                "external placement requires source attributes and a live producer".into(),
            ));
        }
        if source.build_policy() != self.options.build_policy
            || source.frame_target()
                != self
                    .options
                    .build_policy
                    .select(
                        source.entry().size,
                        self.options.profile,
                        self.options.size_classes,
                    )?
                    .frame_raw_bytes
        {
            return Err(PackedWireError::Invalid(
                "external capture/producer build policies differ".into(),
            ));
        }
        self.poisoned = true;
        let initial_container = self.next_container;
        let inode = source.entry().inode;
        let size = source.entry().size;
        if self.placement(inode, size).await?.is_some() {
            return Err(PackedWireError::Invalid(
                "external inode already has a placement".into(),
            ));
        }
        if !self.placement_contract {
            self.spool.enable_placements().await?;
            self.placement_contract = true;
        }
        let mut index = super::V3IndexBuilder::new(
            self.client.clone(),
            V3ObjectKind::LargeIndex,
            format!("{}/indexes", self.prefix),
            256,
            256 * 1024,
        )?;
        let mut frames = Vec::new();
        let mut offsets = Vec::new();
        let mut chunk_raw = 0usize;
        let mut content = Sha256::new();
        content.update(size.to_le_bytes());
        let mut run_start: Option<u64> = None;
        let mut run_length = 0u64;
        let mut run = Sha256::new();
        let mut observed_bytes = 0u64;
        let mut observed_frames = 0u64;
        while let Some((offset, frame)) = source.next_frame().await? {
            if !frames.is_empty()
                && (chunk_raw + frame.raw.len() > super::large_chunk::V3_LARGE_CHUNK_RAW_LIMIT
                    || frames.len() == super::large_chunk::V3_LARGE_CHUNK_FRAME_LIMIT)
            {
                self.flush_external_chunk(inode, &frames, &offsets, &mut index)
                    .await?;
                frames.clear();
                offsets.clear();
                chunk_raw = 0;
            }
            if run_start.is_some_and(|start| start + run_length != offset) {
                content.update(run_start.unwrap().to_le_bytes());
                content.update(run_length.to_le_bytes());
                content.update(run.finalize());
                run = Sha256::new();
                run_start = None;
                run_length = 0;
            }
            if run_start.is_none() {
                run_start = Some(offset);
            }
            run.update(&frame.raw);
            run_length += frame.raw.len() as u64;
            observed_bytes += frame.raw.len() as u64;
            observed_frames += 1;
            chunk_raw += frame.raw.len();
            offsets.push(offset);
            frames.push(frame);
        }
        if let Some(start) = run_start {
            content.update(start.to_le_bytes());
            content.update(run_length.to_le_bytes());
            content.update(run.finalize());
        }
        if !frames.is_empty() {
            self.flush_external_chunk(inode, &frames, &offsets, &mut index)
                .await?;
        }
        if observed_bytes != source.data_bytes() || observed_frames != source.frame_count() {
            return Err(PackedWireError::Invalid(
                "external source inventory/stream totals disagree".into(),
            ));
        }
        source.validate_unchanged()?;
        let placement = super::V3Placement::External {
            inode,
            size,
            data_bytes: observed_bytes,
            extent_count: observed_frames,
            logical_digest: content.finalize().into(),
            extents: index.finish().await?,
        };
        source.validate_unchanged()?;
        let key = inode.to_be_bytes().to_vec();
        self.spool
            .insert(
                V3RootKind::LargePlacements,
                &V3IndexRecord {
                    first_key: key.clone(),
                    last_key: key,
                    value: V3IndexValue::Leaf(placement.encode()?),
                },
            )
            .await?;
        self.poisoned = false;
        Ok(u64::from(self.next_container - initial_container))
    }

    #[cfg(target_os = "linux")]
    async fn flush_external_chunk(
        &mut self,
        inode: u64,
        frames: &[PackedFrameInput],
        offsets: &[u64],
        index: &mut super::V3IndexBuilder<B>,
    ) -> PackedResult<()> {
        let ordinal = self.next_container;
        let built = super::large_chunk::build_large_chunk_with_policy(
            inode,
            u64::from(ordinal),
            self.options.profile,
            self.options.size_classes,
            self.options.data_codec,
            frames,
            self.options.build_policy,
        )?;
        let mut distribution = self.build.clone();
        for frame in &built.directory.frames {
            distribution.observe_frame(frame, true)?;
        }
        self.build = distribution;
        self.register_payload(
            ordinal,
            V3ObjectKind::LargeData,
            &built.bytes,
            &[built.directory],
        )
        .await?;
        for (frame_ordinal, (frame, offset)) in frames.iter().zip(offsets).enumerate() {
            index
                .push(
                    super::V3LargeExtent {
                        inode,
                        file_offset: *offset,
                        logical_len: frame.raw.len() as u32,
                        container_ordinal: ordinal,
                        frame_ordinal: frame_ordinal as u32,
                        raw_offset: 0,
                        raw_len: frame.raw.len() as u32,
                    }
                    .record()?,
                )
                .await?;
        }
        self.next_container = ordinal.checked_add(1).ok_or_else(|| {
            PackedWireError::LimitExceeded("external container ordinal overflows".into())
        })?;
        Ok(())
    }

    pub async fn add_container(
        &mut self,
        container_id: u64,
        groups: &[PackedGroupInput],
        frames: &[PackedFrameInput],
        parent_inodes: &[u64],
    ) -> PackedResult<()> {
        if self.poisoned || groups.len() != parent_inodes.len() || parent_inodes.contains(&0) {
            return Err(PackedWireError::Invalid(
                "wire 005 producer is interrupted or has invalid parents".into(),
            ));
        }
        let built = build_v3_container_with_policy(
            self.next_container,
            container_id,
            self.options.profile,
            self.options.size_classes,
            self.options.metadata_codec,
            self.options.data_codec,
            groups,
            frames,
            self.options.build_policy,
        )?;
        let mut distribution = self.build.clone();
        for page in &built.frame_pages {
            for frame in &page.frames {
                distribution.observe_frame(frame, false)?;
            }
        }
        for (input, group) in groups.iter().zip(&built.groups) {
            distribution.observe_group(group, &GroupMeta::decode(&input.metadata)?)?;
        }
        self.poisoned = true;
        self.build = distribution;
        let ordinal = self.next_container;
        self.register_payload(
            ordinal,
            V3ObjectKind::GroupContainer,
            &built.bytes,
            &built.frame_pages,
        )
        .await?;
        for ((input, group), parent_inode) in groups.iter().zip(&built.groups).zip(parent_inodes) {
            let metadata = GroupMeta::decode(&input.metadata)?;
            let mut first = group.parent_dir_key.to_vec();
            first.extend_from_slice(&group.first_name);
            let mut last = group.parent_dir_key.to_vec();
            last.extend_from_slice(&group.last_name);
            self.spool
                .insert(
                    V3RootKind::Groups,
                    &V3IndexRecord {
                        first_key: first,
                        last_key: last,
                        value: V3IndexValue::Leaf(group.encode_value()?),
                    },
                )
                .await?;
            // Object upload/verification is complete before holding the single
            // spool connection. A cancelled/error batch rolls back and leaves
            // this producer poisoned, including already committed prefixes.
            for (batch_ordinal, entries) in metadata
                .entries()
                .chunks(super::spool::V3_SPOOL_BATCH_ENTRIES)
                .enumerate()
            {
                let mut batch = self.spool.batch().await?;
                for (entry_offset, entry) in entries.iter().enumerate() {
                    let entry_ordinal =
                        batch_ordinal * super::spool::V3_SPOOL_BATCH_ENTRIES + entry_offset;
                    if entry.inode == self.options.root_inode {
                        return Err(PackedWireError::Invalid(
                            "root inode cannot also be a directory entry".into(),
                        ));
                    }
                    let location = V3InodeLocation {
                        group: group.clone(),
                        hot: PackedInodeIndexEntry {
                            inode: entry.inode,
                            parent_inode: *parent_inode,
                            parent_dir_key: group.parent_dir_key,
                            group_id: group.group_id,
                            entry_ordinal: entry_ordinal as u32,
                            name: entry.name.clone(),
                            kind: entry.kind,
                            mode: entry.mode,
                            uid: entry.uid,
                            gid: entry.gid,
                            rdev: entry.rdev,
                            nlink: entry.nlink,
                            atime_ns: entry.atime_ns,
                            mtime_ns: entry.mtime_ns,
                            ctime_ns: entry.ctime_ns,
                            size: entry.size,
                        },
                    };
                    let key = entry.inode.to_be_bytes().to_vec();
                    let external_digest = if self.placement_contract && entry.kind == 1 {
                        let placement = batch
                            .get(V3RootKind::LargePlacements, &key)
                            .await?
                            .map(|value| {
                                super::V3Placement::decode(&value, entry.inode, entry.size)
                            })
                            .transpose()?;
                        match placement {
                            Some(super::V3Placement::External { logical_digest, .. }) => {
                                if entry.flags != 0
                                    || !entry.inline_data.is_empty()
                                    || !entry.extents.is_empty()
                                {
                                    return Err(PackedWireError::Invalid(
                                        "external inode carries conflicting GroupMeta payload"
                                            .into(),
                                    ));
                                }
                                Some(logical_digest)
                            }
                            Some(super::V3Placement::Group { .. }) => None,
                            None => {
                                batch
                                    .insert(
                                        V3RootKind::LargePlacements,
                                        &V3IndexRecord {
                                            first_key: key.clone(),
                                            last_key: key.clone(),
                                            value: V3IndexValue::Leaf(
                                                super::V3Placement::Group {
                                                    inode: entry.inode,
                                                    size: entry.size,
                                                }
                                                .encode()?,
                                            ),
                                        },
                                    )
                                    .await?;
                                None
                            }
                        }
                    } else {
                        None
                    };
                    let signature = inode_signature(entry, frames, external_digest)?;
                    let first = batch
                        .claim_inode(entry.inode, entry.kind, entry.nlink, signature)
                        .await?;
                    let record = V3IndexRecord {
                        first_key: key.clone(),
                        last_key: key.clone(),
                        value: V3IndexValue::Leaf(location.encode_value()?),
                    };
                    if first {
                        batch.insert(V3RootKind::Inodes, &record).await?;
                    } else {
                        let existing =
                            batch.get(V3RootKind::Inodes, &key).await?.ok_or_else(|| {
                                PackedWireError::Invalid(
                                    "hardlink master locator is missing".into(),
                                )
                            })?;
                        let old = V3InodeLocation::decode_value(&existing)?;
                        if (&location.hot.parent_dir_key, &location.hot.name)
                            < (&old.hot.parent_dir_key, &old.hot.name)
                        {
                            batch.replace(V3RootKind::Inodes, &record).await?;
                        }
                    }
                    let mut key = entry.inode.to_be_bytes().to_vec();
                    key.extend_from_slice(&parent_inode.to_be_bytes());
                    key.extend_from_slice(&entry.name);
                    batch
                        .insert(
                            V3RootKind::ReverseNames,
                            &V3IndexRecord {
                                first_key: key.clone(),
                                last_key: key,
                                value: V3IndexValue::Leaf(location.encode_value()?),
                            },
                        )
                        .await?;
                }
                batch.commit().await?;
            }
        }
        self.next_container = self.next_container.checked_add(1).ok_or_else(|| {
            PackedWireError::LimitExceeded("wire 005 container ordinal overflows".into())
        })?;
        self.poisoned = false;
        Ok(())
    }

    pub async fn add_cold_attributes(
        &mut self,
        attributes: &super::V3ColdAttributes,
    ) -> PackedResult<()> {
        if self.poisoned {
            return Err(PackedWireError::Invalid(
                "wire 005 producer was interrupted".into(),
            ));
        }
        let bytes = attributes.encode()?;
        if attributes.inode != self.options.root_inode {
            let value = self
                .spool
                .get(V3RootKind::Inodes, &attributes.inode.to_be_bytes())
                .await?
                .ok_or_else(|| {
                    PackedWireError::Invalid(
                        "cold attributes reference an unpublished inode".into(),
                    )
                })?;
            let location = V3InodeLocation::decode_value(&value)?;
            attributes.validate_for_inode(location.hot.kind, location.hot.mode)?;
            match (&attributes.symlink_target, location.hot.kind) {
                (Some(target), 3) if target.len() as u64 == location.hot.size => {}
                (None, kind) if kind != 3 => {}
                _ => {
                    return Err(PackedWireError::Invalid(
                        "cold target kind/size disagrees with inode".into(),
                    ));
                }
            }
        } else if attributes.symlink_target.is_some() {
            return Err(PackedWireError::Invalid(
                "root directory cannot have symlink target".into(),
            ));
        } else {
            attributes.validate_for_inode(
                2,
                self.root_attributes
                    .as_ref()
                    .map_or(0o040755, |root| root.mode),
            )?;
        }
        if let Some(existing) = self
            .spool
            .get(V3RootKind::ColdAttributes, &attributes.inode.to_be_bytes())
            .await?
        {
            let reference = V3ObjectRef::decode_value(&existing)?;
            let digest: [u8; 32] = Sha256::digest(&bytes).into();
            if reference.kind == V3ObjectKind::ColdAttributes
                && reference.digest == digest
                && reference.object_len == bytes.len() as u64
            {
                return Ok(());
            }
            self.poisoned = true;
            return Err(PackedWireError::Invalid(
                "hardlink cold attributes disagree for one inode".into(),
            ));
        }
        self.poisoned = true;
        let reference = self.upload(V3ObjectKind::ColdAttributes, &bytes).await?;
        let key = attributes.inode.to_be_bytes().to_vec();
        self.spool
            .insert(
                V3RootKind::ColdAttributes,
                &V3IndexRecord {
                    first_key: key.clone(),
                    last_key: key,
                    value: V3IndexValue::Leaf(reference.encode_value()?),
                },
            )
            .await?;
        self.poisoned = false;
        Ok(())
    }

    pub async fn finish(mut self) -> PackedResult<V3ObjectRef> {
        if self.poisoned {
            return Err(PackedWireError::Invalid(
                "wire 005 producer was interrupted before object closure".into(),
            ));
        }
        self.poisoned = true;
        if let Some(value) = self
            .spool
            .get(
                V3RootKind::ColdAttributes,
                &self.options.root_inode.to_be_bytes(),
            )
            .await?
        {
            let reference = V3ObjectRef::decode_value(&value)?;
            let bytes =
                super::read_v3_page(&self.client, &reference, super::cold::V3_COLD_BODY_LIMIT)
                    .await?;
            super::V3ColdAttributes::decode(&reference, &bytes, self.options.root_inode)?
                .validate_for_inode(
                    2,
                    self.root_attributes
                        .as_ref()
                        .map_or(0o040755, |root| root.mode),
                )?;
        }
        self.spool.validate_inode_closure().await?;
        self.spool
            .validate_source_closure(self.root_attributes.is_some())
            .await?;
        self.spool
            .validate_placement_closure(self.placement_contract)
            .await?;
        let kinds = [
            V3RootKind::Groups,
            V3RootKind::Inodes,
            V3RootKind::Containers,
            V3RootKind::Frames,
            V3RootKind::ColdAttributes,
            V3RootKind::ReverseNames,
            V3RootKind::LargePlacements,
        ];
        let mut roots = Vec::with_capacity(kinds.len());
        for kind in kinds {
            roots.push(
                self.spool
                    .build_index(
                        self.client.clone(),
                        kind,
                        format!("{}/indexes", self.prefix),
                    )
                    .await?,
            );
        }
        let source = if let Some(root) = self.root_attributes.clone() {
            Some(super::V3SourceAttributes {
                placement_contract: self.placement_contract,
                root,
                allocations: self
                    .spool
                    .build_source_index(self.client.clone(), format!("{}/indexes", self.prefix))
                    .await?,
            })
        } else {
            None
        };
        let group_dentry_count = super::V3IndexReader::new(self.client.clone(), 0)
            .total_weight(&roots[V3RootKind::Groups as usize])
            .await?;
        let manifest = V3SnapshotManifest {
            snapshot_id: self.options.snapshot_id,
            root_dir_key: self.options.root_dir_key,
            root_inode: self.options.root_inode,
            group_dentry_count,
            build: self.build.clone(),
            profile: self.options.profile,
            size_classes: self.options.size_classes,
            roots: roots.try_into().map_err(|_| {
                PackedWireError::Invalid("wire 005 producer root count mismatch".into())
            })?,
            source,
        };
        let reference = self
            .upload(V3ObjectKind::Manifest, &manifest.encode()?)
            .await?;
        super::AuthenticatedV3Snapshot::open(&self.client, &reference).await?;
        Ok(reference)
    }
}

/// Namespace names, physical frame boundaries, inline admission and codecs
/// are not inode identity. Sparse run boundaries and hot attributes are.
fn inode_signature(
    entry: &crate::workspace_overlay::packed_v3::GroupMetaEntry,
    frames: &[PackedFrameInput],
    external_digest: Option<[u8; 32]>,
) -> PackedResult<[u8; 32]> {
    let mut identity = Sha256::new();
    // This signature compares aliases within the disposable build inventory.
    // It is not serialized into the published packed objects.
    identity.update(b"BrewFS-packed-v3-inode-identity\0");
    for value in [
        u64::from(entry.kind),
        u64::from(entry.mode),
        u64::from(entry.uid),
        u64::from(entry.gid),
        entry.rdev,
        u64::from(entry.nlink),
        entry.atime_ns as u64,
        entry.mtime_ns as u64,
        entry.ctime_ns as u64,
        entry.size,
        u64::from(entry.flags & !crate::workspace_overlay::packed_v3::INLINE_DATA_FLAG),
    ] {
        identity.update(value.to_le_bytes());
    }
    let mut content = Sha256::new();
    content.update(entry.size.to_le_bytes());
    if !entry.inline_data.is_empty() {
        content.update(0u64.to_le_bytes());
        content.update(entry.size.to_le_bytes());
        content.update(Sha256::digest(&entry.inline_data));
    } else {
        let mut start = None;
        let mut length = 0u64;
        let mut run = Sha256::new();
        for extent in &entry.extents {
            if start
                .is_some_and(|offset: u64| offset.checked_add(length) != Some(extent.file_offset))
            {
                content.update(start.unwrap().to_le_bytes());
                content.update(length.to_le_bytes());
                content.update(run.finalize());
                run = Sha256::new();
                start = None;
                length = 0;
            }
            if start.is_none() {
                start = Some(extent.file_offset);
            }
            let frame = frames.get(extent.frame_ordinal as usize).ok_or_else(|| {
                PackedWireError::Invalid("hardlink extent frame is missing".into())
            })?;
            let first = extent.raw_offset as usize;
            let last = first
                .checked_add(extent.logical_len as usize)
                .ok_or_else(|| {
                    PackedWireError::LimitExceeded("hardlink raw range overflows".into())
                })?;
            run.update(frame.raw.get(first..last).ok_or_else(|| {
                PackedWireError::Invalid("hardlink extent exceeds raw frame".into())
            })?);
            length += u64::from(extent.logical_len);
        }
        if let Some(start) = start {
            content.update(start.to_le_bytes());
            content.update(length.to_le_bytes());
            content.update(run.finalize());
        }
    }
    identity.update(external_digest.unwrap_or_else(|| content.finalize().into()));
    Ok(identity.finalize().into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cadapter::localfs::LocalFsBackend;
    use crate::workspace_overlay::packed_v3::{PackedFileInput, pack_group_files};

    fn g15_file(name: &str, inode: u64, bytes: usize) -> PackedFileInput {
        PackedFileInput {
            name: name.as_bytes().to_vec(),
            inode,
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
            data: vec![inode as u8; bytes],
        }
    }

    #[tokio::test]
    async fn g15_static_tiny_copack_authenticates_file_class_and_honest_inline_distribution() {
        use super::super::{AuthenticatedV3Snapshot, V3BuildPolicy, V3FramePolicy, V3IndexReader};
        use crate::workspace_overlay::packed_v3::{SizeClass, pack_group_files_with_policy};
        for inline_data in [false, true] {
            let objects = tempfile::tempdir().unwrap();
            let scratch = tempfile::tempdir().unwrap();
            let client = ObjectClient::new(LocalFsBackend::new(objects.path()));
            let policy = V3BuildPolicy {
                frames: V3FramePolicy::Static1Mib,
                inline_data,
                p90: None,
            };
            let options = V3ProducerOptions {
                snapshot_id: [1; 32],
                root_dir_key: [2; 32],
                root_inode: 1,
                profile: AccessProfile::RandomSmallFile,
                size_classes: SizeClassTable::default(),
                build_policy: policy,
                metadata_codec: PackedCodec::Raw,
                data_codec: PackedCodec::Raw,
            };
            let files = (2..11)
                .map(|inode| g15_file(&format!("f{inode:02}"), inode, 64 * 1024))
                .collect();
            let (group, frames) = pack_group_files_with_policy(
                1,
                [2; 32],
                files,
                options.profile,
                options.size_classes,
                policy,
            )
            .unwrap();
            assert_eq!(frames.len(), 1);
            assert_eq!(frames[0].size_class, SizeClass::Tiny);
            assert_eq!(
                frames[0].raw.len(),
                if inline_data { 384 * 1024 } else { 576 * 1024 }
            );
            let mut producer = V3SnapshotProducer::new(
                client.clone(),
                scratch.path(),
                "static-tiny".into(),
                options,
            )
            .await
            .unwrap();
            producer
                .add_container(1, &[group], &frames, &[1])
                .await
                .unwrap();
            let reference = producer.finish().await.unwrap();
            let snapshot = AuthenticatedV3Snapshot::open(&client, &reference)
                .await
                .unwrap();
            assert_eq!(snapshot.manifest().build.frame_class_counts, [1, 0, 0, 0]);
            assert_eq!(
                snapshot.manifest().build.frame_raw_size_counts,
                [0, 1, 0, 0]
            );
            assert_eq!(
                snapshot.manifest().build.inline_dentries,
                if inline_data { 3 } else { 0 }
            );
            assert_eq!(
                snapshot.manifest().build.inline_payload_bytes,
                if inline_data { 192 * 1024 } else { 0 }
            );
            let reader = V3IndexReader::new(client.clone(), 0);
            for inode in [2, 10] {
                let mut actual = vec![0; 64 * 1024];
                snapshot
                    .read_inode_range(&client, &reader, inode, 0, &mut actual, 32 * 1024 * 1024)
                    .await
                    .unwrap();
                assert_eq!(actual, vec![inode as u8; 64 * 1024]);
            }
        }
    }

    #[tokio::test]
    async fn g15_controls_change_real_frames_inline_and_authenticated_codec_distributions() {
        use super::super::{AuthenticatedV3Snapshot, V3BuildPolicy, V3FramePolicy, V3IndexReader};
        use crate::workspace_overlay::packed_v3::pack_group_files_with_policy;
        for frames in [V3FramePolicy::SizeOnly, V3FramePolicy::Static1Mib] {
            for (metadata_codec, data_codec) in [
                (PackedCodec::Raw, PackedCodec::Zstd),
                (PackedCodec::Zstd, PackedCodec::Raw),
            ] {
                let objects = tempfile::tempdir().unwrap();
                let scratch = tempfile::tempdir().unwrap();
                let client = ObjectClient::new(LocalFsBackend::new(objects.path()));
                let policy = V3BuildPolicy {
                    frames,
                    inline_data: false,
                    p90: None,
                };
                let options = V3ProducerOptions {
                    snapshot_id: [1; 32],
                    root_dir_key: [2; 32],
                    root_inode: 1,
                    profile: AccessProfile::RandomSmallFile,
                    size_classes: SizeClassTable::default(),
                    build_policy: policy,
                    metadata_codec,
                    data_codec,
                };
                let (group, actual_frames) = pack_group_files_with_policy(
                    1,
                    [2; 32],
                    vec![
                        g15_file("a-tiny", 2, 64 * 1024),
                        g15_file("b-medium", 3, 2560 * 1024),
                    ],
                    options.profile,
                    options.size_classes,
                    policy,
                )
                .unwrap();
                let metadata = GroupMeta::decode(&group.metadata).unwrap();
                assert!(
                    metadata
                        .entries()
                        .iter()
                        .all(|entry| entry.inline_data.is_empty())
                );
                assert_eq!(
                    actual_frames.len(),
                    if frames == V3FramePolicy::SizeOnly {
                        2
                    } else {
                        4
                    }
                );
                let mut producer = V3SnapshotProducer::new(
                    client.clone(),
                    scratch.path(),
                    "controls".into(),
                    options,
                )
                .await
                .unwrap();
                producer
                    .add_container(1, &[group], &actual_frames, &[1])
                    .await
                    .unwrap();
                let reference = producer.finish().await.unwrap();
                let snapshot = AuthenticatedV3Snapshot::open(&client, &reference)
                    .await
                    .unwrap();
                let build = &snapshot.manifest().build;
                assert_eq!(build.policy, policy);
                assert_eq!(build.inline_payload_bytes, 0);
                assert_eq!(build.frame_count, actual_frames.len() as u64);
                assert_eq!(build.frame_raw_bytes, (64 + 2560) * 1024);
                assert_eq!(build.requested_metadata_codec, metadata_codec as u8);
                assert_eq!(build.requested_data_codec, data_codec as u8);
                assert_eq!(
                    build.frame_codec_counts[data_codec as usize],
                    build.frame_count
                );
                assert_eq!(build.metadata_codec_counts[metadata_codec as usize], 1);
                assert_eq!(
                    build.frame_raw_size_counts,
                    if frames == V3FramePolicy::SizeOnly {
                        [1, 0, 1, 0]
                    } else {
                        [1, 3, 0, 0]
                    }
                );
                let reader = V3IndexReader::new(client.clone(), 0);
                for (inode, bytes) in [(2, 64 * 1024), (3, 2560 * 1024)] {
                    let mut actual = vec![0; bytes];
                    snapshot
                        .read_inode_range(&client, &reader, inode, 0, &mut actual, 32 * 1024 * 1024)
                        .await
                        .unwrap();
                    assert_eq!(actual, vec![inode as u8; bytes]);
                }
                let mut tampered = client.get_object(&reference.key).await.unwrap().unwrap();
                tampered[super::super::V3_HEADER_LEN + 140] ^= 1;
                assert!(AuthenticatedV3Snapshot::decode(&reference, &tampered).is_err());
            }
        }
    }

    #[tokio::test]
    async fn g15_producer_refuses_inline_payload_under_inline_off_before_upload() {
        let objects = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        let client = ObjectClient::new(LocalFsBackend::new(objects.path()));
        let (group, frames) = pack_group_files(
            1,
            [2; 32],
            vec![g15_file("tiny", 2, 4096)],
            AccessProfile::RandomSmallFile,
            SizeClassTable::default(),
            None,
        )
        .unwrap();
        let mut producer = V3SnapshotProducer::new(
            client,
            scratch.path(),
            "reject".into(),
            V3ProducerOptions {
                snapshot_id: [1; 32],
                root_dir_key: [2; 32],
                root_inode: 1,
                profile: AccessProfile::RandomSmallFile,
                size_classes: SizeClassTable::default(),
                build_policy: super::super::V3BuildPolicy {
                    inline_data: false,
                    p90: None,
                    ..Default::default()
                },
                metadata_codec: PackedCodec::Raw,
                data_codec: PackedCodec::Raw,
            },
        )
        .await
        .unwrap();
        assert!(
            producer
                .add_container(1, &[group], &frames, &[1])
                .await
                .is_err()
        );
        assert_eq!(std::fs::read_dir(objects.path()).unwrap().count(), 0);
    }

    async fn producer_batch_test_fixture(
        temporary: &Path,
        count: usize,
        conflicting_last_alias: bool,
    ) -> (
        V3SnapshotProducer<LocalFsBackend>,
        PackedGroupInput,
        Vec<PackedFrameInput>,
    ) {
        let producer = V3SnapshotProducer::new(
            ObjectClient::new(LocalFsBackend::new(temporary.join("objects"))),
            temporary,
            "batch-test".into(),
            V3ProducerOptions {
                snapshot_id: [1; 32],
                root_dir_key: [2; 32],
                root_inode: 1,
                profile: AccessProfile::RandomSmallFile,
                size_classes: SizeClassTable::default(),
                build_policy: Default::default(),
                metadata_codec: PackedCodec::Raw,
                data_codec: PackedCodec::Raw,
            },
        )
        .await
        .unwrap();
        let entries = (0..count)
            .map(|ordinal| {
                let conflict = conflicting_last_alias && ordinal == count - 1;
                PackedFileInput {
                    name: format!("f{ordinal:04}").into_bytes(),
                    inode: if conflict { 2 } else { ordinal as u64 + 2 },
                    kind: 1,
                    mode: if conflict { 0o100600 } else { 0o100644 },
                    uid: 0,
                    gid: 0,
                    rdev: 0,
                    nlink: if conflicting_last_alias && (ordinal == 0 || conflict) {
                        2
                    } else {
                        1
                    },
                    atime_ns: 0,
                    mtime_ns: 0,
                    ctime_ns: 0,
                    flags: 0,
                    data: Vec::new(),
                }
            })
            .collect();
        let (group, frames) = pack_group_files(
            1,
            [2; 32],
            entries,
            AccessProfile::RandomSmallFile,
            SizeClassTable::default(),
            None,
        )
        .unwrap();
        (producer, group, frames)
    }

    #[tokio::test]
    async fn producer_private_spool_commit_count_is_bounded_for_129_dentries() {
        use std::sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        };
        let temporary = tempfile::tempdir().unwrap();
        let (mut producer, group, frames) =
            producer_batch_test_fixture(temporary.path(), 129, false).await;
        let commits = Arc::new(AtomicU64::new(0));
        let counter = commits.clone();
        {
            let mut connection = producer.spool.pool.acquire().await.unwrap();
            connection
                .lock_handle()
                .await
                .unwrap()
                .set_commit_hook(move || {
                    counter.fetch_add(1, Ordering::SeqCst);
                    true
                });
        }
        producer
            .add_container(1, &[group], &frames, &[1])
            .await
            .unwrap();
        let actual = commits.load(Ordering::SeqCst);
        // This is a workload-level ceiling measured at SQLite's commit hook,
        // not an assertion about the implementation's selected batch constant.
        assert!(
            actual <= 16,
            "129 dentries triggered {actual} private-spool commits"
        );
        let entries: i64 = sea_orm::sqlx::query_scalar("SELECT COUNT(*) FROM inode_identities")
            .fetch_one(&producer.spool.pool)
            .await
            .unwrap();
        assert_eq!(entries, 129);
        producer.finish().await.unwrap();
    }

    #[tokio::test]
    async fn producer_conflict_rolls_back_all_inodes_in_a_small_batch() {
        let temporary = tempfile::tempdir().unwrap();
        let (mut producer, group, frames) =
            producer_batch_test_fixture(temporary.path(), 9, true).await;
        assert!(
            producer
                .add_container(1, &[group], &frames, &[1])
                .await
                .is_err()
        );
        let identities: i64 = sea_orm::sqlx::query_scalar("SELECT COUNT(*) FROM inode_identities")
            .fetch_one(&producer.spool.pool)
            .await
            .unwrap();
        let locators: i64 =
            sea_orm::sqlx::query_scalar("SELECT COUNT(*) FROM records WHERE root IN (1,5)")
                .fetch_one(&producer.spool.pool)
                .await
                .unwrap();
        assert_eq!(
            (identities, locators),
            (0, 0),
            "conflicting alias leaked a partial private batch"
        );
        assert!(producer.poisoned);
        assert!(producer.finish().await.is_err());
        assert!(
            !temporary
                .path()
                .join("objects/batch-test/objects/0")
                .exists()
        );
    }

    #[tokio::test]
    async fn producer_batch_conflict_after_committed_prefix_stays_poisoned() {
        let temporary = tempfile::tempdir().unwrap();
        let objects = temporary.path().join("objects");
        let marker = temporary.path().join("user-file");
        std::fs::write(&marker, b"preserve").unwrap();
        let client = ObjectClient::new(LocalFsBackend::new(&objects));
        let mut producer = V3SnapshotProducer::new(
            client,
            temporary.path(),
            "batch-conflict".into(),
            V3ProducerOptions {
                snapshot_id: [1; 32],
                root_dir_key: [2; 32],
                root_inode: 1,
                profile: AccessProfile::RandomSmallFile,
                size_classes: SizeClassTable::default(),
                build_policy: Default::default(),
                metadata_codec: PackedCodec::Raw,
                data_codec: PackedCodec::Raw,
            },
        )
        .await
        .unwrap();
        let entries: Vec<_> = (0..=super::super::spool::V3_SPOOL_BATCH_ENTRIES)
            .map(|ordinal| PackedFileInput {
                name: format!("f{ordinal:04}").into_bytes(),
                inode: if ordinal == super::super::spool::V3_SPOOL_BATCH_ENTRIES {
                    2
                } else {
                    ordinal as u64 + 2
                },
                kind: 1,
                mode: if ordinal == super::super::spool::V3_SPOOL_BATCH_ENTRIES {
                    0o100600
                } else {
                    0o100644
                },
                uid: 0,
                gid: 0,
                rdev: 0,
                nlink: if ordinal == 0 || ordinal == super::super::spool::V3_SPOOL_BATCH_ENTRIES {
                    2
                } else {
                    1
                },
                atime_ns: 0,
                mtime_ns: 0,
                ctime_ns: 0,
                flags: 0,
                data: Vec::new(),
            })
            .collect();
        let (group, frames) = pack_group_files(
            1,
            [2; 32],
            entries,
            AccessProfile::RandomSmallFile,
            SizeClassTable::default(),
            None,
        )
        .unwrap();
        assert!(
            producer
                .add_container(1, &[group], &frames, &[1])
                .await
                .is_err()
        );
        let committed: i64 = sea_orm::sqlx::query_scalar("SELECT COUNT(*) FROM inode_identities")
            .fetch_one(&producer.spool.pool)
            .await
            .unwrap();
        assert_eq!(
            committed,
            super::super::spool::V3_SPOOL_BATCH_ENTRIES as i64
        );
        assert!(producer.poisoned);
        assert!(producer.finish().await.is_err());
        assert!(!objects.join("batch-conflict/objects/0").exists());
        assert!(
            std::fs::read_dir(temporary.path())
                .unwrap()
                .filter_map(Result::ok)
                .all(|entry| !entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with("brewfs-wire005-index-"))
        );
        assert_eq!(std::fs::read(marker).unwrap(), b"preserve");
    }

    #[tokio::test]
    async fn pm08_producer_requires_complete_consistent_allocations_and_root_identity() {
        for case in 0..7 {
            let temp = tempfile::tempdir().unwrap();
            let client = ObjectClient::new(LocalFsBackend::new(temp.path().join("objects")));
            let mut producer = V3SnapshotProducer::new(
                client.clone(),
                temp.path(),
                "snapshot".into(),
                V3ProducerOptions {
                    snapshot_id: [1; 32],
                    root_dir_key: [2; 32],
                    root_inode: 1,
                    profile: AccessProfile::RandomSmallFile,
                    size_classes: SizeClassTable::default(),
                    build_policy: Default::default(),
                    metadata_codec: PackedCodec::Raw,
                    data_codec: PackedCodec::Raw,
                },
            )
            .await
            .unwrap();
            let root = super::super::V3RootAttributes {
                inode: 1,
                size: 4096,
                blocks: 8,
                mode: 0o040750,
                uid: 123,
                gid: 456,
                nlink: 2,
                atime_ns: 1,
                mtime_ns: 2,
                ctime_ns: 3,
            };
            if case != 2 {
                producer.set_root_attributes(root.clone()).unwrap();
            }
            let (group, frames) = pack_group_files(
                1,
                [2; 32],
                vec![PackedFileInput {
                    name: b"empty".to_vec(),
                    inode: if case == 6 { 1 } else { 2 },
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
                }],
                AccessProfile::RandomSmallFile,
                SizeClassTable::default(),
                None,
            )
            .unwrap();
            let added = producer.add_container(1, &[group], &frames, &[1]).await;
            if case == 6 {
                assert!(added.is_err());
            } else {
                added.unwrap();
            }
            if matches!(case, 1..=4) {
                producer.set_inode_blocks(2, 0).await.unwrap();
                producer.set_inode_blocks(2, 0).await.unwrap(); // repeated alias, idempotent
            }
            match case {
                1 => {
                    producer.set_inode_blocks(3, 0).await.unwrap();
                }
                3 => {
                    assert!(producer.set_inode_blocks(2, 8).await.is_err());
                }
                5 => {
                    let mut conflicting = root;
                    conflicting.blocks += 8;
                    assert!(producer.set_root_attributes(conflicting).is_err());
                }
                _ => {}
            }
            let result = producer.finish().await;
            if case == 4 {
                let reference = result.unwrap();
                let snapshot = super::super::AuthenticatedV3Snapshot::open(&client, &reference)
                    .await
                    .unwrap();
                let reader = super::super::V3IndexReader::new(client, 0);
                assert_eq!(snapshot.source_blocks(&reader, 2).await.unwrap(), Some(0));
            } else {
                assert!(
                    result.is_err(),
                    "case {case} published incomplete/conflicting source attributes"
                );
            }
        }
    }

    #[derive(Clone)]
    struct FaultBackend {
        inner: LocalFsBackend,
        fail: std::sync::Arc<std::sync::atomic::AtomicBool>,
        block: std::sync::Arc<std::sync::atomic::AtomicBool>,
        corrupt: std::sync::Arc<std::sync::atomic::AtomicBool>,
        entered: std::sync::Arc<tokio::sync::Notify>,
    }
    #[async_trait::async_trait]
    impl ObjectBackend for FaultBackend {
        async fn put_object(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
            self.inner.put_object(key, data).await
        }
        async fn put_object_create_only(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
            use std::sync::atomic::Ordering;
            self.entered.notify_one();
            if self.block.load(Ordering::Relaxed) {
                return std::future::pending().await;
            }
            if self.fail.load(Ordering::Relaxed) {
                anyhow::bail!("injected CAS upload failure");
            }
            let mut bytes = data.to_vec();
            if self.corrupt.load(Ordering::Relaxed) {
                let last = bytes.len() - 1;
                bytes[last] ^= 1;
            }
            self.inner.put_object_create_only(key, &bytes).await
        }
        async fn get_object(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
            self.inner.get_object(key).await
        }
        async fn get_object_range(
            &self,
            key: &str,
            offset: u64,
            buf: &mut [u8],
        ) -> anyhow::Result<usize> {
            self.inner.get_object_range(key, offset, buf).await
        }
        async fn get_object_range_stream(
            &self,
            key: &str,
            offset: u64,
            length: u64,
        ) -> anyhow::Result<crate::cadapter::client::ObjectByteStream> {
            self.inner
                .get_object_range_stream(key, offset, length)
                .await
        }
        async fn get_etag(&self, key: &str) -> anyhow::Result<String> {
            self.inner.get_etag(key).await
        }
        async fn delete_object(&self, key: &str) -> anyhow::Result<()> {
            self.inner.delete_object(key).await
        }
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn pm09_placement_closure_rejects_missing_orphan_nonregular_size_and_legacy_records() {
        for mode in 0..5 {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join("source");
            std::fs::write(&path, [37; 8192]).unwrap();
            let source = super::super::CapturedV3SourceFile::capture(
                &path,
                2,
                AccessProfile::RandomSmallFile,
                SizeClassTable::default(),
                None,
                super::super::V3SourceFileLimits::default(),
            )
            .unwrap();
            let mut producer = V3SnapshotProducer::new(
                ObjectClient::new(LocalFsBackend::new(temp.path().join("objects"))),
                temp.path(),
                "snapshot".into(),
                V3ProducerOptions {
                    snapshot_id: [1; 32],
                    root_dir_key: [2; 32],
                    root_inode: 1,
                    profile: AccessProfile::RandomSmallFile,
                    size_classes: SizeClassTable::default(),
                    build_policy: Default::default(),
                    metadata_codec: PackedCodec::Raw,
                    data_codec: PackedCodec::Raw,
                },
            )
            .await
            .unwrap();
            producer
                .set_root_attributes(
                    super::super::CapturedV3SourceRoot::capture(temp.path(), 1)
                        .unwrap()
                        .attributes()
                        .clone(),
                )
                .unwrap();
            producer.placement_contract = true;
            producer
                .set_inode_blocks(2, source.source_blocks())
                .await
                .unwrap();
            producer
                .add_container(
                    1,
                    &[source
                        .group(1, [2; 32], AccessProfile::RandomSmallFile)
                        .unwrap()],
                    source.frames(),
                    &[1],
                )
                .await
                .unwrap();
            match mode {
                0 => {
                    sea_orm::sqlx::query("DELETE FROM records WHERE root=?")
                        .bind(V3RootKind::LargePlacements as i64)
                        .execute(&producer.spool.pool)
                        .await
                        .unwrap();
                }
                1 => {
                    let key = 3u64.to_be_bytes().to_vec();
                    producer
                        .spool
                        .insert(
                            V3RootKind::LargePlacements,
                            &V3IndexRecord {
                                first_key: key.clone(),
                                last_key: key,
                                value: V3IndexValue::Leaf(
                                    super::super::V3Placement::Group { inode: 3, size: 0 }
                                        .encode()
                                        .unwrap(),
                                ),
                            },
                        )
                        .await
                        .unwrap();
                }
                2 => {
                    sea_orm::sqlx::query("UPDATE inode_identities SET kind=2")
                        .execute(&producer.spool.pool)
                        .await
                        .unwrap();
                }
                3 => {
                    let key = 2u64.to_be_bytes().to_vec();
                    producer
                        .spool
                        .replace(
                            V3RootKind::LargePlacements,
                            &V3IndexRecord {
                                first_key: key.clone(),
                                last_key: key,
                                value: V3IndexValue::Leaf(
                                    super::super::V3Placement::Group {
                                        inode: 2,
                                        size: 8193,
                                    }
                                    .encode()
                                    .unwrap(),
                                ),
                            },
                        )
                        .await
                        .unwrap();
                }
                4 => producer.placement_contract = false,
                _ => unreachable!(),
            }
            assert!(producer.finish().await.is_err(), "mode={mode}");
        }
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn external_producer_rejects_failed_corrupt_cancelled_mutated_or_orphaned_sources() {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };
        for mode in 0..5 {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join("source");
            std::fs::write(&path, vec![37; 8192]).unwrap();
            let backend = FaultBackend {
                inner: LocalFsBackend::new(temp.path().join("objects")),
                fail: Arc::new(AtomicBool::new(mode == 0)),
                block: Arc::new(AtomicBool::new(mode == 1)),
                corrupt: Arc::new(AtomicBool::new(mode == 2)),
                entered: Arc::new(tokio::sync::Notify::new()),
            };
            let mut captured = super::super::CapturedV3SourceLayout::capture(
                &path,
                temp.path(),
                2,
                AccessProfile::RandomSmallFile,
                SizeClassTable::default(),
            )
            .await
            .unwrap();
            let mut producer = V3SnapshotProducer::new(
                ObjectClient::new(backend.clone()),
                temp.path(),
                "snapshot".into(),
                V3ProducerOptions {
                    snapshot_id: [1; 32],
                    root_dir_key: [2; 32],
                    root_inode: 1,
                    profile: AccessProfile::RandomSmallFile,
                    size_classes: SizeClassTable::default(),
                    build_policy: Default::default(),
                    metadata_codec: PackedCodec::Raw,
                    data_codec: PackedCodec::Raw,
                },
            )
            .await
            .unwrap();
            producer
                .set_root_attributes(
                    super::super::CapturedV3SourceRoot::capture(temp.path(), 1)
                        .unwrap()
                        .attributes()
                        .clone(),
                )
                .unwrap();
            if mode == 3 {
                std::fs::write(&path, vec![91; 8192]).unwrap();
            }
            if mode == 1 {
                let attempt = producer.add_external_source(&mut captured);
                tokio::pin!(attempt);
                tokio::select! {
                    _ = backend.entered.notified() => {},
                    result = &mut attempt => panic!("blocked upload unexpectedly returned {result:?}"),
                    _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => panic!("external upload did not start"),
                }
                // Dropping the attempt here cancels an in-flight upload.
            } else {
                let result = producer.add_external_source(&mut captured).await;
                assert_eq!(result.is_ok(), mode == 4);
            }
            backend.fail.store(false, Ordering::Relaxed);
            backend.block.store(false, Ordering::Relaxed);
            backend.corrupt.store(false, Ordering::Relaxed);
            if mode != 4 {
                assert!(producer.poisoned);
            }
            // mode 4 has a complete external tree but no namespace inode;
            // reachability closure must reject it rather than publishing PM09.
            assert!(producer.finish().await.is_err());
            drop(captured);
            assert!(
                !std::fs::read_dir(temp.path())
                    .unwrap()
                    .filter_map(Result::ok)
                    .any(|entry| entry
                        .file_name()
                        .to_string_lossy()
                        .starts_with("brewfs-wire005-index-"))
            );
        }
    }

    #[tokio::test]
    async fn producer_rejects_corrupted_upload_failure_and_cancelled_update() {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };
        for mode in 0..3 {
            let temp = tempfile::tempdir().unwrap();
            let backend = FaultBackend {
                inner: LocalFsBackend::new(temp.path().join("objects")),
                fail: Arc::new(AtomicBool::new(mode == 0)),
                block: Arc::new(AtomicBool::new(mode == 1)),
                corrupt: Arc::new(AtomicBool::new(mode == 2)),
                entered: Arc::new(tokio::sync::Notify::new()),
            };
            let observer = Arc::new(crate::cadapter::read_observer::ReadObserver::default());
            let client = ObjectClient::new(backend.clone()).with_read_observer(
                observer.clone(),
                crate::cadapter::read_observer::Engine::PackedV3,
                crate::cadapter::read_observer::Phase::Startup,
                crate::cadapter::read_observer::Origin::Demand,
            );
            let options = V3ProducerOptions {
                snapshot_id: [1; 32],
                root_dir_key: [2; 32],
                root_inode: 1,
                profile: AccessProfile::RandomSmallFile,
                size_classes: SizeClassTable::default(),
                build_policy: Default::default(),
                metadata_codec: PackedCodec::Raw,
                data_codec: PackedCodec::Raw,
            };
            let mut producer =
                V3SnapshotProducer::new(client, temp.path(), "snapshot".into(), options)
                    .await
                    .unwrap();
            let (group, frames) = pack_group_files(
                7,
                [2; 32],
                vec![PackedFileInput {
                    name: b"file".to_vec(),
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
                    data: vec![7; 1024],
                }],
                AccessProfile::RandomSmallFile,
                SizeClassTable::default(),
                None,
            )
            .unwrap();
            let groups = [group];
            let parents = [1];
            let result = tokio::time::timeout(
                std::time::Duration::from_millis(50),
                producer.add_container(1, &groups, &frames, &parents),
            )
            .await;
            if mode == 1 {
                assert!(result.is_err());
            } else if mode == 2 {
                assert!(matches!(
                    result.unwrap(),
                    Err(PackedWireError::HashMismatch { .. })
                ));
                let snapshot = observer.snapshot();
                use crate::cadapter::read_observer::{FailureClass, Ledger};
                let mut authentication_failures = 0;
                let mut backend_successes = 0;
                for ((ledger, context), counters) in &snapshot.rows {
                    assert_eq!(context.class, ReadClass::PublicationVerification);
                    assert!(counters.conserved());
                    assert_eq!((counters.cancelled, counters.inflight), (0, 0));
                    match ledger {
                        // typed_exact owns both exact-body and digest validation.
                        Ledger::ValidatedFetch => {
                            assert_eq!(counters.success, 0);
                            assert_eq!(counters.failed, 1);
                            assert!(counters.received_failed > 0);
                            authentication_failures +=
                                counters.failure_reasons[&FailureClass::Authentication];
                        }
                        Ledger::BackendBody => {
                            assert_eq!(counters.failed, 0);
                            backend_successes += counters.success;
                        }
                        _ => assert_eq!(counters.failed, 0),
                    }
                }
                assert_eq!(authentication_failures, 1);
                assert_eq!(backend_successes, 1);
            } else {
                assert!(result.unwrap().is_err());
            }
            backend.block.store(false, Ordering::Relaxed);
            backend.fail.store(false, Ordering::Relaxed);
            backend.corrupt.store(false, Ordering::Relaxed);
            assert!(
                producer
                    .add_container(2, &groups, &frames, &parents)
                    .await
                    .is_err()
            );
            assert!(producer.finish().await.is_err());
            assert!(
                !std::fs::read_dir(temp.path())
                    .unwrap()
                    .filter_map(Result::ok)
                    .any(|entry| entry
                        .file_name()
                        .to_string_lossy()
                        .starts_with("brewfs-wire005-index-"))
            );
        }
    }

    #[tokio::test]
    async fn producer_observed_publication_readback_preserves_success_and_byte_ledgers() {
        use crate::cadapter::read_observer::{Engine, Ledger, Origin, Phase, ReadObserver};
        let temp = tempfile::tempdir().unwrap();
        let (mut producer, group, frames) =
            producer_batch_test_fixture(temp.path(), 1, false).await;
        let observer = std::sync::Arc::new(ReadObserver::default());
        producer.client = producer.client.clone().with_read_observer(
            observer.clone(),
            Engine::PackedV3,
            Phase::Startup,
            Origin::Demand,
        );
        producer
            .add_container(1, &[group], &frames, &[1])
            .await
            .unwrap();
        let snapshot = observer.snapshot();
        assert!(!snapshot.rows.is_empty());
        let mut received = 0;
        for ((ledger, context), counters) in &snapshot.rows {
            assert_eq!(context.class, ReadClass::PublicationVerification);
            assert_eq!(context.phase, Phase::Startup);
            assert!(counters.conserved());
            assert_eq!(
                (counters.failed, counters.cancelled, counters.inflight),
                (0, 0, 0)
            );
            assert!(counters.success > 0);
            if *ledger == Ledger::BackendBody {
                received += counters.received_success;
            }
        }
        assert!(received > 0);
    }

    #[tokio::test]
    async fn hardlinks_across_groups_preserve_inode_and_reject_divergent_content_or_attributes() {
        for mismatch in [0u8, 1, 2] {
            let temp = tempfile::tempdir().unwrap();
            let client = ObjectClient::new(LocalFsBackend::new(temp.path().join("objects")));
            let options = V3ProducerOptions {
                snapshot_id: [1; 32],
                root_dir_key: [2; 32],
                root_inode: 1,
                profile: AccessProfile::RandomSmallFile,
                size_classes: SizeClassTable::default(),
                build_policy: Default::default(),
                metadata_codec: PackedCodec::Zstd,
                data_codec: PackedCodec::Zstd,
            };
            let mut producer =
                V3SnapshotProducer::new(client.clone(), temp.path(), "hardlinks".into(), options)
                    .await
                    .unwrap();
            for (group_id, name, byte, mode) in [
                (1u64, b"a".to_vec(), 7u8, 0o100644),
                (
                    2,
                    b"b".to_vec(),
                    if mismatch == 1 { 8 } else { 7 },
                    if mismatch == 2 { 0o100600 } else { 0o100644 },
                ),
            ] {
                let (group, frames) = pack_group_files(
                    group_id,
                    [2; 32],
                    vec![PackedFileInput {
                        name,
                        inode: 3,
                        kind: 1,
                        mode,
                        uid: 0,
                        gid: 0,
                        rdev: 0,
                        nlink: 2,
                        atime_ns: 0,
                        mtime_ns: 0,
                        ctime_ns: 0,
                        flags: 0,
                        data: vec![byte; 512 * 1024],
                    }],
                    AccessProfile::RandomSmallFile,
                    SizeClassTable::default(),
                    None,
                )
                .unwrap();
                let result = producer
                    .add_container(group_id, &[group], &frames, &[1])
                    .await;
                if group_id == 1 || mismatch == 0 {
                    result.unwrap();
                } else {
                    assert!(result.is_err());
                }
            }
            if mismatch != 0 {
                assert!(producer.finish().await.is_err());
                continue;
            }
            let reference = producer.finish().await.unwrap();
            let snapshot = super::super::AuthenticatedV3Snapshot::open(&client, &reference)
                .await
                .unwrap();
            let reader = super::super::V3IndexReader::new(client.clone(), 0);
            for name in [b"a".as_slice(), b"b".as_slice()] {
                let entry = snapshot
                    .lookup_dentry(&client, &reader, [2; 32], name, 512 * 1024)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(entry.inode, 3);
                assert_eq!(entry.nlink, 2);
            }
            let first = snapshot
                .reverse_names_page(&reader, 3, None, 1)
                .await
                .unwrap();
            assert_eq!(first.len(), 1);
            assert_eq!(first[0].hot.name, b"a");
            let second = snapshot
                .reverse_names_page(&reader, 3, Some(&first[0].reverse_key()), 1)
                .await
                .unwrap();
            assert_eq!(second.len(), 1);
            assert_eq!(second[0].hot.name, b"b");
            assert!(
                snapshot
                    .reverse_names_page(&reader, 3, Some(&second[0].reverse_key()), 1)
                    .await
                    .unwrap()
                    .is_empty()
            );
            let mut output = [0; 31];
            snapshot
                .read_inode_range(&client, &reader, 3, 524000, &mut output, 32 * 1024 * 1024)
                .await
                .unwrap();
            assert_eq!(output, [7; 31]);
            use crate::meta::MetaLayer;
            let meta = crate::workspace_overlay::packed_v3::PackedV3ReadonlyMeta::from_v3(
                client,
                snapshot,
                64 * 1024 * 1024,
                0,
            );
            assert_eq!(
                meta.get_names(3).await.unwrap(),
                vec![(Some(1), "a".into()), (Some(1), "b".into())]
            );
            assert_eq!(meta.get_paths(3).await.unwrap(), vec!["/a", "/b"]);
        }
    }

    fn link_input(name: &[u8], inode: u64, nlink: u32, bytes: usize) -> PackedFileInput {
        PackedFileInput {
            name: name.to_vec(),
            inode,
            kind: 1,
            mode: 0o100644,
            uid: 0,
            gid: 0,
            rdev: 0,
            nlink,
            atime_ns: 0,
            mtime_ns: 0,
            ctime_ns: 0,
            flags: 0,
            data: vec![7; bytes],
        }
    }

    #[tokio::test]
    async fn hardlink_identity_is_independent_of_inline_admission_and_never_deduplicates_distinct_inodes()
     {
        let temp = tempfile::tempdir().unwrap();
        let client = ObjectClient::new(LocalFsBackend::new(temp.path().join("objects")));
        let options = V3ProducerOptions {
            snapshot_id: [1; 32],
            root_dir_key: [2; 32],
            root_inode: 1,
            profile: AccessProfile::RandomSmallFile,
            size_classes: SizeClassTable::default(),
            build_policy: Default::default(),
            metadata_codec: PackedCodec::Zstd,
            data_codec: PackedCodec::Zstd,
        };
        let mut producer =
            V3SnapshotProducer::new(client.clone(), temp.path(), "inline-links".into(), options)
                .await
                .unwrap();
        let (group, frames) = pack_group_files(
            1,
            [2; 32],
            vec![link_input(b"a", 3, 2, 100 * 1024)],
            AccessProfile::RandomSmallFile,
            SizeClassTable::default(),
            None,
        )
        .unwrap();
        assert!(
            !GroupMeta::decode(&group.metadata).unwrap().entries()[0]
                .inline_data
                .is_empty()
        );
        producer
            .add_container(1, &[group], &frames, &[1])
            .await
            .unwrap();
        let (group, frames) = pack_group_files(
            2,
            [2; 32],
            vec![
                link_input(b"b", 4, 1, 200 * 1024),
                link_input(b"c", 3, 2, 100 * 1024),
                link_input(b"d", 5, 1, 100 * 1024),
            ],
            AccessProfile::RandomSmallFile,
            SizeClassTable::default(),
            None,
        )
        .unwrap();
        assert!(
            GroupMeta::decode(&group.metadata)
                .unwrap()
                .lookup(b"c")
                .unwrap()
                .inline_data
                .is_empty()
        );
        producer
            .add_container(2, &[group], &frames, &[1])
            .await
            .unwrap();
        let reference = producer.finish().await.unwrap();
        let snapshot = super::super::AuthenticatedV3Snapshot::open(&client, &reference)
            .await
            .unwrap();
        let reader = super::super::V3IndexReader::new(client.clone(), 0);
        assert_eq!(
            snapshot
                .lookup_dentry(&client, &reader, [2; 32], b"d", 512 * 1024)
                .await
                .unwrap()
                .unwrap()
                .inode,
            5
        );
        assert_eq!(
            snapshot
                .reverse_names_page(&reader, 3, None, 16)
                .await
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            snapshot
                .reverse_names_page(&reader, 5, None, 16)
                .await
                .unwrap()
                .len(),
            1
        );
        let mut output = [0; 17];
        snapshot
            .read_inode_range(&client, &reader, 3, 90000, &mut output, 32 * 1024 * 1024)
            .await
            .unwrap();
        assert_eq!(output, [7; 17]);
    }

    #[tokio::test]
    async fn incomplete_nlink_and_missing_symlink_cold_target_fail_before_manifest_publication() {
        for symlink in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let client = ObjectClient::new(LocalFsBackend::new(temp.path().join("objects")));
            let options = V3ProducerOptions {
                snapshot_id: [1; 32],
                root_dir_key: [2; 32],
                root_inode: 1,
                profile: AccessProfile::RandomSmallFile,
                size_classes: SizeClassTable::default(),
                build_policy: Default::default(),
                metadata_codec: PackedCodec::Raw,
                data_codec: PackedCodec::Raw,
            };
            let mut producer =
                V3SnapshotProducer::new(client.clone(), temp.path(), "incomplete".into(), options)
                    .await
                    .unwrap();
            let mut file = link_input(b"file", 3, 2, 100);
            if symlink {
                file.kind = 3;
                file.mode = 0o120777;
                file.nlink = 1;
                file.data.clear();
            }
            let (group, frames) = pack_group_files(
                1,
                [2; 32],
                vec![file],
                AccessProfile::RandomSmallFile,
                SizeClassTable::default(),
                None,
            )
            .unwrap();
            producer
                .add_container(1, &[group], &frames, &[1])
                .await
                .unwrap();
            assert!(producer.finish().await.is_err());
            assert!(!temp.path().join("objects/incomplete/objects/0").exists());
        }
    }

    #[tokio::test]
    async fn producer_rejects_acl_mode_default_kind_and_late_root_mode_mismatch() {
        use super::super::{V3ColdAttributes, V3RootAttributes, V3Xattr};
        let mut acl = 2u32.to_le_bytes().to_vec();
        for (tag, permission, id) in [
            (1u16, 7u16, u32::MAX),
            (2, 5, 1234),
            (4, 0, u32::MAX),
            (16, 5, u32::MAX),
            (32, 5, u32::MAX),
        ] {
            acl.extend_from_slice(&tag.to_le_bytes());
            acl.extend_from_slice(&permission.to_le_bytes());
            acl.extend_from_slice(&id.to_le_bytes());
        }
        for scenario in 0..3 {
            let temp = tempfile::tempdir().unwrap();
            let client = ObjectClient::new(LocalFsBackend::new(temp.path().join("objects")));
            let mut producer = V3SnapshotProducer::new(
                client,
                temp.path(),
                "acl-context".into(),
                V3ProducerOptions {
                    snapshot_id: [1; 32],
                    root_dir_key: [2; 32],
                    root_inode: 1,
                    profile: AccessProfile::RandomSmallFile,
                    size_classes: SizeClassTable::default(),
                    build_policy: Default::default(),
                    metadata_codec: PackedCodec::Raw,
                    data_codec: PackedCodec::Raw,
                },
            )
            .await
            .unwrap();
            let mut attrs = V3ColdAttributes {
                inode: 1,
                symlink_target: None,
                xattrs: vec![V3Xattr {
                    name: crate::meta::posix_acl::ACCESS_XATTR.to_vec(),
                    value: acl.clone(),
                }],
                acl: vec![],
            };
            if scenario == 2 {
                producer.add_cold_attributes(&attrs).await.unwrap();
                producer
                    .set_root_attributes(V3RootAttributes {
                        inode: 1,
                        size: 0,
                        blocks: 0,
                        mode: 0o040700,
                        uid: 0,
                        gid: 0,
                        nlink: 2,
                        atime_ns: 0,
                        mtime_ns: 0,
                        ctime_ns: 0,
                    })
                    .unwrap();
                assert!(producer.finish().await.is_err());
            } else {
                let (group, frames) = pack_group_files(
                    1,
                    [2; 32],
                    vec![link_input(b"file", 2, 1, 20)],
                    AccessProfile::RandomSmallFile,
                    SizeClassTable::default(),
                    None,
                )
                .unwrap();
                producer
                    .add_container(1, &[group], &frames, &[1])
                    .await
                    .unwrap();
                attrs.inode = 2;
                if scenario == 1 {
                    attrs.xattrs[0].name = crate::meta::posix_acl::DEFAULT_XATTR.to_vec();
                }
                assert!(producer.add_cold_attributes(&attrs).await.is_err());
            }
        }
    }

    #[tokio::test]
    async fn producer_cold_attributes_are_inode_bound_and_readonly_adapter_queries_them() {
        use super::super::{V3ColdAttributes, V3Xattr};
        use crate::meta::MetaLayer;
        let temp = tempfile::tempdir().unwrap();
        let client = ObjectClient::new(LocalFsBackend::new(temp.path().join("objects")));
        let options = V3ProducerOptions {
            snapshot_id: [1; 32],
            root_dir_key: [2; 32],
            root_inode: 1,
            profile: AccessProfile::RandomSmallFile,
            size_classes: SizeClassTable::default(),
            build_policy: Default::default(),
            metadata_codec: PackedCodec::Zstd,
            data_codec: PackedCodec::Zstd,
        };
        let mut producer =
            V3SnapshotProducer::new(client.clone(), temp.path(), "cold-test".into(), options)
                .await
                .unwrap();
        let target = b"raw-\xff-target".to_vec();
        let files = vec![
            PackedFileInput {
                name: b"file".to_vec(),
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
                data: b"payload".to_vec(),
            },
            PackedFileInput {
                name: b"link".to_vec(),
                inode: 7,
                kind: 3,
                mode: 0o120777,
                uid: 0,
                gid: 0,
                rdev: 0,
                nlink: 1,
                atime_ns: 0,
                mtime_ns: 0,
                ctime_ns: 0,
                flags: 0,
                data: Vec::new(),
            },
        ];
        let (mut group, frames) = pack_group_files(
            1,
            [2; 32],
            files,
            AccessProfile::RandomSmallFile,
            SizeClassTable::default(),
            None,
        )
        .unwrap();
        let mut metadata = GroupMeta::decode(&group.metadata).unwrap();
        metadata
            .entries_mut()
            .iter_mut()
            .find(|entry| entry.inode == 7)
            .unwrap()
            .size = target.len() as u64;
        group.metadata = metadata.encode().unwrap();
        producer
            .add_container(1, &[group], &frames, &[1])
            .await
            .unwrap();
        producer
            .add_cold_attributes(&V3ColdAttributes {
                inode: 7,
                symlink_target: Some(target.clone()),
                xattrs: vec![],
                acl: vec![],
            })
            .await
            .unwrap();
        producer
            .add_cold_attributes(&V3ColdAttributes {
                inode: 3,
                symlink_target: None,
                xattrs: vec![V3Xattr {
                    name: b"user.test".to_vec(),
                    value: vec![0, 255, 1],
                }],
                acl: vec![crate::meta::store::AclRule {
                    acl_type: 1,
                    qualifier: 0,
                    permissions: 7,
                }],
            })
            .await
            .unwrap();
        assert!(
            producer
                .add_cold_attributes(&V3ColdAttributes {
                    inode: 99,
                    symlink_target: None,
                    xattrs: vec![],
                    acl: vec![]
                })
                .await
                .is_err()
        );
        let reference = producer.finish().await.unwrap();
        let snapshot = super::super::AuthenticatedV3Snapshot::open(&client, &reference)
            .await
            .unwrap();
        let meta = crate::workspace_overlay::packed_v3::PackedV3ReadonlyMeta::from_v3(
            client,
            snapshot,
            64 * 1024 * 1024,
            0,
        );
        assert_eq!(meta.read_symlink_bytes(7).await.unwrap(), target);
        assert!(meta.read_symlink(7).await.is_err());
        assert_eq!(
            meta.get_xattr(3, "user.test").await.unwrap(),
            Some(vec![0, 255, 1])
        );
        assert_eq!(meta.list_xattr(3).await.unwrap(), vec!["user.test"]);
        assert_eq!(meta.get_acl(3, 1, 0).await.unwrap().unwrap().permissions, 7);
        assert!(meta.get_xattr(3, "user.missing").await.unwrap().is_none());
        let error = meta
            .set_xattr(3, "user.test", b"forbidden", 0)
            .await
            .unwrap_err();
        assert!(
            matches!(error,crate::meta::store::MetaError::Io(error) if error.raw_os_error()==Some(libc::EROFS))
        );
    }

    #[tokio::test]
    async fn producer_publishes_out_of_order_groups_with_authenticated_sorted_roots() {
        let temp = tempfile::tempdir().unwrap();
        let client = ObjectClient::new(LocalFsBackend::new(temp.path().join("objects")));
        let options = V3ProducerOptions {
            snapshot_id: [1; 32],
            root_dir_key: [2; 32],
            root_inode: 1,
            profile: AccessProfile::RandomSmallFile,
            size_classes: SizeClassTable::default(),
            build_policy: Default::default(),
            metadata_codec: PackedCodec::Zstd,
            data_codec: PackedCodec::Zstd,
        };
        let mut producer =
            V3SnapshotProducer::new(client.clone(), temp.path(), "snapshot".into(), options)
                .await
                .unwrap();
        for (inode, name, byte) in [(9u64, b"z".to_vec(), 9u8), (3, b"a".to_vec(), 3)] {
            let (group, frames) = pack_group_files(
                inode,
                [2; 32],
                vec![PackedFileInput {
                    name,
                    inode,
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
                    data: vec![byte; 512 * 1024],
                }],
                AccessProfile::RandomSmallFile,
                SizeClassTable::default(),
                None,
            )
            .unwrap();
            producer
                .add_container(inode, &[group], &frames, &[1])
                .await
                .unwrap();
        }
        let reference = producer.finish().await.unwrap();
        let snapshot = super::super::AuthenticatedV3Snapshot::open(&client, &reference)
            .await
            .unwrap();
        let reader = super::super::V3IndexReader::new(client.clone(), 0);
        let found = snapshot
            .lookup_dentry(&client, &reader, [2; 32], b"a", 512 * 1024)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(found.inode, 3);
        for inode in [3, 9] {
            let mut output = [0; 37];
            snapshot
                .read_inode_range(
                    &client,
                    &reader,
                    inode,
                    511999,
                    &mut output,
                    32 * 1024 * 1024,
                )
                .await
                .unwrap();
            assert_eq!(output, [inode as u8; 37]);
        }
        assert!(
            !std::fs::read_dir(temp.path())
                .unwrap()
                .filter_map(Result::ok)
                .any(|entry| entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with("brewfs-wire005-index-"))
        );
    }
}
