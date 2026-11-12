//! Private Stage3 vertical slice: actual GM07 entries, regular-file content,
//! PS09 canonical-run commitments and Cold ownership. This is not the complete
//! FD/container graph proof and cannot construct publication authority.
use super::super::super::budget::SnapshotOwned;
use super::super::super::{
    AuthenticatedSnapshot, SNAPSHOT_HEADER_LEN, SnapshotBudgetPool, SnapshotColdAttributes,
    SnapshotFrameDirectoryPage, SnapshotGroupRef, SnapshotInodeLocation, SnapshotLargeExtent,
    SnapshotPlacement,
};
use super::super::payload::{PayloadLimits, PayloadSummary, authenticate_payload};
use super::super::physical::read_metadata;
use super::*;
use crate::cadapter::client::{ObjectBackend, ObjectClient};
use crate::workspace::packed::PackedFrameDescriptor;
use sha2::{Digest, Sha256};

#[derive(Clone, Copy)]
pub(in super::super) struct ContentContexts {
    pub containers: ContextId,
    pub frames: ContextId,
    pub cold: ContextId,
    pub selectors: ContextId,
}

#[derive(Clone, Copy)]
pub(in super::super) struct GroupContentLimits {
    pub max_requested_bytes: u64,
    pub max_decoded_bytes: u64,
    pub max_frame_validation_steps: u64,
    /// Charge every regular alias's true EOF before hashing data or zero holes.
    pub max_logical_hash_bytes: u64,
    pub max_entries: u64,
    pub chunk_bytes: usize,
}

#[derive(Clone, Copy, Debug, Default)]
pub(in super::super) struct GroupContentSummary {
    pub groups: u64,
    pub entries: u64,
    pub cold_objects: u64,
    pub requested_bytes: u64,
    pub decoded_bytes: u64,
    pub frame_validation_steps: u64,
    pub logical_hash_bytes: u64,
}

struct Work {
    limits: GroupContentLimits,
    counts: GroupContentSummary,
}
impl Work {
    fn validate_frame(&mut self, descriptors: usize) -> PackedResult<()> {
        self.counts.frame_validation_steps = self
            .counts
            .frame_validation_steps
            .checked_add(descriptors as u64)
            .filter(|n| *n <= self.limits.max_frame_validation_steps)
            .ok_or_else(|| limit("group content frame-validation work quota exceeded"))?;
        Ok(())
    }
    fn decode(&mut self, bytes: u64) -> PackedResult<()> {
        self.counts.decoded_bytes = self
            .counts
            .decoded_bytes
            .checked_add(bytes)
            .filter(|n| *n <= self.limits.max_decoded_bytes)
            .ok_or_else(|| limit("group content decoded-byte quota exceeded"))?;
        Ok(())
    }
    fn request(&mut self, bytes: u64) -> PackedResult<()> {
        self.counts.requested_bytes = self
            .counts
            .requested_bytes
            .checked_add(bytes)
            .filter(|n| *n <= self.limits.max_requested_bytes)
            .ok_or_else(|| limit("group content requested-byte quota exceeded"))?;
        Ok(())
    }
    fn logical(&mut self, eof: u64) -> PackedResult<()> {
        self.counts.logical_hash_bytes = self
            .counts
            .logical_hash_bytes
            .checked_add(eof)
            .filter(|n| *n <= self.limits.max_logical_hash_bytes)
            .ok_or_else(|| limit("group content logical-hash byte quota exceeded"))?;
        Ok(())
    }
    fn entry(&mut self) -> PackedResult<()> {
        self.counts.entries = self
            .counts
            .entries
            .checked_add(1)
            .filter(|n| *n <= self.limits.max_entries)
            .ok_or_else(|| limit("group content entry quota exceeded"))?;
        Ok(())
    }
}

fn content_live(budget: &SnapshotMountBudget, cancel: &CancellationToken) -> PackedResult<()> {
    if cancel.is_cancelled() {
        return Err(PackedWireError::Backend(
            "group content audit cancelled".into(),
        ));
    }
    if budget.state().closed {
        return Err(limit("group content mount budget closed"));
    }
    Ok(())
}

async fn interruptible<T>(
    future: impl std::future::Future<Output = PackedResult<T>>,
    budget: &SnapshotMountBudget,
    cancel: &CancellationToken,
) -> PackedResult<T> {
    content_live(budget, cancel)?;
    tokio::select! {
        biased;
        _ = cancel.cancelled() => Err(PackedWireError::Backend("group content audit cancelled".into())),
        _ = budget.wait_closed() => Err(limit("group content mount budget closed")),
        result = future => { content_live(budget, cancel)?; result }
    }
}

/// Wire PS09 uses EOF and coalesced data-run commitments. The separate full
/// logical digest includes zero holes and compares aliases across layouts.
struct LogicalHash {
    eof: u64,
    cursor: u64,
    full: Sha256,
    wire: Sha256,
    run: Sha256,
    run_start: Option<u64>,
    run_len: u64,
}
impl LogicalHash {
    fn new(eof: u64) -> Self {
        let mut wire = Sha256::new();
        wire.update(eof.to_le_bytes());
        Self {
            eof,
            cursor: 0,
            full: Sha256::new(),
            wire,
            run: Sha256::new(),
            run_start: None,
            run_len: 0,
        }
    }
    fn flush_run(&mut self) {
        if let Some(start) = self.run_start.take() {
            self.wire.update(start.to_le_bytes());
            self.wire.update(self.run_len.to_le_bytes());
            self.wire
                .update(std::mem::replace(&mut self.run, Sha256::new()).finalize());
            self.run_len = 0;
        }
    }
    async fn holes(
        &mut self,
        end: u64,
        zeros: &[u8],
        budget: &SnapshotMountBudget,
        cancel: &CancellationToken,
    ) -> PackedResult<()> {
        if end < self.cursor || end > self.eof {
            return Err(invalid("logical hole exceeds true EOF or overlaps data"));
        }
        let mut chunks = 0;
        while self.cursor < end {
            content_live(budget, cancel)?;
            let count = (end - self.cursor).min(zeros.len() as u64) as usize;
            self.full.update(&zeros[..count]);
            self.cursor += count as u64;
            chunks += 1;
            if chunks % 16 == 0 {
                tokio::task::yield_now().await;
            }
        }
        Ok(())
    }
    async fn data(
        &mut self,
        start: u64,
        bytes: &[u8],
        zeros: &[u8],
        budget: &SnapshotMountBudget,
        cancel: &CancellationToken,
    ) -> PackedResult<()> {
        let end = start
            .checked_add(bytes.len() as u64)
            .filter(|n| *n <= self.eof)
            .ok_or_else(|| invalid("logical data exceeds true EOF"))?;
        if start < self.cursor {
            return Err(invalid("logical data overlaps a previous extent"));
        }
        if start > self.cursor {
            self.flush_run();
            self.holes(start, zeros, budget, cancel).await?;
        }
        if self.run_start.is_none() {
            self.run_start = Some(start);
        }
        // Hash in bounded chunks so one 8 MiB raw slice cannot starve a cancel.
        for (ordinal, chunk) in bytes.chunks(64 << 10).enumerate() {
            content_live(budget, cancel)?;
            self.full.update(chunk);
            self.run.update(chunk);
            if ordinal % 16 == 15 {
                tokio::task::yield_now().await;
            }
        }
        self.run_len = self
            .run_len
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| limit("logical contiguous-run count overflow"))?;
        self.cursor = end;
        Ok(())
    }
    async fn finish(
        mut self,
        zeros: &[u8],
        budget: &SnapshotMountBudget,
        cancel: &CancellationToken,
    ) -> PackedResult<([u8; 32], [u8; 32])> {
        self.flush_run();
        self.holes(self.eof, zeros, budget, cancel).await?;
        content_live(budget, cancel)?;
        Ok((self.full.finalize().into(), self.wire.finalize().into()))
    }
}

impl SemanticFacts {
    /// Prerequisite: audit_namespace_relations completed on this same facts
    /// instance. It supplies actual canonical/alias/group correspondence only;
    /// no runtime cache or sampled lookup stands in for every GM07 entry.
    pub(in super::super) async fn audit_group_content<B: ObjectBackend + Clone>(
        &mut self,
        client: &ObjectClient<B>,
        snapshot: &AuthenticatedSnapshot,
        contexts: ContentContexts,
        limits: GroupContentLimits,
    ) -> PackedResult<GroupContentSummary> {
        self.begin()?;
        let result = self
            .group_content_inner(client, snapshot, contexts, limits)
            .await;
        self.end(result)
    }

    async fn content_bind(
        &mut self,
        id: ContextId,
        role: SemanticRole,
        reference: &SnapshotObjectRef,
    ) -> PackedResult<()> {
        let len = reference.object_len.to_le_bytes();
        if !self.exists(|| sea_orm::sqlx::query("SELECT 1 FROM contexts c JOIN objects o ON o.key=c.root_key WHERE c.id=? AND c.role=? AND c.finished=1 AND c.root_key=? AND o.kind=? AND o.object_len=? AND o.digest=? AND o.authenticated=1 LIMIT 1")
            .bind(id.0).bind(role.tag()).bind(reference.key.as_bytes()).bind(reference.kind as i64).bind(len.as_slice()).bind(reference.digest.as_slice())).await? {
            return Err(invalid("content context does not bind its completed authenticated manifest root"));
        }
        Ok(())
    }
    async fn content_leaf(
        &mut self,
        id: ContextId,
        key: &[u8],
    ) -> PackedResult<Option<OwnedSemanticRow<Vec<u8>>>> {
        self.row(|| sea_orm::sqlx::query("SELECT value FROM context_leaves WHERE context_id=? AND first_key=? AND last_key=? LIMIT 1").bind(id.0).bind(key).bind(key), |row| column(row, 0)).await
    }
    async fn content_object(
        &mut self,
        contexts: ContentContexts,
        ordinal: u32,
    ) -> PackedResult<OwnedSemanticRow<SnapshotObjectRef>> {
        let key = ordinal.to_be_bytes();
        self.row(|| sea_orm::sqlx::query("SELECT value FROM context_leaves WHERE context_id=? AND first_key=? AND last_key=? LIMIT 1")
            .bind(contexts.containers.0).bind(key.as_slice()).bind(key.as_slice()), |row| {
                let value: Vec<u8> = column(row, 0)?;
                SnapshotObjectRef::decode_value(&value)
            }).await?.ok_or_else(|| invalid("content references absent ContainerIndex ordinal"))
    }
    async fn content_authenticated(&mut self, reference: &SnapshotObjectRef) -> PackedResult<()> {
        self.register_inner(reference).await?;
        self.charge_sql(2)?;
        let result = self.inventory.mark_authenticated(reference).await;
        self.map_error(result)
    }
    async fn content_fd_ref(
        &mut self,
        contexts: ContentContexts,
        container: u32,
        frame: u32,
    ) -> PackedResult<OwnedSemanticRow<SnapshotObjectRef>> {
        let mut key = [0u8; 8];
        key[..4].copy_from_slice(&container.to_be_bytes());
        key[4..].copy_from_slice(&frame.to_be_bytes());
        self.row(|| sea_orm::sqlx::query("SELECT first_key,last_key,value FROM context_leaves WHERE context_id=? AND first_key<=? ORDER BY first_key DESC LIMIT 1")
            .bind(contexts.frames.0).bind(key.as_slice()), |row| {
                let first: Vec<u8> = column(row, 0)?; let last: Vec<u8> = column(row, 1)?; let value: Vec<u8> = column(row, 2)?;
                if first.len()!=8 || last.len()!=8 || first[..4]!=key[..4] || last[..4]!=key[..4] || last.as_slice()<key.as_slice() {
                    return Err(invalid("actual extent is outside every same-container FD index range"));
                }
                SnapshotObjectRef::decode_value(&value)
            }).await?.ok_or_else(|| invalid("actual extent lacks an FD index range"))
    }
    #[allow(clippy::too_many_arguments)]
    async fn content_fd<B: ObjectBackend + Clone>(
        &mut self,
        client: &ObjectClient<B>,
        snapshot: &AuthenticatedSnapshot,
        contexts: ContentContexts,
        container_ordinal: u32,
        frame_ordinal: u32,
        container: &SnapshotObjectRef,
        work: &mut Work,
    ) -> PackedResult<SnapshotOwned<SnapshotFrameDirectoryPage>> {
        let reference = self
            .content_fd_ref(contexts, container_ordinal, frame_ordinal)
            .await?;
        work.request(reference.object_len)?;
        let mut permit = self
            .budget
            .admit(&[(SnapshotBudgetPool::Metadata, 1 << 20)])?;
        let bytes = read_metadata(
            client,
            &reference,
            &self.budget,
            work.limits.chunk_bytes,
            &self.cancel,
        )
        .await?;
        let page = SnapshotFrameDirectoryPage::decode(&reference, &bytes)?;
        let header = snapshot.manifest();
        if page.container_digest != container.digest
            || page.container_len != container.object_len
            || page.profile != header.profile
            || page.size_classes != header.size_classes
            || page.frame_policy != header.build.policy.frames
        {
            return Err(invalid(
                "FD06 source/profile/size classes/frame policy disagree with authenticated manifest/container",
            ));
        }
        let slot = frame_ordinal
            .checked_sub(page.first_ordinal)
            .filter(|n| (*n as usize) < page.frames.len())
            .ok_or_else(|| invalid("actual extent's FD ordinal lies outside decoded page"))?;
        if page.frames[slot as usize].frame_ordinal != frame_ordinal {
            return Err(invalid("FD06 ordinal mismatch"));
        }
        self.content_authenticated(&reference).await?;
        permit.shrink(
            SnapshotBudgetPool::Metadata,
            (std::mem::size_of::<SnapshotFrameDirectoryPage>()
                + page.frames.capacity() * std::mem::size_of::<PackedFrameDescriptor>())
                as u64,
        )?;
        Ok(SnapshotOwned::new(page, permit))
    }
    #[allow(clippy::too_many_arguments)]
    async fn content_extent<B: ObjectBackend + Clone>(
        &mut self,
        client: &ObjectClient<B>,
        snapshot: &AuthenticatedSnapshot,
        contexts: ContentContexts,
        container_ordinal: u32,
        frame_ordinal: u32,
        raw_len: u32,
        raw_offset: u32,
        logical_len: u32,
        file_offset: u64,
        hash: &mut LogicalHash,
        zeros: &[u8],
        work: &mut Work,
        group_claim: Option<(&SnapshotGroupRef, u32)>,
        external_owner: Option<u64>,
    ) -> PackedResult<()> {
        let container = self.content_object(contexts, container_ordinal).await?;
        let expected_kind = if group_claim.is_some() {
            SnapshotObjectKind::GroupContainer
        } else {
            SnapshotObjectKind::LargeData
        };
        if container.kind != expected_kind {
            return Err(invalid(
                "actual entry/LE09 selected a payload of the wrong kind",
            ));
        }
        let external_frame_count = if let Some(owner) = external_owner {
            work.request(container.object_len)?;
            let payload = authenticate_payload(
                client,
                &container,
                container_ordinal,
                &self.budget,
                PayloadLimits {
                    chunk_bytes: work.limits.chunk_bytes,
                    ..Default::default()
                },
                &self.cancel,
            )
            .await?;
            let PayloadSummary::Large {
                inode, frame_count, ..
            } = payload.summary()
            else {
                return Err(invalid("LE09 selected non-LD05 payload prefix"));
            };
            if *inode != owner {
                return Err(invalid(
                    "LD05 prefix owner differs from true External selector inode",
                ));
            }
            let count = *frame_count;
            self.content_authenticated(&container).await?;
            Some(count)
        } else {
            None
        };
        let page = self
            .content_fd(
                client,
                snapshot,
                contexts,
                container_ordinal,
                frame_ordinal,
                &container,
                work,
            )
            .await?;
        let descriptor = &page.frames[(frame_ordinal - page.first_ordinal) as usize];
        if descriptor.raw_len != raw_len
            || raw_offset
                .checked_add(logical_len)
                .is_none_or(|n| n > raw_len)
        {
            return Err(invalid(
                "actual extent raw claim exceeds or disagrees with FD06 raw frame",
            ));
        }
        if external_frame_count.is_some_and(|count| frame_ordinal >= count)
            || (external_frame_count.is_some()
                && descriptor.object_offset < (SNAPSHOT_HEADER_LEN + 32) as u64)
        {
            return Err(invalid(
                "LE09/FD06 frame lies outside actual LD05 frame count/body prefix",
            ));
        }
        if let Some((group, ordinal)) = group_claim
            && (frame_ordinal < group.first_frame
                || frame_ordinal - group.first_frame >= group.frame_count
                || ordinal < descriptor.first_file_slot
                || ordinal > descriptor.last_file_slot
                || descriptor.object_offset < group.meta_offset + u64::from(group.meta_stored_len))
        {
            return Err(invalid(
                "actual GM07 extent does not bind its group/frame/file slot",
            ));
        }
        work.request(u64::from(descriptor.stored_len))?;
        work.decode(u64::from(descriptor.raw_len))?;
        work.validate_frame(page.frames.len())?;
        let raw = interruptible(
            page.read_frame_owned(client, &container, frame_ordinal, &self.budget),
            &self.budget,
            &self.cancel,
        )
        .await?;
        let first = raw_offset as usize;
        let end = first
            .checked_add(logical_len as usize)
            .ok_or_else(|| invalid("actual extent raw end overflow"))?;
        let slice = raw
            .raw
            .get(first..end)
            .ok_or_else(|| invalid("actual extent exceeds decoded frame allocation"))?;
        hash.data(file_offset, slice, zeros, &self.budget, &self.cancel)
            .await
    }
    #[allow(clippy::too_many_arguments)]
    async fn content_external<B: ObjectBackend + Clone>(
        &mut self,
        client: &ObjectClient<B>,
        snapshot: &AuthenticatedSnapshot,
        contexts: ContentContexts,
        inode: u64,
        eof: u64,
        placement: &SnapshotPlacement,
        hash: &mut LogicalHash,
        zeros: &[u8],
        work: &mut Work,
    ) -> PackedResult<[u8; 32]> {
        let SnapshotPlacement::External {
            data_bytes,
            extent_count,
            logical_digest,
            extents,
            ..
        } = placement
        else {
            return Err(invalid("external content requires PS09 External"));
        };
        let inode_key = inode.to_be_bytes();
        let eof_key = eof.to_be_bytes();
        let len = extents.object_len.to_le_bytes();
        let context = self.row(|| sea_orm::sqlx::query("SELECT c.id FROM contexts c JOIN objects o ON o.key=c.root_key WHERE c.role=8 AND c.inode=? AND c.eof=? AND c.finished=1 AND c.root_key=? AND o.kind=? AND o.object_len=? AND o.digest=? AND o.authenticated=1 LIMIT 1")
            .bind(inode_key.as_slice()).bind(eof_key.as_slice()).bind(extents.key.as_bytes()).bind(extents.kind as i64).bind(len.as_slice()).bind(extents.digest.as_slice()), |row| Ok(ContextId(column(row,0)?)))
            .await?.ok_or_else(|| invalid("external content lacks its completed true-inode/EOF extent context"))?;
        let context_id = *context;
        drop(context);
        let _cursor_owner = self.budget.admit(&[(SnapshotBudgetPool::Metadata, 4096)])?;
        let mut cursor = Vec::with_capacity(16);
        let mut bytes = 0u64;
        let mut count = 0u64;
        loop {
            let record = self.row(|| sea_orm::sqlx::query("SELECT first_key,last_key,value FROM context_leaves WHERE context_id=? AND first_key>? ORDER BY first_key LIMIT 1")
                .bind(context_id.0).bind(cursor.as_slice()), |row| Ok(SnapshotIndexRecord {first_key:column(row,0)?,last_key:column(row,1)?,value:SnapshotIndexValue::Leaf(column(row,2)?)})).await?;
            let Some(record) = record else {
                break;
            };
            let extent = SnapshotLargeExtent::decode_record(&record, inode, eof)?;
            cursor.clear();
            cursor.extend_from_slice(&record.first_key);
            drop(record);
            self.content_extent(
                client,
                snapshot,
                contexts,
                extent.container_ordinal,
                extent.frame_ordinal,
                extent.raw_len,
                extent.raw_offset,
                extent.logical_len,
                extent.file_offset,
                hash,
                zeros,
                work,
                None,
                Some(inode),
            )
            .await?;
            bytes = bytes
                .checked_add(u64::from(extent.logical_len))
                .ok_or_else(|| limit("external actual data bytes overflow"))?;
            count = count
                .checked_add(1)
                .ok_or_else(|| limit("external actual extent count overflow"))?;
        }
        if bytes != *data_bytes || count != *extent_count {
            return Err(invalid("PS09 actual data byte/extent totals mismatch"));
        }
        Ok(*logical_digest)
    }
    async fn group_content_inner<B: ObjectBackend + Clone>(
        &mut self,
        client: &ObjectClient<B>,
        snapshot: &AuthenticatedSnapshot,
        contexts: ContentContexts,
        limits: GroupContentLimits,
    ) -> PackedResult<GroupContentSummary> {
        if limits.max_logical_hash_bytes == 0
            || limits.max_decoded_bytes == 0
            || limits.max_entries == 0
            || limits.chunk_bytes == 0
            || limits.chunk_bytes > 1 << 20
        {
            return Err(limit("invalid group content audit limits"));
        }
        let header = snapshot.manifest();
        for (id, role, ordinal) in [
            (contexts.containers, SemanticRole::Containers, 2),
            (contexts.frames, SemanticRole::Frames, 3),
            (contexts.cold, SemanticRole::Cold, 4),
            (contexts.selectors, SemanticRole::NamespaceSelectors, 6),
        ] {
            self.content_bind(id, role, &header.roots[ordinal]).await?;
        }
        let root = header.root_inode.to_be_bytes();
        if !self
            .exists(|| {
                sea_orm::sqlx::query(
                    "SELECT 1 FROM ns_root WHERE id=1 AND inode=? AND dir_key=? LIMIT 1",
                )
                .bind(root.as_slice())
                .bind(header.root_dir_key.as_slice())
            })
            .await?
        {
            return Err(invalid(
                "group content requires namespace facts bound to the same snapshot root",
            ));
        }
        self.execute(|| sea_orm::sqlx::query("CREATE TABLE content_aliases(reverse_key BLOB PRIMARY KEY NOT NULL,inode BLOB NOT NULL CHECK(length(inode)=8),digest BLOB CHECK(digest IS NULL OR length(digest)=32)) WITHOUT ROWID")).await?;
        self.execute(|| {
            sea_orm::sqlx::query(
                "CREATE INDEX content_inode_hashes ON content_aliases(inode,digest)",
            )
        })
        .await?;
        self.execute(|| sea_orm::sqlx::query("CREATE TABLE content_cold(inode BLOB PRIMARY KEY NOT NULL CHECK(length(inode)=8)) WITHOUT ROWID")).await?;
        let _bookkeeping = self
            .budget
            .admit(&[(SnapshotBudgetPool::Metadata, 72 << 10)])?;
        let zeros = vec![0u8; 64 << 10];
        let mut group_cursor = Vec::with_capacity(2048);
        let mut work = Work {
            limits,
            counts: GroupContentSummary::default(),
        };
        loop {
            let group=self.row(|| sea_orm::sqlx::query("SELECT group_key,value FROM ns_groups WHERE group_key>? ORDER BY group_key LIMIT 1").bind(group_cursor.as_slice()),|row| {
                let key:Vec<u8>=column(row,0)?;let value:Vec<u8>=column(row,1)?;Ok((key,SnapshotGroupRef::decode_value(&value)?))
            }).await?;
            let Some(group) = group else {
                break;
            };
            group_cursor.clear();
            group_cursor.extend_from_slice(&group.0);
            let container = self
                .content_object(contexts, group.1.container_ordinal)
                .await?;
            if container.kind != SnapshotObjectKind::GroupContainer {
                return Err(invalid("Groups GR05 ordinal selects non-GC05"));
            }
            work.request(container.object_len)?;
            let payload = authenticate_payload(
                client,
                &container,
                group.1.container_ordinal,
                &self.budget,
                PayloadLimits {
                    chunk_bytes: limits.chunk_bytes,
                    ..Default::default()
                },
                &self.cancel,
            )
            .await?;
            let PayloadSummary::Group {
                profile, groups, ..
            } = payload.summary()
            else {
                return Err(invalid("Groups GR05 selected non-GC05 summary"));
            };
            if *profile != header.profile || !groups.iter().any(|actual| actual == &group.1) {
                return Err(invalid(
                    "Groups/IL05 GR05 differs from actual authenticated GC05 directory",
                ));
            }
            self.content_authenticated(&container).await?;
            work.request(u64::from(group.1.meta_stored_len))?;
            work.decode(u64::from(group.1.meta_raw_len))?;
            let metadata = interruptible(
                group
                    .1
                    .read_metadata_owned(client, &container, 512 << 10, &self.budget),
                &self.budget,
                &self.cancel,
            )
            .await?;
            drop(payload);
            drop(container);
            for (ordinal, entry) in metadata.entries().iter().enumerate() {
                self.live()?;
                work.entry()?;
                let alias=self.row(|| sea_orm::sqlx::query("SELECT reverse_key,value FROM ns_aliases WHERE group_key=? AND ordinal=? LIMIT 1").bind(group.0.as_slice()).bind(ordinal as i64),|row| {
                    let key:Vec<u8>=column(row,0)?;let value:Vec<u8>=column(row,1)?;Ok((key,SnapshotInodeLocation::decode_value(&value)?))
                }).await?.ok_or_else(|| invalid("actual GM07 ordinal is absent from namespace Reverse aliases"))?;
                alias.1.validate_entry(entry)?;
                if alias.1.group != group.1
                    || alias.1.hot.entry_ordinal != ordinal as u32
                    || alias.0 != alias.1.reverse_key()
                {
                    return Err(invalid(
                        "actual GM07 ordinal/name/GR05 do not bind the exact IL05 alias",
                    ));
                }
                // The owner remains charged while temporary full digests exist.
                let digest = if entry.kind == 1 {
                    work.logical(entry.size)?;
                    let mut hash = LogicalHash::new(entry.size);
                    let selector_row = self
                        .content_leaf(contexts.selectors, &entry.inode.to_be_bytes())
                        .await?;
                    let placement = selector_row
                        .as_ref()
                        .map(|value| SnapshotPlacement::decode(value, entry.inode, entry.size))
                        .transpose()?;
                    drop(selector_row);
                    // Drop the SQL row before extent lookups; at most group,
                    // alias, container and FD row owners coexist.
                    let wire_expected = if let Some(
                        placement @ SnapshotPlacement::External { .. },
                    ) = placement
                    {
                        if entry.flags != 0
                            || !entry.inline_data.is_empty()
                            || !entry.extents.is_empty()
                        {
                            return Err(invalid(
                                "External GM07 entry carries conflicting inline/group-frame data",
                            ));
                        }
                        Some(
                            self.content_external(
                                client,
                                snapshot,
                                contexts,
                                entry.inode,
                                entry.size,
                                &placement,
                                &mut hash,
                                &zeros,
                                &mut work,
                            )
                            .await?,
                        )
                    } else {
                        if !entry.inline_data.is_empty() {
                            hash.data(0, &entry.inline_data, &zeros, &self.budget, &self.cancel)
                                .await?;
                        } else {
                            for extent in &entry.extents {
                                self.content_extent(
                                    client,
                                    snapshot,
                                    contexts,
                                    group.1.container_ordinal,
                                    extent.frame_ordinal,
                                    extent.raw_len,
                                    extent.raw_offset,
                                    extent.logical_len,
                                    extent.file_offset,
                                    &mut hash,
                                    &zeros,
                                    &mut work,
                                    Some((&group.1, ordinal as u32)),
                                    None,
                                )
                                .await?;
                            }
                        }
                        None
                    };
                    let (full, wire) = hash.finish(&zeros, &self.budget, &self.cancel).await?;
                    if wire_expected.is_some_and(|expected| expected != wire) {
                        return Err(invalid(
                            "PS09 logical run digest disagrees with actual true-EOF extent bytes",
                        ));
                    }
                    Some(full)
                } else {
                    None
                };
                let inode = entry.inode.to_be_bytes();
                if let Some(digest) = digest
                    && self
                        .exists(|| {
                            sea_orm::sqlx::query(
                                "SELECT 1 FROM content_aliases WHERE inode=? AND digest<>? LIMIT 1",
                            )
                            .bind(inode.as_slice())
                            .bind(digest.as_slice())
                        })
                        .await?
                {
                    return Err(invalid(
                        "hardlink aliases have different actual logical content including holes",
                    ));
                }
                self.execute(|| {
                    sea_orm::sqlx::query(
                        "INSERT INTO content_aliases(reverse_key,inode,digest) VALUES(?,?,?)",
                    )
                    .bind(alias.0.as_slice())
                    .bind(inode.as_slice())
                    .bind(digest.as_ref().map(|value| value.as_slice()))
                })
                .await?;
            }
            work.counts.groups = work
                .counts
                .groups
                .checked_add(1)
                .ok_or_else(|| limit("actual GM07 group count overflow"))?;
        }
        if self.exists(|| sea_orm::sqlx::query("SELECT 1 FROM ns_aliases a LEFT JOIN content_aliases c ON c.reverse_key=a.reverse_key WHERE c.reverse_key IS NULL LIMIT 1")).await? {return Err(invalid("Reverse alias is absent from all actual GM07 entries"));}
        self.content_cold_all(client, snapshot, contexts, &mut work)
            .await?;
        Ok(work.counts)
    }
    async fn content_cold_all<B: ObjectBackend + Clone>(
        &mut self,
        client: &ObjectClient<B>,
        snapshot: &AuthenticatedSnapshot,
        contexts: ContentContexts,
        work: &mut Work,
    ) -> PackedResult<()> {
        let mut cursor = [0u8; 8];
        loop {
            let cold=self.row(|| sea_orm::sqlx::query("SELECT first_key,value FROM context_leaves WHERE context_id=? AND first_key>? ORDER BY first_key LIMIT 1").bind(contexts.cold.0).bind(cursor.as_slice()),|row| {
                let key:Vec<u8>=column(row,0)?;let value:Vec<u8>=column(row,1)?;Ok((be8(&key)?,SnapshotObjectRef::decode_value(&value)?))
            }).await?;
            let Some(cold) = cold else {
                break;
            };
            cursor = cold.0.to_be_bytes();
            let root = snapshot.manifest().root_inode;
            let owner = if cold.0 == root {
                None
            } else {
                Some(
                    self.row(
                        || {
                            sea_orm::sqlx::query(
                                "SELECT value FROM ns_canonical WHERE inode=? LIMIT 1",
                            )
                            .bind(cursor.as_slice())
                        },
                        |row| {
                            let value: Vec<u8> = column(row, 0)?;
                            SnapshotInodeLocation::decode_value(&value)
                        },
                    )
                    .await?
                    .ok_or_else(|| invalid("ColdIndex has no actual canonical/root owner"))?,
                )
            };
            let (kind, mode, size) = owner.as_ref().map_or(
                (
                    2,
                    snapshot
                        .manifest()
                        .source
                        .as_ref()
                        .map_or(0o040755, |source| source.root.mode),
                    0,
                ),
                |owner| (owner.hot.kind, owner.hot.mode, owner.hot.size),
            );
            work.request(cold.1.object_len)?;
            let _decoded = self
                .budget
                .admit(&[(SnapshotBudgetPool::Metadata, 1 << 20)])?;
            let bytes = read_metadata(
                client,
                &cold.1,
                &self.budget,
                work.limits.chunk_bytes,
                &self.cancel,
            )
            .await?;
            let attrs = SnapshotColdAttributes::decode(&cold.1, &bytes, cold.0)?;
            attrs.validate_for_inode(kind, mode)?;
            match (&attrs.symlink_target, kind) {
                (Some(target), 3) if target.len() as u64 == size => {}
                (None, kind) if kind != 3 => {}
                _ => {
                    return Err(invalid(
                        "Cold target presence/true length does not match actual inode kind/EOF",
                    ));
                }
            }
            self.content_authenticated(&cold.1).await?;
            self.execute(|| {
                sea_orm::sqlx::query("INSERT INTO content_cold(inode) VALUES(?)")
                    .bind(cursor.as_slice())
            })
            .await?;
            work.counts.cold_objects = work
                .counts
                .cold_objects
                .checked_add(1)
                .ok_or_else(|| limit("actual Cold count overflow"))?;
        }
        if self.exists(|| sea_orm::sqlx::query("SELECT 1 FROM ns_canonical n LEFT JOIN content_cold c ON c.inode=n.inode WHERE n.kind=3 AND c.inode IS NULL LIMIT 1")).await? {return Err(invalid("actual symlink inode lacks Cold target"));}
        Ok(())
    }
}
