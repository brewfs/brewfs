//! Every incoming CI/FI occurrence and every payload frame is checked here.
//! These disposable facts still cannot construct catalog publication authority.
use super::super::super::{
    AuthenticatedSnapshot, SNAPSHOT_FOOTER_LEN, SNAPSHOT_HEADER_LEN, SnapshotBudgetPool,
    SnapshotBuildProvenance, SnapshotFrameDirectoryPage, SnapshotGroupRef, SnapshotInodeLocation,
    SnapshotLargeExtent, SnapshotPlacement, observer_validation_error, page_read_class,
};
use super::super::payload::{PayloadLimits, PayloadSummary, authenticate_payload};
use super::super::physical::read_metadata;
use super::*;
use crate::cadapter::client::{ObjectBackend, ObjectClient};

#[derive(Clone, Copy)]
pub(in super::super) struct ContainerContexts {
    pub groups: ContextId,
    pub inodes: ContextId,
    pub containers: ContextId,
    pub frames: ContextId,
    pub selectors: ContextId,
}

#[derive(Clone, Copy)]
pub(in super::super) struct ContainerOccurrenceLimits {
    pub max_requested_bytes: u64,
    pub max_decoded_bytes: u64,
    pub max_frame_validation_steps: u64,
    pub chunk_bytes: usize,
}

#[derive(Clone, Copy, Debug, Default)]
pub(in super::super) struct ContainerOccurrenceSummary {
    pub containers: u64,
    pub frame_pages: u64,
    pub groups: u64,
    pub requested_bytes: u64,
    pub decoded_bytes: u64,
    pub frame_validation_steps: u64,
}

struct OccurrenceWork {
    limits: ContainerOccurrenceLimits,
    counts: ContainerOccurrenceSummary,
    observed: SnapshotBuildProvenance,
}
fn add_count(counter: &mut u64, amount: u64) -> PackedResult<()> {
    *counter = counter
        .checked_add(amount)
        .ok_or_else(|| limit("container occurrence counter overflow"))?;
    Ok(())
}
impl OccurrenceWork {
    fn validate_frame(&mut self, descriptors: usize) -> PackedResult<()> {
        add_count(&mut self.counts.frame_validation_steps, descriptors as u64)?;
        if self.counts.frame_validation_steps > self.limits.max_frame_validation_steps {
            return Err(limit(
                "container occurrence frame-validation work quota exceeded",
            ));
        }
        Ok(())
    }
    fn request(&mut self, bytes: u64) -> PackedResult<()> {
        add_count(&mut self.counts.requested_bytes, bytes)?;
        if self.counts.requested_bytes > self.limits.max_requested_bytes {
            return Err(limit("container occurrence requested-byte quota exceeded"));
        }
        Ok(())
    }
    fn decode(&mut self, bytes: u64) -> PackedResult<()> {
        add_count(&mut self.counts.decoded_bytes, bytes)?;
        if self.counts.decoded_bytes > self.limits.max_decoded_bytes {
            return Err(limit("container occurrence decoded-byte quota exceeded"));
        }
        Ok(())
    }
}

async fn occurrence_interruptible<T>(
    future: impl std::future::Future<Output = PackedResult<T>>,
    budget: &SnapshotMountBudget,
    cancel: &CancellationToken,
) -> PackedResult<T> {
    let live = || {
        if cancel.is_cancelled() {
            return Err(PackedWireError::Backend(
                "container occurrence audit cancelled".into(),
            ));
        }
        if budget.state().closed {
            return Err(limit("container occurrence mount budget closed"));
        }
        Ok(())
    };
    live()?;
    tokio::select! {
        biased;
        _ = cancel.cancelled() => Err(PackedWireError::Backend("container occurrence audit cancelled".into())),
        _ = budget.wait_closed() => Err(limit("container occurrence mount budget closed")),
        result = future => { live()?; result }
    }
}

fn container_key(key: &[u8]) -> PackedResult<u32> {
    Ok(u32::from_be_bytes(
        key.try_into()
            .map_err(|_| invalid("CI occurrence key is not BE4"))?,
    ))
}
fn frame_key(container: u32, frame: u32) -> [u8; 8] {
    let mut key = [0; 8];
    key[..4].copy_from_slice(&container.to_be_bytes());
    key[4..].copy_from_slice(&frame.to_be_bytes());
    key
}

impl SemanticFacts {
    /// Namespace and group-content audits must have succeeded on these same
    /// completed contexts. Physical deduplication never skips an ordinal join.
    pub(in super::super) async fn audit_container_occurrences<B: ObjectBackend + Clone>(
        &mut self,
        client: &ObjectClient<B>,
        snapshot: &AuthenticatedSnapshot,
        contexts: ContainerContexts,
        limits: ContainerOccurrenceLimits,
    ) -> PackedResult<ContainerOccurrenceSummary> {
        self.begin()?;
        let result = self
            .container_occurrences_inner(client, snapshot, contexts, limits)
            .await;
        self.end(result)
    }

    async fn occurrence_context(
        &mut self,
        id: ContextId,
        role: SemanticRole,
        reference: &SnapshotObjectRef,
    ) -> PackedResult<()> {
        let len = reference.object_len.to_le_bytes();
        if !self.exists(|| sea_orm::sqlx::query("SELECT 1 FROM contexts c JOIN objects o ON o.key=c.root_key WHERE c.id=? AND c.role=? AND c.finished=1 AND c.root_key=? AND o.kind=? AND o.object_len=? AND o.digest=? AND o.authenticated=1 LIMIT 1")
            .bind(id.0).bind(role.tag()).bind(reference.key.as_bytes()).bind(reference.kind as i64).bind(len.as_slice()).bind(reference.digest.as_slice())).await? {
            return Err(invalid("container occurrence context does not bind completed authenticated manifest root"));
        }
        Ok(())
    }
    async fn occurrence_next_leaf(
        &mut self,
        id: ContextId,
        after: &[u8],
    ) -> PackedResult<Option<OwnedSemanticRow<SnapshotIndexRecord>>> {
        self.row(|| sea_orm::sqlx::query("SELECT first_key,last_key,value FROM context_leaves WHERE context_id=? AND first_key>? ORDER BY first_key LIMIT 1")
            .bind(id.0).bind(after), |row| Ok(SnapshotIndexRecord {
                first_key: column(row,0)?, last_key: column(row,1)?, value: SnapshotIndexValue::Leaf(column(row,2)?)
            })).await
    }
    async fn occurrence_authenticated(
        &mut self,
        reference: &SnapshotObjectRef,
    ) -> PackedResult<()> {
        self.register_inner(reference).await?;
        self.charge_sql(2)?;
        let result = self.inventory.mark_authenticated(reference).await;
        self.map_error(result)
    }
    async fn occurrence_group(
        &mut self,
        context: ContextId,
        group: &SnapshotGroupRef,
    ) -> PackedResult<()> {
        let mut first = group.parent_dir_key.to_vec();
        first.extend_from_slice(&group.first_name);
        let mut last = group.parent_dir_key.to_vec();
        last.extend_from_slice(&group.last_name);
        let row = self.row(|| sea_orm::sqlx::query("SELECT last_key,value FROM context_leaves WHERE context_id=? AND first_key=? LIMIT 1")
            .bind(context.0).bind(first.as_slice()), |row| {
                let fence: Vec<u8> = column(row,0)?;
                let value: Vec<u8> = column(row,1)?;
                Ok((fence,SnapshotGroupRef::decode_value(&value)?))
            }).await?.ok_or_else(|| invalid("actual GC05 group is absent from Groups context"))?;
        if row.0 != last || row.1 != *group {
            return Err(invalid(
                "actual GC05 directory differs from exact Groups fences/GR05",
            ));
        }
        Ok(())
    }

    /// Bind LD05 owner to its canonical regular inode and its true External
    /// selector, then prove this incoming ordinal occurs in that owner's LE09.
    /// Other owners' LE09 references are checked by the group-content audit.
    async fn occurrence_large_owner(
        &mut self,
        contexts: ContainerContexts,
        inode: u64,
        ordinal: u32,
    ) -> PackedResult<()> {
        let key = inode.to_be_bytes();
        let canonical = self.row(|| sea_orm::sqlx::query("SELECT value FROM context_leaves WHERE context_id=? AND first_key=? AND last_key=? LIMIT 1")
            .bind(contexts.inodes.0).bind(key.as_slice()).bind(key.as_slice()), |row| {
                let value: Vec<u8> = column(row,0)?;
                SnapshotInodeLocation::decode_value(&value)
            }).await?.ok_or_else(|| invalid("LD05 owner has no canonical inode"))?;
        if canonical.hot.inode != inode || canonical.hot.kind != 1 {
            return Err(invalid("LD05 owner is not its canonical regular inode"));
        }
        let eof = canonical.hot.size;
        drop(canonical);
        let selector = self.row(|| sea_orm::sqlx::query("SELECT value FROM context_leaves WHERE context_id=? AND first_key=? AND last_key=? LIMIT 1")
            .bind(contexts.selectors.0).bind(key.as_slice()).bind(key.as_slice()), |row| {
                let value: Vec<u8> = column(row,0)?;
                SnapshotPlacement::decode(&value,inode,eof)
            }).await?.ok_or_else(|| invalid("LD05 owner has no required PS09 selector"))?;
        let SnapshotPlacement::External { extents, .. } = &*selector else {
            return Err(invalid(
                "LD05 owner uses Group rather than External selector",
            ));
        };
        let eof_key = eof.to_be_bytes();
        let len = extents.object_len.to_le_bytes();
        let context = self.row(|| sea_orm::sqlx::query("SELECT c.id FROM contexts c JOIN objects o ON o.key=c.root_key WHERE c.role=8 AND c.inode=? AND c.eof=? AND c.finished=1 AND c.root_key=? AND o.kind=? AND o.object_len=? AND o.digest=? AND o.authenticated=1 LIMIT 1")
            .bind(key.as_slice()).bind(eof_key.as_slice()).bind(extents.key.as_bytes()).bind(extents.kind as i64).bind(len.as_slice()).bind(extents.digest.as_slice()), |row| Ok(ContextId(column(row,0)?)))
            .await?.ok_or_else(|| invalid("LD05 owner lacks exact completed External context"))?;
        let context_id = *context;
        drop(context);
        drop(selector);
        let mut cursor = Vec::with_capacity(16);
        loop {
            let Some(record) = self.occurrence_next_leaf(context_id, &cursor).await? else {
                return Err(invalid(
                    "LD05 incoming ordinal is absent from owner's LE09 context",
                ));
            };
            let extent = SnapshotLargeExtent::decode_record(&record, inode, eof)?;
            if extent.container_ordinal == ordinal {
                return Ok(());
            }
            cursor.clear();
            cursor.extend_from_slice(&record.first_key);
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn occurrence_frames<B: ObjectBackend + Clone>(
        &mut self,
        client: &ObjectClient<B>,
        snapshot: &AuthenticatedSnapshot,
        context: ContextId,
        ordinal: u32,
        container: &SnapshotObjectRef,
        payload: &PayloadSummary,
        work: &mut OccurrenceWork,
    ) -> PackedResult<()> {
        let (count, mut body_cursor, external) = match payload {
            PayloadSummary::Group {
                frame_count,
                metadata_end,
                ..
            } => (*frame_count, *metadata_end, false),
            PayloadSummary::Large { frame_count, .. } => {
                (*frame_count, (SNAPSHOT_HEADER_LEN + 32) as u64, true)
            }
        };
        let first_key = frame_key(ordinal, 0);
        let last_key = frame_key(ordinal, u32::MAX);
        let mut after: Option<[u8; 8]> = None;
        let mut next_frame = 0u32;
        let mut raw_total = 0u64;
        // GC directory order need not match frame order. Sort a bounded,
        // admitted index once and advance it across all FD pages.
        let (group_owners, _owner_index) = if let PayloadSummary::Group { groups, .. } = payload {
            let permit = self.budget.admit(&[(
                SnapshotBudgetPool::Metadata,
                (groups.len() * std::mem::size_of::<usize>()) as u64,
            )])?;
            let mut owners = Vec::with_capacity(groups.len());
            owners.extend(
                groups
                    .iter()
                    .enumerate()
                    .filter_map(|(index, group)| (group.frame_count != 0).then_some(index)),
            );
            owners.sort_unstable_by_key(|index| groups[*index].first_frame);
            (owners, Some(permit))
        } else {
            (Vec::new(), None)
        };
        let mut owner_cursor = 0usize;
        loop {
            // The first page includes frame zero. Later pages advance strictly
            // beyond the previous first fence. No namespace-sized Vec is kept.
            let lower = after.as_ref().unwrap_or(&first_key);
            let sql = if after.is_some() {
                "SELECT first_key,last_key,value FROM context_leaves WHERE context_id=? AND first_key>? AND first_key<=? ORDER BY first_key LIMIT 1"
            } else {
                "SELECT first_key,last_key,value FROM context_leaves WHERE context_id=? AND first_key>=? AND first_key<=? ORDER BY first_key LIMIT 1"
            };
            let record = self
                .row(
                    || {
                        sea_orm::sqlx::query(sql)
                            .bind(context.0)
                            .bind(lower.as_slice())
                            .bind(last_key.as_slice())
                    },
                    |row| {
                        Ok(SnapshotIndexRecord {
                            first_key: column(row, 0)?,
                            last_key: column(row, 1)?,
                            value: SnapshotIndexValue::Leaf(column(row, 2)?),
                        })
                    },
                )
                .await?;
            let Some(record) = record else {
                break;
            };
            let SnapshotIndexValue::Leaf(value) = &(*record).value else {
                return Err(invalid("FI occurrence is not a leaf"));
            };
            let reference = SnapshotObjectRef::decode_value(value)?;
            if reference.kind != SnapshotObjectKind::FrameDirectory {
                return Err(invalid("FI occurrence has a non-FD06 object"));
            }
            work.request(reference.object_len)?;
            let _metadata = self
                .budget
                .admit(&[(SnapshotBudgetPool::Metadata, 1 << 20)])?;
            let validation = client.begin_validation(page_read_class(reference.kind)?);
            let result = async {
                let bytes = read_metadata(client,&reference,&self.budget,work.limits.chunk_bytes,&self.cancel).await?;
                let page = SnapshotFrameDirectoryPage::decode(&reference,&bytes)?;
                let header = snapshot.manifest();
                let last = page.frames.last().ok_or_else(|| invalid("empty FD06 page"))?.frame_ordinal;
                if page.container_digest != container.digest || page.container_len != container.object_len
                    || page.profile != header.profile || page.size_classes != header.size_classes
                    || page.frame_policy != header.build.policy.frames
                    || page.first_ordinal != next_frame
                    || record.first_key != frame_key(ordinal,page.first_ordinal)
                    || record.last_key != frame_key(ordinal,last) {
                    return Err(invalid("FI exact fences/continuous ordinals or FD06 full container/profile/policy binding disagree"));
                }
                Ok(page)
            }.await;
            if let Some(validation) = validation {
                match &result {
                    Ok(_) => validation.succeed(),
                    Err(_) if self.cancel.is_cancelled() => drop(validation),
                    Err(error) => validation.fail(observer_validation_error(error.clone()).0),
                }
            }
            let page = result?;
            self.occurrence_authenticated(&reference).await?;
            after = Some(
                record
                    .first_key
                    .as_slice()
                    .try_into()
                    .map_err(|_| invalid("FI occurrence first fence is not BE8"))?,
            );
            drop(record);
            for frame in &page.frames {
                self.live()?;
                if frame.frame_ordinal != next_frame
                    || next_frame >= count
                    || frame.object_offset != body_cursor
                {
                    return Err(invalid(
                        "FD06 frames do not exactly cover actual payload ordinals/contiguous body",
                    ));
                }
                if let PayloadSummary::Group { groups, .. } = payload {
                    while let Some(index) = group_owners.get(owner_cursor) {
                        let group = &groups[*index];
                        if frame.frame_ordinal < group.first_frame + group.frame_count {
                            break;
                        }
                        owner_cursor += 1;
                    }
                    let group = group_owners
                        .get(owner_cursor)
                        .map(|index| &groups[*index])
                        .filter(|group| frame.frame_ordinal >= group.first_frame)
                        .ok_or_else(|| invalid("actual GC05 frame has no group owner"))?;
                    if frame.last_file_slot >= group.entry_count {
                        return Err(invalid(
                            "GC05 frame file slots exceed owning group's actual entry count",
                        ));
                    }
                } else if frame.first_file_slot != 0 || frame.last_file_slot != 0 {
                    return Err(invalid("LD05 frame has nonzero file slots"));
                }
                work.request(u64::from(frame.stored_len))?;
                work.decode(u64::from(frame.raw_len))?;
                work.validate_frame(page.frames.len())?;
                // This includes frames unused by any actual GM07/LE09 extent.
                // The codec output never escapes its Stored/Raw/Decode owners.
                let raw = occurrence_interruptible(
                    page.read_frame_owned(client, container, frame.frame_ordinal, &self.budget),
                    &self.budget,
                    &self.cancel,
                )
                .await?;
                if raw.raw.len() != frame.raw_len as usize {
                    return Err(invalid("decoded FD06 frame has an unexpected raw length"));
                }
                drop(raw);
                work.observed.observe_frame(frame, external)?;
                add_count(&mut raw_total, u64::from(frame.raw_len))?;
                body_cursor = body_cursor
                    .checked_add(u64::from(frame.stored_len))
                    .ok_or_else(|| invalid("FD06 contiguous body cursor overflow"))?;
                next_frame = next_frame
                    .checked_add(1)
                    .ok_or_else(|| invalid("FD06 ordinal overflow"))?;
                if next_frame.is_multiple_of(16) {
                    tokio::task::yield_now().await;
                }
            }
            add_count(&mut work.counts.frame_pages, 1)?;
        }
        if next_frame != count || body_cursor != container.object_len - SNAPSHOT_FOOTER_LEN as u64 {
            return Err(invalid(
                "complete FI occurrences do not cover payload frame count/body through footer",
            ));
        }
        if let PayloadSummary::Large {
            raw_total: expected,
            ..
        } = payload
            && raw_total != *expected
        {
            return Err(invalid(
                "LD05 prefix raw total disagrees with all actual FD06 descriptors",
            ));
        }
        Ok(())
    }

    async fn container_occurrences_inner<B: ObjectBackend + Clone>(
        &mut self,
        client: &ObjectClient<B>,
        snapshot: &AuthenticatedSnapshot,
        contexts: ContainerContexts,
        limits: ContainerOccurrenceLimits,
    ) -> PackedResult<ContainerOccurrenceSummary> {
        if limits.chunk_bytes == 0 || limits.chunk_bytes > 1 << 20 {
            return Err(limit("invalid container occurrence audit limits"));
        }
        let header = snapshot.manifest();
        for (id, role, root) in [
            (contexts.groups, SemanticRole::Groups, 0),
            (contexts.inodes, SemanticRole::Inodes, 1),
            (contexts.containers, SemanticRole::Containers, 2),
            (contexts.frames, SemanticRole::Frames, 3),
            (contexts.selectors, SemanticRole::NamespaceSelectors, 6),
        ] {
            self.occurrence_context(id, role, &header.roots[root])
                .await?;
        }
        let groups = self.context_stats_inner(contexts.groups).await?.leaf_count;
        let containers = self
            .context_stats_inner(contexts.containers)
            .await?
            .leaf_count;
        let frame_pages = self.context_stats_inner(contexts.frames).await?.leaf_count;
        let _bookkeeping = self
            .budget
            .admit(&[(SnapshotBudgetPool::Metadata, 64 << 10)])?;
        let mut work = OccurrenceWork {
            limits,
            counts: ContainerOccurrenceSummary::default(),
            observed: SnapshotBuildProvenance {
                policy: header.build.policy,
                requested_metadata_codec: header.build.requested_metadata_codec,
                requested_data_codec: header.build.requested_data_codec,
                ..Default::default()
            },
        };
        let mut cursor = Vec::with_capacity(4);
        while let Some(record) = self
            .occurrence_next_leaf(contexts.containers, &cursor)
            .await?
        {
            self.live()?;
            let ordinal = container_key(&record.first_key)?;
            if record.first_key != record.last_key {
                return Err(invalid("CI occurrence has non-singleton fences"));
            }
            let SnapshotIndexValue::Leaf(value) = &(*record).value else {
                return Err(invalid("CI occurrence is not a leaf"));
            };
            let reference = SnapshotObjectRef::decode_value(value)?;
            cursor.clear();
            cursor.extend_from_slice(&record.first_key);
            drop(record);
            work.request(reference.object_len)?;
            let payload = authenticate_payload(
                client,
                &reference,
                ordinal,
                &self.budget,
                PayloadLimits {
                    chunk_bytes: limits.chunk_bytes,
                    ..Default::default()
                },
                &self.cancel,
            )
            .await?;
            self.occurrence_authenticated(&reference).await?;
            match payload.summary() {
                PayloadSummary::Group {
                    profile, groups, ..
                } => {
                    if *profile != header.profile {
                        return Err(invalid("actual GC05 profile differs from manifest"));
                    }
                    for group in groups {
                        self.live()?;
                        self.occurrence_group(contexts.groups, group).await?;
                        work.request(u64::from(group.meta_stored_len))?;
                        work.decode(u64::from(group.meta_raw_len))?;
                        let metadata = occurrence_interruptible(
                            group.read_metadata_owned(client, &reference, 512 << 10, &self.budget),
                            &self.budget,
                            &self.cancel,
                        )
                        .await?;
                        work.observed.observe_group(group, &metadata)?;
                        add_count(&mut work.counts.groups, 1)?;
                    }
                }
                PayloadSummary::Large { inode, .. } => {
                    self.occurrence_large_owner(contexts, *inode, ordinal)
                        .await?;
                }
            }
            self.occurrence_frames(
                client,
                snapshot,
                contexts.frames,
                ordinal,
                &reference,
                payload.summary(),
                &mut work,
            )
            .await?;
            add_count(&mut work.counts.containers, 1)?;
        }
        // Each exact Groups match carries the incoming ordinal and unique
        // group_id; actual GC05 directories reject duplicate IDs. Thus these
        // matches are injective. Equal cardinality proves the reverse relation
        // without another table or any physical-key shortcut. The same argument
        // covers every FI range, including unknown-CI orphan occurrences.
        if work.counts.groups != groups
            || work.counts.containers != containers
            || work.counts.frame_pages != frame_pages
        {
            return Err(invalid(
                "Groups/CI/FI contexts contain unmatched or duplicate semantic occurrences",
            ));
        }
        work.observed.validate()?;
        if work.observed != header.build {
            return Err(invalid(
                "complete observed metadata/frame/inline provenance differs from authenticated BP11",
            ));
        }
        self.live()?;
        Ok(work.counts)
    }
}
