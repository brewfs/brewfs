//! Complete IP06 occurrence and namespace relation traversal, without
//! publication authority. Durable protection and source revalidation remain.

use std::path::Path;
use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::cadapter::client::{ObjectBackend, ObjectClient};
use crate::workspace::packed::wire::{PackedResult, PackedWireError};

use super::super::{
    AuthenticatedSnapshot, SnapshotBudgetPool, SnapshotGroupRef, SnapshotIndexPage,
    SnapshotIndexRecord, SnapshotIndexValue, SnapshotInodeLocation, SnapshotLargeExtent,
    SnapshotMountBudget, SnapshotObjectKind, SnapshotObjectRef, SnapshotOwnedPermit,
    SnapshotPlacement, observer_validation_error, page_read_class,
};
use super::inventory::InventoryLimits;
use super::physical::read_metadata;
use super::semantic_facts::{
    ContainerContexts, ContainerOccurrenceLimits, ContentContexts, ContextId, GroupContentLimits,
    NamespaceContexts, SemanticFactLimits, SemanticFacts, SemanticRole,
};

#[derive(Clone, Copy, Debug)]
pub struct SnapshotIndexAuditLimits {
    pub max_objects: u64,
    pub max_authenticated_bytes: u64,
    pub max_requested_bytes: u64,
    pub max_decoded_bytes: u64,
    /// Bound repeated descriptor checks performed by actual frame reads.
    pub max_frame_validation_steps: u64,
    pub max_logical_hash_bytes: u64,
    pub max_contexts: u64,
    pub max_visits: u64,
    pub max_leaf_records: u64,
    pub max_page_records: u64,
    pub max_disk_bytes: u64,
    pub sqlite_cache_bytes: u64,
    pub max_sql_operations: u64,
    pub max_sql_vm_steps: u64,
    pub chunk_bytes: usize,
}

impl Default for SnapshotIndexAuditLimits {
    fn default() -> Self {
        Self {
            max_objects: 1_000_000,
            max_authenticated_bytes: 1 << 40,
            max_requested_bytes: 1 << 40,
            max_decoded_bytes: 1 << 40,
            max_frame_validation_steps: 64_000_000,
            max_logical_hash_bytes: 1 << 40,
            max_contexts: 1_000_008,
            max_visits: 4_000_000,
            max_leaf_records: 16_000_000,
            max_page_records: 16_000_000,
            max_disk_bytes: 512 << 20,
            sqlite_cache_bytes: 256 << 10,
            max_sql_operations: 256_000_000,
            max_sql_vm_steps: 32_000_000_000,
            chunk_bytes: 1 << 20,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SnapshotIndexAuditCounts {
    pub pages: u64,
    pub objects: u64,
    pub contexts: u64,
    pub visits: u64,
    pub leaf_records: u64,
    pub page_records: u64,
    pub authenticated_bytes: u64,
    pub requested_bytes: u64,
    pub decoded_bytes: u64,
    pub frame_validation_steps: u64,
    pub logical_hash_bytes: u64,
    pub sql_operations: u64,
    pub sql_vm_steps: u64,
    pub canonical_inodes: u64,
    pub aliases: u64,
    pub groups: u64,
    pub directories: u64,
    pub root_child_directories: u64,
    pub source_records: u64,
    pub selector_records: u64,
}

/// Authenticated index structure, typed roles and namespace relations. No conversion from
/// this result to VerifiedPackedLower or a catalog mutation is provided.
#[derive(Debug)]
pub struct SnapshotIndexContextAudit {
    manifest: SnapshotObjectRef,
    counts: SnapshotIndexAuditCounts,
    physical_inventory_digest: [u8; 32],
    root_inode: u64,
    highest_inode: u64,
    _roots: SnapshotOwnedPermit,
}

impl SnapshotIndexContextAudit {
    pub fn manifest_reference(&self) -> &SnapshotObjectRef {
        &self.manifest
    }
    pub fn counts(&self) -> SnapshotIndexAuditCounts {
        self.counts
    }
    pub(crate) fn publication_facts(&self) -> ([u8; 32], u64, u64) {
        (
            self.physical_inventory_digest,
            self.root_inode,
            self.highest_inode,
        )
    }
}

/// The production KV journal implements this exact-fullref membership check.
/// It is kept crate private; user callbacks and readback flags are not graph
/// authority. This does not replace global registry retention checks.
#[async_trait::async_trait]
pub(crate) trait SnapshotStagedObjectVerifier: Send + Sync {
    async fn verify_reference(&self, reference: &SnapshotObjectRef) -> PackedResult<()>;
}

fn invalid(message: &str) -> PackedWireError {
    PackedWireError::Invalid(message.into())
}

fn live(budget: &SnapshotMountBudget, cancel: &CancellationToken) -> PackedResult<()> {
    if cancel.is_cancelled() {
        return Err(PackedWireError::Backend(
            "index context audit cancelled".into(),
        ));
    }
    if budget.state().closed {
        return Err(PackedWireError::LimitExceeded(
            "index context audit budget closed".into(),
        ));
    }
    Ok(())
}

fn charge_read(requested: &mut u64, reference: &SnapshotObjectRef, limit: u64) -> PackedResult<()> {
    *requested = requested
        .checked_add(reference.object_len)
        .filter(|n| *n <= limit)
        .ok_or_else(|| {
            PackedWireError::LimitExceeded("index context readback quota exceeded".into())
        })?;
    Ok(())
}

pub async fn audit_snapshot_index_contexts<B: ObjectBackend + Clone>(
    client: &ObjectClient<B>,
    manifest: &SnapshotObjectRef,
    scratch: &Path,
    budget: Arc<SnapshotMountBudget>,
    limits: SnapshotIndexAuditLimits,
    cancel: CancellationToken,
) -> PackedResult<SnapshotIndexContextAudit> {
    audit_snapshot_index_contexts_inner(client, manifest, scratch, budget, limits, cancel, None)
        .await
}

pub(crate) async fn audit_snapshot_staged_index_contexts<B: ObjectBackend + Clone>(
    client: &ObjectClient<B>,
    manifest: &SnapshotObjectRef,
    scratch: &Path,
    budget: Arc<SnapshotMountBudget>,
    limits: SnapshotIndexAuditLimits,
    cancel: CancellationToken,
    staged: &dyn SnapshotStagedObjectVerifier,
) -> PackedResult<SnapshotIndexContextAudit> {
    audit_snapshot_index_contexts_inner(
        client,
        manifest,
        scratch,
        budget,
        limits,
        cancel,
        Some(staged),
    )
    .await
}

async fn audit_snapshot_index_contexts_inner<B: ObjectBackend + Clone>(
    client: &ObjectClient<B>,
    manifest: &SnapshotObjectRef,
    scratch: &Path,
    budget: Arc<SnapshotMountBudget>,
    limits: SnapshotIndexAuditLimits,
    cancel: CancellationToken,
    staged: Option<&dyn SnapshotStagedObjectVerifier>,
) -> PackedResult<SnapshotIndexContextAudit> {
    live(&budget, &cancel)?;
    if manifest.kind != SnapshotObjectKind::Manifest {
        return Err(invalid("index context audit requires a manifest"));
    }
    if limits.chunk_bytes == 0 || limits.chunk_bytes > 1 << 20 || limits.max_requested_bytes == 0 {
        return Err(PackedWireError::LimitExceeded(
            "invalid index context readback quotas".into(),
        ));
    }
    // PM11 has seven roots plus a source root with keys up to 4096 bytes.
    // Admission precedes decoding and covers the complete retained header.
    let roots = budget.admit(&[(SnapshotBudgetPool::Roots, 64 << 10)])?;
    let mut facts = SemanticFacts::create(
        scratch,
        budget.clone(),
        SemanticFactLimits {
            inventory: InventoryLimits {
                max_objects: limits.max_objects,
                max_declared_bytes: limits.max_authenticated_bytes,
                max_disk_bytes: limits.max_disk_bytes,
                sqlite_cache_bytes: limits.sqlite_cache_bytes,
            },
            max_contexts: limits.max_contexts,
            max_visits: limits.max_visits,
            max_leaves: limits.max_leaf_records,
            max_page_records: limits.max_page_records,
            max_sql_operations: limits.max_sql_operations,
            max_sql_vm_steps: limits.max_sql_vm_steps,
        },
        cancel.clone(),
    )
    .await?;
    let result = audit_inner(
        client,
        manifest,
        &budget,
        limits,
        &cancel,
        AuditState {
            facts: &mut facts,
            roots,
            staged,
        },
    )
    .await;
    finish_after_close(result, facts.close(), &budget, &cancel).await
}

pub(super) async fn finish_after_close(
    result: PackedResult<SnapshotIndexContextAudit>,
    close: impl std::future::Future<Output = PackedResult<()>>,
    budget: &SnapshotMountBudget,
    cancel: &CancellationToken,
) -> PackedResult<SnapshotIndexContextAudit> {
    close.await?;
    if result.is_ok() {
        live(budget, cancel)?;
    }
    result
}

struct AuditState<'a> {
    facts: &'a mut SemanticFacts,
    roots: SnapshotOwnedPermit,
    staged: Option<&'a dyn SnapshotStagedObjectVerifier>,
}

async fn audit_inner<B: ObjectBackend + Clone>(
    client: &ObjectClient<B>,
    manifest: &SnapshotObjectRef,
    budget: &Arc<SnapshotMountBudget>,
    limits: SnapshotIndexAuditLimits,
    cancel: &CancellationToken,
    state: AuditState<'_>,
) -> PackedResult<SnapshotIndexContextAudit> {
    let AuditState {
        facts,
        roots,
        staged,
    } = state;
    let mut requested = 0;
    facts.register_object(manifest).await?;
    charge_read(&mut requested, manifest, limits.max_requested_bytes)?;
    let snapshot = {
        let _metadata = budget.admit(&[(SnapshotBudgetPool::Metadata, 3 << 20)])?;
        let validation = client.begin_validation(page_read_class(manifest.kind)?);
        let result = async {
            let bytes = read_metadata(client, manifest, budget, limits.chunk_bytes, cancel).await?;
            AuthenticatedSnapshot::decode(manifest, &bytes)
        }
        .await;
        if let Some(validation) = validation {
            match &result {
                Ok(_) => validation.succeed(),
                Err(_) if cancel.is_cancelled() => drop(validation),
                Err(error) => validation.fail(observer_validation_error(error.clone()).0),
            }
        }
        result?
    };
    facts.mark_authenticated(manifest).await?;
    let header = snapshot.manifest();
    let roles = [
        SemanticRole::Groups,
        SemanticRole::Inodes,
        SemanticRole::Containers,
        SemanticRole::Frames,
        SemanticRole::Cold,
        SemanticRole::Reverse,
        SemanticRole::NamespaceSelectors,
    ];
    let mut contexts = [ContextId(0); 8];
    for (ordinal, role) in roles.into_iter().enumerate() {
        let expected = (role == SemanticRole::Groups).then_some(header.group_dentry_count);
        contexts[ordinal] = facts
            .register_context(role, &header.roots[ordinal], expected)
            .await?;
    }
    let count = if let Some(source) = &header.source {
        contexts[7] = facts
            .register_context(SemanticRole::SourceAllocations, &source.allocations, None)
            .await?;
        8
    } else {
        7
    };
    drain_visits(client, budget, limits, cancel, facts, &mut requested).await?;
    for context in &contexts[..count] {
        facts.finish_context(*context).await?;
    }
    // Canonical IL05 EOF binds PS09; each External root gets its own inode/EOF
    // context even when physical bytes were authenticated in another context.
    let _cursor = budget.admit(&[(SnapshotBudgetPool::Metadata, 4096)])?;
    let mut after: Option<Vec<u8>> = None;
    while let Some(record) = facts
        .next_context_leaf(contexts[6], after.as_deref())
        .await?
    {
        live(budget, cancel)?;
        let inode = singleton_inode_key(&record)?;
        let _decoded = budget.admit(&[(SnapshotBudgetPool::Metadata, 32 << 10)])?;
        let canonical = facts
            .context_leaf(contexts[1], &record.first_key)
            .await?
            .ok_or_else(|| invalid("PS09 selector has no canonical inode"))?;
        let SnapshotIndexValue::Leaf(value) = &canonical.value else {
            return Err(invalid("canonical inode is not a leaf"));
        };
        let location = SnapshotInodeLocation::decode_value(value)?;
        if location.hot.inode != inode || location.hot.kind != 1 {
            return Err(invalid(
                "PS09 selector does not bind a regular canonical inode",
            ));
        }
        let SnapshotIndexValue::Leaf(value) = &record.value else {
            return Err(invalid("PS09 selector is not a leaf"));
        };
        let placement = SnapshotPlacement::decode(value, inode, location.hot.size)?;
        after = Some(record.first_key.clone());
        drop(canonical);
        drop(record);
        if let SnapshotPlacement::External {
            inode,
            size,
            extent_count,
            extents,
            ..
        } = placement
        {
            let context = facts
                .register_context(
                    SemanticRole::ExternalExtents { inode, eof: size },
                    &extents,
                    Some(extent_count),
                )
                .await?;
            drain_visits(client, budget, limits, cancel, facts, &mut requested).await?;
            facts.finish_context(context).await?;
        }
    }
    let namespace = facts
        .audit_namespace_relations(
            &snapshot,
            NamespaceContexts {
                groups: contexts[0],
                inodes: contexts[1],
                reverse: contexts[5],
                selectors: contexts[6],
                source: (count == 8).then_some(contexts[7]),
            },
        )
        .await?;
    // Preserve the unique IP06 page count before content objects join the
    // physical inventory. Payload objects are counted separately as objects.
    let index_summary = facts.finish_all().await?;
    let content = facts
        .audit_group_content(
            client,
            &snapshot,
            ContentContexts {
                containers: contexts[2],
                frames: contexts[3],
                cold: contexts[4],
                selectors: contexts[6],
            },
            GroupContentLimits {
                max_requested_bytes: limits.max_requested_bytes.saturating_sub(requested),
                max_decoded_bytes: limits.max_decoded_bytes,
                max_frame_validation_steps: limits.max_frame_validation_steps,
                max_logical_hash_bytes: limits.max_logical_hash_bytes,
                max_entries: limits.max_leaf_records,
                chunk_bytes: limits.chunk_bytes,
            },
        )
        .await?;
    requested = requested
        .checked_add(content.requested_bytes)
        .filter(|n| *n <= limits.max_requested_bytes)
        .ok_or_else(|| invalid("content readback count overflow or quota exceeded"))?;
    let occurrences = facts
        .audit_container_occurrences(
            client,
            &snapshot,
            ContainerContexts {
                groups: contexts[0],
                inodes: contexts[1],
                containers: contexts[2],
                frames: contexts[3],
                selectors: contexts[6],
            },
            ContainerOccurrenceLimits {
                max_requested_bytes: limits.max_requested_bytes - requested,
                max_decoded_bytes: limits
                    .max_decoded_bytes
                    .checked_sub(content.decoded_bytes)
                    .ok_or_else(|| invalid("content decoded-byte quota exceeded"))?,
                max_frame_validation_steps: limits
                    .max_frame_validation_steps
                    .checked_sub(content.frame_validation_steps)
                    .ok_or_else(|| invalid("content frame-validation quota exceeded"))?,
                chunk_bytes: limits.chunk_bytes,
            },
        )
        .await?;
    requested = requested
        .checked_add(occurrences.requested_bytes)
        .filter(|n| *n <= limits.max_requested_bytes)
        .ok_or_else(|| invalid("occurrence readback count overflow or quota exceeded"))?;
    let decoded_bytes = content
        .decoded_bytes
        .checked_add(occurrences.decoded_bytes)
        .filter(|n| *n <= limits.max_decoded_bytes)
        .ok_or_else(|| invalid("occurrence decoded-byte count overflow or quota exceeded"))?;
    let summary = facts.finish_all().await?;
    let physical_inventory_digest = facts.authenticated_inventory_digest().await?;
    if let Some(staged) = staged {
        let _cursor_owner = budget.admit(&[(SnapshotBudgetPool::Metadata, 8192)])?;
        let mut after = Vec::new();
        let mut observed = 0u64;
        while let Some(reference) = facts.next_authenticated_reference(&after).await? {
            live(budget, cancel)?;
            // Hold the admitted full reference while the actual durable row
            // lookup completes. No all-object Vec or transient naked row.
            staged.verify_reference(&reference).await?;
            after.clear();
            after.extend_from_slice(reference.key.as_bytes());
            observed = observed
                .checked_add(1)
                .ok_or_else(|| invalid("staged member count overflow"))?;
        }
        if observed != summary.inventory.authenticated_objects {
            return Err(invalid("physical graph/member cardinality changed"));
        }
    }
    let summary = facts.finish_all().await?;
    live(budget, cancel)?;
    let frame_validation_steps = content
        .frame_validation_steps
        .checked_add(occurrences.frame_validation_steps)
        .ok_or_else(|| invalid("frame-validation work count overflow"))?;
    let counts = SnapshotIndexAuditCounts {
        pages: index_summary
            .inventory
            .authenticated_objects
            .checked_sub(1)
            .ok_or_else(|| invalid("manifest missing from index inventory"))?,
        objects: summary.inventory.authenticated_objects,
        contexts: summary.contexts,
        visits: summary.visits,
        leaf_records: summary.leaves,
        page_records: summary.page_records,
        authenticated_bytes: summary.inventory.authenticated_bytes,
        requested_bytes: requested,
        decoded_bytes,
        frame_validation_steps,
        logical_hash_bytes: content.logical_hash_bytes,
        sql_operations: summary.sql_operations,
        sql_vm_steps: summary.sql_vm_steps,
        canonical_inodes: namespace.canonical_inodes,
        aliases: namespace.aliases,
        groups: namespace.groups,
        directories: namespace.directories,
        root_child_directories: namespace.root_child_directories,
        source_records: namespace.source_records,
        selector_records: namespace.selector_records,
    };
    Ok(SnapshotIndexContextAudit {
        manifest: manifest.clone(),
        counts,
        physical_inventory_digest,
        root_inode: header.root_inode,
        highest_inode: namespace.highest_inode,
        _roots: roots,
    })
}

async fn drain_visits<B: ObjectBackend + Clone>(
    client: &ObjectClient<B>,
    budget: &Arc<SnapshotMountBudget>,
    limits: SnapshotIndexAuditLimits,
    cancel: &CancellationToken,
    facts: &mut SemanticFacts,
    requested: &mut u64,
) -> PackedResult<()> {
    while let Some(visit) = facts.next_visit().await? {
        live(budget, cancel)?;
        if facts.page_fact(&visit.reference).await?.is_none() {
            charge_read(requested, &visit.reference, limits.max_requested_bytes)?;
            let _metadata = budget.admit(&[(SnapshotBudgetPool::Metadata, 3 << 20)])?;
            let validation = client.begin_validation(page_read_class(visit.reference.kind)?);
            let result = async {
                let bytes =
                    read_metadata(client, &visit.reference, budget, limits.chunk_bytes, cancel)
                        .await?;
                let page = SnapshotIndexPage::decode(&visit.reference, &bytes)?;
                facts.mark_authenticated(&visit.reference).await?;
                facts
                    .store_authenticated_page(&visit.reference, &page)
                    .await
            }
            .await;
            if let Some(validation) = validation {
                match &result {
                    Ok(_) => validation.succeed(),
                    Err(_) if cancel.is_cancelled() => drop(validation),
                    Err(error) => validation.fail(observer_validation_error(error.clone()).0),
                }
            }
            result?;
        }
        let mut after = None;
        while let Some(record) = facts.next_page_record(&visit.reference, after).await? {
            live(budget, cancel)?;
            match &record.record.value {
                SnapshotIndexValue::Child { .. } => {
                    facts
                        .enqueue_child(&visit, record.slot, &record.record)
                        .await?;
                }
                SnapshotIndexValue::Leaf(value) => {
                    validate_leaf(visit.role, &record.record, value)?;
                    facts
                        .insert_leaf(&visit, record.slot, &record.record)
                        .await?;
                }
            }
            after = Some(record.slot);
        }
        facts.finish_visit(visit.id).await?;
    }
    Ok(())
}

fn singleton_inode_key(record: &SnapshotIndexRecord) -> PackedResult<u64> {
    if record.first_key != record.last_key {
        return Err(invalid("singleton inode index has interval fences"));
    }
    let inode = u64::from_be_bytes(
        record
            .first_key
            .as_slice()
            .try_into()
            .map_err(|_| invalid("inode index key is not BE8"))?,
    );
    if inode == 0 || inode > i64::MAX as u64 {
        return Err(invalid("inode index key is outside the supported range"));
    }
    Ok(inode)
}

fn validate_leaf(
    role: SemanticRole,
    record: &SnapshotIndexRecord,
    value: &[u8],
) -> PackedResult<()> {
    match role {
        SemanticRole::Groups => {
            SnapshotGroupRef::decode_value(value)?;
            record.subtree_weight(role.kind())?;
        }
        SemanticRole::Inodes => {
            let inode = singleton_inode_key(record)?;
            if SnapshotInodeLocation::decode_value(value)?.hot.inode != inode {
                return Err(invalid("IL05 canonical key/inode mismatch"));
            }
        }
        SemanticRole::Reverse => {
            let location = SnapshotInodeLocation::decode_value(value)?;
            if record.first_key != record.last_key || record.first_key != location.reverse_key() {
                return Err(invalid("IL05 reverse key mismatch"));
            }
        }
        SemanticRole::Containers | SemanticRole::Frames | SemanticRole::Cold => {
            let reference = SnapshotObjectRef::decode_value(value)?;
            let valid = match role {
                SemanticRole::Containers => {
                    record.first_key.len() == 4
                        && record.first_key == record.last_key
                        && matches!(
                            reference.kind,
                            SnapshotObjectKind::GroupContainer | SnapshotObjectKind::LargeData
                        )
                }
                SemanticRole::Frames => {
                    record.first_key.len() == 8
                        && record.last_key.len() == 8
                        && record.first_key[..4] == record.last_key[..4]
                        && reference.kind == SnapshotObjectKind::FrameDirectory
                }
                _ => {
                    singleton_inode_key(record)?;
                    reference.kind == SnapshotObjectKind::ColdAttributes
                }
            };
            if !valid {
                return Err(invalid("typed object index key/ref kind mismatch"));
            }
        }
        SemanticRole::SourceAllocations => {
            let inode = singleton_inode_key(record)?;
            super::super::source_stat::decode_allocation(value, inode)?;
        }
        SemanticRole::NamespaceSelectors => {
            singleton_inode_key(record)?;
        }
        SemanticRole::ExternalExtents { inode, eof } => {
            SnapshotLargeExtent::decode_record(record, inode, eof)?;
        }
    }
    Ok(())
}
