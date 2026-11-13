//! Physical dependency audit, deliberately separate from publication authority.
//! Namespace joins, parent-edge claims, frame codecs, and external logical
//! digests still need verification before constructing a catalog proof.

use std::path::Path;
use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::cadapter::client::{ObjectBackend, ObjectClient};
use crate::workspace::packed::wire::{PackedResult, PackedWireError, Reader};

use super::super::{
    AuthenticatedSnapshot, SNAPSHOT_FOOTER_LEN, SNAPSHOT_HEADER_LEN, SnapshotBudgetPool,
    SnapshotColdAttributes, SnapshotFrameDirectoryPage, SnapshotGroupRef, SnapshotIndexPage,
    SnapshotIndexRecord, SnapshotIndexValue, SnapshotInodeLocation, SnapshotLargeExtent,
    SnapshotMountBudget, SnapshotObjectKind, SnapshotObjectRef, SnapshotOwnedPermit,
    SnapshotPlacement, observer_backend_error, observer_validation_error, page_read_class,
};
use super::inventory::{InventoryLimits, PhysicalInventory};
use super::payload::{PayloadLimits, authenticate_payload};

#[derive(Clone, Copy, Debug)]
pub struct SnapshotPhysicalAuditLimits {
    pub max_objects: u64,
    pub max_authenticated_bytes: u64,
    pub max_requested_bytes: u64,
    pub max_edges: u64,
    pub max_leaf_records: u64,
    pub max_disk_bytes: u64,
    pub sqlite_cache_bytes: u64,
    pub chunk_bytes: usize,
}

impl Default for SnapshotPhysicalAuditLimits {
    fn default() -> Self {
        Self {
            max_objects: 1_000_000,
            max_authenticated_bytes: 1 << 40,
            max_requested_bytes: 1 << 40,
            max_edges: 4_000_000,
            max_leaf_records: 16_000_000,
            max_disk_bytes: 512 << 20,
            sqlite_cache_bytes: 256 << 10,
            chunk_bytes: 1 << 20,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SnapshotPhysicalAuditCounts {
    pub objects: u64,
    pub authenticated_bytes: u64,
    pub edges: u64,
    pub leaf_records: u64,
    pub requested_bytes: u64,
}

/// All reachable physical refs were fetched and authenticated. This result
/// cannot construct VerifiedPackedLower and cannot authorize catalog writes.
#[derive(Debug)]
pub struct SnapshotPhysicalDependencyAudit {
    manifest: SnapshotObjectRef,
    inventory_digest: [u8; 32],
    counts: SnapshotPhysicalAuditCounts,
    _roots: SnapshotOwnedPermit,
}

impl SnapshotPhysicalDependencyAudit {
    pub fn manifest_reference(&self) -> &SnapshotObjectRef {
        &self.manifest
    }
    pub fn inventory_digest(&self) -> [u8; 32] {
        self.inventory_digest
    }
    pub fn counts(&self) -> SnapshotPhysicalAuditCounts {
        self.counts
    }
}

fn invalid(message: &str) -> PackedWireError {
    PackedWireError::Invalid(message.into())
}

fn live(budget: &SnapshotMountBudget, cancel: &CancellationToken) -> PackedResult<()> {
    if cancel.is_cancelled() {
        return Err(PackedWireError::Backend(
            "physical dependency audit cancelled".into(),
        ));
    }
    if budget.state().closed {
        return Err(PackedWireError::LimitExceeded(
            "physical dependency audit budget closed".into(),
        ));
    }
    Ok(())
}

fn increment(value: &mut u64, amount: u64, limit: u64, label: &str) -> PackedResult<()> {
    *value = value
        .checked_add(amount)
        .filter(|v| *v <= limit)
        .ok_or_else(|| {
            PackedWireError::LimitExceeded(format!("physical dependency audit {label} limit"))
        })?;
    Ok(())
}

/// Enumerate physical refs through a disk-backed pending set, with one decoded
/// metadata page or one streaming payload summary live at a time. A full GET
/// is never used, including for physical-length queries.
pub async fn audit_snapshot_physical_dependencies<B: ObjectBackend + Clone>(
    client: &ObjectClient<B>,
    manifest: &SnapshotObjectRef,
    scratch: &Path,
    budget: Arc<SnapshotMountBudget>,
    limits: SnapshotPhysicalAuditLimits,
    cancel: CancellationToken,
) -> PackedResult<SnapshotPhysicalDependencyAudit> {
    live(&budget, &cancel)?;
    if manifest.kind != SnapshotObjectKind::Manifest {
        return Err(invalid(
            "physical dependency audit requires a manifest reference",
        ));
    }
    if limits.chunk_bytes == 0
        || limits.chunk_bytes > 1 << 20
        || limits.max_edges == 0
        || limits.max_leaf_records == 0
        || limits.max_requested_bytes == 0
    {
        return Err(PackedWireError::LimitExceeded(
            "invalid physical dependency audit limits".into(),
        ));
    }
    let roots = budget.admit(&[(SnapshotBudgetPool::Roots, 8192)])?;
    let mut inventory = PhysicalInventory::create(
        scratch,
        budget.clone(),
        InventoryLimits {
            max_objects: limits.max_objects,
            max_declared_bytes: limits.max_authenticated_bytes,
            max_disk_bytes: limits.max_disk_bytes,
            sqlite_cache_bytes: limits.sqlite_cache_bytes,
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
        &mut inventory,
        roots,
    )
    .await;
    // Await real SQLite worker shutdown before returning success or releasing
    // its cache reservation. Drop retains these owners if this await is lost.
    finish_after_close(result, inventory.close(), &budget, &cancel).await
}

pub(super) async fn finish_after_close(
    result: PackedResult<SnapshotPhysicalDependencyAudit>,
    close: impl std::future::Future<Output = PackedResult<()>>,
    budget: &SnapshotMountBudget,
    cancel: &CancellationToken,
) -> PackedResult<SnapshotPhysicalDependencyAudit> {
    close.await?;
    if result.is_ok() {
        live(budget, cancel)?;
    }
    result
}

async fn register(
    inventory: &mut PhysicalInventory,
    reference: &SnapshotObjectRef,
    counts: &mut SnapshotPhysicalAuditCounts,
    limits: SnapshotPhysicalAuditLimits,
) -> PackedResult<()> {
    increment(&mut counts.edges, 1, limits.max_edges, "edge")?;
    inventory.register(reference).await?;
    Ok(())
}

async fn audit_inner<B: ObjectBackend + Clone>(
    client: &ObjectClient<B>,
    manifest: &SnapshotObjectRef,
    budget: &Arc<SnapshotMountBudget>,
    limits: SnapshotPhysicalAuditLimits,
    cancel: &CancellationToken,
    inventory: &mut PhysicalInventory,
    roots: SnapshotOwnedPermit,
) -> PackedResult<SnapshotPhysicalDependencyAudit> {
    let mut counts = SnapshotPhysicalAuditCounts::default();
    register(inventory, manifest, &mut counts, limits).await?;
    while let Some(reference) = inventory.next_pending().await? {
        live(budget, cancel)?;
        increment(
            &mut counts.requested_bytes,
            reference.object_len,
            limits.max_requested_bytes,
            "readback bytes",
        )?;
        if matches!(
            reference.kind,
            SnapshotObjectKind::GroupContainer | SnapshotObjectKind::LargeData
        ) {
            // Ordinal belongs to the later ContainerIndex/group relation join;
            // it is not encoded in the GC body or a physical object identity.
            let payload = authenticate_payload(
                client,
                &reference,
                0,
                budget,
                PayloadLimits {
                    chunk_bytes: limits.chunk_bytes,
                    ..PayloadLimits::default()
                },
                cancel,
            )
            .await?;
            drop(payload);
        } else {
            // The worst decoded bounded page, wire buffer, pending row and
            // per-record copies fit this reservation independently of depth.
            let _metadata = budget.admit(&[(SnapshotBudgetPool::Metadata, 3 << 20)])?;
            let validation = client.begin_validation(page_read_class(reference.kind)?);
            let result = async {
                let bytes =
                    read_metadata(client, &reference, budget, limits.chunk_bytes, cancel).await?;
                inspect_metadata(inventory, &reference, &bytes, &mut counts, limits).await
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
        live(budget, cancel)?;
        inventory.mark_authenticated(&reference).await?;
    }
    let digest = inventory.final_digest().await?;
    let stats = inventory.stats();
    counts.objects = stats.authenticated_objects;
    counts.authenticated_bytes = stats.authenticated_bytes;
    live(budget, cancel)?;
    Ok(SnapshotPhysicalDependencyAudit {
        manifest: manifest.clone(),
        inventory_digest: digest,
        counts,
        _roots: roots,
    })
}

pub(super) async fn read_metadata<B: ObjectBackend + Clone>(
    client: &ObjectClient<B>,
    reference: &SnapshotObjectRef,
    budget: &Arc<SnapshotMountBudget>,
    chunk_bytes: usize,
    cancel: &CancellationToken,
) -> PackedResult<Vec<u8>> {
    let body_limit = if reference.kind == SnapshotObjectKind::Manifest {
        64 << 10
    } else {
        256 << 10
    };
    let maximum = (SNAPSHOT_HEADER_LEN + body_limit + SNAPSHOT_FOOTER_LEN) as u64;
    reference.encode_value()?;
    if reference.object_len > maximum {
        return Err(PackedWireError::LimitExceeded(
            "physical metadata page exceeds decode bound".into(),
        ));
    }
    let class = page_read_class(reference.kind)?;
    let _stored = budget.admit(&[(SnapshotBudgetPool::Stored, (2 * chunk_bytes) as u64)])?;
    async {
        let size = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(PackedWireError::Backend("physical metadata read cancelled".into())),
            _ = budget.wait_closed() => return Err(PackedWireError::LimitExceeded("physical metadata budget closed".into())),
            size = client.typed_object_size(class, &reference.key) => size.map_err(observer_backend_error)?,
        };
        if size != Some(reference.object_len) {
            return Err(invalid("physical metadata length differs from authenticated reference"));
        }
        let mut bytes = Vec::with_capacity(reference.object_len as usize);
        while (bytes.len() as u64) < reference.object_len {
            live(budget, cancel)?;
            let length = (reference.object_len - bytes.len() as u64).min(chunk_bytes as u64);
            let chunk = tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(PackedWireError::Backend("physical metadata read cancelled".into())),
                _ = budget.wait_closed() => return Err(PackedWireError::LimitExceeded("physical metadata budget closed".into())),
                chunk = client.typed_exact(class, &reference.key, bytes.len() as u64, length, chunk_bytes as u64, Ok) => chunk.map_err(observer_backend_error)?,
            };
            bytes.extend_from_slice(&chunk);
        }
        reference.verify(&bytes, body_limit)?;
        let size = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(PackedWireError::Backend("physical metadata read cancelled".into())),
            _ = budget.wait_closed() => return Err(PackedWireError::LimitExceeded("physical metadata budget closed".into())),
            size = client.typed_object_size(class, &reference.key) => size.map_err(observer_backend_error)?,
        };
        if size != Some(reference.object_len) {
            return Err(invalid("physical metadata length changed during authentication"));
        }
        live(budget, cancel)?;
        Ok(bytes)
    }.await
}

async fn inspect_metadata(
    inventory: &mut PhysicalInventory,
    reference: &SnapshotObjectRef,
    bytes: &[u8],
    counts: &mut SnapshotPhysicalAuditCounts,
    limits: SnapshotPhysicalAuditLimits,
) -> PackedResult<()> {
    match reference.kind {
        SnapshotObjectKind::Manifest => {
            let snapshot = AuthenticatedSnapshot::decode(reference, bytes)?;
            let manifest = snapshot.manifest();
            for root in &manifest.roots {
                register(inventory, root, counts, limits).await?;
            }
            if let Some(source) = &manifest.source {
                register(inventory, &source.allocations, counts, limits).await?;
            }
        }
        SnapshotObjectKind::ColdAttributes => {
            let body = reference.verify(bytes, 256 << 10)?;
            let mut r = Reader::new(body);
            r.take(4)?;
            SnapshotColdAttributes::decode(reference, bytes, r.u64()?)?;
        }
        SnapshotObjectKind::FrameDirectory => {
            SnapshotFrameDirectoryPage::decode(reference, bytes)?;
        }
        _ => {
            let page = SnapshotIndexPage::decode(reference, bytes)?;
            for record in &page.records {
                match &record.value {
                    SnapshotIndexValue::Child { reference, .. } => {
                        register(inventory, reference, counts, limits).await?
                    }
                    SnapshotIndexValue::Leaf(value) => {
                        increment(&mut counts.leaf_records, 1, limits.max_leaf_records, "leaf")?;
                        inspect_leaf(inventory, page.kind, record, value, counts, limits).await?;
                    }
                }
            }
        }
    }
    Ok(())
}

async fn inspect_leaf(
    inventory: &mut PhysicalInventory,
    kind: SnapshotObjectKind,
    record: &SnapshotIndexRecord,
    value: &[u8],
    counts: &mut SnapshotPhysicalAuditCounts,
    limits: SnapshotPhysicalAuditLimits,
) -> PackedResult<()> {
    match kind {
        SnapshotObjectKind::ContainerIndex
        | SnapshotObjectKind::FrameIndex
        | SnapshotObjectKind::ColdIndex => {
            let reference = SnapshotObjectRef::decode_value(value)?;
            let right_kind = match kind {
                SnapshotObjectKind::ContainerIndex => matches!(
                    reference.kind,
                    SnapshotObjectKind::GroupContainer | SnapshotObjectKind::LargeData
                ),
                SnapshotObjectKind::FrameIndex => {
                    reference.kind == SnapshotObjectKind::FrameDirectory
                }
                _ => reference.kind == SnapshotObjectKind::ColdAttributes,
            };
            if !right_kind {
                return Err(invalid("physical index leaf object has the wrong kind"));
            }
            register(inventory, &reference, counts, limits).await?;
        }
        SnapshotObjectKind::GroupIndex => {
            SnapshotGroupRef::decode_value(value)?;
        }
        SnapshotObjectKind::InodeIndex | SnapshotObjectKind::ReverseIndex => {
            SnapshotInodeLocation::decode_value(value)?;
        }
        SnapshotObjectKind::SourceStatsIndex => {
            let inode = u64::from_be_bytes(
                record
                    .first_key
                    .as_slice()
                    .try_into()
                    .map_err(|_| invalid("physical SI05 leaf key length"))?,
            );
            super::super::source_stat::decode_allocation(value, inode)?;
        }
        SnapshotObjectKind::LargeIndex => {
            let mut r = Reader::new(value);
            match r.take(4)? {
                b"PS09" => {
                    let inode = r.u64()?;
                    let size = r.u64()?;
                    if let SnapshotPlacement::External { extents, .. } =
                        SnapshotPlacement::decode(value, inode, size)?
                    {
                        register(inventory, &extents, counts, limits).await?;
                    }
                }
                b"LE09" => {
                    SnapshotLargeExtent::decode_record(record, r.u64()?, i64::MAX as u64)?;
                }
                _ => {
                    return Err(PackedWireError::UnsupportedFormat(
                        "physical large-index leaf version".into(),
                    ));
                }
            }
        }
        _ => return Err(invalid("unexpected physical index leaf kind")),
    }
    Ok(())
}
