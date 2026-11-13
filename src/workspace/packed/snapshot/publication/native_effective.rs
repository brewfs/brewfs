//! Actual frozen workspace source, preserving inode identities and exact holes.
//! SQLite here is disposable scratch; Redis/TiKV remains catalog authority.

use std::fs::{File, OpenOptions};
use std::os::unix::fs::{FileExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use sea_orm::sqlx::{Row, query};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use super::inventory::{InventoryLimits, OwnedSqlRow, PhysicalInventory};
use crate::cadapter::client::{ObjectBackend, ObjectClient};
use crate::chunk::read_plan::{ReadSource, WorkspaceReadPlanProvider};
use crate::chunk::{BlockStore, ChunkLayout};
use crate::meta::layer::MetaLayer;
use crate::meta::store::{AclRule, FileAttr, FileType};
use crate::vfs::fs::PackedVfsDrainFence;
use crate::workspace::meta_layer::WorkspaceMetaLayer;
use crate::workspace::meta_layer::packed_lower::WorkspacePackedLower;
use crate::workspace::model::{DentryOp, ExtentKind, SealPhase, ValueOp};
use crate::workspace::packed::PackedReadonlyMeta;
use crate::workspace::packed::snapshot::{
    SnapshotBudgetPool, SnapshotColdAttributes, SnapshotMountBudget, SnapshotObjectKind,
    SnapshotObjectRef, SnapshotOwned, SnapshotOwnedPermit, SnapshotXattr,
};
use crate::workspace::packed::wire::{PackedResult, PackedWireError};
use crate::workspace::packed_reader_lifecycle::{PackedReaderRequestOwner, PackedReaderSession};
use crate::workspace::resolver::{Resolution, resolve_extent_coverage, resolve_inode_state};
use crate::workspace::stores::kv_backend::WorkspaceKvBackend;
use crate::workspace::stores::kv_store::KvWorkspaceStore;
use crate::workspace::stores::kv_store::packed_native_freeze::PackedNativeQuiesceFence;
use crate::workspace::stores::kv_store::packed_native_freeze::{
    FrozenNativeDeltaHash, PackedNativePhaseFence, PackedNativeRecoveryReadFence,
    promote_verified_data_drained, promote_verified_hashed, reissue_verified_native_phase,
};

const SOURCE_WINDOW: u64 = 64 << 10;
const QUERY_OWNER: u64 = 2 << 20;
const MAX_EXTENT_ROWS: usize = 1024;

/// Cancels only this capture if its caller goes away. The owned driver still
/// retains the actual source call and cleanup owners until that call returns.
struct CaptureCancellationGuard(Option<CancellationToken>);

impl CaptureCancellationGuard {
    fn disarm(&mut self) {
        self.0.take();
    }
}

impl Drop for CaptureCancellationGuard {
    fn drop(&mut self) {
        if let Some(cancel) = self.0.take() {
            cancel.cancel();
        }
    }
}

fn invalid(message: impl std::fmt::Display) -> PackedWireError {
    PackedWireError::Invalid(format!("frozen native source: {message}"))
}
fn source_error(message: impl std::fmt::Display) -> PackedWireError {
    PackedWireError::Backend(format!("frozen native source: {message}"))
}
fn limit(message: &str) -> PackedWireError {
    PackedWireError::LimitExceeded(format!("frozen native source: {message}"))
}
fn add(value: &mut u64, increment: u64, maximum: u64) -> PackedResult<()> {
    *value = value
        .checked_add(increment)
        .ok_or_else(|| limit("counter overflow"))?;
    if *value > maximum {
        return Err(limit("configured capture quota exceeded"));
    }
    Ok(())
}
fn bytes_u64(row: &OwnedSqlRow, index: usize) -> PackedResult<u64> {
    let bytes: Vec<u8> = row.row.try_get(index).map_err(source_error)?;
    Ok(u64::from_be_bytes(
        bytes
            .try_into()
            .map_err(|_| invalid("numeric row encoding"))?,
    ))
}
fn field(hash: &mut Sha256, bytes: &[u8]) {
    hash.update((bytes.len() as u64).to_le_bytes());
    hash.update(bytes);
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct NativeCaptureLimits {
    pub max_inodes: u64,
    pub max_names: u64,
    pub max_spans: u64,
    pub max_logical_bytes: u64,
    pub max_data_bytes: u64,
    pub max_payload_disk_bytes: u64,
    pub max_sqlite_disk_bytes: u64,
    pub max_producer_spool_disk_bytes: u64,
    pub sqlite_cache_bytes: u64,
    pub max_sql_vm_steps: u64,
}

impl NativeCaptureLimits {
    fn validate(self) -> PackedResult<()> {
        if self.max_inodes == 0
            || self.max_names == 0
            || self.max_spans == 0
            || self.max_logical_bytes == 0
            || self.max_data_bytes == 0
            || self.max_payload_disk_bytes == 0
            || self.max_sql_vm_steps == 0
            || self.max_producer_spool_disk_bytes < 16 << 10
            || self.max_inodes > i64::MAX as u64
            || self.max_names > i64::MAX as u64
        {
            return Err(limit("invalid fixed capture limits"));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct NativeSourceCounts {
    pub inodes: u64,
    pub names: u64,
    pub directories: u64,
    pub spans: u64,
    pub logical_bytes: u64,
    pub data_bytes: u64,
    pub payload_disk_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct NativeHot {
    pub inode: i64,
    pub size: u64,
    pub blocks: u64,
    pub kind: u8,
    pub mode: u32,
    pub rdev: u32,
    pub uid: u32,
    pub gid: u32,
    pub atime_ns: i64,
    pub mtime_ns: i64,
    pub ctime_ns: i64,
    pub nlink: u32,
}

fn kind(kind: FileType) -> u8 {
    match kind {
        FileType::File => 1,
        FileType::Dir => 2,
        FileType::Symlink => 3,
        FileType::Fifo => 4,
        FileType::Socket => 5,
        FileType::CharDevice => 6,
        FileType::BlockDevice => 7,
    }
}
impl NativeHot {
    fn from_attr(attr: &FileAttr) -> PackedResult<Self> {
        let value = Self {
            inode: attr.ino,
            size: attr.size,
            blocks: attr.blocks,
            kind: kind(attr.kind),
            mode: (attr.mode & 0o7777) | attr.kind.mode_type_bits(),
            rdev: attr.rdev,
            uid: attr.uid,
            gid: attr.gid,
            atime_ns: attr.atime,
            mtime_ns: attr.mtime,
            ctime_ns: attr.ctime,
            nlink: attr.nlink,
        };
        crate::workspace::packed::meta::validate_snapshot_hot_attributes(
            u64::try_from(value.inode).map_err(source_error)?,
            value.kind,
            value.mode,
            value.nlink,
            u64::from(value.rdev),
        )?;
        if value.size > i64::MAX as u64 {
            return Err(limit("source size exceeds off_t"));
        }
        Ok(value)
    }
    pub(crate) fn encode(&self) -> PackedResult<Vec<u8>> {
        serde_json::to_vec(self).map_err(source_error)
    }
    pub(crate) fn decode(bytes: &[u8]) -> PackedResult<Self> {
        if bytes.len() > 2048 {
            return Err(limit("source hot row exceeds fixed bound"));
        }
        serde_json::from_slice(bytes).map_err(source_error)
    }
}

/// Complete on-disk effective namespace/data capture. The actual frozen source,
/// reader generation and catalog authority stay alive with this artifact.
/// Only comparison with a concrete authenticated candidate can make a receipt.
pub(crate) struct FrozenNativeArtifact<K, S>
where
    K: WorkspaceKvBackend + 'static,
    S: BlockStore + Send + Sync + 'static,
{
    scratch: PhysicalInventory,
    cancel: CancellationToken,
    native: Arc<PackedNativeQuiesceFence<K>>,
    native_phase: Option<Arc<PackedNativePhaseFence<K>>>,
    local: PackedVfsDrainFence<S, WorkspaceMetaLayer<KvWorkspaceStore<K>>>,
    lower: Arc<WorkspacePackedLower>,
    session: Arc<dyn PackedReaderSession>,
    _reader_owner: PackedReaderRequestOwner,
    _control: SnapshotOwnedPermit,
    budget: Arc<SnapshotMountBudget>,
    layout: ChunkLayout,
    limits: NativeCaptureLimits,
    counts: NativeSourceCounts,
    highest_inode: i64,
    digest: Option<[u8; 32]>,
    produced_manifest: Option<SnapshotObjectRef>,
}

/// This receipt attests to actual native capture and exact candidate comparison.
/// A separate complete graph proof and same-CAS journal/registry authority are
/// still required for publication. There is no constructor from caller hashes.
pub(crate) struct VerifiedFrozenNativeView<K, S>
where
    K: WorkspaceKvBackend + 'static,
    S: BlockStore + Send + Sync + 'static,
{
    source: FrozenNativeArtifact<K, S>,
    candidate_manifest: SnapshotObjectRef,
}

/// Exact source equality plus the actual native Hashed successor. Final graph
/// proof and atomic carrier/head/PWB/PPJ/registry publication remain separate.
pub(crate) struct VerifiedHashedNativeView<K, S>
where
    K: WorkspaceKvBackend + 'static,
    S: BlockStore + Send + Sync + 'static,
{
    source: VerifiedFrozenNativeView<K, S>,
}

/// An error retains the complete frozen source and genuine hash ownership.
/// The attempted successor is recovery input and grants no authority by itself.
pub(crate) struct NativePromotionFailure<K, S>
where
    K: WorkspaceKvBackend + 'static,
    S: BlockStore + Send + Sync + 'static,
{
    pub(crate) error: crate::workspace::error::WorkspaceError,
    pub(crate) attempted_successor: Option<Box<crate::workspace::model::SealJournal>>,
    pub(crate) source: Box<VerifiedFrozenNativeView<K, S>>,
    pub(crate) native_hash: Arc<FrozenNativeDeltaHash<K>>,
}

pub(crate) struct NativeSourceEdge {
    pub parent: i64,
    pub name: Vec<u8>,
    pub hot: NativeHot,
}

pub(crate) struct NativeFrameCursor {
    inode: i64,
    after: Option<u64>,
    active: Option<(u64, u64, u64)>,
    target: u64,
    size_class: crate::workspace::packed::SizeClass,
}
pub(crate) struct OwnedNativeFrame {
    pub offset: u64,
    pub frame: crate::workspace::packed::PackedFrameInput,
    pub permit: SnapshotOwnedPermit,
}

impl<K, S> VerifiedFrozenNativeView<K, S>
where
    K: WorkspaceKvBackend + 'static,
    S: BlockStore + Send + Sync + 'static,
{
    pub(crate) fn candidate_manifest(&self) -> &SnapshotObjectRef {
        &self.candidate_manifest
    }
    /// Only the actual source producer installs this manifest. A replayed
    /// Building journal can therefore prove its freshly rebuilt candidate
    /// before the immutable recovery predecessor has a durable target.
    pub(crate) fn has_produced_candidate(&self) -> bool {
        self.source.produced_manifest.as_ref() == Some(&self.candidate_manifest)
    }
    pub(crate) fn source_digest(&self) -> [u8; 32] {
        self.source.digest.expect("completed source")
    }
    pub(crate) fn counts(&self) -> NativeSourceCounts {
        self.source.counts
    }
    pub(crate) fn highest_inode(&self) -> i64 {
        self.source.highest_inode
    }
    pub(crate) fn native_quiesce(&self) -> &Arc<PackedNativeQuiesceFence<K>> {
        &self.source.native
    }
    pub(crate) fn native_reader_session(&self) -> Arc<dyn PackedReaderSession> {
        self.source.session.clone()
    }
    pub(crate) fn native_phase_authority(&self) -> Option<&Arc<PackedNativePhaseFence<K>>> {
        self.source.native_phase.as_ref()
    }
    pub(crate) async fn promote_native_hashed(
        self,
        native_hash: Arc<FrozenNativeDeltaHash<K>>,
    ) -> PackedResult<Result<VerifiedHashedNativeView<K, S>, NativePromotionFailure<K, S>>> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        // The full local source, reader generation, hash and permits stay owned
        // across both actual backend CAS operations and uncertainty checks.
        tokio::spawn(async move {
            let result = self.promote_hashed_owned(native_hash).await;
            let _ = sender.send(result);
        });
        receiver
            .await
            .map_err(|_| source_error("native phase driver stopped"))
    }

    async fn promote_hashed_owned(
        mut self,
        native_hash: Arc<FrozenNativeDeltaHash<K>>,
    ) -> Result<VerifiedHashedNativeView<K, S>, NativePromotionFailure<K, S>> {
        let recover_phase = self.native_quiesce().recovery_basis().is_some_and(|basis| {
            matches!(
                basis.current_native_journal().phase,
                SealPhase::DataDrained | SealPhase::Hashed
            )
        });
        let first = if recover_phase {
            reissue_verified_native_phase(&self, native_hash.clone()).await
        } else {
            promote_verified_data_drained(&self, native_hash.clone()).await
        };
        let drained = match first {
            Ok(drained) => drained,
            Err(failure) => {
                if let Some(committed) = failure.committed {
                    self.source.native_phase = Some(committed);
                }
                return Err(NativePromotionFailure {
                    error: failure.error,
                    attempted_successor: failure.attempted_successor,
                    source: Box::new(self),
                    native_hash,
                });
            }
        };
        self.source.native_phase = Some(drained.clone());
        if drained.is_hashed() {
            return Ok(VerifiedHashedNativeView { source: self });
        }
        match promote_verified_hashed(&self, drained).await {
            Ok(hashed) => {
                self.source.native_phase = Some(hashed);
                Ok(VerifiedHashedNativeView { source: self })
            }
            Err(failure) => {
                if let Some(committed) = failure.committed {
                    self.source.native_phase = Some(committed);
                }
                Err(NativePromotionFailure {
                    error: failure.error,
                    attempted_successor: failure.attempted_successor,
                    source: Box::new(self),
                    native_hash,
                })
            }
        }
    }
    pub(crate) async fn validate(&self) -> PackedResult<()> {
        self.source.validate().await
    }
}

impl<K, S> VerifiedHashedNativeView<K, S>
where
    K: WorkspaceKvBackend + 'static,
    S: BlockStore + Send + Sync + 'static,
{
    pub(crate) fn candidate_manifest(&self) -> &SnapshotObjectRef {
        self.source.candidate_manifest()
    }
    pub(crate) fn source_digest(&self) -> [u8; 32] {
        self.source.source_digest()
    }
    pub(crate) fn counts(&self) -> NativeSourceCounts {
        self.source.counts()
    }
    pub(crate) fn highest_inode(&self) -> i64 {
        self.source.highest_inode()
    }
    pub(crate) fn native_quiesce(&self) -> &Arc<PackedNativeQuiesceFence<K>> {
        self.source.native_quiesce()
    }
    pub(crate) fn phase_authority(&self) -> &Arc<PackedNativePhaseFence<K>> {
        self.source
            .native_phase_authority()
            .expect("typed Hashed source")
    }
    pub(crate) async fn validate(&self) -> PackedResult<()> {
        if !self.phase_authority().is_hashed() {
            return Err(invalid("Hashed source phase"));
        }
        self.source.validate().await
    }
}

impl<K, S> FrozenNativeArtifact<K, S>
where
    K: WorkspaceKvBackend + 'static,
    S: BlockStore + Send + Sync + 'static,
{
    pub(crate) async fn capture(
        native: Arc<PackedNativeQuiesceFence<K>>,
        local: PackedVfsDrainFence<S, WorkspaceMetaLayer<KvWorkspaceStore<K>>>,
        parent: PathBuf,
        limits: NativeCaptureLimits,
        cancel: CancellationToken,
    ) -> PackedResult<Self> {
        limits.validate()?;
        let cancel = cancel.child_token();
        let mut cancellation_guard = CaptureCancellationGuard(Some(cancel.clone()));
        let budget = native.mount_budget();
        let control = budget.admit(&[(SnapshotBudgetPool::Control, 16 << 10)])?;
        let (sender, receiver) = tokio::sync::oneshot::channel();
        // The owned driver covers real upper and lower backend awaits. Caller
        // cancellation stops new work; no in-flight source call loses
        // its pin, generation, permits or cleanup owner before actual return.
        tokio::spawn(async move {
            let result = Self::capture_owned(native, local, &parent, limits, cancel, control).await;
            let _ = sender.send(result);
        });
        let result = receiver
            .await
            .map_err(|_| source_error("capture driver stopped"))?;
        cancellation_guard.disarm();
        result
    }

    async fn capture_owned(
        native: Arc<PackedNativeQuiesceFence<K>>,
        local: PackedVfsDrainFence<S, WorkspaceMetaLayer<KvWorkspaceStore<K>>>,
        parent: &Path,
        limits: NativeCaptureLimits,
        cancel: CancellationToken,
        control: SnapshotOwnedPermit,
    ) -> PackedResult<Self> {
        if cancel.is_cancelled() {
            return Err(source_error("capture cancelled"));
        }
        local.validate_local().await.map_err(source_error)?;
        native.validate().await.map_err(source_error)?;
        let (meta, upper, layout) = local.frozen_source_components();
        let view = meta.view_context().await;
        let guard = native.source_guard();
        let budget = native.mount_budget();
        let lower = meta
            .packed_lower()
            .cloned()
            .ok_or_else(|| invalid("source lacks packed lower"))?;
        if !native.is_same_store(meta.store())
            || view.workspace_id != guard.workspace_id
            || view.head_layer_id != guard.expected_head_layer_id
            || view.head_epoch != guard.expected_head_epoch
            || view.lease_id != guard.lease_id
            || view.holder_generation != guard.holder_generation
            || lower.binding != native.binding().binding
            || !Arc::ptr_eq(&budget, &lower.budget)
            || !lower.frozen_upper_matches(&upper, layout)
            || layout.chunk_size == 0
        {
            return Err(invalid(
                "VFS/native/catalog/binding/budget source identity mismatch",
            ));
        }
        let session = lower
            .authority
            .reader_session()
            .ok_or_else(|| invalid("source lacks real reader session"))?;
        let reader_owner = lower
            .authority
            .retain_reader_request()
            .map_err(source_error)?
            .ok_or_else(|| invalid("source lacks real retained reader generation"))?;
        if session.binding() != &lower.binding {
            return Err(invalid("source reader binding mismatch"));
        }
        session.validate().await.map_err(source_error)?;
        let mut scratch = PhysicalInventory::create(
            parent,
            budget.clone(),
            InventoryLimits {
                max_objects: limits.max_inodes,
                max_declared_bytes: limits.max_logical_bytes,
                max_disk_bytes: limits.max_sqlite_disk_bytes,
                sqlite_cache_bytes: limits.sqlite_cache_bytes,
            },
            cancel.clone(),
        )
        .await?;
        scratch
            .semantic_set_vm_limit(limits.max_sql_vm_steps)
            .await?;
        for statement in [
            "CREATE TABLE native_inodes(ino INTEGER PRIMARY KEY,hot BLOB NOT NULL,cold BLOB,kind INTEGER NOT NULL,refs INTEGER NOT NULL DEFAULT 0,subdirs INTEGER NOT NULL DEFAULT 0,captured INTEGER NOT NULL DEFAULT 0)",
            "CREATE INDEX native_pending ON native_inodes(captured,ino)",
            "CREATE TABLE native_edges(parent INTEGER NOT NULL,name BLOB NOT NULL,ino INTEGER NOT NULL,kind INTEGER NOT NULL,matched INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(parent,name)) WITHOUT ROWID",
            "CREATE TABLE native_names(name BLOB PRIMARY KEY,ordinal INTEGER NOT NULL,sequence BLOB NOT NULL,op INTEGER NOT NULL,ino INTEGER,kind INTEGER) WITHOUT ROWID",
            "CREATE TABLE native_cold_rows(tag INTEGER NOT NULL,name BLOB NOT NULL,ordinal INTEGER NOT NULL,op INTEGER NOT NULL,value BLOB,PRIMARY KEY(tag,name)) WITHOUT ROWID",
            "CREATE TABLE native_spans(ino INTEGER NOT NULL,start BLOB NOT NULL,length BLOB NOT NULL,hole INTEGER NOT NULL,digest BLOB NOT NULL,PRIMARY KEY(ino,start)) WITHOUT ROWID",
        ] {
            scratch.semantic_execute(|| query(statement)).await?;
        }
        let mut source = Self {
            scratch,
            cancel,
            native,
            native_phase: None,
            local,
            lower,
            session,
            _reader_owner: reader_owner,
            _control: control,
            budget,
            layout,
            limits,
            counts: NativeSourceCounts::default(),
            highest_inode: 1,
            digest: None,
            produced_manifest: None,
        };
        let (root, _) = source.inode(1).await?;
        if root.kind != 2 {
            return Err(invalid("root is not a directory"));
        }
        source.insert_inode(&root).await?;
        loop {
            source.validate().await?;
            let next = source
                .scratch
                .semantic_fetch_optional(|| {
                    query("SELECT ino,hot FROM native_inodes WHERE captured=0 ORDER BY ino LIMIT 1")
                })
                .await?;
            let Some(next) = next else {
                break;
            };
            let ino: i64 = next.row.try_get(0).map_err(source_error)?;
            let hot: Vec<u8> = next.row.try_get(1).map_err(source_error)?;
            let hot = NativeHot::decode(&hot)?;
            if hot.inode != ino {
                return Err(invalid("source queue inode identity"));
            }
            let cold = source.capture_cold(ino, &hot).await?;
            let cold_bytes = cold.encode()?;
            source
                .scratch
                .semantic_execute_sized(QUERY_OWNER, || {
                    query("UPDATE native_inodes SET cold=? WHERE ino=?")
                        .bind(cold_bytes)
                        .bind(ino)
                })
                .await?;
            if hot.kind == 2 {
                add(&mut source.counts.directories, 1, source.limits.max_inodes)?;
                source.capture_directory(ino).await?;
            } else if hot.kind == 1 {
                add(
                    &mut source.counts.logical_bytes,
                    hot.size,
                    source.limits.max_logical_bytes,
                )?;
                source.capture_file(ino, hot.size).await?;
            }
            source
                .scratch
                .semantic_execute(|| {
                    query("UPDATE native_inodes SET captured=1 WHERE ino=?").bind(ino)
                })
                .await?;
        }
        // A closed namespace must account for each real alias. Directory
        // references are exactly one (except root) and nlink is 2+subdirs.
        let bad = source.scratch.semantic_fetch_optional(|| query("SELECT ino FROM native_inodes WHERE captured!=1 OR cold IS NULL OR (kind=2 AND ((ino=1 AND refs!=0) OR (ino!=1 AND refs!=1))) LIMIT 1")).await?;
        if bad.is_some() {
            return Err(invalid("incomplete or multiply-parented namespace"));
        }
        let mut after = 0i64;
        while let Some(row) = source.scratch.semantic_fetch_optional(|| query("SELECT ino,hot,refs,subdirs FROM native_inodes WHERE ino>? ORDER BY ino LIMIT 1").bind(after)).await? {
            after = row.row.try_get(0).map_err(source_error)?;
            let bytes: Vec<u8> = row.row.try_get(1).map_err(source_error)?;
            let hot = NativeHot::decode(&bytes)?;
            let refs: i64 = row.row.try_get(2).map_err(source_error)?;
            let subdirs: i64 = row.row.try_get(3).map_err(source_error)?;
            let expected = if hot.kind == 2 { subdirs.checked_add(2) } else { Some(refs) };
            if expected != Some(i64::from(hot.nlink)) { return Err(invalid("source hardlink/directory nlink disagrees with full namespace")); }
        }
        source.digest = Some(source.compute_digest().await?);
        source.validate().await?;
        Ok(source)
    }

    fn check_capture_cancellation(&self) -> PackedResult<()> {
        if self.cancel.is_cancelled() {
            return Err(source_error("capture cancelled"));
        }
        Ok(())
    }

    pub(crate) async fn validate(&self) -> PackedResult<()> {
        self.check_capture_cancellation()?;
        self.local.validate_local().await.map_err(source_error)?;
        if let Some(phase) = &self.native_phase {
            phase.validate().await.map_err(source_error)?;
        } else {
            self.native.validate().await.map_err(source_error)?;
        }
        self.session.validate().await.map_err(source_error)?;
        if self.session.binding() != &self.lower.binding || self.budget.state().closed {
            return Err(invalid("reader binding or budget became invalid"));
        }
        self.check_capture_cancellation()?;
        Ok(())
    }

    pub(crate) fn highest_inode(&self) -> i64 {
        self.highest_inode
    }
    pub(crate) fn source_digest(&self) -> PackedResult<[u8; 32]> {
        self.digest
            .ok_or_else(|| invalid("native capture is incomplete"))
    }
    pub(crate) fn counts(&self) -> NativeSourceCounts {
        self.counts
    }
    pub(crate) fn native_quiesce(&self) -> &Arc<PackedNativeQuiesceFence<K>> {
        &self.native
    }
    pub(crate) fn native_reader_session(&self) -> Arc<dyn PackedReaderSession> {
        self.session.clone()
    }
    pub(crate) fn mount_budget(&self) -> Arc<SnapshotMountBudget> {
        self.budget.clone()
    }

    pub(crate) async fn next_inode_after(
        &mut self,
        after: i64,
    ) -> PackedResult<Option<SnapshotOwned<(NativeHot, SnapshotColdAttributes)>>> {
        let permit = self
            .budget
            .admit(&[(SnapshotBudgetPool::Metadata, 2 << 20)])?;
        let row = self
            .scratch
            .semantic_fetch_optional_sized(QUERY_OWNER, || {
                query("SELECT hot,cold FROM native_inodes WHERE ino>? ORDER BY ino LIMIT 1")
                    .bind(after)
            })
            .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let hot: Vec<u8> = row.row.try_get(0).map_err(source_error)?;
        let cold: Vec<u8> = row.row.try_get(1).map_err(source_error)?;
        let hot = NativeHot::decode(&hot)?;
        let reference = SnapshotObjectRef::from_bytes(
            "native/source/cold".into(),
            SnapshotObjectKind::ColdAttributes,
            &cold,
        )?;
        let cold = SnapshotColdAttributes::decode(&reference, &cold, hot.inode as u64)?;
        Ok(Some(SnapshotOwned::new((hot, cold), permit)))
    }

    pub(crate) async fn next_edge_after(
        &mut self,
        parent: i64,
        name: &[u8],
    ) -> PackedResult<Option<SnapshotOwned<NativeSourceEdge>>> {
        let permit = self
            .budget
            .admit(&[(SnapshotBudgetPool::Metadata, 32 << 10)])?;
        let row = self.scratch.semantic_fetch_optional(|| query("SELECT e.parent,e.name,i.hot FROM native_edges e JOIN native_inodes i ON e.ino=i.ino WHERE (e.parent,e.name)>(?,?) ORDER BY e.parent,e.name LIMIT 1").bind(parent).bind(name.to_vec())).await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let parent: i64 = row.row.try_get(0).map_err(source_error)?;
        let name: Vec<u8> = row.row.try_get(1).map_err(source_error)?;
        let hot: Vec<u8> = row.row.try_get(2).map_err(source_error)?;
        Ok(Some(SnapshotOwned::new(
            NativeSourceEdge {
                parent,
                name,
                hot: NativeHot::decode(&hot)?,
            },
            permit,
        )))
    }

    pub(crate) fn frame_cursor(
        &self,
        inode: i64,
        target: u64,
        size_class: crate::workspace::packed::SizeClass,
    ) -> PackedResult<NativeFrameCursor> {
        if inode <= 0 || target == 0 || target > 4 << 20 {
            return Err(limit("native frame target"));
        }
        Ok(NativeFrameCursor {
            inode,
            after: None,
            active: None,
            target,
            size_class,
        })
    }

    pub(crate) async fn next_data_frame(
        &mut self,
        cursor: &mut NativeFrameCursor,
    ) -> PackedResult<Option<OwnedNativeFrame>> {
        let permit = self.budget.admit(&[
            (
                SnapshotBudgetPool::Raw,
                cursor
                    .target
                    .checked_mul(2)
                    .ok_or_else(|| limit("frame charge overflow"))?,
            ),
            (SnapshotBudgetPool::Control, 8192),
        ])?;
        let mut raw = Vec::with_capacity(cursor.target as usize);
        let mut first_offset = None;
        let file = self.open_payload(cursor.inode, false)?;
        while raw.len() < cursor.target as usize {
            if cursor.active.is_none() {
                let row = if let Some(after) = cursor.after {
                    self.scratch.semantic_fetch_optional(|| query("SELECT start,length FROM native_spans WHERE ino=? AND hole=0 AND start>? ORDER BY start LIMIT 1").bind(cursor.inode).bind(after.to_be_bytes().to_vec())).await?
                } else {
                    self.scratch.semantic_fetch_optional(|| query("SELECT start,length FROM native_spans WHERE ino=? AND hole=0 ORDER BY start LIMIT 1").bind(cursor.inode)).await?
                };
                let Some(row) = row else {
                    break;
                };
                let start = bytes_u64(&row, 0)?;
                let length = bytes_u64(&row, 1)?;
                if length == 0 || length > SOURCE_WINDOW {
                    return Err(invalid("native source stream span"));
                }
                cursor.active = Some((start, length, 0));
                cursor.after = Some(start);
            }
            let (start, length, used) = cursor.active.unwrap();
            let offset = start
                .checked_add(used)
                .ok_or_else(|| limit("source stream offset"))?;
            if let Some(first) = first_offset {
                if offset != first + raw.len() as u64 {
                    break;
                }
            } else {
                first_offset = Some(offset);
            }
            let take = (length - used).min(cursor.target - raw.len() as u64) as usize;
            let before = raw.len();
            raw.resize(before + take, 0);
            file.read_exact_at(&mut raw[before..], offset)
                .map_err(source_error)?;
            cursor.active = if used + take as u64 == length {
                None
            } else {
                Some((start, length, used + take as u64))
            };
        }
        if raw.is_empty() {
            return Ok(None);
        }
        self.validate().await?;
        Ok(Some(OwnedNativeFrame {
            offset: first_offset.unwrap(),
            frame: crate::workspace::packed::PackedFrameInput {
                raw,
                size_class: cursor.size_class,
                codec: crate::workspace::packed::PackedCodec::Raw as u8,
                first_file_slot: 0,
                last_file_slot: 0,
            },
            permit,
        }))
    }

    pub(crate) async fn produce_candidate<O>(
        self,
        client: ObjectClient<O>,
        temporary: PathBuf,
        prefix: String,
        incarnation: uuid::Uuid,
        options: crate::workspace::packed::snapshot::SnapshotProducerOptions,
    ) -> PackedResult<(Self, SnapshotObjectRef)>
    where
        O: ObjectBackend + Clone + Send + Sync + 'static,
    {
        // The registered backend owns PPJ authority for the parent prefix.
        // Its durable PPJ staging incarnation fixes every physical key.
        // Recovery authenticates existing registry objects; no retired key
        // can be adopted or reissued by that guarded before-PUT path.
        crate::workspace::packed::snapshot::validate_key(&prefix)?;
        if incarnation.is_nil()
            || prefix.len() > 3800
            || prefix.ends_with('/')
            || self.produced_manifest.is_some()
        {
            return Err(invalid("native producer requires fresh staging scope"));
        }
        let prefix = format!("{prefix}/native-incarnation-{incarnation}");
        let (sender, receiver) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let result = self
                .produce_owned(client, &temporary, prefix, options)
                .await;
            let _ = sender.send(result);
        });
        receiver
            .await
            .map_err(|_| source_error("native producer driver stopped"))?
    }

    async fn produce_owned<O>(
        mut self,
        client: ObjectClient<O>,
        temporary: &Path,
        prefix: String,
        options: crate::workspace::packed::snapshot::SnapshotProducerOptions,
    ) -> PackedResult<(Self, SnapshotObjectRef)>
    where
        O: ObjectBackend + Clone + Send + Sync + 'static,
    {
        self.validate().await?;
        if self.digest.is_none() || options.root_inode != 1 {
            return Err(invalid("producer requires complete original-ID source"));
        }
        // Producer sorting, SQLite cache/worker and encoding scratch are owned
        // metadata, separate from the runtime decoder Workspace pool. Preserve
        // the full prior 8+8 MiB allowance and hold it through real SQLx pool
        // close and backend completion. Frame inputs retain separate owners
        // until actual chunk completion.
        let permit = Arc::new(self.budget.admit(&[
            (SnapshotBudgetPool::Metadata, 16 << 20),
            (SnapshotBudgetPool::Raw, 16 << 20),
            (SnapshotBudgetPool::Stored, 32 << 20),
            (SnapshotBudgetPool::Control, 64 << 10),
        ])?);
        let mut producer = crate::workspace::packed::snapshot::SnapshotProducer::new_native(
            client,
            temporary,
            prefix,
            options,
            permit.clone(),
            self.limits.max_producer_spool_disk_bytes,
        )
        .await?;
        let pool = producer.native_spool_pool();
        let result = async {
            producer
                .bound_native_spool(self.limits.max_producer_spool_disk_bytes)
                .await?;
            producer.add_frozen_native_source(&mut self).await?;
            self.validate().await?;
            let manifest = producer.finish().await?;
            self.validate().await?;
            Ok::<_, PackedWireError>(manifest)
        }
        .await;
        // Keep all source and producer owners while SQLx actually closes its
        // cache/worker and completes every prior statement/transaction.
        pool.close().await;
        drop(permit);
        let manifest = result?;
        self.produced_manifest = Some(manifest.clone());
        Ok((self, manifest))
    }

    async fn inode(&self, ino: i64) -> PackedResult<(NativeHot, Option<Vec<u8>>)> {
        let _permit = self
            .budget
            .admit(&[(SnapshotBudgetPool::Metadata, 512 << 10)])?;
        let rows = self.native.frozen_inode(ino).await.map_err(source_error)?;
        match resolve_inode_state(self.native.mapping().old_layers(), &rows, ino)
            .map_err(source_error)?
        {
            Resolution::Present(value) => {
                let attr =
                    crate::workspace::meta_layer::file_attr(&value.inode).map_err(source_error)?;
                Ok((NativeHot::from_attr(&attr)?, value.inode.symlink_target))
            }
            Resolution::Masked => Err(invalid("visible dentry references a deleted inode")),
            Resolution::Absent => {
                let attr = self
                    .lower
                    .metadata
                    .stat_fresh(ino)
                    .await
                    .map_err(source_error)?
                    .ok_or_else(|| invalid("visible dentry references an absent inode"))?;
                self.validate().await?;
                Ok((NativeHot::from_attr(&attr)?, None))
            }
        }
    }

    async fn insert_inode(&mut self, hot: &NativeHot) -> PackedResult<()> {
        let bytes = hot.encode()?;
        let count = self.scratch.semantic_execute(|| query("INSERT INTO native_inodes(ino,hot,kind) VALUES(?,?,?) ON CONFLICT(ino) DO NOTHING").bind(hot.inode).bind(bytes).bind(i64::from(hot.kind))).await?;
        if count == 1 {
            add(&mut self.counts.inodes, 1, self.limits.max_inodes)?;
        }
        self.highest_inode = self.highest_inode.max(hot.inode);
        Ok(())
    }

    async fn capture_directory(&mut self, parent: i64) -> PackedResult<()> {
        self.scratch
            .semantic_execute(|| query("DELETE FROM native_names"))
            .await?;
        for ordinal in 0..2 {
            let mut after = None;
            loop {
                let page = self
                    .native
                    .frozen_dentry_page(ordinal, parent, after.as_deref())
                    .await
                    .map_err(source_error)?;
                if page.is_empty() {
                    break;
                }
                for row in page.iter() {
                    row.validate().map_err(source_error)?;
                    crate::workspace::packed::meta::validate_name(&row.name)?;
                    if row.parent_ino != parent {
                        return Err(invalid("dentry parent mismatch"));
                    }
                    let op = i64::from(matches!(row.op, DentryOp::Whiteout));
                    let kind = row.entry_type.map(|kind| i64::from(kind) + 1);
                    self.scratch.semantic_execute(|| query("INSERT INTO native_names(name,ordinal,sequence,op,ino,kind) VALUES(?,?,?,?,?,?) ON CONFLICT(name) DO UPDATE SET ordinal=excluded.ordinal,sequence=excluded.sequence,op=excluded.op,ino=excluded.ino,kind=excluded.kind WHERE excluded.ordinal<native_names.ordinal OR (excluded.ordinal=native_names.ordinal AND excluded.sequence>native_names.sequence)")
                        .bind(row.name.clone()).bind(ordinal as i64).bind(row.sequence.to_be_bytes().to_vec()).bind(op).bind(row.ino).bind(kind)).await?;
                }
                after = page.after.clone();
            }
        }
        if let Some(attr) = self
            .lower
            .metadata
            .stat_fresh(parent)
            .await
            .map_err(source_error)?
            && attr.kind == FileType::Dir
        {
            let handle = self
                .lower
                .metadata
                .opendir(parent)
                .await
                .map_err(source_error)?;
            if handle.page_source.is_none() {
                return Err(invalid("lower directory is not bounded/paged"));
            }
            let mut offset = 0u64;
            loop {
                let page = handle
                    .get_entries_page_raw_owned(offset, 32)
                    .await
                    .map_err(source_error)?;
                if page.is_empty() {
                    break;
                }
                for entry in page.iter() {
                    crate::workspace::packed::meta::validate_name(&entry.name)?;
                    self.scratch.semantic_execute(|| query("INSERT INTO native_names(name,ordinal,sequence,op,ino,kind) VALUES(?,2,?,0,?,?) ON CONFLICT(name) DO NOTHING")
                        .bind(entry.name.clone()).bind(0u64.to_be_bytes().to_vec()).bind(entry.ino).bind(i64::from(kind(entry.kind)))).await?;
                }
                offset = offset
                    .checked_add(page.len() as u64)
                    .ok_or_else(|| limit("directory cursor overflow"))?;
                self.validate().await?;
            }
        }
        let mut after = Vec::<u8>::new();
        while let Some(row) = self.scratch.semantic_fetch_optional(|| query("SELECT name,ino,kind FROM native_names WHERE op=0 AND name>? ORDER BY name LIMIT 1").bind(after.clone())).await? {
            let name: Vec<u8> = row.row.try_get(0).map_err(source_error)?;
            let ino: i64 = row.row.try_get(1).map_err(source_error)?;
            let expected_kind: i64 = row.row.try_get(2).map_err(source_error)?;
            let (hot, _) = self.inode(ino).await?;
            if i64::from(hot.kind) != expected_kind || ino == 1 { return Err(invalid("dentry kind, root alias or directory loop")); }
            self.insert_inode(&hot).await?;
            if hot.kind == 2 {
                let existing = self.scratch.semantic_fetch_optional(|| query("SELECT refs FROM native_inodes WHERE ino=?").bind(ino)).await?.ok_or_else(|| invalid("directory lost source inode"))?;
                let refs: i64 = existing.row.try_get(0).map_err(source_error)?;
                if refs != 0 { return Err(invalid("directory cycle or multiple parents")); }
                self.scratch.semantic_execute(|| query("UPDATE native_inodes SET subdirs=subdirs+1 WHERE ino=?").bind(parent)).await?;
            }
            self.scratch.semantic_execute(|| query("UPDATE native_inodes SET refs=refs+1 WHERE ino=?").bind(ino)).await?;
            self.scratch.semantic_execute(|| query("INSERT INTO native_edges(parent,name,ino,kind) VALUES(?,?,?,?)").bind(parent).bind(name.clone()).bind(ino).bind(expected_kind)).await?;
            add(&mut self.counts.names, 1, self.limits.max_names)?;
            after = name;
        }
        self.validate().await
    }

    async fn cold_row(
        &mut self,
        tag: i64,
        name: &[u8],
        ordinal: i64,
        op: i64,
        value: Option<&[u8]>,
    ) -> PackedResult<()> {
        self.scratch.semantic_execute_sized(QUERY_OWNER, || query("INSERT INTO native_cold_rows(tag,name,ordinal,op,value) VALUES(?,?,?,?,?) ON CONFLICT(tag,name) DO UPDATE SET ordinal=excluded.ordinal,op=excluded.op,value=excluded.value WHERE excluded.ordinal<native_cold_rows.ordinal")
            .bind(tag).bind(name.to_vec()).bind(ordinal).bind(op).bind(value.map(<[u8]>::to_vec))).await?;
        Ok(())
    }

    async fn capture_cold(
        &mut self,
        ino: i64,
        hot: &NativeHot,
    ) -> PackedResult<SnapshotOwned<SnapshotColdAttributes>> {
        let permit = self
            .budget
            .admit(&[(SnapshotBudgetPool::Metadata, 2 << 20)])?;
        self.scratch
            .semantic_execute(|| query("DELETE FROM native_cold_rows"))
            .await?;
        let mut target = None;
        if let Some(cold) = self
            .lower
            .metadata
            .frozen_cold_attributes_owned(ino)
            .await
            .map_err(source_error)?
        {
            target = cold.symlink_target.clone();
            for attr in &cold.xattrs {
                self.cold_row(0, &attr.name, 2, 0, Some(&attr.value))
                    .await?;
            }
            for rule in &cold.acl {
                let mut key = vec![rule.acl_type];
                key.extend_from_slice(&rule.qualifier.to_be_bytes());
                self.cold_row(1, &key, 2, 0, Some(&rule.permissions.to_be_bytes()))
                    .await?;
            }
        }
        let (_, native_target) = self.inode(ino).await?;
        let native_rows = self.native.frozen_inode(ino).await.map_err(source_error)?;
        if matches!(
            resolve_inode_state(self.native.mapping().old_layers(), &native_rows, ino)
                .map_err(source_error)?,
            Resolution::Present(_)
        ) {
            target = native_target;
        }
        for ordinal in 0..2 {
            let mut after = None;
            loop {
                let page = self
                    .native
                    .frozen_xattr_page(ordinal, ino, after.as_deref())
                    .await
                    .map_err(source_error)?;
                if page.is_empty() {
                    break;
                }
                for attr in page.iter() {
                    let op = match (attr.op, attr.value.as_ref()) {
                        (ValueOp::Put, Some(_)) => 0,
                        (ValueOp::Whiteout, None) => 1,
                        _ => return Err(invalid("xattr delta op/value")),
                    };
                    self.cold_row(0, &attr.name, ordinal as i64, op, attr.value.as_deref())
                        .await?;
                }
                after = page.after.clone();
            }
            let mut after = None;
            loop {
                let page = self
                    .native
                    .frozen_acl_page(ordinal, ino, after.as_deref())
                    .await
                    .map_err(source_error)?;
                if page.is_empty() {
                    break;
                }
                for acl in page.iter() {
                    let op = match (acl.op, acl.value.as_ref()) {
                        (ValueOp::Put, Some(_)) => 0,
                        (ValueOp::Whiteout, None) => 1,
                        _ => return Err(invalid("ACL delta op/value")),
                    };
                    let mut key = vec![acl.acl_type];
                    key.extend_from_slice(
                        &u32::try_from(acl.acl_id)
                            .map_err(source_error)?
                            .to_be_bytes(),
                    );
                    self.cold_row(1, &key, ordinal as i64, op, acl.value.as_deref())
                        .await?;
                }
                after = page.after.clone();
            }
        }
        let mut cold = SnapshotColdAttributes {
            inode: ino as u64,
            symlink_target: target,
            xattrs: Vec::new(),
            acl: Vec::new(),
        };
        let mut total = 28u64
            + cold
                .symlink_target
                .as_ref()
                .map_or(0, |value| value.len() as u64);
        for tag in 0..2 {
            let mut after = Vec::<u8>::new();
            while let Some(row) = self.scratch.semantic_fetch_optional_sized(QUERY_OWNER, || query("SELECT name,value FROM native_cold_rows WHERE tag=? AND op=0 AND name>? ORDER BY name LIMIT 1").bind(tag).bind(after.clone())).await? {
                let name: Vec<u8> = row.row.try_get(0).map_err(source_error)?;
                let value: Vec<u8> = row.row.try_get(1).map_err(source_error)?;
                let encoded_bytes = if tag == 0 { 6 + name.len() as u64 + value.len() as u64 } else { 9 };
                add(&mut total, encoded_bytes, super::super::cold::SNAPSHOT_COLD_BODY_LIMIT as u64)?;
                if (tag == 0 && cold.xattrs.len() == 1024) || (tag == 1 && cold.acl.len() == 1024) {
                    return Err(limit("source cold attribute count"));
                }
                if tag == 0 {
                    cold.xattrs.push(SnapshotXattr { name: name.clone(), value });
                } else {
                    let key: [u8; 5] = name.as_slice().try_into().map_err(|_| invalid("ACL source key"))?;
                    cold.acl.push(AclRule { acl_type: key[0], qualifier: u32::from_be_bytes(key[1..].try_into().unwrap()), permissions: u32::from_be_bytes(value.try_into().map_err(|_| invalid("ACL permissions"))?) });
                }
                after = name;
            }
        }
        cold.validate_for_inode(hot.kind, hot.mode)?;
        if hot.kind == 3
            && cold
                .symlink_target
                .as_ref()
                .map(|target| target.len() as u64)
                != Some(hot.size)
        {
            return Err(invalid("source symlink bytes/EOF mismatch"));
        }
        self.validate().await?;
        Ok(SnapshotOwned::new(cold, permit))
    }

    fn payload_path(&self, ino: i64) -> PackedResult<PathBuf> {
        if ino <= 0 {
            return Err(invalid("payload inode identity"));
        }
        Ok(self
            .scratch
            .private_directory()?
            .join(format!("inode-{ino:016x}")))
    }

    fn open_payload(&self, ino: i64, create: bool) -> PackedResult<File> {
        let mut options = OpenOptions::new();
        options
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .mode(0o600);
        if create {
            options.write(true).create_new(true);
        }
        options.open(self.payload_path(ino)?).map_err(source_error)
    }

    async fn record_span(
        &mut self,
        file: &File,
        ino: i64,
        offset: u64,
        length: u64,
        source: Option<&ReadSource>,
        lower: Option<&crate::chunk::read_plan::PreparedUnifiedRead>,
    ) -> PackedResult<()> {
        self.check_capture_cancellation()?;
        if length == 0 || length > SOURCE_WINDOW {
            return Err(invalid("source span window"));
        }
        let permit = self.budget.admit(&[
            (SnapshotBudgetPool::Raw, 2 * SOURCE_WINDOW),
            (SnapshotBudgetPool::Control, 4096),
        ])?;
        let hole = source.is_none_or(|source| matches!(source, ReadSource::Hole));
        let digest = if hole {
            [0; 32]
        } else {
            add(
                &mut self.counts.data_bytes,
                length,
                // This is an executable payload write-byte reservation, not a
                // filesystem physical-allocation guarantee. st_blocks below
                // remains a separately measured disk limit; a filesystem quota
                // is still needed for a hard physical scratch boundary.
                self.limits
                    .max_data_bytes
                    .min(self.limits.max_payload_disk_bytes),
            )?;
            let mut output = vec![0; length as usize];
            let source = source.unwrap();
            if let Some(lower) = lower {
                lower
                    .fetcher
                    .ensure_generation(lower.plan.generation)
                    .await
                    .map_err(source_error)?;
                self.check_capture_cancellation()?;
                lower
                    .fetcher
                    .read_source(source, &mut output)
                    .await
                    .map_err(source_error)?;
                lower
                    .fetcher
                    .ensure_generation(lower.plan.generation)
                    .await
                    .map_err(source_error)?;
            } else {
                self.lower
                    .upper
                    .read_source(source, &mut output)
                    .await
                    .map_err(source_error)?;
            }
            self.validate().await?;
            let before = file
                .metadata()
                .map_err(source_error)?
                .blocks()
                .checked_mul(512)
                .ok_or_else(|| limit("payload disk block overflow"))?;
            file.write_all_at(&output, offset).map_err(source_error)?;
            let after = file
                .metadata()
                .map_err(source_error)?
                .blocks()
                .checked_mul(512)
                .ok_or_else(|| limit("payload disk block overflow"))?;
            add(
                &mut self.counts.payload_disk_bytes,
                after
                    .checked_sub(before)
                    .ok_or_else(|| invalid("payload disk allocation regressed"))?,
                self.limits.max_payload_disk_bytes,
            )?;
            Sha256::digest(&output).into()
        };
        self.scratch
            .semantic_execute(|| {
                query("INSERT INTO native_spans(ino,start,length,hole,digest) VALUES(?,?,?,?,?)")
                    .bind(ino)
                    .bind(offset.to_be_bytes().to_vec())
                    .bind(length.to_be_bytes().to_vec())
                    .bind(i64::from(hole))
                    .bind(digest.to_vec())
            })
            .await?;
        add(&mut self.counts.spans, 1, self.limits.max_spans)?;
        drop(permit);
        Ok(())
    }

    async fn capture_file(&mut self, ino: i64, size: u64) -> PackedResult<()> {
        let file = self.open_payload(ino, true)?;
        file.set_len(size).map_err(source_error)?;
        let lower_size = self
            .lower
            .metadata
            .stat_fresh(ino)
            .await
            .map_err(source_error)?
            .filter(|attr| attr.kind == FileType::File)
            .map_or(0, |attr| attr.size);
        let mut absolute = 0u64;
        while absolute < size {
            self.validate().await?;
            let _rows_permit = self
                .budget
                .admit(&[(SnapshotBudgetPool::Metadata, 4 << 20)])?;
            let chunk = absolute / self.layout.chunk_size;
            let base = chunk
                .checked_mul(self.layout.chunk_size)
                .ok_or_else(|| limit("chunk base overflow"))?;
            let chunk_end = base.saturating_add(self.layout.chunk_size).min(size);
            let mut rows = Vec::new();
            for ordinal in 0..2 {
                let mut after = None;
                loop {
                    let page = self
                        .native
                        .frozen_extent_page(ordinal, ino, chunk, after.as_deref())
                        .await
                        .map_err(source_error)?;
                    if page.is_empty() {
                        break;
                    }
                    if rows.len() + page.len() > MAX_EXTENT_ROWS {
                        return Err(limit("native chunk has too many extent rows"));
                    }
                    rows.extend(page.iter().cloned());
                    after = page.after.clone();
                }
            }
            while absolute < chunk_end {
                let end = absolute.saturating_add(SOURCE_WINDOW).min(chunk_end);
                let coverage = resolve_extent_coverage(
                    self.native.mapping().old_layers(),
                    &rows,
                    ino,
                    chunk,
                    (absolute - base)..(end - base),
                )
                .map_err(source_error)?;
                for span in &coverage.covered {
                    let source = match span.kind {
                        ExtentKind::Hole => ReadSource::Hole,
                        ExtentKind::Data {
                            slice_id,
                            slice_offset,
                        } => ReadSource::LegacySlice {
                            slice_id,
                            slice_offset,
                        },
                    };
                    self.record_span(
                        &file,
                        ino,
                        base + span.logical_offset,
                        span.length,
                        Some(&source),
                        None,
                    )
                    .await?;
                }
                for gap in coverage.absent {
                    let gap_start = base + gap.start;
                    let gap_end = base + gap.end;
                    let lower_end = gap_end.min(lower_size);
                    if gap_start < lower_end {
                        let prepared = self
                            .lower
                            .metadata
                            .prepare_unified_read(ino, chunk, gap.start, lower_end - gap_start)
                            .await
                            .map_err(source_error)?
                            .ok_or_else(|| invalid("lower lacks real prepared plan"))?;
                        prepared
                            .plan
                            .validate(gap.start, lower_end - gap_start)
                            .map_err(source_error)?;
                        for span in &prepared.plan.segments {
                            self.record_span(
                                &file,
                                ino,
                                base + span.logical_offset,
                                span.length,
                                Some(&span.source),
                                Some(&prepared),
                            )
                            .await?;
                        }
                    }
                    let extension = gap_start.max(lower_size);
                    if extension < gap_end {
                        self.record_span(&file, ino, extension, gap_end - extension, None, None)
                            .await?;
                    }
                }
                absolute = end;
            }
        }
        let mut cursor = 0u64;
        while let Some(row) = self.scratch.semantic_fetch_optional(|| query("SELECT start,length FROM native_spans WHERE ino=? AND start>=? ORDER BY start LIMIT 1").bind(ino).bind(cursor.to_be_bytes().to_vec())).await? {
            let start = bytes_u64(&row, 0)?;
            if start != cursor { return Err(invalid("source spans have a gap or overlap")); }
            cursor = cursor.checked_add(bytes_u64(&row, 1)?).ok_or_else(|| limit("span EOF overflow"))?;
        }
        if cursor != size {
            return Err(invalid("source spans do not cover EOF exactly"));
        }
        self.validate().await
    }

    async fn compute_digest(&mut self) -> PackedResult<[u8; 32]> {
        let mut hash = Sha256::new();
        hash.update(b"BrewFS-packed-frozen-native-effective-view\0");
        field(&mut hash, self.native.canonical_receipt_bytes());
        let mut ino = 0i64;
        while let Some(row) = self
            .scratch
            .semantic_fetch_optional_sized(QUERY_OWNER, || {
                query("SELECT ino,hot,cold FROM native_inodes WHERE ino>? ORDER BY ino LIMIT 1")
                    .bind(ino)
            })
            .await?
        {
            ino = row.row.try_get(0).map_err(source_error)?;
            let hot: Vec<u8> = row.row.try_get(1).map_err(source_error)?;
            let cold: Vec<u8> = row.row.try_get(2).map_err(source_error)?;
            hash.update(ino.to_le_bytes());
            field(&mut hash, &hot);
            field(&mut hash, &cold);
        }
        let mut parent = 0i64;
        let mut name = Vec::<u8>::new();
        while let Some(row) = self.scratch.semantic_fetch_optional(|| query("SELECT parent,name,ino,kind FROM native_edges WHERE (parent,name)>(?,?) ORDER BY parent,name LIMIT 1").bind(parent).bind(name.clone())).await? {
            parent = row.row.try_get(0).map_err(source_error)?;
            name = row.row.try_get(1).map_err(source_error)?;
            let child: i64 = row.row.try_get(2).map_err(source_error)?;
            let kind: i64 = row.row.try_get(3).map_err(source_error)?;
            hash.update(parent.to_le_bytes()); field(&mut hash, &name); hash.update(child.to_le_bytes()); hash.update(kind.to_le_bytes());
        }
        let mut ino = 0i64;
        let mut start = 0u64;
        while let Some(row) = self.scratch.semantic_fetch_optional(|| query("SELECT ino,start,length,hole,digest FROM native_spans WHERE (ino,start)>(?,?) ORDER BY ino,start LIMIT 1").bind(ino).bind(start.to_be_bytes().to_vec())).await? {
            ino = row.row.try_get(0).map_err(source_error)?;
            start = bytes_u64(&row, 1)?;
            let length = bytes_u64(&row, 2)?;
            let hole: i64 = row.row.try_get(3).map_err(source_error)?;
            let digest: Vec<u8> = row.row.try_get(4).map_err(source_error)?;
            hash.update(ino.to_le_bytes()); hash.update(start.to_le_bytes()); hash.update(length.to_le_bytes()); hash.update(hole.to_le_bytes()); field(&mut hash, &digest);
        }
        Ok(hash.finalize().into())
    }

    pub(crate) async fn compare_candidate<O>(
        self,
        candidate: Arc<PackedReadonlyMeta<O>>,
    ) -> PackedResult<VerifiedFrozenNativeView<K, S>>
    where
        O: ObjectBackend + Clone + Send + Sync + 'static,
    {
        let expected_manifest = self.produced_manifest.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let _ = sender.send(self.compare_owned(candidate, expected_manifest).await);
        });
        receiver
            .await
            .map_err(|_| source_error("comparison driver stopped"))?
    }

    /// Compare a fresh source capture with the actual durable PPJ target. This
    /// route consumes real typed recovery authority, not a caller manifest or
    /// source digest, and brackets the complete actual comparison with it.
    pub(crate) async fn compare_recovery_candidate<O>(
        self,
        recovery: Arc<PackedNativeRecoveryReadFence<K>>,
        candidate: Arc<PackedReadonlyMeta<O>>,
    ) -> PackedResult<VerifiedFrozenNativeView<K, S>>
    where
        O: ObjectBackend + Clone + Send + Sync + 'static,
    {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let result = async {
                recovery.validate().await.map_err(source_error)?;
                let record = recovery
                    .basis()
                    .ok_or_else(|| invalid("pre-PNB seed reader has no durable candidate basis"))?
                    .record();
                let target = record
                    .commit_target
                    .as_ref()
                    .ok_or_else(|| invalid("recovery PPJ has no candidate target"))?;
                if !Arc::ptr_eq(&self.native, recovery.native_quiesce())
                    || self.produced_manifest.is_some()
                    || self.digest != Some(record.source.effective_view_digest)
                    || !record.source.snapshot_backed
                    || record.source.frozen_view_token != self.native.canonical_receipt_digest()
                    || candidate.manifest_reference() != &target.binding.manifest
                {
                    return Err(invalid("recovery candidate/source/durable basis mismatch"));
                }
                let manifest = target.binding.manifest.clone();
                let verified = self.compare_owned(candidate, Some(manifest)).await?;
                recovery.validate().await.map_err(source_error)?;
                Ok(verified)
            }
            .await;
            let _ = sender.send(result);
        });
        receiver
            .await
            .map_err(|_| source_error("recovery comparison driver stopped"))?
    }

    async fn compare_owned<O>(
        mut self,
        candidate: Arc<PackedReadonlyMeta<O>>,
        expected_manifest: Option<SnapshotObjectRef>,
    ) -> PackedResult<VerifiedFrozenNativeView<K, S>>
    where
        O: ObjectBackend + Clone + Send + Sync + 'static,
    {
        self.validate().await?;
        if self.digest.is_none()
            || expected_manifest.as_ref() != Some(candidate.manifest_reference())
            || !Arc::ptr_eq(&self.budget, &candidate.mount_budget())
            || self.layout.chunk_size != candidate.chunk_size()
        {
            return Err(invalid("candidate capture/budget/layout mismatch"));
        }
        self.scratch
            .semantic_execute(|| query("UPDATE native_edges SET matched=0"))
            .await?;
        let mut after = 0i64;
        while let Some(row) = self
            .scratch
            .semantic_fetch_optional_sized(QUERY_OWNER, || {
                query("SELECT ino,hot,cold FROM native_inodes WHERE ino>? ORDER BY ino LIMIT 1")
                    .bind(after)
            })
            .await?
        {
            let ino: i64 = row.row.try_get(0).map_err(source_error)?;
            after = ino;
            let bytes: Vec<u8> = row.row.try_get(1).map_err(source_error)?;
            let hot = NativeHot::decode(&bytes)?;
            let candidate_attr = candidate
                .stat_fresh(ino)
                .await
                .map_err(source_error)?
                .ok_or_else(|| invalid("candidate omits a source inode"))?;
            if NativeHot::from_attr(&candidate_attr)? != hot {
                return Err(invalid("candidate changes original inode hot attributes"));
            }
            let source_cold: Vec<u8> = row.row.try_get(2).map_err(source_error)?;
            let candidate_cold = candidate
                .frozen_cold_attributes_owned(ino)
                .await
                .map_err(source_error)?;
            let empty = SnapshotColdAttributes {
                inode: ino as u64,
                symlink_target: None,
                xattrs: Vec::new(),
                acl: Vec::new(),
            };
            let candidate_cold = candidate_cold.as_deref().unwrap_or(&empty).encode()?;
            if candidate_cold != source_cold {
                return Err(invalid(
                    "candidate changes cold attributes or raw symlink/xattr/ACL bytes",
                ));
            }
            if hot.kind == 2 {
                let handle = candidate.opendir(ino).await.map_err(source_error)?;
                if handle.page_source.is_none() {
                    return Err(invalid("candidate directory lacks bounded pages"));
                }
                let mut offset = 0u64;
                loop {
                    let page = handle
                        .get_entries_page_raw_owned(offset, 32)
                        .await
                        .map_err(source_error)?;
                    if page.is_empty() {
                        break;
                    }
                    for entry in page.iter() {
                        let matched = self.scratch.semantic_execute(|| query("UPDATE native_edges SET matched=1 WHERE parent=? AND name=? AND ino=? AND kind=? AND matched=0")
                            .bind(ino).bind(entry.name.clone()).bind(entry.ino).bind(i64::from(kind(entry.kind)))).await?;
                        if matched != 1 {
                            return Err(invalid(
                                "candidate adds, duplicates or changes namespace/hardlink identity",
                            ));
                        }
                    }
                    offset = offset
                        .checked_add(page.len() as u64)
                        .ok_or_else(|| limit("candidate directory offset overflow"))?;
                    self.validate().await?;
                }
            } else if hot.kind == 1 {
                self.compare_file(&candidate, ino, hot.size).await?;
            }
            self.validate().await?;
        }
        if self
            .scratch
            .semantic_fetch_optional(|| {
                query("SELECT parent FROM native_edges WHERE matched=0 LIMIT 1")
            })
            .await?
            .is_some()
        {
            return Err(invalid("candidate omits a source namespace entry"));
        }
        if self.compute_digest().await? != self.digest.unwrap() {
            return Err(invalid(
                "captured source artifact changed during comparison",
            ));
        }
        self.validate().await?;
        let candidate_manifest = candidate.manifest_reference().clone();
        Ok(VerifiedFrozenNativeView {
            source: self,
            candidate_manifest,
        })
    }

    async fn compare_file<O>(
        &mut self,
        candidate: &PackedReadonlyMeta<O>,
        ino: i64,
        size: u64,
    ) -> PackedResult<()>
    where
        O: ObjectBackend + Clone + Send + Sync + 'static,
    {
        let file = self.open_payload(ino, false)?;
        let mut cursor = 0u64;
        while let Some(row) = self.scratch.semantic_fetch_optional(|| query("SELECT start,length,hole,digest FROM native_spans WHERE ino=? AND start>=? ORDER BY start LIMIT 1").bind(ino).bind(cursor.to_be_bytes().to_vec())).await? {
            let start = bytes_u64(&row, 0)?;
            let length = bytes_u64(&row, 1)?;
            let hole: i64 = row.row.try_get(2).map_err(source_error)?;
            let expected_digest: Vec<u8> = row.row.try_get(3).map_err(source_error)?;
            if start != cursor || length == 0 || length > SOURCE_WINDOW { return Err(invalid("comparison source span coverage")); }
            let _permit = self.budget.admit(&[(SnapshotBudgetPool::Raw, 3 * SOURCE_WINDOW), (SnapshotBudgetPool::Control, 4096)])?;
            let chunk = start / self.layout.chunk_size;
            let base = chunk * self.layout.chunk_size;
            let prepared = candidate.prepare_unified_read(ino, chunk, start-base, length).await.map_err(source_error)?.ok_or_else(|| invalid("candidate lacks real prepared read"))?;
            prepared.plan.validate(start-base, length).map_err(source_error)?;
            let mut actual = vec![0; length as usize];
            prepared.fetcher.ensure_generation(prepared.plan.generation).await.map_err(source_error)?;
            for segment in &prepared.plan.segments {
                let is_hole = matches!(segment.source, ReadSource::Hole);
                if is_hole != (hole == 1) { return Err(invalid("candidate changes exact Data/Hole coverage")); }
                let offset = segment.logical_offset.checked_sub(start-base).ok_or_else(|| invalid("candidate segment precedes span"))? as usize;
                let end = offset.checked_add(segment.length as usize).ok_or_else(|| limit("candidate segment output overflow"))?;
                let output = actual.get_mut(offset..end).ok_or_else(|| invalid("candidate segment exceeds span"))?;
                prepared.fetcher.read_source(&segment.source, output).await.map_err(source_error)?;
            }
            prepared.fetcher.ensure_generation(prepared.plan.generation).await.map_err(source_error)?;
            if hole == 0 {
                let mut expected = vec![0; length as usize];
                file.read_exact_at(&mut expected, start).map_err(source_error)?;
                if expected != actual || Sha256::digest(&expected).as_slice() != expected_digest.as_slice() { return Err(invalid("candidate changes logical bytes or captured payload changed")); }
            } else if actual.iter().any(|byte| *byte != 0) { return Err(invalid("candidate Hole emitted nonzero data")); }
            cursor = start.checked_add(length).ok_or_else(|| limit("comparison span overflow"))?;
            self.validate().await?;
        }
        if cursor != size {
            return Err(invalid("comparison spans do not cover exact EOF"));
        }
        Ok(())
    }
}
