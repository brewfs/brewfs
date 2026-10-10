//! Disk-backed Linux namespace inventory and bounded PM08 construction.
//!
//! BestEffortDetected is an explicit detection policy, not an atomic snapshot.
//! Every path (including directories/root and aliases) is revalidated before
//! upload, during payload capture, and before/after manifest construction.
//! A returned manifest ref is not a workspace head publication.

use std::fs::Metadata;
#[cfg(test)]
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use sea_orm::sqlx::{Row, Sqlite, SqlitePool, Transaction};
use sha2::{Digest, Sha256};

use super::source_root::{MAX_SOURCE_PATH_BYTES, V3SourceEntry, V3SourceRoot};
use super::{
    CapturedV3SourceFile, CapturedV3SourceRoot, V3ColdAttributes, V3IndexSpool, V3ObjectKind,
    V3ObjectRef, V3ProducerOptions, V3SnapshotProducer, V3SourceFileLimits,
};
use crate::cadapter::client::{ObjectBackend, ObjectClient};
use crate::workspace_overlay::packed_v3::wire::{PackedResult, PackedWireError};
use crate::workspace_overlay::packed_v3::{
    GroupMeta, GroupMetaEntry, GroupMetaExtent, GroupPackingLimits, INLINE_DATA_FLAG,
    INLINE_FILE_MAX_BYTES, PackedFrameInput, PackedGroupInput, directory_key,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum V3SourceConsistency {
    BestEffortDetected,
    SnapshotBacked,
}

impl V3SourceConsistency {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BestEffortDetected => "best-effort-detected",
            Self::SnapshotBacked => "snapshot-backed",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum V3SourceHardlinkPolicy {
    VisibleLinks,
    RejectExternal,
}

impl V3SourceHardlinkPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::VisibleLinks => "visible-links",
            Self::RejectExternal => "reject-external",
        }
    }
}

#[derive(Clone, Debug)]
pub struct V3SourceNamespaceOptions {
    pub root_inode: u64,
    pub consistency: V3SourceConsistency,
    pub hardlink_policy: V3SourceHardlinkPolicy,
    pub file_limits: V3SourceFileLimits,
}

#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct V3SourceNamespaceReport {
    pub source_entries: u64,
    pub unique_inodes: u64,
    pub regular_dentries: u64,
    pub logical_bytes: u64,
    pub source_data_bytes: u64,
    pub groups: u64,
    pub frames: u64,
    pub inline_payload_bytes: u64,
    pub peak_group_raw_payload_bytes: u64,
    pub external_files: u64,
    pub external_chunks: u64,
    pub external_frames: u64,
    pub external_data_bytes: u64,
}

pub struct V3SourceNamespaceSnapshot {
    pub reference: V3ObjectRef,
    pub report: V3SourceNamespaceReport,
    pub provenance: super::V3SourceProvenance,
    final_source_proof: V3FinalSourceProof,
}

/// Historical capture boundary issued only after the real importer's final
/// complete path/root/provider fence. This is neither a native workspace
/// effective-view seal nor permission to publish a catalog root.
#[derive(Debug)]
pub(crate) struct V3FinalSourceProof {
    manifest: V3ObjectRef,
    consistency: V3SourceConsistency,
    path_inventory_digest: [u8; 32],
    provider_fence_digest: [u8; 32],
}

impl V3FinalSourceProof {
    pub(crate) fn manifest_reference(&self) -> &V3ObjectRef {
        &self.manifest
    }

    pub(crate) fn snapshot_backed(&self) -> bool {
        self.consistency == V3SourceConsistency::SnapshotBacked
    }

    pub(crate) fn receipt_digest(&self) -> PackedResult<[u8; 32]> {
        let reference = self.manifest.encode_value()?;
        let mut hash = Sha256::new();
        hash.update(b"BrewFS packed v3 completed source capture\0");
        hash.update([u8::from(self.snapshot_backed())]);
        hash.update((reference.len() as u32).to_le_bytes());
        hash.update(reference);
        hash.update(self.path_inventory_digest);
        hash.update(self.provider_fence_digest);
        Ok(hash.finalize().into())
    }
}

impl V3SourceNamespaceSnapshot {
    pub(crate) fn into_final_source_proof(self) -> PackedResult<V3FinalSourceProof> {
        if self.reference != self.final_source_proof.manifest {
            return Err(invalid("manifest changed after final source fence"));
        }
        Ok(self.final_source_proof)
    }
}

pub struct V3SourceNamespaceInventory {
    root: Arc<V3SourceRoot>,
    temporary: PathBuf,
    root_capture: CapturedV3SourceRoot,
    spool: V3IndexSpool,
    options: V3SourceNamespaceOptions,
    report: V3SourceNamespaceReport,
}

fn invalid(what: &str) -> PackedWireError {
    PackedWireError::Invalid(format!("source namespace {what}"))
}
fn backend(what: &str) -> PackedWireError {
    PackedWireError::Backend(format!("source namespace {what} failed"))
}

fn source_id(meta: &Metadata) -> Vec<u8> {
    [meta.dev().to_be_bytes(), meta.ino().to_be_bytes()].concat()
}

pub(super) fn source_token(meta: &Metadata) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"BrewFS-source-stat-05\0");
    for field in [
        meta.dev(),
        meta.ino(),
        meta.len(),
        u64::from(meta.mode()),
        u64::from(meta.uid()),
        u64::from(meta.gid()),
        meta.rdev(),
        meta.nlink(),
        meta.mtime() as u64,
        meta.mtime_nsec() as u64,
        meta.ctime() as u64,
        meta.ctime_nsec() as u64,
        meta.blocks(),
    ] {
        digest.update(field.to_le_bytes());
    }
    digest.finalize().into()
}

fn kind(meta: &Metadata) -> PackedResult<u8> {
    match meta.mode() & 0o170000 {
        0o100000 => Ok(1),
        0o040000 => Ok(2),
        0o120000 => Ok(3),
        0o010000 => Ok(4),
        0o140000 => Ok(5),
        0o020000 => Ok(6),
        0o060000 => Ok(7),
        _ => Err(PackedWireError::UnsupportedFormat(
            "unknown source inode type".into(),
        )),
    }
}

fn hot(meta: &Metadata) -> PackedResult<GroupMetaEntry> {
    let kind = kind(meta)?;
    let entry = GroupMetaEntry {
        name: b"entry".to_vec(),
        inode: 1,
        kind,
        mode: meta.mode(),
        uid: meta.uid(),
        gid: meta.gid(),
        rdev: meta.rdev(),
        nlink: if kind == 2 {
            u32::try_from(meta.nlink()).map_err(|_| invalid("directory nlink overflows"))?
        } else {
            1
        },
        atime_ns: super::source_file::ns(meta.atime(), meta.atime_nsec())?,
        mtime_ns: super::source_file::ns(meta.mtime(), meta.mtime_nsec())?,
        ctime_ns: super::source_file::ns(meta.ctime(), meta.ctime_nsec())?,
        size: meta.len(),
        flags: 0,
        inline_data: Arc::from([]),
        extents: vec![],
    };
    GroupMeta::new(vec![entry.clone()])?.encode_restart()?;
    Ok(entry)
}

struct CapturedNode {
    hot: GroupMetaEntry,
    cold: V3ColdAttributes,
    id: Vec<u8>,
    token: [u8; 32],
    source_nlink: u64,
    blocks: u64,
}

// Bound writer work, not just the SELECT result page. Every transaction is
// owned and rolls back on errors or cancellation; the private spool never
// publishes a source snapshot or relaxes its SQLite synchronization policy.
const SOURCE_BATCH: usize = 128;

struct SourceInventoryBatch {
    transaction: Transaction<'static, Sqlite>,
    units: usize,
    source_entries: u64,
    unique_inodes: u64,
}

impl SourceInventoryBatch {
    async fn begin(pool: &SqlitePool) -> PackedResult<Self> {
        Ok(Self {
            transaction: pool
                .begin()
                .await
                .map_err(|_| backend("inventory transaction"))?,
            units: 0,
            source_entries: 0,
            unique_inodes: 0,
        })
    }

    async fn insert(
        &mut self,
        path: &[u8],
        parent: Option<&[u8]>,
        name: &[u8],
        node: CapturedNode,
    ) -> PackedResult<()> {
        if self.units >= SOURCE_BATCH {
            return Err(invalid("inventory transaction exceeds batch bound"));
        }
        let cold = node.cold.encode()?;
        let existing =
            sea_orm::sqlx::query("SELECT token,cold,kind FROM source_inodes WHERE source_id=?")
                .bind(&node.id)
                .fetch_optional(&mut *self.transaction)
                .await
                .map_err(|_| backend("identity lookup"))?;
        let mut unique = false;
        if let Some(existing) = existing {
            let token: Vec<u8> = existing
                .try_get(0)
                .map_err(|_| invalid("invalid spooled token"))?;
            let prior_cold: Vec<u8> = existing
                .try_get(1)
                .map_err(|_| invalid("invalid spooled cold attrs"))?;
            let prior_kind: i64 = existing
                .try_get(2)
                .map_err(|_| invalid("invalid spooled kind"))?;
            if node.hot.kind == 2
                || token != node.token
                || prior_cold != cold
                || prior_kind != i64::from(node.hot.kind)
            {
                return Err(invalid(
                    "directory cycle/alias or changed hardlink identity",
                ));
            }
        } else {
            let bytes = GroupMeta::new(vec![node.hot.clone()])?.encode_restart()?;
            sea_orm::sqlx::query("INSERT INTO source_inodes(source_id,hot,cold,token,kind,source_nlink,blocks) VALUES(?,?,?,?,?,?,?)")
                .bind(&node.id).bind(bytes).bind(cold).bind(node.token.to_vec()).bind(i64::from(node.hot.kind))
                .bind(node.source_nlink.to_le_bytes().to_vec()).bind(node.blocks.to_le_bytes().to_vec())
                .execute(&mut *self.transaction).await.map_err(|_| backend("identity insert"))?;
            unique = true;
        }
        sea_orm::sqlx::query(
            "INSERT INTO source_paths(path,parent,name,source_id,kind) VALUES(?,?,?,?,?)",
        )
        .bind(path)
        .bind(parent)
        .bind(name)
        .bind(&node.id)
        .bind(i64::from(node.hot.kind))
        .execute(&mut *self.transaction)
        .await
        .map_err(|_| invalid("duplicate source path"))?;
        self.unique_inodes += u64::from(unique);
        self.source_entries += u64::from(parent.is_some());
        self.units += 1;
        Ok(())
    }

    async fn mark_scanned(&mut self, directory: &[u8]) -> PackedResult<()> {
        if self.units >= SOURCE_BATCH {
            return Err(invalid("inventory transaction exceeds batch bound"));
        }
        sea_orm::sqlx::query("UPDATE source_paths SET scanned=1 WHERE path=?")
            .bind(directory)
            .execute(&mut *self.transaction)
            .await
            .map_err(|_| backend("directory completion"))?;
        self.units += 1;
        Ok(())
    }

    async fn commit(self) -> PackedResult<(u64, u64)> {
        self.transaction
            .commit()
            .await
            .map_err(|_| backend("inventory commit"))?;
        Ok((self.source_entries, self.unique_inodes))
    }
}

fn capture_node(path: &V3SourceEntry) -> PackedResult<CapturedNode> {
    let before = path.metadata()?;
    let hot = hot(&before)?;
    let target = if hot.kind == 3 {
        let bytes = path.read_link()?;
        if bytes.len() as u64 != hot.size {
            return Err(invalid("symlink target changed"));
        }
        Some(bytes)
    } else {
        None
    };
    // l*xattr calls never follow links or open FIFO/socket/device data.
    let cold = path.capture_xattrs(1, target)?;
    let token = source_token(&before);
    validate_path(path, &token)?;
    Ok(CapturedNode {
        hot,
        cold,
        id: source_id(&before),
        token,
        source_nlink: before.nlink(),
        blocks: before.blocks(),
    })
}

fn validate_path(path: &V3SourceEntry, expected: &[u8]) -> PackedResult<()> {
    let meta = path.metadata()?;
    if source_token(&meta).as_slice() != expected {
        return Err(invalid("changed during inventory/build"));
    }
    Ok(())
}

fn decode_hot(bytes: &[u8]) -> PackedResult<GroupMetaEntry> {
    let metadata = GroupMeta::decode_restart(bytes)?;
    if metadata.len() != 1 {
        return Err(invalid("spooled hot record is not singular"));
    }
    Ok(metadata.entries()[0].clone())
}

fn decode_cold(bytes: &[u8]) -> PackedResult<V3ColdAttributes> {
    let reference =
        V3ObjectRef::from_bytes("source/cold".into(), V3ObjectKind::ColdAttributes, bytes)?;
    V3ColdAttributes::decode(&reference, bytes, 1)
}

async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> PackedResult<T> + Send + 'static,
) -> PackedResult<T> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|_| backend("capture task"))?
}

impl V3SourceNamespaceInventory {
    /// Validate resolved output destinations without resolving the source tree
    /// as one PATH_MAX-limited pathname. Call before creating any destination.
    pub fn validate_output_paths(source: &Path, destinations: &[PathBuf]) -> PackedResult<()> {
        let root = V3SourceRoot::open(source, V3SourceConsistency::BestEffortDetected)?;
        for destination in destinations {
            let mut existing = destination.as_path();
            while !existing.is_dir() {
                existing = existing
                    .parent()
                    .ok_or_else(|| invalid("output ancestor missing"))?;
            }
            if root.contains_directory(existing)? {
                return Err(invalid("output/manifest must be outside source tree"));
            }
        }
        root.validate_unchanged()
    }

    pub async fn capture(
        source: &Path,
        temporary: &Path,
        options: V3SourceNamespaceOptions,
    ) -> PackedResult<Self> {
        if options.root_inode == 0 || options.root_inode > i64::MAX as u64 {
            return Err(invalid("root inode is invalid"));
        }
        options.file_limits.validate()?;
        let source = source.to_owned();
        let temporary = temporary.to_owned();
        let root_inode = options.root_inode;
        let consistency = options.consistency;
        let (root, temporary, root_capture) = blocking(move || {
            let root = V3SourceRoot::open(&source, consistency)?;
            let root_capture = CapturedV3SourceRoot::capture_at(root.entry(vec![])?, root_inode)?;
            let temporary =
                std::fs::canonicalize(&temporary).map_err(|_| backend("spool resolution"))?;
            if root.contains_directory(&temporary)? {
                return Err(invalid("spool must be outside source tree"));
            }
            Ok((root, temporary, root_capture))
        })
        .await?;
        let spool = V3IndexSpool::create(&temporary).await?;
        for schema in [
            "CREATE TABLE source_inodes (source_id BLOB PRIMARY KEY, hot BLOB NOT NULL, cold BLOB NOT NULL, token BLOB NOT NULL, kind INTEGER NOT NULL, source_nlink BLOB NOT NULL, blocks BLOB NOT NULL, master_path BLOB, visible_links INTEGER, snapshot_inode INTEGER) WITHOUT ROWID",
            "CREATE TABLE source_paths (path BLOB PRIMARY KEY, parent BLOB, name BLOB NOT NULL, source_id BLOB NOT NULL, kind INTEGER NOT NULL, scanned INTEGER NOT NULL DEFAULT 0) WITHOUT ROWID",
            "CREATE INDEX source_path_identity ON source_paths(source_id, path)",
            "CREATE INDEX source_path_parent ON source_paths(parent, name)",
            "CREATE INDEX source_directory_work ON source_paths(kind, scanned, path)",
            "CREATE UNIQUE INDEX source_snapshot_inode ON source_inodes(snapshot_inode)",
        ] {
            sea_orm::sqlx::query(schema)
                .execute(&spool.pool)
                .await
                .map_err(|_| backend("inventory schema"))?;
        }
        let mut inventory = Self {
            root,
            temporary,
            root_capture,
            spool,
            options,
            report: V3SourceNamespaceReport::default(),
        };
        inventory.capture_source().await?;
        inventory.assign_inodes().await?;
        inventory.validate_unchanged().await?;
        Ok(inventory)
    }

    async fn rotate_source_batch(
        &mut self,
        batch: SourceInventoryBatch,
    ) -> PackedResult<SourceInventoryBatch> {
        let (entries, inodes) = batch.commit().await?;
        self.report.source_entries += entries;
        self.report.unique_inodes += inodes;
        SourceInventoryBatch::begin(&self.spool.pool).await
    }

    async fn capture_source(&mut self) -> PackedResult<()> {
        let path = self.root.entry(vec![])?;
        let mut node = blocking(move || capture_node(&path)).await?;
        let root_attrs = self.root_capture.attributes();
        // Preserve the first pinned root atime, before directory enumeration.
        node.hot.atime_ns = root_attrs.atime_ns;
        if (
            node.hot.size,
            node.blocks,
            node.hot.mode,
            node.hot.uid,
            node.hot.gid,
            node.hot.nlink,
            node.hot.mtime_ns,
            node.hot.ctime_ns,
        ) != (
            root_attrs.size,
            root_attrs.blocks,
            root_attrs.mode,
            root_attrs.uid,
            root_attrs.gid,
            root_attrs.nlink,
            root_attrs.mtime_ns,
            root_attrs.ctime_ns,
        ) || node.cold.xattrs != self.root_capture.cold_attributes().xattrs
        {
            return Err(invalid("root changed before inventory"));
        }
        let mut batch = SourceInventoryBatch::begin(&self.spool.pool).await?;
        batch.insert(&[], None, &[], node).await?;
        loop {
            let directory: Option<Vec<u8>> = sea_orm::sqlx::query_scalar(
                "SELECT path FROM source_paths WHERE kind=2 AND scanned=0 ORDER BY path LIMIT 1",
            )
            .fetch_optional(&mut *batch.transaction)
            .await
            .map_err(|_| backend("directory queue"))?;
            let Some(directory) = directory else {
                break;
            };
            let path = self.root.entry(directory.clone())?;
            let mut iterator = blocking(move || path.read_dir()).await?;
            loop {
                // One iterator and one bounded attribute record stay resident;
                // descendants and name sorting live only in SQLite.
                let (next_iterator, next) = blocking(move || {
                    let next = iterator.next_name()?;
                    Ok((iterator, next))
                })
                .await?;
                iterator = next_iterator;
                let Some(name) = next else {
                    break;
                };
                let mut path = directory.clone();
                if !path.is_empty() {
                    path.push(b'/');
                }
                path.extend_from_slice(&name);
                if path.len() > MAX_SOURCE_PATH_BYTES {
                    return Err(PackedWireError::LimitExceeded(
                        "source relative path exceeds 64KiB budget".into(),
                    ));
                }
                let entry = self.root.entry(path.clone())?;
                let node = blocking(move || capture_node(&entry)).await?;
                batch.insert(&path, Some(&directory), &name, node).await?;
                if batch.units == SOURCE_BATCH {
                    batch = self.rotate_source_batch(batch).await?;
                }
            }
            let token: Vec<u8> = sea_orm::sqlx::query_scalar("SELECT i.token FROM source_inodes i JOIN source_paths p ON p.source_id=i.source_id WHERE p.path=?")
                .bind(&directory).fetch_one(&mut *batch.transaction).await.map_err(|_| backend("directory fence"))?;
            let path = self.root.entry(directory.clone())?;
            blocking(move || validate_path(&path, &token)).await?;
            batch.mark_scanned(&directory).await?;
            if batch.units == SOURCE_BATCH {
                batch = self.rotate_source_batch(batch).await?;
            }
        }
        let (entries, inodes) = batch.commit().await?;
        self.report.source_entries += entries;
        self.report.unique_inodes += inodes;
        Ok(())
    }

    async fn assign_inodes(&self) -> PackedResult<()> {
        // A page of stable identities bounds both the materialized rows and
        // writer work. The source_path_identity index serves both subqueries.
        let mut identity_cursor: Option<Vec<u8>> = None;
        loop {
            let identities: Vec<Vec<u8>> = if let Some(key) = &identity_cursor {
                sea_orm::sqlx::query_scalar("SELECT source_id FROM source_inodes WHERE source_id>? ORDER BY source_id LIMIT 128")
                    .bind(key).fetch_all(&self.spool.pool).await
            } else {
                sea_orm::sqlx::query_scalar("SELECT source_id FROM source_inodes ORDER BY source_id LIMIT 128")
                    .fetch_all(&self.spool.pool).await
            }.map_err(|_| backend("link grouping scan"))?;
            if identities.is_empty() {
                break;
            }
            let mut transaction = self
                .spool
                .pool
                .begin()
                .await
                .map_err(|_| backend("link grouping transaction"))?;
            for id in &identities {
                sea_orm::sqlx::query("UPDATE source_inodes SET master_path=(SELECT MIN(path) FROM source_paths p WHERE p.source_id=source_inodes.source_id), visible_links=(SELECT COUNT(*) FROM source_paths p WHERE p.source_id=source_inodes.source_id) WHERE source_id=?")
                    .bind(id).execute(&mut *transaction).await.map_err(|_| backend("link grouping"))?;
            }
            transaction
                .commit()
                .await
                .map_err(|_| backend("link grouping commit"))?;
            identity_cursor = identities.last().cloned();
        }
        sea_orm::sqlx::query(
            "CREATE UNIQUE INDEX source_master_path ON source_inodes(master_path)",
        )
        .execute(&self.spool.pool)
        .await
        .map_err(|_| backend("master path index"))?;
        let mut cursor: Option<Vec<u8>> = None;
        let mut next_inode = 1u64;
        loop {
            let rows = if let Some(key) = &cursor {
                sea_orm::sqlx::query("SELECT source_id,master_path,kind,source_nlink,visible_links FROM source_inodes WHERE master_path>? ORDER BY master_path LIMIT 128")
                    .bind(key).fetch_all(&self.spool.pool).await
            } else {
                sea_orm::sqlx::query("SELECT source_id,master_path,kind,source_nlink,visible_links FROM source_inodes ORDER BY master_path LIMIT 128")
                    .fetch_all(&self.spool.pool).await
            }.map_err(|_| backend("stable inode scan"))?;
            if rows.is_empty() {
                break;
            }
            let mut transaction = self
                .spool
                .pool
                .begin()
                .await
                .map_err(|_| backend("inode assignment transaction"))?;
            for row in rows {
                let id: Vec<u8> = row.try_get(0).map_err(|_| invalid("source identity"))?;
                let path: Vec<u8> = row.try_get(1).map_err(|_| invalid("master path"))?;
                let kind: i64 = row.try_get(2).map_err(|_| invalid("source kind"))?;
                let links: Vec<u8> = row.try_get(3).map_err(|_| invalid("source links"))?;
                let visible: i64 = row.try_get(4).map_err(|_| invalid("visible links"))?;
                let links = u64::from_le_bytes(
                    links
                        .try_into()
                        .map_err(|_| invalid("source link framing"))?,
                );
                if visible < 1
                    || visible > i64::from(u32::MAX)
                    || (kind != 2
                        && (visible as u64 > links
                            || (self.options.hardlink_policy
                                == V3SourceHardlinkPolicy::RejectExternal
                                && visible as u64 != links)))
                {
                    return Err(invalid(
                        "hardlinks exceed source nlink or escape reject-external view",
                    ));
                }
                let inode = if path.is_empty() {
                    self.options.root_inode
                } else {
                    if next_inode == self.options.root_inode {
                        next_inode += 1;
                    }
                    let inode = next_inode;
                    next_inode = next_inode
                        .checked_add(1)
                        .ok_or_else(|| invalid("inode allocator overflow"))?;
                    if inode > i64::MAX as u64 {
                        return Err(invalid("inode allocator exceeds i64"));
                    }
                    inode
                };
                sea_orm::sqlx::query("UPDATE source_inodes SET snapshot_inode=? WHERE source_id=?")
                    .bind(inode as i64)
                    .bind(id)
                    .execute(&mut *transaction)
                    .await
                    .map_err(|_| backend("inode assignment"))?;
                cursor = Some(path);
            }
            transaction
                .commit()
                .await
                .map_err(|_| backend("inode assignment commit"))?;
        }
        Ok(())
    }

    pub async fn validate_unchanged(&self) -> PackedResult<()> {
        self.checked_source_view_digest().await.map(|_| ())
    }

    async fn checked_source_view_digest(&self) -> PackedResult<[u8; 32]> {
        self.root.validate_unchanged()?;
        self.root_capture.validate_unchanged()?;
        let mut digest = Sha256::new();
        digest.update(b"BrewFS packed v3 checked source paths\0");
        let mut path_count = 0u64;
        let mut cursor: Option<Vec<u8>> = None;
        loop {
            let rows = if let Some(key) = &cursor {
                sea_orm::sqlx::query("SELECT p.path,i.token,i.source_id,i.snapshot_inode FROM source_paths p JOIN source_inodes i ON i.source_id=p.source_id WHERE p.path>? ORDER BY p.path LIMIT 128")
                    .bind(key).fetch_all(&self.spool.pool).await
            } else {
                sea_orm::sqlx::query("SELECT p.path,i.token,i.source_id,i.snapshot_inode FROM source_paths p JOIN source_inodes i ON i.source_id=p.source_id ORDER BY p.path LIMIT 128")
                    .fetch_all(&self.spool.pool).await
            }.map_err(|_| backend("fence scan"))?;
            if rows.is_empty() {
                break;
            }
            let mut paths = Vec::with_capacity(rows.len());
            for row in rows {
                let path: Vec<u8> = row.try_get(0).map_err(|_| invalid("spooled path"))?;
                let token: Vec<u8> = row.try_get(1).map_err(|_| invalid("spooled token"))?;
                let source_id: Vec<u8> = row.try_get(2).map_err(|_| invalid("source identity"))?;
                let inode: Option<i64> = row.try_get(3).map_err(|_| invalid("source inode"))?;
                if token.len() != 32 || source_id.len() != 16 || inode.is_some_and(|n| n <= 0) {
                    return Err(invalid("source identity framing"));
                }
                digest.update((path.len() as u64).to_le_bytes());
                digest.update(&path);
                digest.update(&source_id);
                digest.update(&token);
                digest.update(inode.unwrap_or(0).to_le_bytes());
                path_count = path_count
                    .checked_add(1)
                    .ok_or_else(|| invalid("fence path count overflow"))?;
                cursor = Some(path.clone());
                paths.push((self.root.entry(path)?, token));
            }
            blocking(move || {
                for (path, token) in paths {
                    validate_path(&path, &token)?;
                }
                Ok(())
            })
            .await?;
        }
        self.root_capture.validate_unchanged()?;
        self.root.validate_unchanged()?;
        digest.update(path_count.to_le_bytes());
        Ok(digest.finalize().into())
    }

    pub fn report(&self) -> &V3SourceNamespaceReport {
        &self.report
    }

    pub async fn build_snapshot<B: ObjectBackend + Clone + 'static>(
        mut self,
        client: ObjectClient<B>,
        prefix: String,
        options: V3ProducerOptions,
    ) -> PackedResult<V3SourceNamespaceSnapshot> {
        if options.root_inode != self.options.root_inode {
            return Err(invalid("producer root differs from inventory"));
        }
        self.validate_unchanged().await?;
        let mut producer =
            V3SnapshotProducer::new(client, &self.temporary, prefix, options.clone()).await?;
        producer.set_root_attributes(self.root_capture.attributes().clone())?;
        let root_cold = self.root_capture.cold_attributes();
        if !root_cold.xattrs.is_empty() || !root_cold.acl.is_empty() {
            producer.add_cold_attributes(root_cold).await?;
        }
        let mut cursor: Option<(Vec<u8>, Vec<u8>)> = None;
        let mut pending = NamespaceGroup::default();
        let mut group_id = 1u64;
        let limits = GroupPackingLimits::for_profile(options.profile);
        loop {
            // One row includes hot/cold bytes; LIMIT 1 prevents 128 large cold
            // records from materializing together. Parent/name index is paged.
            // A nullable OR only constrains parent>NULL and rescans the prefix
            // on every row. A tuple seek advances both BLOB index components.
            // LIMIT 1 retains the budget for a possibly large cold record.
            let row = if let Some((parent, name)) = &cursor {
                sea_orm::sqlx::query("SELECT p.path,p.parent,p.name,i.hot,i.cold,i.token,i.blocks,i.snapshot_inode,i.visible_links,parent.snapshot_inode FROM source_paths p JOIN source_inodes i ON i.source_id=p.source_id JOIN source_paths pp ON pp.path=p.parent JOIN source_inodes parent ON parent.source_id=pp.source_id WHERE (p.parent,p.name)>(?,?) ORDER BY p.parent,p.name LIMIT 1")
                    .bind(parent).bind(name).fetch_optional(&self.spool.pool).await
            } else {
                sea_orm::sqlx::query("SELECT p.path,p.parent,p.name,i.hot,i.cold,i.token,i.blocks,i.snapshot_inode,i.visible_links,parent.snapshot_inode FROM source_paths p JOIN source_inodes i ON i.source_id=p.source_id JOIN source_paths pp ON pp.path=p.parent JOIN source_inodes parent ON parent.source_id=pp.source_id WHERE p.parent IS NOT NULL ORDER BY p.parent,p.name LIMIT 1")
                    .fetch_optional(&self.spool.pool).await
            }.map_err(|_| backend("dentry scan"))?;
            let Some(row) = row else {
                break;
            };
            let relative: Vec<u8> = row.try_get(0).map_err(|_| invalid("dentry path"))?;
            let parent: Vec<u8> = row.try_get(1).map_err(|_| invalid("dentry parent"))?;
            let name: Vec<u8> = row.try_get(2).map_err(|_| invalid("dentry name"))?;
            let mut entry = decode_hot(
                &row.try_get::<Vec<u8>, _>(3)
                    .map_err(|_| invalid("hot bytes"))?,
            )?;
            let mut cold = decode_cold(
                &row.try_get::<Vec<u8>, _>(4)
                    .map_err(|_| invalid("cold bytes"))?,
            )?;
            let token: Vec<u8> = row.try_get(5).map_err(|_| invalid("token bytes"))?;
            let blocks: Vec<u8> = row.try_get(6).map_err(|_| invalid("blocks bytes"))?;
            let blocks =
                u64::from_le_bytes(blocks.try_into().map_err(|_| invalid("blocks framing"))?);
            let inode: i64 = row.try_get(7).map_err(|_| invalid("snapshot inode"))?;
            let visible: i64 = row.try_get(8).map_err(|_| invalid("snapshot nlink"))?;
            let parent_inode: i64 = row.try_get(9).map_err(|_| invalid("parent inode"))?;
            entry.inode = inode as u64;
            entry.name = name.clone();
            if entry.kind != 2 {
                entry.nlink = visible as u32;
            }
            cold.inode = entry.inode;
            let path = self.root.entry(relative)?;
            let mut frames = vec![];
            if entry.kind == 1 {
                if let Some(super::V3Placement::External { data_bytes, .. }) =
                    producer.placement(entry.inode, entry.size).await?
                {
                    // Every alias keeps the inventory token/cold/global fences,
                    // while its payload and logical tree are produced once.
                    blocking(move || validate_path(&path, &token)).await?;
                    self.report.source_data_bytes = self
                        .report
                        .source_data_bytes
                        .checked_add(data_bytes)
                        .ok_or_else(|| invalid("source data total overflow"))?;
                } else {
                    let file_limits = self.options.file_limits;
                    let producer_options = options.clone();
                    let capture_path = path.clone();
                    let capture_token = token.clone();
                    let captured = blocking(move || {
                        let captured = CapturedV3SourceFile::capture_at_for_placement(
                            capture_path,
                            inode as u64,
                            producer_options.profile,
                            producer_options.size_classes,
                            file_limits,
                            producer_options.build_policy,
                        )?;
                        if let Some(captured) = &captured {
                            if source_token(captured.source_metadata()).as_slice() != capture_token
                            {
                                return Err(invalid("file changed since inventory"));
                            }
                            captured.validate_unchanged()?;
                        }
                        Ok(captured)
                    })
                    .await?;
                    if let Some(captured) = captured {
                        // Data acquisition may advance atime. All aliases use the
                        // first inventoried hot attrs, with the fixed visible nlink.
                        let (payload_entry, payload_frames, payload_cold) = captured.into_parts();
                        let mut expected_cold = cold.clone();
                        expected_cold.inode = inode as u64;
                        if payload_cold != expected_cold {
                            return Err(invalid("file cold attributes changed"));
                        }
                        entry.extents = payload_entry.extents;
                        frames = payload_frames;
                        self.report.source_data_bytes = self
                            .report
                            .source_data_bytes
                            .checked_add(frames.iter().map(|f| f.raw.len() as u64).sum::<u64>())
                            .ok_or_else(|| invalid("source data total overflow"))?;
                    } else {
                        let mut captured = super::CapturedV3SourceLayout::capture_at(
                            path,
                            &self.temporary,
                            inode as u64,
                            options.profile,
                            options.size_classes,
                            options.build_policy,
                        )
                        .await?;
                        if source_token(captured.source_metadata()).as_slice() != token
                            || captured.cold_attributes() != &cold
                        {
                            return Err(invalid("external file changed since inventory"));
                        }
                        let chunks = producer.add_external_source(&mut captured).await?;
                        self.report.external_files += 1;
                        self.report.external_chunks += chunks;
                        self.report.external_frames += captured.frame_count();
                        self.report.external_data_bytes += captured.data_bytes();
                        self.report.source_data_bytes = self
                            .report
                            .source_data_bytes
                            .checked_add(captured.data_bytes())
                            .ok_or_else(|| invalid("source data total overflow"))?;
                    }
                }
                self.report.regular_dentries += 1;
                self.report.logical_bytes = self
                    .report
                    .logical_bytes
                    .checked_add(entry.size)
                    .ok_or_else(|| invalid("logical total overflow"))?;
            } else {
                blocking(move || validate_path(&path, &token)).await?;
            }
            if !pending.can_push(parent_inode as u64, &entry, &frames, limits, &options) {
                pending
                    .flush(
                        &mut producer,
                        &self.spool,
                        group_id,
                        &options,
                        &mut self.report,
                    )
                    .await?;
                group_id = group_id
                    .checked_add(1)
                    .ok_or_else(|| invalid("group id overflow"))?;
            }
            pending.push(parent_inode as u64, entry, frames, &options)?;
            pending.allocations.push((inode as u64, blocks));
            if cold.symlink_target.is_some() || !cold.xattrs.is_empty() || !cold.acl.is_empty() {
                // Cold publication requires the inode locator from its group.
                pending.cold.push(cold.inode);
            }
            cursor = Some((parent, name));
        }
        pending
            .flush(
                &mut producer,
                &self.spool,
                group_id,
                &options,
                &mut self.report,
            )
            .await?;
        self.validate_unchanged().await?;
        let reference = producer.finish().await?;
        let path_inventory_digest = self.checked_source_view_digest().await?;
        let provider_fence_digest = self.root.checked_fence_digest()?;
        let final_source_proof = V3FinalSourceProof {
            manifest: reference.clone(),
            consistency: self.options.consistency,
            path_inventory_digest,
            provider_fence_digest,
        };
        Ok(V3SourceNamespaceSnapshot {
            reference,
            report: self.report.clone(),
            provenance: self.root.provenance(),
            final_source_proof,
        })
    }
}

#[derive(Default)]
struct NamespaceGroup {
    parent: u64,
    entries: Vec<GroupMetaEntry>,
    frames: Vec<PackedFrameInput>,
    frame_targets: Vec<usize>,
    cold: Vec<u64>,
    allocations: Vec<(u64, u64)>,
    payload: usize,
    inline: usize,
    metadata_bound: usize,
}

impl NamespaceGroup {
    fn can_push(
        &self,
        parent: u64,
        entry: &GroupMetaEntry,
        frames: &[PackedFrameInput],
        limits: GroupPackingLimits,
        options: &V3ProducerOptions,
    ) -> bool {
        if self.entries.is_empty() {
            return true;
        }
        let bytes = frames.iter().map(|f| f.raw.len()).sum::<usize>();
        let inline = options.build_policy.inline_data
            && entry.kind == 1
            && is_dense(entry, bytes)
            && bytes > 0
            && bytes < INLINE_FILE_MAX_BYTES
            && self.inline + bytes <= 224 * 1024;
        let metadata = 160
            + entry.name.len()
            + if inline {
                bytes
            } else {
                // Filling a tail can split each incoming extent in two.
                entry.extents.len() * 48
            };
        self.parent == parent
            && self.entries.len() < limits.max_entries
            && self.payload < limits.target_logical_bytes
            && self.payload.saturating_add(bytes) <= limits.max_logical_bytes
            && self.metadata_bound.saturating_add(metadata) <= limits.max_metadata_bytes
    }

    fn push(
        &mut self,
        parent: u64,
        mut entry: GroupMetaEntry,
        frames: Vec<PackedFrameInput>,
        options: &V3ProducerOptions,
    ) -> PackedResult<()> {
        self.parent = parent;
        let slot = self.entries.len() as u32;
        let bytes = frames.iter().map(|f| f.raw.len()).sum::<usize>();
        self.payload += bytes;
        let dense = is_dense(&entry, bytes);
        if options.build_policy.inline_data
            && entry.kind == 1
            && dense
            && bytes > 0
            && bytes < INLINE_FILE_MAX_BYTES
            && self.inline + bytes <= 224 * 1024
        {
            entry.inline_data = frames
                .into_iter()
                .flat_map(|f| f.raw)
                .collect::<Vec<_>>()
                .into();
            entry.flags = INLINE_DATA_FLAG;
            entry.extents.clear();
            self.inline += bytes;
        } else if !frames.is_empty() {
            let decision =
                options
                    .build_policy
                    .select(entry.size, options.profile, options.size_classes)?;
            // Capture emits one full extent for each frame. A frame at most
            // fills one tail and one new frame; disable splitting if that
            // conservative bound would exceed the existing 256-record limit.
            let split = entry.extents.len() <= 128;
            if frames.len() != entry.extents.len() {
                return Err(invalid("source frame/extent count differs"));
            }
            let mut mapped = Vec::with_capacity(entry.extents.len() * if split { 2 } else { 1 });
            for (ordinal, (frame, extent)) in frames.into_iter().zip(&entry.extents).enumerate() {
                if extent.frame_ordinal as usize != ordinal
                    || extent.raw_offset != 0
                    || extent.logical_len as usize != frame.raw.len()
                    || extent.raw_len != extent.logical_len
                {
                    return Err(invalid("source frame/extent mapping differs"));
                }
                let mut consumed = 0usize;
                while consumed < frame.raw.len() {
                    let target = decision.frame_raw_bytes as usize;
                    let remaining = frame.raw.len() - consumed;
                    let reuse = self.frames.last().is_some_and(|tail| {
                        tail.size_class == frame.size_class
                            && self.frame_targets.last() == Some(&target)
                            && tail.raw.len() < target
                            && (split || remaining <= target - tail.raw.len())
                    });
                    if !reuse {
                        self.frame_targets.push(target);
                        self.frames.push(PackedFrameInput {
                            raw: Vec::new(),
                            size_class: frame.size_class,
                            codec: frame.codec,
                            first_file_slot: slot,
                            last_file_slot: slot,
                        });
                    }
                    let ordinal = self.frames.len() as u32 - 1;
                    let tail = self.frames.last_mut().unwrap();
                    let offset = tail.raw.len() as u32;
                    let take = remaining.min(target - tail.raw.len());
                    tail.raw
                        .extend_from_slice(&frame.raw[consumed..consumed + take]);
                    tail.last_file_slot = slot;
                    mapped.push(GroupMetaExtent {
                        file_offset: extent.file_offset + consumed as u64,
                        logical_len: take as u32,
                        frame_ordinal: ordinal,
                        raw_offset: offset,
                        raw_len: tail.raw.len() as u32,
                    });
                    consumed += take;
                }
            }
            entry.extents = mapped;
        }
        self.metadata_bound +=
            160 + entry.name.len() + entry.extents.len() * 24 + entry.inline_data.len();
        self.entries.push(entry);
        Ok(())
    }

    async fn flush<B: ObjectBackend + Clone + 'static>(
        &mut self,
        producer: &mut V3SnapshotProducer<B>,
        spool: &V3IndexSpool,
        group_id: u64,
        options: &V3ProducerOptions,
        report: &mut V3SourceNamespaceReport,
    ) -> PackedResult<()> {
        if self.entries.is_empty() {
            return Ok(());
        }
        let parent_dir_key = if self.parent == options.root_inode {
            options.root_dir_key
        } else {
            directory_key(options.snapshot_id, self.parent)
        };
        // A shared tail may grow after an earlier entry was placed. Every
        // extent authenticates the final raw frame length, including aliases.
        for entry in &mut self.entries {
            for extent in &mut entry.extents {
                extent.raw_len = self
                    .frames
                    .get(extent.frame_ordinal as usize)
                    .ok_or_else(|| invalid("group extent frame is missing"))?
                    .raw
                    .len() as u32;
            }
        }
        let group = PackedGroupInput {
            group_id,
            parent_dir_key,
            metadata: GroupMeta::new(std::mem::take(&mut self.entries))?.encode()?,
            frame_ordinals: (0..self.frames.len() as u32).collect(),
            entry_count: 0,
            file_count: 0,
            layout_profile: options.profile,
        };
        let metadata = GroupMeta::decode(&group.metadata)?;
        let group = PackedGroupInput {
            entry_count: metadata.len() as u32,
            file_count: metadata.entries().iter().filter(|e| e.kind == 1).count() as u32,
            ..group
        };
        // Final GM07 bound is checked before any container PUT.
        metadata.encode_restart()?;
        producer
            .add_container(group_id, &[group], &self.frames, &[self.parent])
            .await?;
        producer.set_inode_blocks_batch(&self.allocations).await?;
        for inode in &self.cold {
            let bytes: Vec<u8> = sea_orm::sqlx::query_scalar(
                "SELECT cold FROM source_inodes WHERE snapshot_inode=?",
            )
            .bind(*inode as i64)
            .fetch_one(&spool.pool)
            .await
            .map_err(|_| backend("group cold lookup"))?;
            let mut cold = decode_cold(&bytes)?;
            cold.inode = *inode;
            producer.add_cold_attributes(&cold).await?;
        }
        report.groups += 1;
        report.frames += self.frames.len() as u64;
        report.inline_payload_bytes += self.inline as u64;
        report.peak_group_raw_payload_bytes =
            report.peak_group_raw_payload_bytes.max(self.payload as u64);
        *self = Self::default();
        Ok(())
    }
}

fn is_dense(entry: &GroupMetaEntry, bytes: usize) -> bool {
    entry.size == bytes as u64
        && entry.extents.first().is_some_and(|e| e.file_offset == 0)
        && entry
            .extents
            .windows(2)
            .all(|w| w[0].file_offset + u64::from(w[0].logical_len) == w[1].file_offset)
}

#[cfg(test)]
mod tests {
    use super::super::{AuthenticatedV3Snapshot, V3IndexReader};
    use super::*;
    use crate::cadapter::localfs::LocalFsBackend;
    use crate::workspace_overlay::packed_v3::{AccessProfile, PackedCodec, SizeClassTable};
    use std::io::{Seek, SeekFrom, Write};
    use std::os::unix::net::UnixListener;

    fn options(codec: PackedCodec) -> V3ProducerOptions {
        V3ProducerOptions {
            snapshot_id: [1; 32],
            root_dir_key: [2; 32],
            root_inode: 7,
            profile: AccessProfile::RandomSmallFile,
            size_classes: SizeClassTable::default(),
            build_policy: Default::default(),
            metadata_codec: codec,
            data_codec: codec,
        }
    }

    #[test]
    fn g15_namespace_never_copacks_same_class_with_different_frame_targets() {
        let source = tempfile::tempdir().unwrap();
        let mut group = NamespaceGroup::default();
        let options = options(PackedCodec::Raw);
        for (inode, size) in [(2, 1536 * 1024), (3, 3 * 1024 * 1024)] {
            let path = source.path().join(format!("f{inode}"));
            std::fs::write(&path, vec![inode as u8; size]).unwrap();
            let captured = CapturedV3SourceFile::capture(
                &path,
                inode,
                options.profile,
                options.size_classes,
                None,
                V3SourceFileLimits::default(),
            )
            .unwrap();
            let (entry, frames, _) = captured.into_parts();
            group.push(7, entry, frames, &options).unwrap();
        }
        assert_eq!(
            group.frames.len(),
            2,
            "different frame targets shared a tail"
        );
        assert_eq!(group.frames[0].raw.len(), 1536 * 1024);
        assert_eq!(group.frames[1].raw.len(), 3 * 1024 * 1024);
    }
    fn inventory_options(policy: V3SourceHardlinkPolicy) -> V3SourceNamespaceOptions {
        V3SourceNamespaceOptions {
            root_inode: 7,
            consistency: V3SourceConsistency::BestEffortDetected,
            hardlink_policy: policy,
            file_limits: V3SourceFileLimits::default(),
        }
    }
    async fn inventory(source: &Path, temporary: &Path) -> V3SourceNamespaceInventory {
        V3SourceNamespaceInventory::capture(
            source,
            temporary,
            inventory_options(V3SourceHardlinkPolicy::VisibleLinks),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn final_source_proof_comes_from_import_and_survives_later_source_changes() {
        let source = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        let objects = tempfile::tempdir().unwrap();
        std::fs::write(source.path().join("file"), b"captured bytes").unwrap();
        let client = ObjectClient::new(LocalFsBackend::new(objects.path()));
        let mut built = inventory(source.path(), scratch.path())
            .await
            .build_snapshot(
                client.clone(),
                "final-source".into(),
                options(PackedCodec::Raw),
            )
            .await
            .unwrap();
        let reference = built.reference.clone();
        let original = built.final_source_proof.receipt_digest().unwrap();
        // Public reporting fields cannot promote best-effort stat fences into
        // a real readonly snapshot lease or change historical proof identity.
        built.provenance.consistency = "snapshot-backed";
        std::fs::write(source.path().join("file"), b"later source bytes").unwrap();
        let proof = built.into_final_source_proof().unwrap();
        assert!(!proof.snapshot_backed());
        assert_eq!(proof.manifest_reference(), &reference);
        assert_eq!(proof.receipt_digest().unwrap(), original);
        assert_ne!(original, [0; 32]);
        AuthenticatedV3Snapshot::open(&client, proof.manifest_reference())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn final_source_proof_rejects_a_replaced_public_manifest() {
        let source = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        let objects = tempfile::tempdir().unwrap();
        let client = ObjectClient::new(LocalFsBackend::new(objects.path()));
        let mut built = inventory(source.path(), scratch.path())
            .await
            .build_snapshot(client, "replaced-source".into(), options(PackedCodec::Raw))
            .await
            .unwrap();
        built.reference.digest[0] ^= 1;
        assert!(built.into_final_source_proof().is_err());
    }

    #[tokio::test]
    async fn g15_namespace_static_inline_off_keeps_sparse_hardlink_external_and_source_blocks() {
        use super::super::{V3BuildPolicy, V3FramePolicy};
        let source = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        let objects = tempfile::tempdir().unwrap();
        std::fs::write(source.path().join("a"), vec![7; 64 * 1024]).unwrap();
        std::fs::hard_link(source.path().join("a"), source.path().join("b")).unwrap();
        for (name, size, offset, bytes, value) in [
            ("c", 8 * 1024 * 1024, 2 * 1024 * 1024, 1536 * 1024, 3u8),
            ("d", 72 * 1024 * 1024, 32 * 1024 * 1024, 2560 * 1024, 4u8),
        ] {
            let mut file = std::fs::File::create(source.path().join(name)).unwrap();
            file.set_len(size).unwrap();
            file.seek(SeekFrom::Start(offset)).unwrap();
            file.write_all(&vec![value; bytes]).unwrap();
            file.sync_all().unwrap();
        }
        let client = ObjectClient::new(LocalFsBackend::new(objects.path()));
        let mut options = options(PackedCodec::Zstd);
        options.metadata_codec = PackedCodec::Raw;
        options.build_policy = V3BuildPolicy {
            frames: V3FramePolicy::Static1Mib,
            inline_data: false,
            p90: None,
        };
        let published = inventory(source.path(), scratch.path())
            .await
            .build_snapshot(client.clone(), "g15-source".into(), options.clone())
            .await
            .unwrap();
        let snapshot = AuthenticatedV3Snapshot::open(&client, &published.reference)
            .await
            .unwrap();
        let reader = V3IndexReader::new(client.clone(), 0);
        let build = &snapshot.manifest().build;
        assert_eq!(build.policy, options.build_policy);
        assert_eq!(build.inline_payload_bytes, 0);
        assert_eq!(published.report.inline_payload_bytes, 0);
        assert_eq!(build.external_frame_count, 3);
        assert_eq!(build.frame_count, 6);
        assert_eq!(build.frame_raw_bytes, 4224 * 1024);
        assert_eq!(build.frame_raw_size_counts, [1, 5, 0, 0]);
        let a = entry(&snapshot, &client, &reader, [2; 32], b"a").await;
        let b = entry(&snapshot, &client, &reader, [2; 32], b"b").await;
        assert_eq!(a.inode, b.inode);
        assert_eq!(a.nlink, 2);
        assert!(a.inline_data.is_empty());
        let c = entry(&snapshot, &client, &reader, [2; 32], b"c").await;
        let d = entry(&snapshot, &client, &reader, [2; 32], b"d").await;
        for (inode, offset, expected) in [
            (a.inode, 0, vec![7; 16]),
            (
                c.inode,
                2 * 1024 * 1024 - 8,
                [vec![0; 8], vec![3; 8]].concat(),
            ),
            (
                d.inode,
                32 * 1024 * 1024 - 8,
                [vec![0; 8], vec![4; 8]].concat(),
            ),
            (d.inode, 72 * 1024 * 1024 - 16, vec![0; 16]),
        ] {
            let mut actual = vec![255; expected.len()];
            snapshot
                .read_inode_range(
                    &client,
                    &reader,
                    inode,
                    offset,
                    &mut actual,
                    32 * 1024 * 1024,
                )
                .await
                .unwrap();
            assert_eq!(actual, expected);
        }
        for (name, inode) in [("a", a.inode), ("c", c.inode), ("d", d.inode)] {
            assert_eq!(
                snapshot.source_blocks(&reader, inode).await.unwrap(),
                Some(
                    std::fs::metadata(source.path().join(name))
                        .unwrap()
                        .blocks()
                )
            );
        }
    }

    async fn reset_inode_assignment(inventory: &V3SourceNamespaceInventory) {
        sea_orm::sqlx::query("DROP INDEX source_master_path")
            .execute(&inventory.spool.pool)
            .await
            .unwrap();
        sea_orm::sqlx::query(
            "UPDATE source_inodes SET master_path=NULL,visible_links=NULL,snapshot_inode=NULL",
        )
        .execute(&inventory.spool.pool)
        .await
        .unwrap();
    }

    async fn reset_source_scan(inventory: &mut V3SourceNamespaceInventory) {
        sea_orm::sqlx::query("DROP INDEX source_master_path")
            .execute(&inventory.spool.pool)
            .await
            .unwrap();
        for table in ["source_paths", "source_inodes"] {
            sea_orm::sqlx::query(&format!("DELETE FROM {table}"))
                .execute(&inventory.spool.pool)
                .await
                .unwrap();
        }
        inventory.report = V3SourceNamespaceReport::default();
    }

    #[tokio::test]
    async fn namespace_capture_commit_count_is_bounded_and_keeps_sync_policy() {
        use std::sync::atomic::{AtomicU64, Ordering};
        let source = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        for i in 0..257 {
            std::fs::write(source.path().join(format!("f{i:04}")), []).unwrap();
        }
        let mut captured = inventory(source.path(), scratch.path()).await;
        reset_source_scan(&mut captured).await;
        let synchronous: i64 = sea_orm::sqlx::query_scalar("PRAGMA synchronous")
            .fetch_one(&captured.spool.pool)
            .await
            .unwrap();
        let journal: String = sea_orm::sqlx::query_scalar("PRAGMA journal_mode")
            .fetch_one(&captured.spool.pool)
            .await
            .unwrap();
        let commits = Arc::new(AtomicU64::new(0));
        let counter = commits.clone();
        {
            let mut connection = captured.spool.pool.acquire().await.unwrap();
            connection
                .lock_handle()
                .await
                .unwrap()
                .set_commit_hook(move || {
                    counter.fetch_add(1, Ordering::SeqCst);
                    true
                });
        }
        captured.capture_source().await.unwrap();
        let actual = commits.load(Ordering::SeqCst);
        assert!(
            actual <= 8,
            "257 entries triggered {actual} capture commits"
        );
        assert_eq!(captured.report.source_entries, 257);
        assert_eq!(captured.report.unique_inodes, 258);
        let scanned: i64 = sea_orm::sqlx::query_scalar(
            "SELECT COUNT(*) FROM source_paths WHERE kind=2 AND scanned=1",
        )
        .fetch_one(&captured.spool.pool)
        .await
        .unwrap();
        assert_eq!(scanned, 1);
        assert_eq!(
            sea_orm::sqlx::query_scalar::<_, i64>("PRAGMA synchronous")
                .fetch_one(&captured.spool.pool)
                .await
                .unwrap(),
            synchronous
        );
        assert_eq!(
            sea_orm::sqlx::query_scalar::<_, String>("PRAGMA journal_mode")
                .fetch_one(&captured.spool.pool)
                .await
                .unwrap(),
            journal
        );
        captured.assign_inodes().await.unwrap();
        captured.validate_unchanged().await.unwrap();
    }

    #[tokio::test]
    async fn namespace_capture_commit_failure_rolls_back_rows_and_report() {
        let source = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        for i in 0..140 {
            std::fs::write(source.path().join(format!("f{i:04}")), []).unwrap();
        }
        let mut captured = inventory(source.path(), scratch.path()).await;
        reset_source_scan(&mut captured).await;
        {
            let mut connection = captured.spool.pool.acquire().await.unwrap();
            connection
                .lock_handle()
                .await
                .unwrap()
                .set_commit_hook(|| false);
        }
        assert!(captured.capture_source().await.is_err());
        assert_eq!(captured.report.source_entries, 0);
        assert_eq!(captured.report.unique_inodes, 0);
        for table in ["source_paths", "source_inodes"] {
            let count: i64 = sea_orm::sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
                .fetch_one(&captured.spool.pool)
                .await
                .unwrap();
            assert_eq!(count, 0, "commit failure retained {table}");
        }
    }

    #[tokio::test]
    async fn namespace_capture_duplicate_path_rolls_back_new_identity_and_keeps_prefix() {
        let source = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        for name in ["a", "b", "c"] {
            std::fs::write(source.path().join(name), []).unwrap();
        }
        let mut captured = inventory(source.path(), scratch.path()).await;
        reset_source_scan(&mut captured).await;
        let mut prefix = SourceInventoryBatch::begin(&captured.spool.pool)
            .await
            .unwrap();
        prefix
            .insert(
                b"a",
                Some(b""),
                b"a",
                capture_node(&captured.root.entry(b"a".to_vec()).unwrap()).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(prefix.commit().await.unwrap(), (1, 1));
        let mut batch = SourceInventoryBatch::begin(&captured.spool.pool)
            .await
            .unwrap();
        batch
            .insert(
                b"b",
                Some(b""),
                b"b",
                capture_node(&captured.root.entry(b"b".to_vec()).unwrap()).unwrap(),
            )
            .await
            .unwrap();
        assert!(
            batch
                .insert(
                    b"b",
                    Some(b""),
                    b"b",
                    capture_node(&captured.root.entry(b"c".to_vec()).unwrap()).unwrap()
                )
                .await
                .is_err()
        );
        drop(batch);
        let paths: Vec<Vec<u8>> =
            sea_orm::sqlx::query_scalar("SELECT path FROM source_paths ORDER BY path")
                .fetch_all(&captured.spool.pool)
                .await
                .unwrap();
        assert_eq!(paths, vec![b"a".to_vec()]);
        let identities: i64 = sea_orm::sqlx::query_scalar("SELECT COUNT(*) FROM source_inodes")
            .fetch_one(&captured.spool.pool)
            .await
            .unwrap();
        assert_eq!(identities, 1);
    }

    #[tokio::test]
    async fn namespace_cancelled_capture_batch_rolls_back_and_releases_connection() {
        let source = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        std::fs::write(source.path().join("a"), []).unwrap();
        let marker = scratch.path().join("user-file");
        std::fs::write(&marker, b"preserve").unwrap();
        let mut captured = inventory(source.path(), scratch.path()).await;
        reset_source_scan(&mut captured).await;
        let pool = captured.spool.pool.clone();
        let node = capture_node(&captured.root.entry(b"a".to_vec()).unwrap()).unwrap();
        let (ready, received) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let mut batch = SourceInventoryBatch::begin(&pool).await.unwrap();
            batch.insert(b"a", Some(b""), b"a", node).await.unwrap();
            ready.send(()).unwrap();
            std::future::pending::<()>().await;
            batch.commit().await.unwrap();
        });
        received.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        let count: i64 = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            sea_orm::sqlx::query_scalar("SELECT COUNT(*) FROM source_paths")
                .fetch_one(&captured.spool.pool),
        )
        .await
        .expect("cancelled capture batch retained the only connection")
        .unwrap();
        assert_eq!(count, 0);
        let identities: i64 = sea_orm::sqlx::query_scalar("SELECT COUNT(*) FROM source_inodes")
            .fetch_one(&captured.spool.pool)
            .await
            .unwrap();
        assert_eq!(identities, 0);
        drop(captured);
        assert_spools_removed(scratch.path());
        assert_eq!(std::fs::read(marker).unwrap(), b"preserve");
    }

    #[tokio::test]
    async fn namespace_assignment_commit_count_is_bounded_for_257_entries() {
        use std::sync::atomic::{AtomicU64, Ordering};
        let source = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        for i in 0..257 {
            std::fs::write(source.path().join(format!("f{i:04}")), []).unwrap();
        }
        let captured = inventory(source.path(), scratch.path()).await;
        reset_inode_assignment(&captured).await;
        let commits = Arc::new(AtomicU64::new(0));
        let counter = commits.clone();
        {
            let mut connection = captured.spool.pool.acquire().await.unwrap();
            connection
                .lock_handle()
                .await
                .unwrap()
                .set_commit_hook(move || {
                    counter.fetch_add(1, Ordering::SeqCst);
                    true
                });
        }
        captured.assign_inodes().await.unwrap();
        let actual = commits.load(Ordering::SeqCst);
        assert!(
            actual <= 16,
            "257 entries triggered {actual} assignment commits"
        );
        let assigned: i64 = sea_orm::sqlx::query_scalar(
            "SELECT COUNT(*) FROM source_inodes WHERE snapshot_inode IS NOT NULL",
        )
        .fetch_one(&captured.spool.pool)
        .await
        .unwrap();
        assert_eq!(assigned, 258);
        let stable: Vec<(Vec<u8>, i64)> = sea_orm::sqlx::query_as(
            "SELECT master_path,snapshot_inode FROM source_inodes ORDER BY master_path",
        )
        .fetch_all(&captured.spool.pool)
        .await
        .unwrap();
        assert_eq!(stable[0], (vec![], 7));
        for (i, (path, inode)) in stable.into_iter().skip(1).enumerate() {
            assert_eq!(path, format!("f{i:04}").as_bytes());
            assert_eq!(inode, (i + 1 + usize::from(i >= 6)) as i64);
        }
    }

    #[tokio::test]
    async fn namespace_assignment_rejected_links_roll_back_current_page() {
        let source = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        for i in 0..140 {
            std::fs::write(source.path().join(format!("f{i:04}")), []).unwrap();
        }
        let captured = inventory(source.path(), scratch.path()).await;
        reset_inode_assignment(&captured).await;
        sea_orm::sqlx::query("UPDATE source_inodes SET source_nlink=? WHERE source_id=(SELECT source_id FROM source_paths WHERE path=?)")
            .bind(0u64.to_le_bytes().to_vec())
            .bind(b"f0090".as_slice())
            .execute(&captured.spool.pool)
            .await
            .unwrap();
        assert!(captured.assign_inodes().await.is_err());
        let assigned: i64 = sea_orm::sqlx::query_scalar(
            "SELECT COUNT(*) FROM source_inodes WHERE snapshot_inode IS NOT NULL",
        )
        .fetch_one(&captured.spool.pool)
        .await
        .unwrap();
        assert_eq!(
            assigned, 0,
            "failed assignment retained a partial uncommitted page"
        );
    }

    #[tokio::test]
    async fn namespace_assignment_late_error_preserves_only_committed_prefix() {
        let source = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        for i in 0..257 {
            std::fs::write(source.path().join(format!("f{i:04}")), []).unwrap();
        }
        let captured = inventory(source.path(), scratch.path()).await;
        reset_inode_assignment(&captured).await;
        sea_orm::sqlx::query("CREATE TEMP TRIGGER fail_assignment BEFORE UPDATE OF snapshot_inode ON source_inodes WHEN NEW.snapshot_inode IS NOT NULL AND NEW.master_path=X'6630313330' BEGIN SELECT RAISE(ABORT,'injected source assignment failure'); END")
            .execute(&captured.spool.pool).await.unwrap();
        assert!(captured.assign_inodes().await.is_err());
        let assigned: i64 = sea_orm::sqlx::query_scalar(
            "SELECT COUNT(*) FROM source_inodes WHERE snapshot_inode IS NOT NULL",
        )
        .fetch_one(&captured.spool.pool)
        .await
        .unwrap();
        assert_eq!(assigned, 128);
        let tail: i64 = sea_orm::sqlx::query_scalar("SELECT COUNT(*) FROM source_inodes WHERE master_path>=? AND snapshot_inode IS NOT NULL")
            .bind(b"f0127".as_slice()).fetch_one(&captured.spool.pool).await.unwrap();
        assert_eq!(tail, 0);
    }

    #[tokio::test]
    async fn namespace_link_grouping_error_rolls_back_current_identity_page() {
        let source = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        for i in 0..140 {
            std::fs::write(source.path().join(format!("f{i:04}")), []).unwrap();
        }
        let captured = inventory(source.path(), scratch.path()).await;
        reset_inode_assignment(&captured).await;
        let failed_id: Vec<u8> = sea_orm::sqlx::query_scalar(
            "SELECT source_id FROM source_inodes ORDER BY source_id LIMIT 1 OFFSET 90",
        )
        .fetch_one(&captured.spool.pool)
        .await
        .unwrap();
        // The identity comes from this private source database, not user SQL.
        sea_orm::sqlx::query(&format!("CREATE TEMP TRIGGER fail_grouping BEFORE UPDATE OF master_path ON source_inodes WHEN NEW.source_id=X'{}' BEGIN SELECT RAISE(ABORT,'injected source grouping failure'); END", hex::encode(failed_id)))
            .execute(&captured.spool.pool).await.unwrap();
        assert!(captured.assign_inodes().await.is_err());
        let grouped: i64 = sea_orm::sqlx::query_scalar("SELECT COUNT(*) FROM source_inodes WHERE master_path IS NOT NULL OR visible_links IS NOT NULL")
            .fetch_one(&captured.spool.pool).await.unwrap();
        assert_eq!(grouped, 0);
    }

    #[tokio::test]
    async fn namespace_batched_capture_preserves_cross_directory_hardlinks_and_queue() {
        use std::sync::atomic::{AtomicU64, Ordering};
        let source = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        for directory in ["a", "b", "c"] {
            std::fs::create_dir(source.path().join(directory)).unwrap();
        }
        for i in 0..130 {
            let name = format!("f{i:04}");
            std::fs::write(source.path().join("a").join(&name), []).unwrap();
            std::fs::hard_link(
                source.path().join("a").join(&name),
                source.path().join("b").join(&name),
            )
            .unwrap();
            std::fs::write(source.path().join("c").join(&name), []).unwrap();
        }
        let mut captured = V3SourceNamespaceInventory::capture(
            source.path(),
            scratch.path(),
            inventory_options(V3SourceHardlinkPolicy::RejectExternal),
        )
        .await
        .unwrap();
        let before: Vec<(Vec<u8>, i64)> = sea_orm::sqlx::query_as(
            "SELECT master_path,snapshot_inode FROM source_inodes ORDER BY master_path",
        )
        .fetch_all(&captured.spool.pool)
        .await
        .unwrap();
        reset_source_scan(&mut captured).await;
        let commits = Arc::new(AtomicU64::new(0));
        let counter = commits.clone();
        {
            let mut connection = captured.spool.pool.acquire().await.unwrap();
            connection
                .lock_handle()
                .await
                .unwrap()
                .set_commit_hook(move || {
                    counter.fetch_add(1, Ordering::SeqCst);
                    true
                });
        }
        captured.capture_source().await.unwrap();
        captured.assign_inodes().await.unwrap();
        captured.validate_unchanged().await.unwrap();
        assert_eq!(captured.report.source_entries, 393);
        assert_eq!(captured.report.unique_inodes, 264);
        let queued: i64 = sea_orm::sqlx::query_scalar(
            "SELECT COUNT(*) FROM source_paths WHERE kind=2 AND scanned=0",
        )
        .fetch_one(&captured.spool.pool)
        .await
        .unwrap();
        assert_eq!(queued, 0);
        let aliases: i64 = sea_orm::sqlx::query_scalar("SELECT COUNT(*) FROM source_inodes WHERE kind=1 AND visible_links=2 AND substr(master_path,1,2)=?")
            .bind(b"a/".as_slice()).fetch_one(&captured.spool.pool).await.unwrap();
        assert_eq!(aliases, 130);
        let after: Vec<(Vec<u8>, i64)> = sea_orm::sqlx::query_as(
            "SELECT master_path,snapshot_inode FROM source_inodes ORDER BY master_path",
        )
        .fetch_all(&captured.spool.pool)
        .await
        .unwrap();
        assert_eq!(before, after);
        let actual = commits.load(Ordering::SeqCst);
        assert!(
            actual <= 20,
            "393 dentries triggered {actual} capture and assignment commits"
        );
    }

    #[tokio::test]
    async fn namespace_cancelled_real_capture_rolls_back_tail_and_keeps_committed_prefix() {
        use std::sync::atomic::{AtomicU64, Ordering};
        let source = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        for i in 0..257 {
            std::fs::write(source.path().join(format!("f{i:04}")), []).unwrap();
        }
        let mut captured = inventory(source.path(), scratch.path()).await;
        reset_source_scan(&mut captured).await;
        let pool = captured.spool.pool.clone();
        let commits = Arc::new(AtomicU64::new(0));
        let counter = commits.clone();
        let (ready, received) = tokio::sync::oneshot::channel();
        let gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let worker_gate = gate.clone();
        {
            let mut connection = pool.acquire().await.unwrap();
            let mut handle = connection.lock_handle().await.unwrap();
            handle.set_commit_hook(move || {
                counter.fetch_add(1, Ordering::SeqCst);
                true
            });
            let mut ready = Some(ready);
            let mut operations = 0usize;
            handle.set_progress_handler(1, move || {
                if commits.load(Ordering::SeqCst) == 1 {
                    operations += 1;
                    if operations >= 500
                        && let Some(ready) = ready.take()
                    {
                        let _ = ready.send(());
                        let (lock, changed) = &*worker_gate;
                        let mut released = lock.lock().unwrap();
                        while !*released {
                            released = changed.wait(released).unwrap();
                        }
                    }
                }
                true
            });
        }
        let captured = Arc::new(tokio::sync::Mutex::new(captured));
        let task_inventory = captured.clone();
        let task = tokio::spawn(async move { task_inventory.lock().await.capture_source().await });
        let reached = tokio::time::timeout(std::time::Duration::from_secs(5), received).await;
        task.abort();
        let cancelled = task.await;
        {
            let (lock, changed) = &*gate;
            *lock.lock().unwrap() = true;
            changed.notify_all();
        }
        reached
            .expect("capture did not reach the second transaction")
            .unwrap();
        assert!(cancelled.unwrap_err().is_cancelled());
        let count: i64 = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            sea_orm::sqlx::query_scalar("SELECT COUNT(*) FROM source_paths").fetch_one(&pool),
        )
        .await
        .expect("cancelled real capture retained the only connection")
        .unwrap();
        assert_eq!(count, 128);
        let identities: i64 = sea_orm::sqlx::query_scalar("SELECT COUNT(*) FROM source_inodes")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(identities, 128);
        let captured = captured.lock().await;
        assert_eq!(captured.report.source_entries, 127);
        assert_eq!(captured.report.unique_inodes, 128);
    }

    #[tokio::test]
    async fn namespace_rejects_symlink_in_source_root_ancestors() {
        let source = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        std::fs::create_dir(source.path().join("actual")).unwrap();
        std::fs::create_dir(source.path().join("actual/root")).unwrap();
        std::os::unix::fs::symlink(source.path().join("actual"), source.path().join("alias"))
            .unwrap();
        assert!(
            V3SourceNamespaceInventory::capture(
                &source.path().join("alias/root"),
                scratch.path(),
                inventory_options(V3SourceHardlinkPolicy::VisibleLinks),
            )
            .await
            .is_err()
        );
        assert_spools_removed(scratch.path());
    }

    #[tokio::test]
    async fn namespace_fd_capture_imports_paths_beyond_path_max_with_cold_and_external_payload() {
        use std::os::fd::{AsRawFd, FromRawFd};
        let source = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        let objects = tempfile::tempdir().unwrap();
        let name = "d".repeat(160);
        let component = std::ffi::CString::new(name.clone()).unwrap();
        let mut directory = std::fs::File::open(source.path()).unwrap();
        let mut relative_bytes = 0usize;
        for _ in 0..34 {
            assert_eq!(
                unsafe { libc::mkdirat(directory.as_raw_fd(), component.as_ptr(), 0o755) },
                0
            );
            let fd = unsafe {
                libc::openat(
                    directory.as_raw_fd(),
                    component.as_ptr(),
                    libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                )
            };
            assert!(fd >= 0);
            directory = unsafe { std::fs::File::from_raw_fd(fd) };
            relative_bytes += name.len() + 1;
        }
        assert!(relative_bytes > 4096);
        for (filename, external) in [(c"small", false), (c"external", true)] {
            let fd = unsafe {
                libc::openat(
                    directory.as_raw_fd(),
                    filename.as_ptr(),
                    libc::O_CREAT | libc::O_RDWR | libc::O_EXCL | libc::O_CLOEXEC,
                    0o644,
                )
            };
            assert!(fd >= 0);
            let mut file = unsafe { std::fs::File::from_raw_fd(fd) };
            if external {
                file.set_len(72 * 1024 * 1024).unwrap();
            }
            file.write_all(b"payload").unwrap();
            assert_eq!(
                unsafe {
                    libc::fsetxattr(
                        file.as_raw_fd(),
                        c"user.deep".as_ptr(),
                        b"cold".as_ptr().cast(),
                        4,
                        0,
                    )
                },
                0
            );
        }
        assert_eq!(
            unsafe {
                libc::symlinkat(
                    c"../outside-target".as_ptr(),
                    directory.as_raw_fd(),
                    c"link".as_ptr(),
                )
            },
            0
        );
        assert_eq!(
            unsafe { libc::mkfifoat(directory.as_raw_fd(), c"fifo".as_ptr(), 0o644) },
            0
        );
        let client = ObjectClient::new(LocalFsBackend::new(objects.path()));
        let built = inventory(source.path(), scratch.path())
            .await
            .build_snapshot(
                client.clone(),
                "deep-path".into(),
                options(PackedCodec::Raw),
            )
            .await
            .unwrap();
        assert_eq!(built.report.source_entries, 38);
        assert_eq!(built.report.external_files, 1);
        let snapshot = AuthenticatedV3Snapshot::open(&client, &built.reference)
            .await
            .unwrap();
        let reader = V3IndexReader::new(client.clone(), 0);
        let mut parent = [2; 32];
        for _ in 0..34 {
            let directory = entry(&snapshot, &client, &reader, parent, name.as_bytes()).await;
            assert_eq!(directory.kind, 2);
            parent = directory_key([1; 32], directory.inode);
        }
        for filename in [b"small".as_slice(), b"external".as_slice()] {
            let file = entry(&snapshot, &client, &reader, parent, filename).await;
            let mut bytes = [0u8; 7];
            snapshot
                .read_inode_range(
                    &client,
                    &reader,
                    file.inode,
                    0,
                    &mut bytes,
                    32 * 1024 * 1024,
                )
                .await
                .unwrap();
            assert_eq!(&bytes, b"payload");
            let cold = snapshot
                .cold_attributes(&client, &reader, file.inode)
                .await
                .unwrap()
                .unwrap();
            assert!(
                cold.xattrs
                    .iter()
                    .any(|x| x.name == b"user.deep" && x.value == b"cold")
            );
        }
        let link = entry(&snapshot, &client, &reader, parent, b"link").await;
        assert_eq!(
            snapshot
                .cold_attributes(&client, &reader, link.inode)
                .await
                .unwrap()
                .unwrap()
                .symlink_target,
            Some(b"../outside-target".to_vec())
        );
        assert_eq!(
            entry(&snapshot, &client, &reader, parent, b"fifo")
                .await
                .kind,
            4
        );
        assert_spools_removed(scratch.path());
    }

    #[tokio::test]
    async fn namespace_snapshot_backed_rejects_an_ordinary_directory_without_uploads() {
        let source = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        let mut opts = inventory_options(V3SourceHardlinkPolicy::VisibleLinks);
        opts.consistency = V3SourceConsistency::SnapshotBacked;
        let error =
            match V3SourceNamespaceInventory::capture(source.path(), scratch.path(), opts).await {
                Ok(_) => panic!("ordinary directory issued a frozen lease"),
                Err(error) => error,
            };
        assert!(matches!(error, PackedWireError::UnsupportedFormat(_)));
        assert_spools_removed(scratch.path());
    }

    #[tokio::test]
    #[ignore = "requires an owned Linux Btrfs readonly snapshot; see G02 draft harness"]
    async fn namespace_real_btrfs_snapshot_freezes_mutable_source_and_preserves_provenance() {
        let original = PathBuf::from(
            std::env::var_os("BREWFS_TEST_BTRFS_ORIGINAL").expect("owned mutable source"),
        );
        let readonly = PathBuf::from(
            std::env::var_os("BREWFS_TEST_BTRFS_SNAPSHOT").expect("owned readonly snapshot"),
        );
        let scratch = tempfile::tempdir().unwrap();
        let objects = tempfile::tempdir().unwrap();
        let mut opts = inventory_options(V3SourceHardlinkPolicy::VisibleLinks);
        opts.consistency = V3SourceConsistency::SnapshotBacked;
        let captured = V3SourceNamespaceInventory::capture(&readonly, scratch.path(), opts)
            .await
            .unwrap();
        std::fs::write(original.join("frozen"), b"after-source-mutation").unwrap();
        captured.validate_unchanged().await.unwrap();
        let client = ObjectClient::new(LocalFsBackend::new(objects.path()));
        let built = captured
            .build_snapshot(client.clone(), "frozen".into(), options(PackedCodec::Raw))
            .await
            .unwrap();
        assert_eq!(built.provenance.consistency, "snapshot-backed");
        assert_eq!(built.provenance.provider, "linux-btrfs-readonly-snapshot");
        assert!(built.provenance.filesystem_uuid.is_some());
        assert!(built.provenance.snapshot_uuid.is_some());
        assert!(built.provenance.parent_snapshot_uuid.is_some());
        let snapshot = AuthenticatedV3Snapshot::open(&client, &built.reference)
            .await
            .unwrap();
        let reader = V3IndexReader::new(client.clone(), 0);
        let file = entry(&snapshot, &client, &reader, [2; 32], b"frozen").await;
        let mut bytes = vec![0u8; file.size as usize];
        snapshot
            .read_inode_range(
                &client,
                &reader,
                file.inode,
                0,
                &mut bytes,
                32 * 1024 * 1024,
            )
            .await
            .unwrap();
        assert_eq!(bytes, b"before-source-mutation");
        assert_spools_removed(scratch.path());
    }
    /// Records real manifest create-only upload, then revokes the provider
    /// during its second real manifest read, after upload byte verification.
    /// Backend operations return success: rejection must be the source fence.
    #[derive(Clone)]
    struct RevokeAfterManifestBackend {
        inner: LocalFsBackend,
        state: Arc<RevokeAfterManifestState>,
    }

    struct RevokeAfterManifestState {
        snapshot: PathBuf,
        manifest: std::sync::Mutex<Option<V3ObjectRef>>,
        uploaded: std::sync::atomic::AtomicBool,
        verification_ranges: std::sync::atomic::AtomicUsize,
        manifest_gets: std::sync::atomic::AtomicUsize,
        revoked: std::sync::atomic::AtomicBool,
    }

    #[async_trait::async_trait]
    impl ObjectBackend for RevokeAfterManifestBackend {
        async fn put_object(&self, key: &str, bytes: &[u8]) -> anyhow::Result<()> {
            self.inner.put_object(key, bytes).await
        }

        async fn put_object_create_only(&self, key: &str, bytes: &[u8]) -> anyhow::Result<()> {
            self.inner.put_object_create_only(key, bytes).await?;
            if bytes.starts_with(b"BRFPM005") {
                let reference = V3ObjectRef::from_bytes(key.into(), V3ObjectKind::Manifest, bytes)?;
                *self.state.manifest.lock().unwrap() = Some(reference);
                self.state
                    .uploaded
                    .store(true, std::sync::atomic::Ordering::SeqCst);
            }
            Ok(())
        }

        async fn get_object_range(
            &self,
            key: &str,
            offset: u64,
            bytes: &mut [u8],
        ) -> anyhow::Result<usize> {
            let count = self.inner.get_object_range(key, offset, bytes).await?;
            let manifest = self.state.manifest.lock().unwrap().clone();
            if let Some(manifest) = manifest.filter(|manifest| manifest.key == key) {
                anyhow::ensure!(
                    offset == 0 && count as u64 == manifest.object_len,
                    "expected complete bounded manifest range"
                );
                let previous = self
                    .state
                    .verification_ranges
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if previous == 1 {
                    // First range is upload byte verification; second is
                    // AuthenticatedV3Snapshot::open inside producer.finish.
                    // Both actual file reads succeed. Revoking here does not
                    // turn backend/manifest validation into an error.
                    self.state
                        .manifest_gets
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let path = self.state.snapshot.clone();
                    let success = tokio::task::spawn_blocking(move || {
                        std::process::Command::new("btrfs")
                            .args(["property", "set", "-ts"])
                            .arg(path)
                            .args(["ro", "false"])
                            .status()
                    })
                    .await??
                    .success();
                    anyhow::ensure!(success, "owned snapshot readonly revocation failed");
                    self.state
                        .revoked
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                }
            }
            Ok(count)
        }

        async fn get_object(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
            self.inner.get_object(key).await
        }

        async fn get_etag(&self, key: &str) -> anyhow::Result<String> {
            self.inner.get_etag(key).await
        }

        async fn delete_object(&self, key: &str) -> anyhow::Result<()> {
            self.inner.delete_object(key).await
        }
    }

    #[tokio::test]
    #[ignore = "requires an owned Btrfs snapshot and admin readonly revocation; see G02 harness"]
    async fn namespace_btrfs_guard_revoked_after_manifest_upload_refuses_trusted_ref() {
        use std::sync::atomic::Ordering;
        let readonly = PathBuf::from(
            std::env::var_os("BREWFS_TEST_BTRFS_SNAPSHOT").expect("owned readonly snapshot"),
        );
        struct RestoreReadonly(PathBuf);
        impl Drop for RestoreReadonly {
            fn drop(&mut self) {
                let status = std::process::Command::new("btrfs")
                    .args(["property", "set", "-ts"])
                    .arg(&self.0)
                    .args(["ro", "true"])
                    .status();
                assert!(
                    status.is_ok_and(|status| status.success()),
                    "owned snapshot readonly restoration failed"
                );
            }
        }
        // Install restoration before any operation capable of revoking ro.
        let restore = RestoreReadonly(readonly.clone());
        let scratch = tempfile::tempdir().unwrap();
        let objects = tempfile::tempdir().unwrap();
        let mut opts = inventory_options(V3SourceHardlinkPolicy::VisibleLinks);
        opts.consistency = V3SourceConsistency::SnapshotBacked;
        let captured = V3SourceNamespaceInventory::capture(&readonly, scratch.path(), opts)
            .await
            .unwrap();
        captured.validate_unchanged().await.unwrap();
        let state = Arc::new(RevokeAfterManifestState {
            snapshot: readonly.clone(),
            manifest: std::sync::Mutex::new(None),
            uploaded: false.into(),
            verification_ranges: 0.into(),
            manifest_gets: 0.into(),
            revoked: false.into(),
        });
        let backend = RevokeAfterManifestBackend {
            inner: LocalFsBackend::new(objects.path()),
            state: state.clone(),
        };
        let error = match captured
            .build_snapshot(
                ObjectClient::new(backend),
                "revoked-after-manifest".into(),
                options(PackedCodec::Raw),
            )
            .await
        {
            Ok(_) => panic!("returned trusted snapshot ref after readonly guard was revoked"),
            Err(error) => error,
        };
        assert!(
            state.uploaded.load(Ordering::SeqCst),
            "manifest PUT must complete"
        );
        assert_eq!(
            state.verification_ranges.load(Ordering::SeqCst),
            2,
            "manifest upload verification must complete before revocation"
        );
        assert_eq!(state.manifest_gets.load(Ordering::SeqCst), 1);
        assert!(state.revoked.load(Ordering::SeqCst));
        assert!(
            matches!(
                &error,
                PackedWireError::Invalid(_) | PackedWireError::UnsupportedFormat(_)
            ),
            "must fail provider/stat guard, not backend verification: {error}"
        );
        let message = error.to_string();
        assert!(
            message.contains("source changed")
                || message.contains("readonly snapshot")
                || message.contains("snapshot identity/readonly/generation"),
            "{message}"
        );
        assert_spools_removed(scratch.path());
        // The immutable manifest exists and is valid, but remained orphaned:
        // build_snapshot returned no ref and made no workspace head publication.
        let reference = state.manifest.lock().unwrap().clone().unwrap();
        let client = ObjectClient::new(LocalFsBackend::new(objects.path()));
        let snapshot = AuthenticatedV3Snapshot::open(&client, &reference)
            .await
            .unwrap();
        let reader = V3IndexReader::new(client.clone(), 0);
        let frozen = entry(&snapshot, &client, &reader, [2; 32], b"frozen").await;
        let mut payload = vec![0u8; frozen.size as usize];
        snapshot
            .read_inode_range(
                &client,
                &reader,
                frozen.inode,
                0,
                &mut payload,
                32 * 1024 * 1024,
            )
            .await
            .unwrap();
        assert_eq!(payload, b"before-source-mutation");
        drop(restore);
        // Verify successful restoration through the same actual provider.
        assert!(V3SourceRoot::open(&readonly, V3SourceConsistency::SnapshotBacked).is_ok());
    }

    async fn entry(
        snapshot: &AuthenticatedV3Snapshot,
        client: &ObjectClient<LocalFsBackend>,
        reader: &V3IndexReader<LocalFsBackend>,
        parent: [u8; 32],
        name: &[u8],
    ) -> GroupMetaEntry {
        snapshot
            .lookup_dentry(client, reader, parent, name, 32 * 1024 * 1024)
            .await
            .unwrap()
            .unwrap()
    }
    fn set_xattr(path: &Path, name: &[u8], value: &[u8]) {
        let path = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
        let name = std::ffi::CString::new(name).unwrap();
        // SAFETY: both strings are terminated and value is a live byte slice.
        assert_eq!(
            unsafe {
                libc::lsetxattr(
                    path.as_ptr(),
                    name.as_ptr(),
                    value.as_ptr().cast(),
                    value.len(),
                    0,
                )
            },
            0
        );
    }
    fn assert_spools_removed(path: &Path) {
        assert!(
            !std::fs::read_dir(path)
                .unwrap()
                .filter_map(Result::ok)
                .any(|e| e
                    .file_name()
                    .to_string_lossy()
                    .starts_with("brewfs-wire005-index-"))
        );
    }

    #[tokio::test]
    async fn cancelling_inventory_removes_only_its_owned_spool() {
        let source = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        std::fs::write(scratch.path().join("user-file"), b"preserve").unwrap();
        for i in 0..100 {
            std::fs::write(source.path().join(format!("f{i}")), []).unwrap();
        }
        let source_path = source.path().to_owned();
        let scratch_path = scratch.path().to_owned();
        let task = tokio::spawn(async move {
            V3SourceNamespaceInventory::capture(
                &source_path,
                &scratch_path,
                inventory_options(V3SourceHardlinkPolicy::VisibleLinks),
            )
            .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if std::fs::read_dir(scratch.path())
                    .unwrap()
                    .filter_map(Result::ok)
                    .any(|e| {
                        e.file_name()
                            .to_string_lossy()
                            .starts_with("brewfs-wire005-index-")
                    })
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        task.abort();
        assert!(matches!(task.await, Err(error) if error.is_cancelled()));
        assert_spools_removed(scratch.path());
        assert_eq!(
            std::fs::read(scratch.path().join("user-file")).unwrap(),
            b"preserve"
        );
    }

    #[test]
    fn copack_never_doubles_an_entry_past_the_existing_extent_limit() {
        let mut group = NamespaceGroup::default();
        let mut entry = hot(&std::fs::metadata(file!()).unwrap()).unwrap();
        entry.kind = 1;
        entry.mode = 0o100640;
        entry.size = 200 * 8192;
        entry.extents = (0..200)
            .map(|ordinal| GroupMetaExtent {
                file_offset: u64::from(ordinal) * 8192,
                logical_len: 8192,
                frame_ordinal: ordinal,
                raw_offset: 0,
                raw_len: 8192,
            })
            .collect();
        let frames = (0..200)
            .map(|_| PackedFrameInput {
                raw: vec![8; 8192],
                size_class: crate::workspace_overlay::packed_v3::SizeClass::Small,
                codec: 0,
                first_file_slot: 0,
                last_file_slot: 0,
            })
            .collect();
        group
            .push(7, entry, frames, &options(PackedCodec::Raw))
            .unwrap();
        assert_eq!(group.entries[0].extents.len(), 200);
    }

    #[tokio::test]
    async fn external_namespace_routes_dense_holes_many_extents_and_shared_hardlinks() {
        use std::os::unix::fs::FileExt;
        let source = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        // Force small groups to publish before the first external selector,
        // exercising bounded migration of previously published regular inodes.
        for index in 0..520 {
            std::fs::write(
                source.path().join(format!("a{index:03}")),
                [index as u8; 73],
            )
            .unwrap();
        }
        let dense_path = source.path().join("zz-dense");
        let mut dense = std::fs::File::create(&dense_path).unwrap();
        for i in 0..20u8 {
            dense.write_all(&vec![i; 1024 * 1024]).unwrap();
        }
        dense.sync_all().unwrap();
        std::fs::hard_link(&dense_path, source.path().join("zz-dense-alias")).unwrap();
        let hole_path = source.path().join("zz-hole");
        std::fs::File::create(&hole_path)
            .unwrap()
            .set_len(72 * 1024 * 1024)
            .unwrap();
        let sparse_path = source.path().join("zz-sparse");
        let mut sparse = std::fs::File::create(&sparse_path).unwrap();
        for i in 0..300u64 {
            sparse.seek(SeekFrom::Start(i * 8192)).unwrap();
            sparse.write_all(&[i as u8; 4096]).unwrap();
        }
        sparse.set_len(4 * 1024 * 1024).unwrap();
        sparse.sync_all().unwrap();
        for codec in [PackedCodec::Raw, PackedCodec::Zstd] {
            let objects = tempfile::tempdir().unwrap();
            let client = ObjectClient::new(LocalFsBackend::new(objects.path()));
            let built = inventory(source.path(), scratch.path())
                .await
                .build_snapshot(client.clone(), "snapshot".into(), options(codec))
                .await
                .unwrap();
            assert_eq!(built.report.external_files, 3);
            assert_eq!(built.report.external_chunks, 3); // 2 dense + 1 sparse; hole has no chunks
            assert_eq!(built.report.external_frames, 305);
            let snapshot = AuthenticatedV3Snapshot::open(&client, &built.reference)
                .await
                .unwrap();
            assert!(
                snapshot
                    .manifest()
                    .source
                    .as_ref()
                    .unwrap()
                    .placement_contract
            );
            let reader = V3IndexReader::new(client.clone(), 0);
            let dense_entry = entry(&snapshot, &client, &reader, [2; 32], b"zz-dense").await;
            let alias = entry(&snapshot, &client, &reader, [2; 32], b"zz-dense-alias").await;
            assert_eq!(alias.inode, dense_entry.inode);
            assert_eq!(alias.nlink, 2);
            let small = entry(&snapshot, &client, &reader, [2; 32], b"a000").await;
            assert!(matches!(
                snapshot
                    .placement(&reader, small.inode, small.size)
                    .await
                    .unwrap(),
                Some(super::super::V3Placement::Group { .. })
            ));
            let sparse_entry = entry(&snapshot, &client, &reader, [2; 32], b"zz-sparse").await;
            let hole_entry = entry(&snapshot, &client, &reader, [2; 32], b"zz-hole").await;
            for (path, inode, ranges) in [
                (
                    &dense_path,
                    alias.inode,
                    vec![
                        (1, 8193),
                        (4 * 1024 * 1024 - 11, 29),
                        (16 * 1024 * 1024 - 17, 65536),
                        (20 * 1024 * 1024 - 4096, 4096),
                    ],
                ),
                (
                    &sparse_path,
                    sparse_entry.inode,
                    vec![
                        (1, 31),
                        (255 * 8192 + 3, 20000),
                        (299 * 8192 + 2048, 8192),
                        (3 * 1024 * 1024, 8192),
                    ],
                ),
                (
                    &hole_path,
                    hole_entry.inode,
                    vec![(65 * 1024 * 1024 + 3, 8192)],
                ),
            ] {
                let file = std::fs::File::open(path).unwrap();
                for (offset, length) in ranges {
                    let mut expected = vec![0; length];
                    file.read_exact_at(&mut expected, offset).unwrap();
                    let mut actual = vec![255; length];
                    snapshot
                        .read_inode_range(
                            &client,
                            &reader,
                            inode,
                            offset,
                            &mut actual,
                            32 * 1024 * 1024,
                        )
                        .await
                        .unwrap();
                    assert_eq!(actual, expected, "offset={offset} codec={codec:?}");
                }
                assert_eq!(
                    snapshot.source_blocks(&reader, inode).await.unwrap(),
                    Some(path.metadata().unwrap().blocks())
                );
            }
            let mut untouched = [123; 1024];
            assert!(
                snapshot
                    .read_inode_range(&client, &reader, alias.inode, 4096, &mut untouched, 1024)
                    .await
                    .is_err()
            );
            assert_eq!(untouched, [123; 1024]);
            use crate::meta::MetaLayer;
            let meta = crate::workspace_overlay::packed_v3::PackedV3ReadonlyMeta::from_v3(
                client,
                snapshot,
                32 * 1024 * 1024,
                0,
            );
            assert!(
                meta.get_slices(crate::vfs::chunk_id_for(alias.inode as i64, 0).unwrap())
                    .await
                    .is_err()
            );
        }
        assert_eq!(std::fs::read_dir(scratch.path()).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn complete_namespace_preserves_raw_paths_hardlinks_specials_sparse_and_cold() {
        let source = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        std::fs::create_dir(source.path().join("deep")).unwrap();
        std::fs::create_dir(source.path().join("deep/child")).unwrap();
        let raw_name = b"raw-\xff";
        let raw_path = source
            .path()
            .join(std::ffi::OsString::from_vec(raw_name.to_vec()));
        let data: Vec<_> = (0..300 * 1024).map(|i| (i * 37 % 251) as u8).collect();
        std::fs::write(&raw_path, &data).unwrap();
        std::fs::hard_link(&raw_path, source.path().join("deep/alias")).unwrap();
        std::fs::hard_link(&raw_path, scratch.path().join("outside-alias")).unwrap();
        set_xattr(source.path(), b"user.root", b"\x00\xffroot");
        set_xattr(&raw_path, b"user.binary", b"\x00\xffvalue");
        set_xattr(&source.path().join("deep"), b"user.directory", b"dir");
        let target = std::ffi::OsString::from_vec(b"../outside-\xff".to_vec());
        std::os::unix::fs::symlink(target, source.path().join("link")).unwrap();
        let fifo =
            std::ffi::CString::new(source.path().join("fifo").as_os_str().as_bytes()).unwrap();
        // SAFETY: FIFO pathname is terminated; no data descriptor is opened.
        assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o620) }, 0);
        let _socket = UnixListener::bind(source.path().join("socket")).unwrap();
        std::fs::write(source.path().join("empty"), []).unwrap();
        std::fs::File::create(source.path().join("hole"))
            .unwrap()
            .set_len(1024 * 1024)
            .unwrap();
        let sparse_path = source.path().join("deep/child/sparse");
        let mut sparse = std::fs::File::create(&sparse_path).unwrap();
        sparse.set_len(8 * 1024 * 1024).unwrap();
        sparse.seek(SeekFrom::Start(4096)).unwrap();
        sparse.write_all(b"begin").unwrap();
        sparse.seek(SeekFrom::Start(6 * 1024 * 1024)).unwrap();
        sparse.write_all(b"end").unwrap();
        sparse.sync_all().unwrap();
        let expected_sparse = std::fs::read(&sparse_path).unwrap();
        for codec in [PackedCodec::Raw, PackedCodec::Zstd] {
            let objects = tempfile::tempdir().unwrap();
            let client = ObjectClient::new(LocalFsBackend::new(objects.path()));
            let captured = inventory(source.path(), scratch.path()).await;
            let root = captured.root_capture.attributes().clone();
            let built = captured
                .build_snapshot(client.clone(), "namespace".into(), options(codec))
                .await
                .unwrap();
            assert_eq!(built.report.source_entries, 10);
            assert_eq!(built.report.unique_inodes, 10); // root included; the alias is one inode
            assert_eq!(built.report.regular_dentries, 5);
            assert_eq!(built.report.groups, 3);
            let snapshot = AuthenticatedV3Snapshot::open(&client, &built.reference)
                .await
                .unwrap();
            assert_eq!(snapshot.manifest().source.as_ref().unwrap().root, root);
            let reader = V3IndexReader::new(client.clone(), 0);
            let raw = entry(&snapshot, &client, &reader, [2; 32], raw_name).await;
            let directory = entry(&snapshot, &client, &reader, [2; 32], b"deep").await;
            let directory_key = directory_key([1; 32], directory.inode);
            let alias = entry(&snapshot, &client, &reader, directory_key, b"alias").await;
            assert_eq!(raw.inode, alias.inode);
            assert_eq!(raw.nlink, 2);
            assert_eq!(alias.nlink, 2);
            assert_ne!(raw.inode, 7);
            let mut output = vec![0; data.len()];
            snapshot
                .read_inode_range(
                    &client,
                    &reader,
                    alias.inode,
                    0,
                    &mut output,
                    32 * 1024 * 1024,
                )
                .await
                .unwrap();
            assert_eq!(output, data);
            assert_eq!(
                snapshot.source_blocks(&reader, raw.inode).await.unwrap(),
                Some(raw_path.metadata().unwrap().blocks())
            );
            let cold = snapshot
                .cold_attributes(&client, &reader, raw.inode)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(cold.xattrs[0].value, b"\x00\xffvalue");
            let link = entry(&snapshot, &client, &reader, [2; 32], b"link").await;
            assert_eq!(link.kind, 3);
            assert_eq!(
                snapshot
                    .cold_attributes(&client, &reader, link.inode)
                    .await
                    .unwrap()
                    .unwrap()
                    .symlink_target,
                Some(b"../outside-\xff".to_vec())
            );
            for (name, kind) in [(b"fifo".as_slice(), 4), (b"socket".as_slice(), 5)] {
                assert_eq!(
                    entry(&snapshot, &client, &reader, [2; 32], name).await.kind,
                    kind
                );
            }
            let child = entry(&snapshot, &client, &reader, directory_key, b"child").await;
            let sparse = entry(
                &snapshot,
                &client,
                &reader,
                directory_key_fn(child.inode),
                b"sparse",
            )
            .await;
            for (offset, count) in [
                (0, 8192),
                (6 * 1024 * 1024 - 100, 8192),
                (8 * 1024 * 1024 - 4096, 4096),
            ] {
                let mut output = vec![1; count];
                snapshot
                    .read_inode_range(
                        &client,
                        &reader,
                        sparse.inode,
                        offset as u64,
                        &mut output,
                        32 * 1024 * 1024,
                    )
                    .await
                    .unwrap();
                assert_eq!(output, expected_sparse[offset..offset + count]);
            }
            for name in [b"empty".as_slice(), b"hole".as_slice()] {
                let file = entry(&snapshot, &client, &reader, [2; 32], name).await;
                assert!(file.extents.is_empty());
                if file.size != 0 {
                    let mut output = vec![1; 4096];
                    snapshot
                        .read_inode_range(
                            &client,
                            &reader,
                            file.inode,
                            100,
                            &mut output,
                            32 * 1024 * 1024,
                        )
                        .await
                        .unwrap();
                    assert_eq!(output, vec![0; 4096]);
                }
            }
            assert_spools_removed(scratch.path());
        }
        assert!(
            V3SourceNamespaceInventory::capture(
                source.path(),
                scratch.path(),
                inventory_options(V3SourceHardlinkPolicy::RejectExternal)
            )
            .await
            .is_err()
        );
        assert_spools_removed(scratch.path());
    }
    fn directory_key_fn(inode: u64) -> [u8; 32] {
        directory_key([1; 32], inode)
    }

    #[tokio::test]
    async fn namespace_fences_reject_payload_xattr_rename_and_path_replacement() {
        for mutation in 0..5 {
            let source = tempfile::tempdir().unwrap();
            let scratch = tempfile::tempdir().unwrap();
            let path = source.path().join("file");
            std::fs::write(&path, b"before").unwrap();
            let captured = inventory(source.path(), scratch.path()).await;
            match mutation {
                0 => std::fs::write(&path, b"after!").unwrap(),
                1 => set_xattr(&path, b"user.changed", b"value"),
                2 => std::fs::rename(&path, source.path().join("renamed")).unwrap(),
                3 => {
                    std::fs::remove_file(&path).unwrap();
                    std::fs::write(&path, b"before").unwrap();
                }
                _ => {
                    std::os::unix::fs::symlink("/dev/null", source.path().join("new-link")).unwrap()
                }
            }
            assert!(captured.validate_unchanged().await.is_err());
            let objects = tempfile::tempdir().unwrap();
            let client = ObjectClient::new(LocalFsBackend::new(objects.path()));
            assert!(
                captured
                    .build_snapshot(client, "namespace".into(), options(PackedCodec::Raw))
                    .await
                    .is_err()
            );
            assert_eq!(std::fs::read_dir(objects.path()).unwrap().count(), 0);
            assert_spools_removed(scratch.path());
        }
    }

    #[tokio::test]
    async fn namespace_empty_tree_validates_limits_and_owned_spool_cleanup() {
        let source = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        let mut invalid_options = inventory_options(V3SourceHardlinkPolicy::VisibleLinks);
        invalid_options.file_limits.max_extents = 0;
        assert!(
            V3SourceNamespaceInventory::capture(source.path(), scratch.path(), invalid_options)
                .await
                .is_err()
        );
        assert!(
            V3SourceNamespaceInventory::capture(
                source.path(),
                source.path(),
                inventory_options(V3SourceHardlinkPolicy::VisibleLinks)
            )
            .await
            .is_err()
        );
        let captured = inventory(source.path(), scratch.path()).await;
        drop(captured);
        assert_spools_removed(scratch.path());
        let captured = inventory(source.path(), scratch.path()).await;
        let objects = tempfile::tempdir().unwrap();
        let client = ObjectClient::new(LocalFsBackend::new(objects.path()));
        let built = captured
            .build_snapshot(
                client.clone(),
                "namespace".into(),
                options(PackedCodec::Raw),
            )
            .await
            .unwrap();
        assert_eq!(built.report.source_entries, 0);
        assert_eq!(built.report.unique_inodes, 1);
        assert_eq!(built.report.groups, 0);
        let snapshot = AuthenticatedV3Snapshot::open(&client, &built.reference)
            .await
            .unwrap();
        let reader = V3IndexReader::new(client.clone(), 0);
        assert!(
            snapshot
                .readdir_page(&client, &reader, [2; 32], 0, 50, 1024 * 1024)
                .await
                .unwrap()
                .is_empty()
        );
        assert_spools_removed(scratch.path());
    }

    #[tokio::test]
    async fn namespace_multigroup_pagination_orders_raw_names_and_inode_assignment_is_stable() {
        let source = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        for i in (0..520).rev() {
            std::fs::write(source.path().join(format!("f{i:04}")), []).unwrap();
        }
        let first = inventory(source.path(), scratch.path()).await;
        let second = inventory(source.path(), scratch.path()).await;
        // Atime is captured per inventory and can change after read_dir; stable
        // inode assignment must not depend on traversal/insertion order.
        let select = "SELECT master_path,snapshot_inode FROM source_inodes ORDER BY master_path";
        let a = sea_orm::sqlx::query(select)
            .fetch_all(&first.spool.pool)
            .await
            .unwrap();
        let b = sea_orm::sqlx::query(select)
            .fetch_all(&second.spool.pool)
            .await
            .unwrap();
        assert_eq!(
            a.iter()
                .map(|r| (r.get::<Vec<u8>, _>(0), r.get::<i64, _>(1)))
                .collect::<Vec<_>>(),
            b.iter()
                .map(|r| (r.get::<Vec<u8>, _>(0), r.get::<i64, _>(1)))
                .collect::<Vec<_>>()
        );
        drop(second);
        let objects = tempfile::tempdir().unwrap();
        let client = ObjectClient::new(LocalFsBackend::new(objects.path()));
        let built = first
            .build_snapshot(
                client.clone(),
                "namespace".into(),
                options(PackedCodec::Zstd),
            )
            .await
            .unwrap();
        assert_eq!(built.report.groups, 2);
        let snapshot = AuthenticatedV3Snapshot::open(&client, &built.reference)
            .await
            .unwrap();
        let reader = V3IndexReader::new(client.clone(), 0);
        let mut names = vec![];
        loop {
            let page = snapshot
                .readdir_page(
                    &client,
                    &reader,
                    [2; 32],
                    names.len() as u64,
                    37,
                    1024 * 1024,
                )
                .await
                .unwrap();
            if page.is_empty() {
                break;
            }
            names.extend(page.into_iter().map(|e| e.name));
        }
        assert_eq!(
            names,
            (0..520)
                .map(|i| format!("f{i:04}").into_bytes())
                .collect::<Vec<_>>()
        );
        assert_spools_removed(scratch.path());
    }

    #[tokio::test]
    async fn namespace_copack_fills_partial_frame_and_preserves_split_ranges() {
        let source = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        for i in 0..3 {
            std::fs::write(
                source.path().join(format!("f{i}")),
                vec![i as u8 + 1; 300 * 1024],
            )
            .unwrap();
        }
        std::fs::write(source.path().join("tiny"), vec![9; 1000]).unwrap();
        for codec in [PackedCodec::Raw, PackedCodec::Zstd] {
            let objects = tempfile::tempdir().unwrap();
            let client = ObjectClient::new(LocalFsBackend::new(objects.path()));
            let built = inventory(source.path(), scratch.path())
                .await
                .build_snapshot(client.clone(), "namespace".into(), options(codec))
                .await
                .unwrap();
            assert_eq!(
                built.report.frames, 2,
                "same-class files must fill the shared 512KiB tail"
            );
            assert_eq!(built.report.inline_payload_bytes, 1000);
            let snapshot = AuthenticatedV3Snapshot::open(&client, &built.reference)
                .await
                .unwrap();
            let reader = V3IndexReader::new(client.clone(), 0);
            for i in 0..3 {
                let file = entry(
                    &snapshot,
                    &client,
                    &reader,
                    [2; 32],
                    format!("f{i}").as_bytes(),
                )
                .await;
                let mut output = vec![0; 300 * 1024];
                snapshot
                    .read_inode_range(
                        &client,
                        &reader,
                        file.inode,
                        0,
                        &mut output,
                        32 * 1024 * 1024,
                    )
                    .await
                    .unwrap();
                assert_eq!(output, vec![i as u8 + 1; 300 * 1024]);
            }
        }
        assert_spools_removed(scratch.path());
    }
}
