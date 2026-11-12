//! Disk-backed producer sorting; unordered source traversal never builds a
//! namespace-sized Vec. Only bounded pages are materialized for index upload.

use super::{
    SnapshotIndexBuilder, SnapshotIndexRecord, SnapshotIndexValue, SnapshotObjectRef,
    SnapshotRootKind,
};
use crate::cadapter::client::{ObjectBackend, ObjectClient};
use crate::workspace::packed::wire::{PackedResult, PackedWireError};
use sea_orm::sqlx::{
    Row, Sqlite, SqliteConnection, SqlitePool, Transaction,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

// Private spool tag, distinct from the seven PM07 roots. It is not a public
// SnapshotRootKind and cannot index the fixed legacy manifest root array.
const SOURCE_STATS_ROOT: i64 = 7;

/// Limits writer work per private-spool transaction, independent of group size.
pub(crate) const SNAPSHOT_SPOOL_BATCH_ENTRIES: usize = 128;

/// An owned SQLx transaction rolls back on error/cancellation. Only private,
/// rebuildable index records are batched; this does not publish a manifest or
/// weaken object verification, source fences, or the durable workspace head.
pub(crate) struct SnapshotIndexBatch<'a> {
    transaction: Transaction<'a, Sqlite>,
}

impl SnapshotIndexBatch<'_> {
    pub(crate) async fn insert(
        &mut self,
        root: SnapshotRootKind,
        record: &SnapshotIndexRecord,
    ) -> PackedResult<()> {
        SnapshotIndexSpool::insert_for_on(
            &mut self.transaction,
            root as i64,
            root.object_kind(),
            record,
        )
        .await
    }

    pub(crate) async fn get(
        &mut self,
        root: SnapshotRootKind,
        key: &[u8],
    ) -> PackedResult<Option<Vec<u8>>> {
        SnapshotIndexSpool::get_for_on(&mut self.transaction, root as i64, key).await
    }

    pub(crate) async fn claim_inode(
        &mut self,
        inode: u64,
        kind: u8,
        nlink: u32,
        signature: [u8; 32],
    ) -> PackedResult<bool> {
        SnapshotIndexSpool::claim_inode_on(&mut self.transaction, inode, kind, nlink, signature)
            .await
    }

    pub(crate) async fn replace(
        &mut self,
        root: SnapshotRootKind,
        record: &SnapshotIndexRecord,
    ) -> PackedResult<()> {
        SnapshotIndexSpool::replace_on(&mut self.transaction, root, record).await
    }

    pub(crate) async fn commit(self) -> PackedResult<()> {
        self.transaction
            .commit()
            .await
            .map_err(|_| PackedWireError::Backend("packed spool batch commit failed".into()))
    }
}

struct PrivateSpoolDirectory {
    path: PathBuf,
}

impl Drop for PrivateSpoolDirectory {
    fn drop(&mut self) {
        // The directory was exclusively created by this invocation and contains
        // only its SQLite files. Keeping a guard before the first await also
        // cleans up a cancelled connection/schema initialization.
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

pub struct SnapshotIndexSpool {
    pub(super) pool: SqlitePool,
    path: PathBuf,
    _directory: Option<PrivateSpoolDirectory>,
    native_owner: Option<Arc<super::SnapshotOwnedPermit>>,
}

impl Drop for SnapshotIndexSpool {
    fn drop(&mut self) {
        let Some(owner) = self.native_owner.take() else {
            return;
        };
        // Native producer cancellation/runtime shutdown must retain its cache,
        // worker, scratch directory and permits until SQLx acknowledges close.
        // SQLite close uses worker channels and does not require Tokio timers.
        let cleanup = Arc::new(Mutex::new(Some((
            self.pool.clone(),
            self._directory.take(),
            owner,
        ))));
        let worker_cleanup = cleanup.clone();
        if std::thread::Builder::new()
            .name("brewfs-v3-native-spool-close".into())
            .stack_size(128 << 10)
            .spawn(move || {
                if let Some((pool, directory, owner)) = worker_cleanup.lock().unwrap().take() {
                    futures::executor::block_on(pool.close());
                    drop(directory);
                    drop(owner);
                }
            })
            .is_err()
            && let Some(retained) = cleanup.lock().unwrap().take()
        {
            // No shutdown proof exists when OS thread creation fails.
            std::mem::forget(retained);
        }
    }
}

impl SnapshotIndexSpool {
    pub(crate) async fn batch(&self) -> PackedResult<SnapshotIndexBatch<'_>> {
        Ok(SnapshotIndexBatch {
            transaction: self.pool.begin().await.map_err(|_| {
                PackedWireError::Backend("packed spool batch transaction failed".into())
            })?,
        })
    }
    pub async fn create(directory: &Path) -> PackedResult<Self> {
        Self::create_inner(directory, None, None).await
    }

    pub(crate) async fn create_native(
        directory: &Path,
        owner: Arc<super::SnapshotOwnedPermit>,
        max_disk_bytes: u64,
    ) -> PackedResult<Self> {
        if max_disk_bytes < 16 << 10 || max_disk_bytes / 4096 >= u64::from(u32::MAX) {
            return Err(PackedWireError::LimitExceeded(
                "native spool disk quota".into(),
            ));
        }
        Self::create_inner(directory, Some(owner), Some(max_disk_bytes)).await
    }

    async fn create_inner(
        directory: &Path,
        native_owner: Option<Arc<super::SnapshotOwnedPermit>>,
        max_disk_bytes: Option<u64>,
    ) -> PackedResult<Self> {
        let directory_path =
            directory.join(format!("brewfs-snapshot-index-{}", uuid::Uuid::new_v4()));
        let mut directory_builder = std::fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            directory_builder.mode(0o700);
        }
        directory_builder.create(&directory_path).map_err(|_| {
            PackedWireError::Backend("cannot create private packed spool directory".into())
        })?;
        let owned_directory = PrivateSpoolDirectory {
            path: directory_path,
        };
        let path = owned_directory.path.join("index.sqlite");
        let mut open = std::fs::OpenOptions::new();
        open.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            open.mode(0o600);
        }
        open.open(&path).map_err(|_| {
            PackedWireError::Backend("cannot create private packed index spool".into())
        })?;
        let mut options = SqliteConnectOptions::new()
            .filename(&path)
            .in_memory(false)
            .create_if_missing(false)
            .optimize_on_close(false, None);
        if let Some(maximum) = max_disk_bytes {
            options = options
                .pragma("page_size", "4096")
                .pragma("max_page_count", (maximum / 4096).to_string())
                .pragma("mmap_size", "0")
                .pragma("journal_mode", "OFF");
        }
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .map_err(|_| {
                PackedWireError::Backend("cannot open private packed index spool".into())
            })?;
        let spool = Self {
            pool,
            path,
            _directory: Some(owned_directory),
            native_owner,
        };
        let result=async {
            let pool=spool.pool.clone();
            sea_orm::sqlx::query("PRAGMA cache_size=-2048").execute(&pool).await.map_err(|_|PackedWireError::Backend("cannot bound packed spool cache".into()))?;
            sea_orm::sqlx::query("PRAGMA temp_store=FILE").execute(&pool).await.map_err(|_|PackedWireError::Backend("cannot configure disk-backed packed sorting".into()))?;
            sea_orm::sqlx::query("CREATE TABLE records (root INTEGER NOT NULL, first_key BLOB NOT NULL, last_key BLOB NOT NULL, value BLOB NOT NULL, PRIMARY KEY(root, first_key)) WITHOUT ROWID").execute(&pool).await.map_err(|_|PackedWireError::Backend("cannot initialize packed index spool".into()))?;
            sea_orm::sqlx::query("CREATE TABLE inode_identities (inode BLOB PRIMARY KEY, signature BLOB NOT NULL, kind INTEGER NOT NULL, expected_links INTEGER NOT NULL, observed_links INTEGER NOT NULL) WITHOUT ROWID").execute(&pool).await.map_err(|_|PackedWireError::Backend("cannot initialize packed inode identities".into()))?;
            Ok::<_,PackedWireError>(())
        }.await;
        match result {
            Ok(()) => Ok(spool),
            Err(error) => {
                // Keep the native owner and private directory through the actual
                // worker shutdown even if schema/PRAGMA initialization fails.
                spool.pool.close().await;
                Err(error)
            }
        }
    }

    pub async fn insert(
        &self,
        root: SnapshotRootKind,
        record: &SnapshotIndexRecord,
    ) -> PackedResult<()> {
        self.insert_for(root as i64, root.object_kind(), record)
            .await
    }

    async fn insert_for(
        &self,
        root: i64,
        kind: super::SnapshotObjectKind,
        record: &SnapshotIndexRecord,
    ) -> PackedResult<()> {
        let mut connection = self
            .pool
            .acquire()
            .await
            .map_err(|_| PackedWireError::Backend("packed spool connection failed".into()))?;
        Self::insert_for_on(&mut connection, root, kind, record).await
    }

    async fn insert_for_on(
        connection: &mut SqliteConnection,
        root: i64,
        kind: super::SnapshotObjectKind,
        record: &SnapshotIndexRecord,
    ) -> PackedResult<()> {
        let SnapshotIndexValue::Leaf(value) = &record.value else {
            return Err(PackedWireError::Invalid(
                "packed spool requires leaf records".into(),
            ));
        };
        super::SnapshotIndexPage {
            kind,
            height: 0,
            records: vec![record.clone()],
        }
        .encode()?;
        sea_orm::sqlx::query(
            "INSERT INTO records(root, first_key, last_key, value) VALUES(?, ?, ?, ?)",
        )
        .bind(root)
        .bind(&record.first_key)
        .bind(&record.last_key)
        .bind(value)
        .execute(connection)
        .await
        .map_err(|_| PackedWireError::Invalid("duplicate or invalid packed spool record".into()))?;
        Ok(())
    }

    pub async fn get(&self, root: SnapshotRootKind, key: &[u8]) -> PackedResult<Option<Vec<u8>>> {
        self.get_for(root as i64, key).await
    }

    async fn get_for(&self, root: i64, key: &[u8]) -> PackedResult<Option<Vec<u8>>> {
        let mut connection = self
            .pool
            .acquire()
            .await
            .map_err(|_| PackedWireError::Backend("packed spool connection failed".into()))?;
        Self::get_for_on(&mut connection, root, key).await
    }

    async fn get_for_on(
        connection: &mut SqliteConnection,
        root: i64,
        key: &[u8],
    ) -> PackedResult<Option<Vec<u8>>> {
        let row = sea_orm::sqlx::query("SELECT value FROM records WHERE root=? AND first_key=?")
            .bind(root)
            .bind(key)
            .fetch_optional(connection)
            .await
            .map_err(|_| PackedWireError::Backend("packed spool lookup failed".into()))?;
        row.map(|row| {
            row.try_get::<Vec<u8>, _>(0)
                .map_err(|_| PackedWireError::Invalid("invalid packed spool value".into()))
        })
        .transpose()
    }

    pub(crate) async fn set_allocations(&self, values: &[(u64, u64)]) -> PackedResult<()> {
        for values in values.chunks(SNAPSHOT_SPOOL_BATCH_ENTRIES) {
            let mut batch = self.batch().await?;
            for &(inode, blocks) in values {
                Self::set_allocation_on(&mut batch.transaction, inode, blocks).await?;
            }
            batch.commit().await?;
        }
        Ok(())
    }

    async fn set_allocation_on(
        connection: &mut SqliteConnection,
        inode: u64,
        blocks: u64,
    ) -> PackedResult<()> {
        let value = super::source_stat::encode_allocation(inode, blocks)?;
        let key = inode.to_be_bytes().to_vec();
        if let Some(existing) = Self::get_for_on(connection, SOURCE_STATS_ROOT, &key).await? {
            if existing == value {
                return Ok(());
            }
            return Err(PackedWireError::Invalid(
                "hardlink aliases disagree on allocated blocks".into(),
            ));
        }
        Self::insert_for_on(
            connection,
            SOURCE_STATS_ROOT,
            super::SnapshotObjectKind::SourceStatsIndex,
            &SnapshotIndexRecord {
                first_key: key.clone(),
                last_key: key,
                value: SnapshotIndexValue::Leaf(value),
            },
        )
        .await
    }

    pub(crate) async fn validate_source_closure(&self, required: bool) -> PackedResult<()> {
        let invalid: Option<Vec<u8>> = if required {
            sea_orm::sqlx::query_scalar("SELECT i.inode FROM inode_identities i LEFT JOIN records r ON r.root=? AND r.first_key=i.inode WHERE r.first_key IS NULL UNION ALL SELECT r.first_key FROM records r LEFT JOIN inode_identities i ON i.inode=r.first_key WHERE r.root=? AND i.inode IS NULL LIMIT 1")
                .bind(SOURCE_STATS_ROOT).bind(SOURCE_STATS_ROOT).fetch_optional(&self.pool).await
        } else {
            sea_orm::sqlx::query_scalar("SELECT first_key FROM records WHERE root=? LIMIT 1")
                .bind(SOURCE_STATS_ROOT).fetch_optional(&self.pool).await
        }.map_err(|_| PackedWireError::Backend("packed source allocation closure validation failed".into()))?;
        if invalid.is_some() {
            return Err(PackedWireError::Invalid(
                "source allocations are incomplete, orphaned or lack PM08 root attributes".into(),
            ));
        }
        Ok(())
    }

    pub(crate) async fn enable_placements(&self) -> PackedResult<()> {
        let mut after: Option<Vec<u8>> = None;
        loop {
            let row = if let Some(key) = &after {
                sea_orm::sqlx::query("SELECT r.first_key,r.value FROM records r JOIN inode_identities i ON i.inode=r.first_key WHERE r.root=? AND i.kind=1 AND r.first_key>? ORDER BY r.first_key LIMIT 1")
                    .bind(SnapshotRootKind::Inodes as i64).bind(key).fetch_optional(&self.pool).await
            } else {
                sea_orm::sqlx::query("SELECT r.first_key,r.value FROM records r JOIN inode_identities i ON i.inode=r.first_key WHERE r.root=? AND i.kind=1 ORDER BY r.first_key LIMIT 1")
                    .bind(SnapshotRootKind::Inodes as i64).fetch_optional(&self.pool).await
            }.map_err(|_| PackedWireError::Backend("placement migration scan failed".into()))?;
            let Some(row) = row else {
                break;
            };
            let key: Vec<u8> = row.try_get(0).map_err(|_| {
                PackedWireError::Invalid("placement migration key is corrupt".into())
            })?;
            let value: Vec<u8> = row.try_get(1).map_err(|_| {
                PackedWireError::Invalid("placement migration locator is corrupt".into())
            })?;
            let location = super::SnapshotInodeLocation::decode_value(&value)?;
            self.insert(
                SnapshotRootKind::LargePlacements,
                &SnapshotIndexRecord {
                    first_key: key.clone(),
                    last_key: key.clone(),
                    value: SnapshotIndexValue::Leaf(
                        super::SnapshotPlacement::Group {
                            inode: location.hot.inode,
                            size: location.hot.size,
                        }
                        .encode()?,
                    ),
                },
            )
            .await?;
            after = Some(key);
        }
        Ok(())
    }

    pub(crate) async fn validate_placement_closure(&self, required: bool) -> PackedResult<()> {
        let root = SnapshotRootKind::LargePlacements as i64;
        let invalid: Option<Vec<u8>> = if required {
            sea_orm::sqlx::query_scalar("SELECT i.inode FROM inode_identities i LEFT JOIN records r ON r.root=? AND r.first_key=i.inode WHERE i.kind=1 AND r.first_key IS NULL UNION ALL SELECT r.first_key FROM records r LEFT JOIN inode_identities i ON i.inode=r.first_key WHERE r.root=? AND (i.inode IS NULL OR i.kind<>1) LIMIT 1")
                .bind(root).bind(root).fetch_optional(&self.pool).await
        } else {
            sea_orm::sqlx::query_scalar("SELECT first_key FROM records WHERE root=? LIMIT 1").bind(root).fetch_optional(&self.pool).await
        }.map_err(|_| PackedWireError::Backend("placement closure scan failed".into()))?;
        if invalid.is_some() {
            return Err(PackedWireError::Invalid(
                "required placements are missing, orphaned, nonregular or unversioned".into(),
            ));
        }
        if !required {
            return Ok(());
        }
        let mut after: Option<Vec<u8>> = None;
        loop {
            let row = if let Some(key) = &after {
                sea_orm::sqlx::query("SELECT p.first_key,p.last_key,p.value,i.value FROM records p JOIN records i ON i.root=? AND i.first_key=p.first_key WHERE p.root=? AND p.first_key>? ORDER BY p.first_key LIMIT 1")
                    .bind(SnapshotRootKind::Inodes as i64).bind(root).bind(key).fetch_optional(&self.pool).await
            } else {
                sea_orm::sqlx::query("SELECT p.first_key,p.last_key,p.value,i.value FROM records p JOIN records i ON i.root=? AND i.first_key=p.first_key WHERE p.root=? ORDER BY p.first_key LIMIT 1")
                    .bind(SnapshotRootKind::Inodes as i64).bind(root).fetch_optional(&self.pool).await
            }.map_err(|_| PackedWireError::Backend("placement identity scan failed".into()))?;
            let Some(row) = row else {
                break;
            };
            let key: Vec<u8> = row
                .try_get(0)
                .map_err(|_| PackedWireError::Invalid("placement key is corrupt".into()))?;
            let last: Vec<u8> = row
                .try_get(1)
                .map_err(|_| PackedWireError::Invalid("placement fence is corrupt".into()))?;
            let value: Vec<u8> = row
                .try_get(2)
                .map_err(|_| PackedWireError::Invalid("placement value is corrupt".into()))?;
            let location: Vec<u8> = row
                .try_get(3)
                .map_err(|_| PackedWireError::Invalid("placement locator is corrupt".into()))?;
            let location = super::SnapshotInodeLocation::decode_value(&location)?;
            if key != last || key != location.hot.inode.to_be_bytes() {
                return Err(PackedWireError::Invalid(
                    "placement key/fences disagree with inode".into(),
                ));
            }
            super::SnapshotPlacement::decode(&value, location.hot.inode, location.hot.size)?;
            after = Some(key);
        }
        Ok(())
    }

    pub async fn claim_inode(
        &self,
        inode: u64,
        kind: u8,
        nlink: u32,
        signature: [u8; 32],
    ) -> PackedResult<bool> {
        let mut batch = self.batch().await?;
        let first = batch.claim_inode(inode, kind, nlink, signature).await?;
        batch.commit().await?;
        Ok(first)
    }

    async fn claim_inode_on(
        connection: &mut SqliteConnection,
        inode: u64,
        kind: u8,
        nlink: u32,
        signature: [u8; 32],
    ) -> PackedResult<bool> {
        let key = inode.to_be_bytes().to_vec();
        let existing=sea_orm::sqlx::query("SELECT signature,kind,expected_links,observed_links FROM inode_identities WHERE inode=?").bind(&key).fetch_optional(&mut *connection).await.map_err(|_|PackedWireError::Backend("packed identity lookup failed".into()))?;
        let first = if let Some(row) = existing {
            let old: Vec<u8> = row.try_get(0).map_err(|_| {
                PackedWireError::Invalid("packed inode signature is corrupt".into())
            })?;
            let old_kind: i64 = row
                .try_get(1)
                .map_err(|_| PackedWireError::Invalid("packed inode kind is corrupt".into()))?;
            let links: i64 = row
                .try_get(2)
                .map_err(|_| PackedWireError::Invalid("packed inode nlink is corrupt".into()))?;
            let observed: i64 = row.try_get(3).map_err(|_| {
                PackedWireError::Invalid("packed inode link count is corrupt".into())
            })?;
            if old.as_slice() != signature
                || old_kind != i64::from(kind)
                || kind == 2
                || links != i64::from(nlink)
                || observed >= links
            {
                return Err(PackedWireError::Invalid(
                    "hardlink aliases disagree in content/attributes or exceed nlink".into(),
                ));
            }
            sea_orm::sqlx::query(
                "UPDATE inode_identities SET observed_links=observed_links+1 WHERE inode=?",
            )
            .bind(&key)
            .execute(&mut *connection)
            .await
            .map_err(|_| PackedWireError::Backend("packed link count update failed".into()))?;
            false
        } else {
            sea_orm::sqlx::query("INSERT INTO inode_identities(inode,signature,kind,expected_links,observed_links) VALUES(?,?,?,?,1)").bind(&key).bind(signature.to_vec()).bind(i64::from(kind)).bind(i64::from(nlink)).execute(&mut *connection).await.map_err(|_|PackedWireError::Backend("packed identity insert failed".into()))?;
            true
        };
        Ok(first)
    }

    pub async fn replace(
        &self,
        root: SnapshotRootKind,
        record: &SnapshotIndexRecord,
    ) -> PackedResult<()> {
        let mut connection = self
            .pool
            .acquire()
            .await
            .map_err(|_| PackedWireError::Backend("packed spool connection failed".into()))?;
        Self::replace_on(&mut connection, root, record).await
    }

    async fn replace_on(
        connection: &mut SqliteConnection,
        root: SnapshotRootKind,
        record: &SnapshotIndexRecord,
    ) -> PackedResult<()> {
        let SnapshotIndexValue::Leaf(value) = &record.value else {
            return Err(PackedWireError::Invalid(
                "packed spool replacement requires a leaf".into(),
            ));
        };
        super::SnapshotIndexPage {
            kind: root.object_kind(),
            height: 0,
            records: vec![record.clone()],
        }
        .encode()?;
        let result = sea_orm::sqlx::query(
            "UPDATE records SET last_key=?,value=? WHERE root=? AND first_key=?",
        )
        .bind(&record.last_key)
        .bind(value)
        .bind(root as i64)
        .bind(&record.first_key)
        .execute(connection)
        .await
        .map_err(|_| PackedWireError::Backend("packed locator replacement failed".into()))?;
        if result.rows_affected() != 1 {
            return Err(PackedWireError::Invalid(
                "packed locator replacement is missing its key".into(),
            ));
        }
        Ok(())
    }

    pub async fn validate_inode_closure(&self) -> PackedResult<()> {
        let incomplete:Option<Vec<u8>>=sea_orm::sqlx::query_scalar("SELECT inode FROM inode_identities WHERE kind<>2 AND observed_links<>expected_links LIMIT 1").fetch_optional(&self.pool).await.map_err(|_|PackedWireError::Backend("packed nlink validation failed".into()))?;
        if incomplete.is_some() {
            return Err(PackedWireError::Invalid(
                "snapshot nlink does not match its visible dentries".into(),
            ));
        }
        let missing:Option<Vec<u8>>=sea_orm::sqlx::query_scalar("SELECT i.inode FROM inode_identities i LEFT JOIN records r ON r.root=? AND r.first_key=i.inode WHERE i.kind=3 AND r.first_key IS NULL LIMIT 1").bind(SnapshotRootKind::ColdAttributes as i64).fetch_optional(&self.pool).await.map_err(|_|PackedWireError::Backend("packed cold closure validation failed".into()))?;
        if missing.is_some() {
            return Err(PackedWireError::Invalid(
                "published symlink is missing its cold target".into(),
            ));
        }
        Ok(())
    }

    pub async fn build_index<B: ObjectBackend + Clone + 'static>(
        &self,
        client: ObjectClient<B>,
        root: SnapshotRootKind,
        prefix: String,
    ) -> PackedResult<SnapshotObjectRef> {
        self.build_index_for(client, root as i64, root.object_kind(), prefix)
            .await
    }

    pub(crate) async fn build_source_index<B: ObjectBackend + Clone + 'static>(
        &self,
        client: ObjectClient<B>,
        prefix: String,
    ) -> PackedResult<SnapshotObjectRef> {
        self.build_index_for(
            client,
            SOURCE_STATS_ROOT,
            super::SnapshotObjectKind::SourceStatsIndex,
            prefix,
        )
        .await
    }

    async fn build_index_for<B: ObjectBackend + Clone + 'static>(
        &self,
        client: ObjectClient<B>,
        root: i64,
        kind: super::SnapshotObjectKind,
        prefix: String,
    ) -> PackedResult<SnapshotObjectRef> {
        let mut builder = SnapshotIndexBuilder::new(client, kind, prefix, 1024, 256 * 1024)?;
        let mut last: Option<Vec<u8>> = None;
        loop {
            let rows=if let Some(key)=&last {
                sea_orm::sqlx::query("SELECT first_key,last_key,value FROM records WHERE root=? AND first_key>? ORDER BY first_key LIMIT 128").bind(root).bind(key).fetch_all(&self.pool).await
            } else {
                sea_orm::sqlx::query("SELECT first_key,last_key,value FROM records WHERE root=? ORDER BY first_key LIMIT 128").bind(root).fetch_all(&self.pool).await
            }.map_err(|_|PackedWireError::Backend("packed spool ordered scan failed".into()))?;
            if rows.is_empty() {
                break;
            }
            for row in rows {
                let first_key: Vec<u8> = row
                    .try_get(0)
                    .map_err(|_| PackedWireError::Invalid("invalid packed spool key".into()))?;
                let last_key: Vec<u8> = row
                    .try_get(1)
                    .map_err(|_| PackedWireError::Invalid("invalid packed spool fence".into()))?;
                let value: Vec<u8> = row
                    .try_get(2)
                    .map_err(|_| PackedWireError::Invalid("invalid packed spool value".into()))?;
                last = Some(first_key.clone());
                builder
                    .push(SnapshotIndexRecord {
                        first_key,
                        last_key,
                        value: SnapshotIndexValue::Leaf(value),
                    })
                    .await?;
            }
        }
        builder.finish().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cadapter::localfs::LocalFsBackend;

    fn leaf(inode: u64) -> SnapshotIndexRecord {
        let key = inode.to_be_bytes().to_vec();
        SnapshotIndexRecord {
            first_key: key.clone(),
            last_key: key,
            value: SnapshotIndexValue::Leaf(vec![1]),
        }
    }

    #[tokio::test]
    async fn spool_batch_conflicting_hardlink_rolls_back_identity_and_records() {
        let temporary = tempfile::tempdir().unwrap();
        let spool = SnapshotIndexSpool::create(temporary.path()).await.unwrap();
        let mut batch = spool.batch().await.unwrap();
        assert!(batch.claim_inode(2, 1, 2, [7; 32]).await.unwrap());
        batch
            .insert(SnapshotRootKind::Inodes, &leaf(2))
            .await
            .unwrap();
        batch.commit().await.unwrap();

        let mut batch = spool.batch().await.unwrap();
        assert!(!batch.claim_inode(2, 1, 2, [7; 32]).await.unwrap());
        batch
            .insert(SnapshotRootKind::ReverseNames, &leaf(2))
            .await
            .unwrap();
        assert!(batch.claim_inode(2, 1, 2, [8; 32]).await.is_err());
        drop(batch);
        assert!(
            spool
                .get(SnapshotRootKind::ReverseNames, &2u64.to_be_bytes())
                .await
                .unwrap()
                .is_none()
        );
        let observed: i64 = sea_orm::sqlx::query_scalar(
            "SELECT observed_links FROM inode_identities WHERE inode=?",
        )
        .bind(2u64.to_be_bytes().to_vec())
        .fetch_one(&spool.pool)
        .await
        .unwrap();
        assert_eq!(observed, 1);
        assert!(!spool.claim_inode(2, 1, 2, [7; 32]).await.unwrap());
        spool.validate_inode_closure().await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_spool_batch_rolls_back_and_releases_only_connection() {
        let temporary = tempfile::tempdir().unwrap();
        let spool =
            std::sync::Arc::new(SnapshotIndexSpool::create(temporary.path()).await.unwrap());
        let marker = temporary.path().join("user-file");
        std::fs::write(&marker, b"preserve").unwrap();
        let path = spool.path.clone();
        let (ready, received) = tokio::sync::oneshot::channel();
        let task_spool = spool.clone();
        let task = tokio::spawn(async move {
            let mut batch = task_spool.batch().await.unwrap();
            batch.claim_inode(2, 1, 1, [7; 32]).await.unwrap();
            batch
                .insert(SnapshotRootKind::Inodes, &leaf(2))
                .await
                .unwrap();
            ready.send(()).unwrap();
            std::future::pending::<()>().await;
            batch.commit().await.unwrap();
        });
        received.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        let count: i64 = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            sea_orm::sqlx::query_scalar("SELECT COUNT(*) FROM inode_identities")
                .fetch_one(&spool.pool),
        )
        .await
        .expect("cancelled batch retained the only spool connection")
        .unwrap();
        assert_eq!(count, 0);
        assert!(
            spool
                .get(SnapshotRootKind::Inodes, &2u64.to_be_bytes())
                .await
                .unwrap()
                .is_none()
        );
        drop(spool);
        assert!(!path.parent().unwrap().exists());
        assert_eq!(std::fs::read(marker).unwrap(), b"preserve");
    }

    #[tokio::test]
    async fn allocation_batch_conflict_rolls_back_current_chunk_and_preserves_prefix() {
        let temporary = tempfile::tempdir().unwrap();
        let spool = SnapshotIndexSpool::create(temporary.path()).await.unwrap();
        spool.set_allocations(&[(999, 8)]).await.unwrap();
        let mut values: Vec<_> = (1..=SNAPSHOT_SPOOL_BATCH_ENTRIES as u64)
            .map(|inode| (inode, 0))
            .collect();
        values.extend([(1000, 0), (999, 9)]);
        assert!(spool.set_allocations(&values).await.is_err());
        assert!(
            spool
                .get_for(SOURCE_STATS_ROOT, &128u64.to_be_bytes())
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            spool
                .get_for(SOURCE_STATS_ROOT, &1000u64.to_be_bytes())
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            spool
                .get_for(SOURCE_STATS_ROOT, &999u64.to_be_bytes())
                .await
                .unwrap()
                .unwrap(),
            super::super::source_stat::encode_allocation(999, 8).unwrap()
        );
    }
    #[tokio::test]
    async fn cancelled_spool_initialization_removes_owned_directory() {
        use std::future::Future;
        let temporary = tempfile::tempdir().unwrap();
        let marker = temporary.path().join("user-file");
        std::fs::write(&marker, b"preserve").unwrap();
        let mut pending = Box::pin(SnapshotIndexSpool::create(temporary.path()));
        let result = std::future::poll_fn(|context| match pending.as_mut().poll(context) {
            std::task::Poll::Pending => std::task::Poll::Ready(None),
            std::task::Poll::Ready(result) => std::task::Poll::Ready(Some(result)),
        })
        .await;
        drop(pending);
        drop(result);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let files: Vec<_> = std::fs::read_dir(temporary.path())
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.file_name())
            .collect();
        assert_eq!(files, vec![std::ffi::OsString::from("user-file")]);
    }

    #[tokio::test]
    async fn disk_spool_sorts_unordered_records_and_cleans_only_its_database() {
        let temporary = tempfile::tempdir().unwrap();
        let backend = LocalFsBackend::new(temporary.path().join("objects"));
        let client = ObjectClient::new(backend);
        let marker = temporary.path().join("user-file");
        std::fs::write(&marker, b"preserve").unwrap();
        let spool = SnapshotIndexSpool::create(temporary.path()).await.unwrap();
        let spool_path = spool.path.clone();
        spool.pool.acquire().await.unwrap().close().await.unwrap();
        let reopened = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(&spool_path)
                    .in_memory(false),
            )
            .await
            .unwrap();
        let count: i64 = sea_orm::sqlx::query_scalar("SELECT COUNT(*) FROM records")
            .fetch_one(&reopened)
            .await
            .unwrap();
        assert_eq!(count, 0);
        reopened.close().await;

        for i in [8u32, 1, 5, 2, 7, 4, 6, 3] {
            let key = i.to_be_bytes().to_vec();
            spool
                .insert(
                    SnapshotRootKind::Inodes,
                    &SnapshotIndexRecord {
                        first_key: key.clone(),
                        last_key: key,
                        value: SnapshotIndexValue::Leaf(i.to_le_bytes().to_vec()),
                    },
                )
                .await
                .unwrap();
        }
        let root = spool
            .build_index(client.clone(), SnapshotRootKind::Inodes, "indexes".into())
            .await
            .unwrap();
        let reader = super::super::SnapshotIndexReader::new(client, 0);
        for i in 1u32..=8 {
            assert_eq!(
                reader
                    .lookup(&root, &i.to_be_bytes())
                    .await
                    .unwrap()
                    .as_ref()
                    .map(|value| value.as_ref()),
                Some(&i.to_le_bytes()[..])
            );
        }
        drop(spool);
        assert!(!spool_path.exists());
        assert_eq!(std::fs::read(marker).unwrap(), b"preserve");
    }
}
