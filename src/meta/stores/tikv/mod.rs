//! TiKV-backed metadata store.
//!
//! The store uses TiKV's transactional API for namespace and file-layout
//! metadata. Read-only operations use optimistic transactions. Mutating paths
//! use pessimistic transactions and `get_for_update` on the keys that
//! participate in the metadata CAS.

use crate::chunk::SliceDesc;
use crate::meta::config::{Config, DatabaseType, default_tikv_namespace};
use crate::meta::file_lock::{
    FileLockInfo, FileLockQuery, FileLockRange, FileLockType, PlockRecord,
};
use crate::meta::store::{
    DirEntry, FileAttr, FileType, MetaError, MetaStore, MetaStoreCapabilities, OpenFlags,
    RetryReason, SetAttrFlags, SetAttrRequest, StatFsSnapshot, stat_fs_snapshot_from_usage,
    stat_fs_used_bytes,
};
use crate::meta::{INODE_ID_KEY, SLICE_ID_KEY};
use async_trait::async_trait;
use chrono::Utc;
use rand::{RngCore, rng};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::future::Future;
use std::ops::Bound;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tikv_client::{
    BoundRange, CheckLevel, Key, KvPair, Transaction, TransactionClient, TransactionOptions,
};
use tokio::sync::Mutex;
use uuid::Uuid;

const ROOT_INODE: i64 = 1;
const ROOT_SIZE: u64 = 4096;
const FIRST_ALLOCATED_INODE: i64 = 2;
const SCAN_BATCH_LIMIT: u32 = 1024;
const TXN_MAX_RETRIES: usize = 10;
const SLICE_ID_LEASE_SIZE: i64 = 1024;
const DELAYED_PENDING_PREFIX: &str = "gc/delayed/pending/";
const DELAYED_META_DELETED_PREFIX: &str = "gc/delayed/meta_deleted/";
const UNCOMMITTED_PENDING_PREFIX: &str = "gc/uncommitted/pending/";
const UNCOMMITTED_ORPHAN_PREFIX: &str = "gc/uncommitted/orphan/";
const DELAYED_ID_COUNTER: &str = "gc/delayed/id";
const UNCOMMITTED_ID_COUNTER: &str = "gc/uncommitted/id";

type TiKvTxnFuture<'txn, T> = Pin<Box<dyn Future<Output = Result<T, MetaError>> + Send + 'txn>>;

#[derive(Debug, Clone, Copy)]
struct CounterLease {
    next: i64,
    end: i64,
}

#[derive(Clone, Copy)]
enum TiKvTxnMode {
    Read,
    Write,
}

/// TiKV metadata backend.
#[derive(Clone)]
pub struct TiKvMetaStore {
    pd_endpoints: Vec<String>,
    namespace: String,
    client: TransactionClient,
    lock_session: Uuid,
    counter_leases: Arc<Mutex<HashMap<String, CounterLease>>>,
    _config: Config,
}

impl fmt::Debug for TiKvMetaStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TiKvMetaStore")
            .field("pd_endpoints", &self.pd_endpoints)
            .field("namespace", &self.namespace)
            .finish_non_exhaustive()
    }
}

impl TiKvMetaStore {
    async fn from_config_inner(config: Config) -> Result<Self, MetaError> {
        let (pd_endpoints, namespace) = match &config.database.db_config {
            DatabaseType::TiKv {
                pd_endpoints,
                namespace,
            } => (pd_endpoints.clone(), normalize_namespace(namespace)),
            _ => {
                return Err(MetaError::Config(
                    "TiKvMetaStore requires database.type = tikv".to_string(),
                ));
            }
        };

        if pd_endpoints.is_empty() {
            return Err(MetaError::Config(
                "TiKvMetaStore requires at least one PD endpoint".to_string(),
            ));
        }

        let client = TransactionClient::new(pd_endpoints.clone())
            .await
            .map_err(|e| Self::tikv_err("connect", e))?;

        Ok(Self {
            pd_endpoints,
            namespace,
            client,
            lock_session: Uuid::new_v4(),
            counter_leases: Arc::new(Mutex::new(HashMap::new())),
            _config: config,
        })
    }

    /// Create or open the store from a backend path containing `brewfs.yml`.
    #[allow(dead_code)]
    pub async fn new(backend_path: &Path) -> Result<Self, MetaError> {
        let config =
            Config::from_path(backend_path).map_err(|e| MetaError::Config(e.to_string()))?;
        Self::from_config_inner(config).await
    }

    /// Build a TiKV metadata store from an already parsed configuration.
    #[allow(dead_code)]
    pub async fn from_config(config: Config) -> Result<Self, MetaError> {
        Self::from_config_inner(config).await
    }

    #[allow(dead_code)]
    pub fn pd_endpoints(&self) -> &[String] {
        &self.pd_endpoints
    }

    #[allow(dead_code)]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    #[allow(dead_code)]
    pub(crate) fn inode_key(&self, ino: i64) -> Vec<u8> {
        self.key_bytes(&format!("inode/{ino}"))
    }

    pub(crate) fn dentry_key(&self, parent: i64, name: &str) -> Vec<u8> {
        self.key_bytes(&format!("dentry/{parent}/{name}"))
    }

    fn dentry_prefix(&self, parent: i64) -> Vec<u8> {
        self.key_bytes(&format!("dentry/{parent}/"))
    }

    pub(crate) fn chunk_key(&self, chunk_id: u64) -> Vec<u8> {
        self.key_bytes(&format!("chunk/{chunk_id}"))
    }

    fn chunk_prefix(&self) -> Vec<u8> {
        self.key_bytes("chunk/")
    }

    fn inode_prefix(&self) -> Vec<u8> {
        self.key_bytes("inode/")
    }

    fn link_parent_key(&self, ino: i64) -> Vec<u8> {
        self.key_bytes(&format!("link_parent/{ino}"))
    }

    fn plock_key(&self, ino: i64) -> Vec<u8> {
        self.key_bytes(&format!("plock/{ino}"))
    }

    fn delayed_pending_key(&self, id: i64) -> Vec<u8> {
        self.key_bytes(&format!("{DELAYED_PENDING_PREFIX}{id}"))
    }

    fn delayed_pending_prefix(&self) -> Vec<u8> {
        self.key_bytes(DELAYED_PENDING_PREFIX)
    }

    fn delayed_meta_deleted_key(&self, id: i64) -> Vec<u8> {
        self.key_bytes(&format!("{DELAYED_META_DELETED_PREFIX}{id}"))
    }

    fn delayed_meta_deleted_prefix(&self) -> Vec<u8> {
        self.key_bytes(DELAYED_META_DELETED_PREFIX)
    }

    fn uncommitted_pending_key(&self, slice_id: u64) -> Vec<u8> {
        self.key_bytes(&format!("{UNCOMMITTED_PENDING_PREFIX}{slice_id}"))
    }

    fn uncommitted_pending_prefix(&self) -> Vec<u8> {
        self.key_bytes(UNCOMMITTED_PENDING_PREFIX)
    }

    fn uncommitted_orphan_key(&self, slice_id: u64) -> Vec<u8> {
        self.key_bytes(&format!("{UNCOMMITTED_ORPHAN_PREFIX}{slice_id}"))
    }

    fn uncommitted_orphan_prefix(&self) -> Vec<u8> {
        self.key_bytes(UNCOMMITTED_ORPHAN_PREFIX)
    }

    pub(crate) fn counter_key(&self, name: &str) -> Vec<u8> {
        self.key_bytes(&format!("counter/{name}"))
    }

    fn key_bytes(&self, suffix: &str) -> Vec<u8> {
        Self::scoped_key(&self.namespace, suffix)
    }

    pub(crate) fn scoped_key(namespace: &str, suffix: &str) -> Vec<u8> {
        format!("{}/{}", namespace, suffix).into_bytes()
    }

    fn tikv_err(operation: &str, error: tikv_client::Error) -> MetaError {
        let message = error.to_string();
        let lower = message.to_ascii_lowercase();
        if lower.contains("write conflict")
            || lower.contains("pessimisticlock")
            || lower.contains("lock conflict")
            || lower.contains("txnlock")
        {
            MetaError::ContinueRetry(RetryReason::TransactionConflict)
        } else {
            MetaError::Internal(format!("TiKV {operation} failed: {message}"))
        }
    }

    async fn begin_read(&self, operation: &str) -> Result<Transaction, MetaError> {
        let options = TransactionOptions::new_optimistic().drop_check(CheckLevel::Warn);
        self.client
            .begin_with_options(options)
            .await
            .map_err(|e| Self::tikv_err(operation, e))
    }

    async fn begin_write(&self, operation: &str) -> Result<Transaction, MetaError> {
        let options = TransactionOptions::new_pessimistic().drop_check(CheckLevel::Warn);
        self.client
            .begin_with_options(options)
            .await
            .map_err(|e| Self::tikv_err(operation, e))
    }

    async fn commit_write(&self, txn: &mut Transaction, operation: &str) -> Result<(), MetaError> {
        txn.commit()
            .await
            .map(|_| ())
            .map_err(|e| Self::tikv_err(operation, e))
    }

    async fn rollback_best_effort(txn: &mut Transaction, operation: &str) {
        if let Err(err) = txn.rollback().await {
            log::debug!("TiKV {operation} rollback failed: {err}");
        }
    }

    async fn retry_delay(attempt: usize) {
        let jitter_bound = ((attempt + 1) * (attempt + 1)).max(1) as u64;
        let jitter = rng().next_u64() % jitter_bound;
        tokio::time::sleep(Duration::from_millis(20 + jitter)).await;
    }

    /// Run a retry-safe TiKV transaction closure.
    ///
    /// The closure may execute more than once when TiKV reports a retryable
    /// conflict, so it should only perform TiKV reads/writes and in-memory
    /// staging. External side effects belong after this helper returns.
    async fn run_txn<T, F>(
        &self,
        operation: &'static str,
        mode: TiKvTxnMode,
        mut task: F,
    ) -> Result<T, MetaError>
    where
        F: for<'txn> FnMut(&'txn TiKvMetaStore, &'txn mut Transaction) -> TiKvTxnFuture<'txn, T>,
    {
        for attempt in 0..TXN_MAX_RETRIES {
            let mut txn = match mode {
                TiKvTxnMode::Read => self.begin_read(operation).await?,
                TiKvTxnMode::Write => self.begin_write(operation).await?,
            };

            let result = task(self, &mut txn).await;
            match (mode, result) {
                (TiKvTxnMode::Read, Ok(value)) => {
                    Self::rollback_best_effort(&mut txn, operation).await;
                    return Ok(value);
                }
                (
                    TiKvTxnMode::Read,
                    Err(MetaError::ContinueRetry(RetryReason::TransactionConflict)),
                ) => {
                    Self::rollback_best_effort(&mut txn, operation).await;
                }
                (TiKvTxnMode::Read, Err(err)) => {
                    Self::rollback_best_effort(&mut txn, operation).await;
                    return Err(err);
                }
                (TiKvTxnMode::Write, Ok(value)) => {
                    match self.commit_write(&mut txn, operation).await {
                        Ok(()) => return Ok(value),
                        Err(MetaError::ContinueRetry(RetryReason::TransactionConflict)) => {
                            Self::rollback_best_effort(&mut txn, operation).await;
                        }
                        Err(err) => {
                            Self::rollback_best_effort(&mut txn, operation).await;
                            return Err(err);
                        }
                    }
                }
                (
                    TiKvTxnMode::Write,
                    Err(MetaError::ContinueRetry(RetryReason::TransactionConflict)),
                ) => {
                    Self::rollback_best_effort(&mut txn, operation).await;
                }
                (TiKvTxnMode::Write, Err(err)) => {
                    Self::rollback_best_effort(&mut txn, operation).await;
                    return Err(err);
                }
            }

            if attempt + 1 < TXN_MAX_RETRIES {
                Self::retry_delay(attempt).await;
            }
        }

        Err(MetaError::MaxRetriesExceeded)
    }

    async fn read_txn<T, F>(&self, operation: &'static str, task: F) -> Result<T, MetaError>
    where
        F: for<'txn> FnMut(&'txn TiKvMetaStore, &'txn mut Transaction) -> TiKvTxnFuture<'txn, T>,
    {
        self.run_txn(operation, TiKvTxnMode::Read, task).await
    }

    async fn write_txn<T, F>(&self, operation: &'static str, task: F) -> Result<T, MetaError>
    where
        F: for<'txn> FnMut(&'txn TiKvMetaStore, &'txn mut Transaction) -> TiKvTxnFuture<'txn, T>,
    {
        self.run_txn(operation, TiKvTxnMode::Write, task).await
    }

    fn now() -> i64 {
        Utc::now().timestamp_nanos_opt().unwrap_or(0)
    }

    fn now_secs() -> i64 {
        Utc::now().timestamp()
    }

    fn root_node() -> StoredNode {
        let now = Self::now();
        StoredNode {
            ino: ROOT_INODE,
            parent: ROOT_INODE,
            name: "/".to_string(),
            kind: StoredNodeKind::Dir,
            size: ROOT_SIZE,
            blocks: ROOT_SIZE.div_ceil(512),
            mode: 0o40755,
            rdev: 0,
            uid: 0,
            gid: 0,
            atime: now,
            mtime: now,
            ctime: now,
            nlink: 2,
            symlink_target: None,
            deleted: false,
        }
    }

    fn decode_node(bytes: &[u8]) -> Result<StoredNode, MetaError> {
        serde_json::from_slice(bytes)
            .map_err(|e| MetaError::Serialization(format!("TiKV node decode failed: {e}")))
    }

    fn decode_dentry(bytes: &[u8]) -> Result<StoredDentry, MetaError> {
        serde_json::from_slice(bytes)
            .map_err(|e| MetaError::Serialization(format!("TiKV dentry decode failed: {e}")))
    }

    fn decode_slices(bytes: &[u8]) -> Result<Vec<SliceDesc>, MetaError> {
        serde_json::from_slice(bytes)
            .map_err(|e| MetaError::Serialization(format!("TiKV slice list decode failed: {e}")))
    }

    fn decode_counter(bytes: &[u8]) -> Result<i64, MetaError> {
        serde_json::from_slice(bytes)
            .map_err(|e| MetaError::Serialization(format!("TiKV counter decode failed: {e}")))
    }

    fn decode_link_parents(bytes: &[u8]) -> Result<Vec<StoredLinkParent>, MetaError> {
        serde_json::from_slice(bytes)
            .map_err(|e| MetaError::Serialization(format!("TiKV link parent decode failed: {e}")))
    }

    fn decode_plocks(bytes: &[u8]) -> Result<Vec<StoredPlock>, MetaError> {
        serde_json::from_slice(bytes)
            .map_err(|e| MetaError::Serialization(format!("TiKV plock decode failed: {e}")))
    }

    fn decode_delayed_slice(bytes: &[u8]) -> Result<StoredDelayedSliceRecord, MetaError> {
        serde_json::from_slice(bytes).map_err(|e| {
            MetaError::Serialization(format!("TiKV delayed slice record decode failed: {e}"))
        })
    }

    fn decode_uncommitted_slice(bytes: &[u8]) -> Result<StoredUncommittedSliceRecord, MetaError> {
        serde_json::from_slice(bytes).map_err(|e| {
            MetaError::Serialization(format!("TiKV uncommitted slice record decode failed: {e}"))
        })
    }

    fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, MetaError> {
        serde_json::to_vec(value)
            .map_err(|e| MetaError::Serialization(format!("TiKV value encode failed: {e}")))
    }

    async fn txn_get_raw(
        txn: &mut Transaction,
        key: Vec<u8>,
        lock: bool,
        operation: &str,
    ) -> Result<Option<Vec<u8>>, MetaError> {
        let result = if lock {
            txn.get_for_update(key).await
        } else {
            txn.get(key).await
        };
        result.map_err(|e| Self::tikv_err(operation, e))
    }

    async fn txn_put_raw(
        txn: &mut Transaction,
        key: Vec<u8>,
        value: Vec<u8>,
        operation: &str,
    ) -> Result<(), MetaError> {
        txn.put(key, value)
            .await
            .map_err(|e| Self::tikv_err(operation, e))
    }

    async fn txn_delete_raw(
        txn: &mut Transaction,
        key: Vec<u8>,
        operation: &str,
    ) -> Result<(), MetaError> {
        txn.delete(key)
            .await
            .map_err(|e| Self::tikv_err(operation, e))
    }

    async fn txn_scan_prefix(
        txn: &mut Transaction,
        prefix: Vec<u8>,
        limit: Option<usize>,
        operation: &str,
    ) -> Result<Vec<KvPair>, MetaError> {
        let mut out = Vec::new();
        let mut remaining = limit.unwrap_or(usize::MAX);
        if remaining == 0 {
            return Ok(out);
        }

        let upper = match prefix_range_end(&prefix) {
            Some(end) => Bound::Excluded(Key::from(end)),
            None => Bound::Unbounded,
        };
        let mut lower = Bound::Included(Key::from(prefix));

        while remaining > 0 {
            let batch_limit = remaining.min(SCAN_BATCH_LIMIT as usize) as u32;
            let range = BoundRange::new(lower.clone(), upper.clone());
            let batch: Vec<KvPair> = txn
                .scan(range, batch_limit)
                .await
                .map_err(|e| Self::tikv_err(operation, e))?
                .collect();

            let batch_len = batch.len();
            if batch_len == 0 {
                break;
            }

            for pair in batch {
                let last_key: Vec<u8> = pair.key().clone().into();
                lower = Bound::Excluded(Key::from(last_key));
                out.push(pair);
            }

            remaining = remaining.saturating_sub(batch_len);
            if batch_len < batch_limit as usize {
                break;
            }
        }

        Ok(out)
    }

    async fn txn_get_node(
        &self,
        txn: &mut Transaction,
        ino: i64,
        lock: bool,
        operation: &str,
    ) -> Result<Option<StoredNode>, MetaError> {
        Self::txn_get_raw(txn, self.inode_key(ino), lock, operation)
            .await?
            .as_deref()
            .map(Self::decode_node)
            .transpose()
    }

    async fn txn_put_node(
        &self,
        txn: &mut Transaction,
        node: &StoredNode,
        operation: &str,
    ) -> Result<(), MetaError> {
        Self::txn_put_raw(
            txn,
            self.inode_key(node.ino),
            Self::encode(node)?,
            operation,
        )
        .await
    }

    async fn txn_get_link_parents(
        &self,
        txn: &mut Transaction,
        ino: i64,
        lock: bool,
        operation: &str,
    ) -> Result<Vec<StoredLinkParent>, MetaError> {
        Self::txn_get_raw(txn, self.link_parent_key(ino), lock, operation)
            .await?
            .as_deref()
            .map(Self::decode_link_parents)
            .transpose()
            .map(|parents| parents.unwrap_or_default())
    }

    async fn txn_put_link_parents(
        &self,
        txn: &mut Transaction,
        ino: i64,
        parents: &[StoredLinkParent],
        operation: &str,
    ) -> Result<(), MetaError> {
        Self::txn_put_raw(
            txn,
            self.link_parent_key(ino),
            Self::encode(&parents)?,
            operation,
        )
        .await
    }

    async fn txn_delete_link_parents(
        &self,
        txn: &mut Transaction,
        ino: i64,
        operation: &str,
    ) -> Result<(), MetaError> {
        Self::txn_delete_raw(txn, self.link_parent_key(ino), operation).await
    }

    async fn txn_get_plocks(
        &self,
        txn: &mut Transaction,
        ino: i64,
        lock: bool,
        operation: &str,
    ) -> Result<Vec<StoredPlock>, MetaError> {
        Self::txn_get_raw(txn, self.plock_key(ino), lock, operation)
            .await?
            .as_deref()
            .map(Self::decode_plocks)
            .transpose()
            .map(|plocks| plocks.unwrap_or_default())
    }

    async fn txn_put_plocks_or_delete(
        &self,
        txn: &mut Transaction,
        ino: i64,
        plocks: &[StoredPlock],
        operation: &str,
    ) -> Result<(), MetaError> {
        let key = self.plock_key(ino);
        if plocks.is_empty() {
            Self::txn_delete_raw(txn, key, operation).await
        } else {
            Self::txn_put_raw(txn, key, Self::encode(&plocks)?, operation).await
        }
    }

    async fn txn_get_dentry(
        &self,
        txn: &mut Transaction,
        parent: i64,
        name: &str,
        lock: bool,
        operation: &str,
    ) -> Result<Option<StoredDentry>, MetaError> {
        Self::txn_get_raw(txn, self.dentry_key(parent, name), lock, operation)
            .await?
            .as_deref()
            .map(Self::decode_dentry)
            .transpose()
    }

    async fn txn_put_dentry(
        &self,
        txn: &mut Transaction,
        parent: i64,
        name: &str,
        dentry: &StoredDentry,
        operation: &str,
    ) -> Result<(), MetaError> {
        Self::txn_put_raw(
            txn,
            self.dentry_key(parent, name),
            Self::encode(dentry)?,
            operation,
        )
        .await
    }

    async fn txn_require_dir(
        &self,
        txn: &mut Transaction,
        ino: i64,
        lock: bool,
        operation: &str,
    ) -> Result<StoredNode, MetaError> {
        let node = self
            .txn_get_node(txn, ino, lock, operation)
            .await?
            .ok_or(MetaError::ParentNotFound(ino))?;
        if node.kind != StoredNodeKind::Dir {
            return Err(MetaError::NotDirectory(ino));
        }
        Ok(node)
    }

    async fn txn_next_counter(
        &self,
        txn: &mut Transaction,
        name: &str,
        first_value: i64,
        operation: &str,
    ) -> Result<i64, MetaError> {
        self.txn_alloc_counter_range(txn, name, first_value, 1, operation)
            .await
    }

    async fn txn_alloc_counter_range(
        &self,
        txn: &mut Transaction,
        name: &str,
        first_value: i64,
        count: i64,
        operation: &str,
    ) -> Result<i64, MetaError> {
        if count <= 0 {
            return Err(MetaError::Internal(format!(
                "TiKV counter allocation count must be positive: {count}"
            )));
        }
        let key = self.counter_key(name);
        let current = Self::txn_get_raw(txn, key.clone(), true, operation)
            .await?
            .as_deref()
            .map(Self::decode_counter)
            .transpose()?
            .unwrap_or(first_value);
        let next = current
            .checked_add(count)
            .ok_or_else(|| MetaError::Internal(format!("TiKV counter overflow: {name}")))?;
        Self::txn_put_raw(txn, key, Self::encode(&next)?, operation).await?;
        Ok(current)
    }

    async fn alloc_counter_range(&self, name: String, count: i64) -> Result<i64, MetaError> {
        let operation = "next_id";
        self.write_txn(operation, |store, txn| {
            let name = name.clone();
            Box::pin(async move {
                store
                    .txn_alloc_counter_range(txn, &name, 1, count, operation)
                    .await
            })
        })
        .await
    }

    async fn next_leased_slice_id(&self) -> Result<i64, MetaError> {
        let mut leases = self.counter_leases.lock().await;
        if let Some(lease) = leases.get_mut(SLICE_ID_KEY)
            && lease.next < lease.end
        {
            let id = lease.next;
            lease.next += 1;
            return Ok(id);
        }

        let start = self
            .alloc_counter_range(SLICE_ID_KEY.to_string(), SLICE_ID_LEASE_SIZE)
            .await?;
        let end = start
            .checked_add(SLICE_ID_LEASE_SIZE)
            .ok_or_else(|| MetaError::Internal("TiKV slice id lease overflow".to_string()))?;
        leases.insert(
            SLICE_ID_KEY.to_string(),
            CounterLease {
                next: start + 1,
                end,
            },
        );
        Ok(start)
    }

    async fn try_set_plock(
        &self,
        inode: i64,
        owner: i64,
        new_lock: PlockRecord,
        lock_type: FileLockType,
        range: FileLockRange,
    ) -> Result<(), MetaError> {
        let operation = "set_plock";
        let sid = self.lock_session;

        self.write_txn(operation, |store, txn| {
            Box::pin(async move {
                let mut plocks = store.txn_get_plocks(txn, inode, true, operation).await?;
                Self::apply_plock_update(
                    &mut plocks,
                    sid,
                    owner,
                    new_lock,
                    lock_type,
                    range,
                    inode,
                )?;
                store
                    .txn_put_plocks_or_delete(txn, inode, &plocks, operation)
                    .await
            })
        })
        .await
    }

    fn apply_plock_update(
        plocks: &mut Vec<StoredPlock>,
        sid: Uuid,
        owner: i64,
        new_lock: PlockRecord,
        lock_type: FileLockType,
        range: FileLockRange,
        inode: i64,
    ) -> Result<(), MetaError> {
        let current_idx = plocks
            .iter()
            .position(|plock| plock.sid == sid && plock.owner == owner);

        if lock_type == FileLockType::UnLock {
            if let Some(idx) = current_idx {
                let updated = PlockRecord::update_locks(plocks[idx].records.clone(), new_lock);
                if updated.is_empty() {
                    plocks.remove(idx);
                } else {
                    plocks[idx].records = updated;
                }
            }
            return Ok(());
        }

        for plock in plocks.iter() {
            if plock.sid == sid && plock.owner == owner {
                continue;
            }
            if PlockRecord::check_conflict(&lock_type, &range, &plock.records) {
                return Err(MetaError::LockConflict {
                    inode,
                    owner,
                    range,
                });
            }
        }

        match current_idx {
            Some(idx) => {
                plocks[idx].records =
                    PlockRecord::update_locks(plocks[idx].records.clone(), new_lock);
            }
            None => plocks.push(StoredPlock {
                sid,
                owner,
                records: vec![new_lock],
            }),
        }

        Ok(())
    }

    async fn txn_next_inode(
        &self,
        txn: &mut Transaction,
        operation: &str,
    ) -> Result<i64, MetaError> {
        self.txn_next_counter(txn, INODE_ID_KEY, FIRST_ALLOCATED_INODE, operation)
            .await
    }

    async fn txn_get_slices(
        &self,
        txn: &mut Transaction,
        chunk_id: u64,
        lock: bool,
        operation: &str,
    ) -> Result<Vec<SliceDesc>, MetaError> {
        Self::txn_get_raw(txn, self.chunk_key(chunk_id), lock, operation)
            .await?
            .as_deref()
            .map(Self::decode_slices)
            .transpose()
            .map(|slices| slices.unwrap_or_default())
    }

    async fn txn_put_slices_or_delete(
        &self,
        txn: &mut Transaction,
        chunk_id: u64,
        slices: &[SliceDesc],
        operation: &str,
    ) -> Result<(), MetaError> {
        let chunk_key = self.chunk_key(chunk_id);
        if slices.is_empty() {
            Self::txn_delete_raw(txn, chunk_key, operation).await
        } else {
            Self::txn_put_raw(txn, chunk_key, Self::encode(&slices)?, operation).await
        }
    }

    async fn txn_stage_delayed_slice_records(
        &self,
        txn: &mut Transaction,
        chunk_id: u64,
        delayed_slices: &[(u64, u64, u32)],
        now: i64,
        operation: &str,
    ) -> Result<(), MetaError> {
        if delayed_slices.is_empty() {
            return Ok(());
        }

        let counter_key = self.counter_key(DELAYED_ID_COUNTER);
        let mut next_id = Self::txn_get_raw(txn, counter_key.clone(), true, operation)
            .await?
            .as_deref()
            .map(Self::decode_counter)
            .transpose()?
            .unwrap_or(0);

        for (slice_id, offset, size) in delayed_slices {
            next_id = next_id
                .checked_add(1)
                .ok_or_else(|| MetaError::Internal("TiKV delayed slice id overflow".to_string()))?;
            let record = StoredDelayedSliceRecord {
                id: next_id,
                slice_id: *slice_id,
                chunk_id,
                offset: *offset,
                size: u64::from(*size),
                created_at: now,
                reason: "compact".to_string(),
                status: "pending".to_string(),
            };
            Self::txn_put_raw(
                txn,
                self.delayed_pending_key(next_id),
                Self::encode(&record)?,
                operation,
            )
            .await?;
        }

        Self::txn_put_raw(txn, counter_key, Self::encode(&next_id)?, operation).await
    }

    async fn txn_create_node(
        &self,
        txn: &mut Transaction,
        parent: i64,
        name: String,
        kind: StoredNodeKind,
        operation: &str,
    ) -> Result<i64, MetaError> {
        self.txn_create_node_with_target(txn, parent, name, kind, None, operation)
            .await
    }

    async fn txn_create_node_with_target(
        &self,
        txn: &mut Transaction,
        parent: i64,
        name: String,
        kind: StoredNodeKind,
        symlink_target: Option<String>,
        operation: &str,
    ) -> Result<i64, MetaError> {
        self.txn_create_node_with_attrs(txn, parent, name, kind, symlink_target, None, operation)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn txn_create_node_with_attrs(
        &self,
        txn: &mut Transaction,
        parent: i64,
        name: String,
        kind: StoredNodeKind,
        symlink_target: Option<String>,
        attrs: Option<(u32, u32, u32, u32)>,
        operation: &str,
    ) -> Result<i64, MetaError> {
        let mut parent_node = self.txn_require_dir(txn, parent, true, operation).await?;
        if self
            .txn_get_dentry(txn, parent, &name, true, operation)
            .await?
            .is_some()
        {
            return Err(MetaError::AlreadyExists { parent, name });
        }

        let ino = self.txn_next_inode(txn, operation).await?;
        let now = Self::now();
        let target_len = symlink_target.as_ref().map(|target| target.len() as u64);
        let (size, blocks, mode, nlink) = match kind {
            StoredNodeKind::File => (0, 0, 0o100644, 1),
            StoredNodeKind::Dir => (ROOT_SIZE, ROOT_SIZE.div_ceil(512), 0o40755, 2),
            StoredNodeKind::Symlink => {
                let size = target_len.unwrap_or(0);
                (size, size.div_ceil(512), 0o120777, 1)
            }
            StoredNodeKind::Fifo => (0, 0, FileType::Fifo.mode_type_bits() | 0o644, 1),
            StoredNodeKind::Socket => (0, 0, FileType::Socket.mode_type_bits() | 0o644, 1),
            StoredNodeKind::CharDevice => (0, 0, FileType::CharDevice.mode_type_bits() | 0o644, 1),
            StoredNodeKind::BlockDevice => {
                (0, 0, FileType::BlockDevice.mode_type_bits() | 0o644, 1)
            }
        };
        let (mode, uid, gid, rdev) = attrs.unwrap_or((mode, 0, 0, 0));
        let node = StoredNode {
            ino,
            parent,
            name: name.clone(),
            kind,
            size,
            blocks,
            mode,
            rdev,
            uid,
            gid,
            atime: now,
            mtime: now,
            ctime: now,
            nlink,
            symlink_target,
            deleted: false,
        };
        let dentry = StoredDentry { ino, kind };

        if kind == StoredNodeKind::Dir {
            parent_node.nlink = parent_node.nlink.saturating_add(1);
        }
        parent_node.mtime = now;
        parent_node.ctime = now;

        self.txn_put_node(txn, &parent_node, operation).await?;
        self.txn_put_node(txn, &node, operation).await?;
        self.txn_put_dentry(txn, parent, &name, &dentry, operation)
            .await?;
        Ok(ino)
    }
    async fn txn_remove_non_dir_dentry(
        &self,
        txn: &mut Transaction,
        parent: i64,
        name: &str,
        dentry: StoredDentry,
        now: i64,
        operation: &str,
    ) -> Result<(), MetaError> {
        if dentry.kind == StoredNodeKind::Dir {
            return Err(MetaError::NotSupported(
                "TiKV unlink for directories is not supported; use rmdir".to_string(),
            ));
        }

        let mut node = self
            .txn_get_node(txn, dentry.ino, true, operation)
            .await?
            .ok_or(MetaError::NotFound(dentry.ino))?;
        if node.kind == StoredNodeKind::Dir {
            return Err(MetaError::NotSupported(
                "TiKV unlink for directories is not supported; use rmdir".to_string(),
            ));
        }
        if node.deleted || node.nlink == 0 {
            return Err(MetaError::NotFound(dentry.ino));
        }

        Self::txn_delete_raw(txn, self.dentry_key(parent, name), operation).await?;

        if node.nlink > 1 {
            let mut link_parents = self
                .txn_get_link_parents(txn, node.ino, true, operation)
                .await?;
            let before = link_parents.len();
            link_parents.retain(|link| !(link.parent == parent && link.name == name));
            if link_parents.len() == before {
                return Err(MetaError::Internal(format!(
                    "expected link parent binding {parent}/{name} for inode {}",
                    node.ino
                )));
            }

            node.nlink -= 1;
            node.deleted = false;
            if node.nlink == 1 {
                let remaining = link_parents.first().cloned().ok_or_else(|| {
                    MetaError::Internal(format!(
                        "missing remaining link parent for inode {}",
                        node.ino
                    ))
                })?;
                node.parent = remaining.parent;
                node.name = remaining.name;
                self.txn_delete_link_parents(txn, node.ino, operation)
                    .await?;
            } else {
                node.parent = 0;
                node.name.clear();
                self.txn_put_link_parents(txn, node.ino, &link_parents, operation)
                    .await?;
            }
        } else {
            node.nlink = 0;
            node.deleted = true;
            node.parent = 0;
            node.name.clear();
            self.txn_delete_link_parents(txn, node.ino, operation)
                .await?;
        }

        node.mtime = now;
        node.ctime = now;
        self.txn_put_node(txn, &node, operation).await
    }

    #[allow(clippy::too_many_arguments)]
    async fn txn_move_node_binding(
        &self,
        txn: &mut Transaction,
        node: &mut StoredNode,
        old_parent: i64,
        old_name: &str,
        new_parent: i64,
        new_name: &str,
        operation: &str,
    ) -> Result<(), MetaError> {
        if node.kind == StoredNodeKind::Dir || node.nlink <= 1 {
            node.parent = new_parent;
            node.name = new_name.to_string();
            return Ok(());
        }

        let mut link_parents = self
            .txn_get_link_parents(txn, node.ino, true, operation)
            .await?;
        let mut updated = false;
        for link in &mut link_parents {
            if link.parent == old_parent && link.name == old_name {
                link.parent = new_parent;
                link.name = new_name.to_string();
                updated = true;
                break;
            }
        }

        if !updated {
            return Err(MetaError::Internal(format!(
                "expected link parent binding {old_parent}/{old_name} for inode {}",
                node.ino
            )));
        }

        node.parent = 0;
        node.name.clear();
        self.txn_put_link_parents(txn, node.ino, &link_parents, operation)
            .await
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum StoredNodeKind {
    File,
    Dir,
    Symlink,
    Fifo,
    Socket,
    CharDevice,
    BlockDevice,
}

impl From<FileType> for StoredNodeKind {
    fn from(kind: FileType) -> Self {
        match kind {
            FileType::File => StoredNodeKind::File,
            FileType::Dir => StoredNodeKind::Dir,
            FileType::Symlink => StoredNodeKind::Symlink,
            FileType::Fifo => StoredNodeKind::Fifo,
            FileType::Socket => StoredNodeKind::Socket,
            FileType::CharDevice => StoredNodeKind::CharDevice,
            FileType::BlockDevice => StoredNodeKind::BlockDevice,
        }
    }
}

impl From<StoredNodeKind> for FileType {
    fn from(kind: StoredNodeKind) -> Self {
        match kind {
            StoredNodeKind::File => FileType::File,
            StoredNodeKind::Dir => FileType::Dir,
            StoredNodeKind::Symlink => FileType::Symlink,
            StoredNodeKind::Fifo => FileType::Fifo,
            StoredNodeKind::Socket => FileType::Socket,
            StoredNodeKind::CharDevice => FileType::CharDevice,
            StoredNodeKind::BlockDevice => FileType::BlockDevice,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
struct StoredDentry {
    ino: i64,
    kind: StoredNodeKind,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct StoredLinkParent {
    parent: i64,
    name: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct StoredPlock {
    sid: Uuid,
    owner: i64,
    records: Vec<PlockRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct StoredDelayedSliceRecord {
    id: i64,
    slice_id: u64,
    chunk_id: u64,
    offset: u64,
    size: u64,
    created_at: i64,
    reason: String,
    status: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct StoredUncommittedSliceRecord {
    id: i64,
    slice_id: u64,
    chunk_id: u64,
    size: u64,
    created_at: i64,
    operation: String,
    status: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredNode {
    ino: i64,
    parent: i64,
    name: String,
    kind: StoredNodeKind,
    size: u64,
    blocks: u64,
    mode: u32,
    #[serde(default)]
    rdev: u32,
    uid: u32,
    gid: u32,
    atime: i64,
    mtime: i64,
    ctime: i64,
    nlink: u32,
    #[serde(default)]
    symlink_target: Option<String>,
    #[serde(default)]
    deleted: bool,
}

impl StoredNode {
    fn to_attr(&self) -> FileAttr {
        let size = self
            .symlink_target
            .as_ref()
            .map(|target| target.len() as u64)
            .unwrap_or(self.size);
        let blocks = if self.symlink_target.is_some() {
            size.div_ceil(512)
        } else {
            self.blocks
        };
        FileAttr {
            ino: self.ino,
            size,
            blocks,
            kind: self.kind.into(),
            mode: self.mode,
            rdev: self.rdev,
            uid: self.uid,
            gid: self.gid,
            atime: self.atime,
            mtime: self.mtime,
            ctime: self.ctime,
            nlink: self.nlink,
        }
    }
}

#[async_trait]
impl MetaStore for TiKvMetaStore {
    fn name(&self) -> &'static str {
        "tikv"
    }

    fn capabilities(&self) -> MetaStoreCapabilities {
        MetaStoreCapabilities {
            namespace: true,
            file_data: true,
            hardlinks: true,
            symlinks: true,
            rename_exchange: true,
            stat_fs: true,
            compaction: true,
            plocks: true,
            ..MetaStoreCapabilities::default()
        }
    }

    async fn from_config(config: Config) -> Result<Self, MetaError> {
        Self::from_config_inner(config).await
    }

    async fn stat(&self, ino: i64) -> Result<Option<FileAttr>, MetaError> {
        let operation = "stat";
        self.read_txn(operation, |store, txn| {
            Box::pin(async move {
                store
                    .txn_get_node(txn, ino, false, operation)
                    .await
                    .map(|node| node.map(|node| node.to_attr()))
            })
        })
        .await
    }

    async fn stat_fs(&self) -> Result<StatFsSnapshot, MetaError> {
        let operation = "stat_fs";
        self.read_txn(operation, |store, txn| {
            Box::pin(async move {
                let pairs =
                    Self::txn_scan_prefix(txn, store.inode_prefix(), None, operation).await?;
                let mut used_space = 0u64;
                let mut used_inodes = 0u64;

                for pair in pairs {
                    let node = Self::decode_node(pair.value())?;
                    if node.deleted || node.nlink == 0 {
                        continue;
                    }

                    let attr = node.to_attr();
                    if attr.kind != FileType::Dir {
                        used_space =
                            used_space.saturating_add(stat_fs_used_bytes(attr.size, attr.blocks));
                    }
                    used_inodes = used_inodes.saturating_add(1);
                }

                Ok(stat_fs_snapshot_from_usage(used_space, used_inodes))
            })
        })
        .await
    }

    async fn lookup(&self, parent: i64, name: &str) -> Result<Option<i64>, MetaError> {
        let operation = "lookup";
        let name = name.to_string();
        self.read_txn(operation, |store, txn| {
            let name = name.clone();
            Box::pin(async move {
                store
                    .txn_get_dentry(txn, parent, &name, false, operation)
                    .await
                    .map(|dentry| dentry.map(|dentry| dentry.ino))
            })
        })
        .await
    }

    async fn lookup_path(&self, path: &str) -> Result<Option<(i64, FileType)>, MetaError> {
        if path.is_empty() {
            return Ok(None);
        }
        if path == "/" {
            return Ok(Some((ROOT_INODE, FileType::Dir)));
        }

        let operation = "lookup_path";
        let path = path.to_string();
        self.read_txn(operation, |store, txn| {
            let path = path.clone();
            Box::pin(async move {
                let mut current = ROOT_INODE;
                for segment in path.split('/').filter(|part| !part.is_empty()) {
                    let Some(dentry) = store
                        .txn_get_dentry(txn, current, segment, false, operation)
                        .await?
                    else {
                        return Ok(None);
                    };
                    current = dentry.ino;
                }

                Ok(store
                    .txn_get_node(txn, current, false, operation)
                    .await?
                    .map(|node| (node.ino, node.kind.into())))
            })
        })
        .await
    }

    async fn readdir(&self, ino: i64) -> Result<Vec<DirEntry>, MetaError> {
        let operation = "readdir";
        self.read_txn(operation, |store, txn| {
            Box::pin(async move {
                let node = store
                    .txn_get_node(txn, ino, false, operation)
                    .await?
                    .ok_or(MetaError::NotFound(ino))?;
                if node.kind != StoredNodeKind::Dir {
                    return Err(MetaError::NotDirectory(ino));
                }

                let prefix = store.dentry_prefix(ino);
                let pairs = Self::txn_scan_prefix(txn, prefix.clone(), None, operation).await?;
                let mut out = Vec::with_capacity(pairs.len());
                for pair in pairs {
                    let (key, value): (Key, Vec<u8>) = pair.into();
                    let key: Vec<u8> = key.into();
                    let name = String::from_utf8(key[prefix.len()..].to_vec()).map_err(|e| {
                        MetaError::Serialization(format!("TiKV dentry key is not UTF-8: {e}"))
                    })?;
                    let dentry = Self::decode_dentry(&value)?;
                    out.push(DirEntry {
                        name,
                        ino: dentry.ino,
                        kind: dentry.kind.into(),
                    });
                }
                Ok(out)
            })
        })
        .await
    }

    async fn mkdir(&self, parent: i64, name: String) -> Result<i64, MetaError> {
        let operation = "mkdir";
        self.write_txn(operation, |store, txn| {
            let name = name.clone();
            Box::pin(async move {
                store
                    .txn_create_node(txn, parent, name, StoredNodeKind::Dir, operation)
                    .await
            })
        })
        .await
    }

    async fn rmdir(&self, parent: i64, name: &str) -> Result<(), MetaError> {
        let operation = "rmdir";
        let name = name.to_string();
        self.write_txn(operation, |store, txn| {
            let name = name.clone();
            Box::pin(async move {
                let mut parent_node = store.txn_require_dir(txn, parent, true, operation).await?;
                let dentry = store
                    .txn_get_dentry(txn, parent, &name, true, operation)
                    .await?
                    .ok_or(MetaError::NotFound(parent))?;
                if dentry.kind != StoredNodeKind::Dir {
                    return Err(MetaError::NotDirectory(dentry.ino));
                }
                let dir_node = store
                    .txn_get_node(txn, dentry.ino, true, operation)
                    .await?
                    .ok_or(MetaError::NotFound(dentry.ino))?;
                if dir_node.kind != StoredNodeKind::Dir {
                    return Err(MetaError::NotDirectory(dentry.ino));
                }

                let child_prefix = store.dentry_prefix(dentry.ino);
                if !Self::txn_scan_prefix(txn, child_prefix, Some(1), operation)
                    .await?
                    .is_empty()
                {
                    return Err(MetaError::DirectoryNotEmpty(dentry.ino));
                }

                let now = Self::now();
                parent_node.nlink = parent_node.nlink.saturating_sub(1);
                parent_node.mtime = now;
                parent_node.ctime = now;
                store.txn_put_node(txn, &parent_node, operation).await?;
                Self::txn_delete_raw(txn, store.dentry_key(parent, &name), operation).await?;
                Self::txn_delete_raw(txn, store.inode_key(dentry.ino), operation).await
            })
        })
        .await
    }

    async fn create_file(&self, parent: i64, name: String) -> Result<i64, MetaError> {
        let operation = "create_file";
        self.write_txn(operation, |store, txn| {
            let name = name.clone();
            Box::pin(async move {
                store
                    .txn_create_node(txn, parent, name, StoredNodeKind::File, operation)
                    .await
            })
        })
        .await
    }

    #[tracing::instrument(level = "trace", skip(self), fields(parent, name, kind = ?kind, mode, rdev))]
    async fn create_node(
        &self,
        parent: i64,
        name: String,
        kind: FileType,
        mode: u32,
        uid: u32,
        gid: u32,
        rdev: u32,
    ) -> Result<i64, MetaError> {
        if !kind.is_special_node() {
            return Err(MetaError::NotSupported(format!(
                "create_node does not create {:?}",
                kind
            )));
        }

        let operation = "create_node";
        self.write_txn(operation, |store, txn| {
            let name = name.clone();
            Box::pin(async move {
                store
                    .txn_create_node_with_attrs(
                        txn,
                        parent,
                        name,
                        kind.into(),
                        None,
                        Some((kind.mode_type_bits() | (mode & 0o7777), uid, gid, rdev)),
                        operation,
                    )
                    .await
            })
        })
        .await
    }

    async fn link(&self, ino: i64, parent: i64, name: &str) -> Result<FileAttr, MetaError> {
        if ino == ROOT_INODE {
            return Err(MetaError::NotSupported(
                "cannot create hard links to the root inode".to_string(),
            ));
        }

        let operation = "link";
        let name = name.to_string();
        self.write_txn(operation, |store, txn| {
            let name = name.clone();
            Box::pin(async move {
                let mut parent_node = store.txn_require_dir(txn, parent, true, operation).await?;
                if store
                    .txn_get_dentry(txn, parent, &name, true, operation)
                    .await?
                    .is_some()
                {
                    return Err(MetaError::AlreadyExists {
                        parent,
                        name: name.to_string(),
                    });
                }

                let mut node = store
                    .txn_get_node(txn, ino, true, operation)
                    .await?
                    .ok_or(MetaError::NotFound(ino))?;
                if node.kind == StoredNodeKind::Dir {
                    return Err(MetaError::NotSupported(
                        "cannot create hard links to directories".to_string(),
                    ));
                }
                if node.kind == StoredNodeKind::Symlink {
                    return Err(MetaError::NotSupported(
                        "cannot create hard links to symbolic links".to_string(),
                    ));
                }
                if node.deleted || node.nlink == 0 {
                    return Err(MetaError::NotFound(ino));
                }

                let mut link_parents = if node.nlink == 1 {
                    vec![StoredLinkParent {
                        parent: node.parent,
                        name: node.name.clone(),
                    }]
                } else {
                    store
                        .txn_get_link_parents(txn, ino, true, operation)
                        .await?
                };
                link_parents.push(StoredLinkParent {
                    parent,
                    name: name.to_string(),
                });

                let now = Self::now();
                node.nlink = node.nlink.saturating_add(1);
                node.parent = 0;
                node.name.clear();
                node.deleted = false;
                node.mtime = now;
                node.ctime = now;
                parent_node.mtime = now;
                parent_node.ctime = now;

                store
                    .txn_put_link_parents(txn, ino, &link_parents, operation)
                    .await?;
                store.txn_put_node(txn, &node, operation).await?;
                store.txn_put_node(txn, &parent_node, operation).await?;
                store
                    .txn_put_dentry(
                        txn,
                        parent,
                        &name,
                        &StoredDentry {
                            ino,
                            kind: node.kind,
                        },
                        operation,
                    )
                    .await?;

                Ok(node.to_attr())
            })
        })
        .await
    }

    async fn symlink(
        &self,
        parent: i64,
        name: &str,
        target: &str,
    ) -> Result<(i64, FileAttr), MetaError> {
        let operation = "symlink";
        let name = name.to_string();
        let target = target.to_string();
        self.write_txn(operation, |store, txn| {
            let name = name.clone();
            let target = target.clone();
            Box::pin(async move {
                let ino = store
                    .txn_create_node_with_target(
                        txn,
                        parent,
                        name,
                        StoredNodeKind::Symlink,
                        Some(target),
                        operation,
                    )
                    .await?;
                let attr = store
                    .txn_get_node(txn, ino, false, operation)
                    .await?
                    .ok_or(MetaError::NotFound(ino))?
                    .to_attr();
                Ok((ino, attr))
            })
        })
        .await
    }

    async fn read_symlink(&self, ino: i64) -> Result<String, MetaError> {
        let operation = "read_symlink";
        self.read_txn(operation, |store, txn| {
            Box::pin(async move {
                let node = store
                    .txn_get_node(txn, ino, false, operation)
                    .await?
                    .ok_or(MetaError::NotFound(ino))?;
                if node.kind != StoredNodeKind::Symlink {
                    return Err(MetaError::NotSupported(format!(
                        "inode {ino} is not a symbolic link"
                    )));
                }
                node.symlink_target.ok_or_else(|| {
                    MetaError::Internal(format!("symlink target missing for inode {ino}"))
                })
            })
        })
        .await
    }

    async fn unlink(&self, parent: i64, name: &str) -> Result<(), MetaError> {
        let operation = "unlink";
        let name = name.to_string();
        self.write_txn(operation, |store, txn| {
            let name = name.clone();
            Box::pin(async move {
                let mut parent_node = store.txn_require_dir(txn, parent, true, operation).await?;
                let dentry = store
                    .txn_get_dentry(txn, parent, &name, true, operation)
                    .await?
                    .ok_or(MetaError::NotFound(parent))?;

                let now = Self::now();
                store
                    .txn_remove_non_dir_dentry(txn, parent, &name, dentry, now, operation)
                    .await?;
                parent_node.mtime = now;
                parent_node.ctime = now;
                store.txn_put_node(txn, &parent_node, operation).await
            })
        })
        .await
    }

    async fn rename(
        &self,
        old_parent: i64,
        old_name: &str,
        new_parent: i64,
        new_name: String,
    ) -> Result<(), MetaError> {
        if old_parent == new_parent && old_name == new_name {
            return Ok(());
        }

        let operation = "rename";
        let old_name = old_name.to_string();
        self.write_txn(operation, |store, txn| {
            let old_name = old_name.clone();
            let new_name = new_name.clone();
            Box::pin(async move {
                let mut old_parent_node = store
                    .txn_require_dir(txn, old_parent, true, operation)
                    .await?;
                let mut new_parent_node = if old_parent == new_parent {
                    old_parent_node.clone()
                } else {
                    store
                        .txn_require_dir(txn, new_parent, true, operation)
                        .await?
                };
                let source_dentry = store
                    .txn_get_dentry(txn, old_parent, &old_name, true, operation)
                    .await?
                    .ok_or(MetaError::NotFound(old_parent))?;

                let mut source_node = store
                    .txn_get_node(txn, source_dentry.ino, true, operation)
                    .await?
                    .ok_or(MetaError::NotFound(source_dentry.ino))?;
                if source_node.deleted || source_node.nlink == 0 {
                    return Err(MetaError::NotFound(source_dentry.ino));
                }

                let destination = store
                    .txn_get_dentry(txn, new_parent, &new_name, true, operation)
                    .await?;
                let now = Self::now();
                let mut old_parent_nlink_delta = 0;
                let mut new_parent_nlink_delta = 0;

                if let Some(dest_dentry) = destination {
                    if dest_dentry.ino == source_dentry.ino {
                        return Ok(());
                    }

                    let dest_node = store
                        .txn_get_node(txn, dest_dentry.ino, true, operation)
                        .await?
                        .ok_or(MetaError::NotFound(dest_dentry.ino))?;

                    match (source_node.kind, dest_node.kind) {
                        (StoredNodeKind::Dir, StoredNodeKind::Dir) => {
                            let child_prefix = store.dentry_prefix(dest_dentry.ino);
                            if !Self::txn_scan_prefix(txn, child_prefix, Some(1), operation)
                                .await?
                                .is_empty()
                            {
                                return Err(MetaError::DirectoryNotEmpty(dest_dentry.ino));
                            }
                            Self::txn_delete_raw(
                                txn,
                                store.dentry_key(new_parent, &new_name),
                                operation,
                            )
                            .await?;
                            Self::txn_delete_raw(txn, store.inode_key(dest_dentry.ino), operation)
                                .await?;
                            store
                                .txn_delete_link_parents(txn, dest_dentry.ino, operation)
                                .await?;
                            new_parent_nlink_delta -= 1;
                        }
                        (StoredNodeKind::Dir, _) => {
                            return Err(MetaError::Io(std::io::Error::from(
                                std::io::ErrorKind::NotADirectory,
                            )));
                        }
                        (_, StoredNodeKind::Dir) => {
                            return Err(MetaError::Io(std::io::Error::from(
                                std::io::ErrorKind::IsADirectory,
                            )));
                        }
                        _ => {
                            store
                                .txn_remove_non_dir_dentry(
                                    txn,
                                    new_parent,
                                    &new_name,
                                    dest_dentry,
                                    now,
                                    operation,
                                )
                                .await?;
                        }
                    }
                }

                store
                    .txn_move_node_binding(
                        txn,
                        &mut source_node,
                        old_parent,
                        &old_name,
                        new_parent,
                        &new_name,
                        operation,
                    )
                    .await?;
                source_node.ctime = now;
                source_node.mtime = now;

                old_parent_node.mtime = now;
                old_parent_node.ctime = now;
                new_parent_node.mtime = now;
                new_parent_node.ctime = now;
                if source_node.kind == StoredNodeKind::Dir && old_parent != new_parent {
                    old_parent_nlink_delta -= 1;
                    new_parent_nlink_delta += 1;
                }

                Self::txn_delete_raw(txn, store.dentry_key(old_parent, &old_name), operation)
                    .await?;
                store.txn_put_node(txn, &source_node, operation).await?;
                if old_parent == new_parent {
                    old_parent_node.nlink = apply_nlink_delta(
                        old_parent_node.nlink,
                        old_parent_nlink_delta + new_parent_nlink_delta,
                    );
                    store.txn_put_node(txn, &old_parent_node, operation).await?;
                } else {
                    old_parent_node.nlink =
                        apply_nlink_delta(old_parent_node.nlink, old_parent_nlink_delta);
                    new_parent_node.nlink =
                        apply_nlink_delta(new_parent_node.nlink, new_parent_nlink_delta);
                    store.txn_put_node(txn, &old_parent_node, operation).await?;
                    store.txn_put_node(txn, &new_parent_node, operation).await?;
                }

                store
                    .txn_put_dentry(txn, new_parent, &new_name, &source_dentry, operation)
                    .await
            })
        })
        .await
    }

    async fn rename_exchange(
        &self,
        old_parent: i64,
        old_name: &str,
        new_parent: i64,
        new_name: &str,
    ) -> Result<(), MetaError> {
        if old_parent == new_parent && old_name == new_name {
            return Ok(());
        }

        let operation = "rename_exchange";
        let old_name = old_name.to_string();
        let new_name = new_name.to_string();
        self.write_txn(operation, |store, txn| {
            let old_name = old_name.clone();
            let new_name = new_name.clone();
            Box::pin(async move {
                let mut old_parent_node = store
                    .txn_require_dir(txn, old_parent, true, operation)
                    .await?;
                let mut new_parent_node = if old_parent == new_parent {
                    old_parent_node.clone()
                } else {
                    store
                        .txn_require_dir(txn, new_parent, true, operation)
                        .await?
                };

                let old_dentry = store
                    .txn_get_dentry(txn, old_parent, &old_name, true, operation)
                    .await?
                    .ok_or(MetaError::NotFound(old_parent))?;
                let new_dentry = store
                    .txn_get_dentry(txn, new_parent, &new_name, true, operation)
                    .await?
                    .ok_or(MetaError::NotFound(new_parent))?;

                if old_dentry.ino == new_dentry.ino {
                    return Ok(());
                }

                let mut old_node = store
                    .txn_get_node(txn, old_dentry.ino, true, operation)
                    .await?
                    .ok_or(MetaError::NotFound(old_dentry.ino))?;
                let mut new_node = store
                    .txn_get_node(txn, new_dentry.ino, true, operation)
                    .await?
                    .ok_or(MetaError::NotFound(new_dentry.ino))?;

                if old_node.deleted || old_node.nlink == 0 {
                    return Err(MetaError::NotFound(old_dentry.ino));
                }
                if new_node.deleted || new_node.nlink == 0 {
                    return Err(MetaError::NotFound(new_dentry.ino));
                }

                let now = Self::now();
                let mut old_parent_nlink_delta = 0;
                let mut new_parent_nlink_delta = 0;

                if old_parent != new_parent {
                    if old_node.kind == StoredNodeKind::Dir {
                        old_parent_nlink_delta -= 1;
                        new_parent_nlink_delta += 1;
                    }
                    if new_node.kind == StoredNodeKind::Dir {
                        new_parent_nlink_delta -= 1;
                        old_parent_nlink_delta += 1;
                    }
                }

                store
                    .txn_move_node_binding(
                        txn,
                        &mut old_node,
                        old_parent,
                        &old_name,
                        new_parent,
                        &new_name,
                        operation,
                    )
                    .await?;
                store
                    .txn_move_node_binding(
                        txn,
                        &mut new_node,
                        new_parent,
                        &new_name,
                        old_parent,
                        &old_name,
                        operation,
                    )
                    .await?;

                old_node.mtime = now;
                old_node.ctime = now;
                new_node.mtime = now;
                new_node.ctime = now;

                Self::txn_put_raw(
                    txn,
                    store.dentry_key(old_parent, &old_name),
                    Self::encode(&new_dentry)?,
                    operation,
                )
                .await?;
                Self::txn_put_raw(
                    txn,
                    store.dentry_key(new_parent, &new_name),
                    Self::encode(&old_dentry)?,
                    operation,
                )
                .await?;
                store.txn_put_node(txn, &old_node, operation).await?;
                store.txn_put_node(txn, &new_node, operation).await?;

                old_parent_node.mtime = now;
                old_parent_node.ctime = now;
                new_parent_node.mtime = now;
                new_parent_node.ctime = now;
                if old_parent == new_parent {
                    old_parent_node.nlink = apply_nlink_delta(
                        old_parent_node.nlink,
                        old_parent_nlink_delta + new_parent_nlink_delta,
                    );
                    store.txn_put_node(txn, &old_parent_node, operation).await?;
                } else {
                    old_parent_node.nlink =
                        apply_nlink_delta(old_parent_node.nlink, old_parent_nlink_delta);
                    new_parent_node.nlink =
                        apply_nlink_delta(new_parent_node.nlink, new_parent_nlink_delta);
                    store.txn_put_node(txn, &old_parent_node, operation).await?;
                    store.txn_put_node(txn, &new_parent_node, operation).await?;
                }

                Ok(())
            })
        })
        .await
    }

    async fn set_file_size(&self, ino: i64, size: u64) -> Result<(), MetaError> {
        let operation = "set_file_size";
        self.write_txn(operation, |store, txn| {
            Box::pin(async move {
                let mut node = store
                    .txn_get_node(txn, ino, true, operation)
                    .await?
                    .ok_or(MetaError::NotFound(ino))?;
                if node.kind != StoredNodeKind::File {
                    return Err(MetaError::NotSupported(
                        "TiKV set_file_size currently supports only regular files".to_string(),
                    ));
                }
                let now = Self::now();
                node.size = size;
                node.blocks = size.div_ceil(512);
                node.mtime = now;
                node.ctime = now;
                store.txn_put_node(txn, &node, operation).await
            })
        })
        .await
    }

    async fn extend_file_size(&self, ino: i64, size: u64) -> Result<(), MetaError> {
        let operation = "extend_file_size";
        self.write_txn(operation, |store, txn| {
            Box::pin(async move {
                let mut node = store
                    .txn_get_node(txn, ino, true, operation)
                    .await?
                    .ok_or(MetaError::NotFound(ino))?;
                if node.kind != StoredNodeKind::File {
                    return Err(MetaError::NotSupported(
                        "TiKV extend_file_size currently supports only regular files".to_string(),
                    ));
                }

                if size > node.size {
                    let now = Self::now();
                    node.size = size;
                    node.blocks = size.div_ceil(512);
                    node.mtime = now;
                    node.ctime = now;
                    store.txn_put_node(txn, &node, operation).await?;
                }
                Ok(())
            })
        })
        .await
    }

    async fn set_attr(
        &self,
        ino: i64,
        req: &SetAttrRequest,
        flags: SetAttrFlags,
    ) -> Result<FileAttr, MetaError> {
        let operation = "set_attr";
        let req = *req;
        let clear_suid = flags.contains(SetAttrFlags::CLEAR_SUID);
        let clear_sgid = flags.contains(SetAttrFlags::CLEAR_SGID);
        let set_atime_now = flags.contains(SetAttrFlags::SET_ATIME_NOW);
        let set_mtime_now = flags.contains(SetAttrFlags::SET_MTIME_NOW);
        self.write_txn(operation, |store, txn| {
            Box::pin(async move {
                let mut node = store
                    .txn_get_node(txn, ino, true, operation)
                    .await?
                    .ok_or(MetaError::NotFound(ino))?;
                let now = Self::now();
                let mut ctime_update = false;

                if let Some(mode) = req.mode {
                    let kind_bits = node.mode & 0o170000;
                    node.mode = kind_bits | (mode & 0o7777);
                    ctime_update = true;
                }
                if let Some(uid) = req.uid {
                    node.uid = uid;
                    ctime_update = true;
                }
                if let Some(gid) = req.gid {
                    node.gid = gid;
                    ctime_update = true;
                }
                if clear_suid {
                    node.mode &= !0o4000;
                    ctime_update = true;
                }
                if clear_sgid {
                    node.mode &= !0o2000;
                    ctime_update = true;
                }

                if let Some(size) = req.size {
                    if node.kind != StoredNodeKind::File {
                        return Err(MetaError::NotSupported(
                            "truncate flag only supported for regular files".to_string(),
                        ));
                    }
                    if node.size != size {
                        node.size = size;
                        node.blocks = size.div_ceil(512);
                        node.mtime = now;
                    }
                    ctime_update = true;
                }

                if set_atime_now {
                    node.atime = now;
                    ctime_update = true;
                } else if let Some(atime) = req.atime {
                    node.atime = atime;
                    ctime_update = true;
                }

                if set_mtime_now {
                    node.mtime = now;
                    ctime_update = true;
                } else if let Some(mtime) = req.mtime {
                    node.mtime = mtime;
                    ctime_update = true;
                }

                if let Some(ctime) = req.ctime {
                    node.ctime = ctime;
                } else if ctime_update {
                    node.ctime = now;
                }

                store.txn_put_node(txn, &node, operation).await?;
                Ok(node.to_attr())
            })
        })
        .await
    }

    async fn open(&self, ino: i64, flags: OpenFlags) -> Result<FileAttr, MetaError> {
        let operation = "open";
        let truncate = flags.contains(OpenFlags::TRUNC);
        self.write_txn(operation, |store, txn| {
            Box::pin(async move {
                let mut node = store
                    .txn_get_node(txn, ino, true, operation)
                    .await?
                    .ok_or(MetaError::NotFound(ino))?;
                if node.kind == StoredNodeKind::Symlink {
                    return Err(MetaError::NotSupported(
                        "opening symlink targets is not implemented".to_string(),
                    ));
                }
                if truncate && node.kind != StoredNodeKind::File {
                    return Err(MetaError::NotSupported(
                        "truncate flag only supported for regular files".to_string(),
                    ));
                }

                let now = Self::now();
                node.atime = now;
                if truncate {
                    node.size = 0;
                    node.blocks = 0;
                    node.mtime = now;
                    node.ctime = now;
                }

                store.txn_put_node(txn, &node, operation).await?;
                Ok(node.to_attr())
            })
        })
        .await
    }

    async fn close(&self, ino: i64) -> Result<(), MetaError> {
        if self.stat(ino).await?.is_some() {
            Ok(())
        } else {
            Err(MetaError::NotFound(ino))
        }
    }

    async fn get_names(&self, ino: i64) -> Result<Vec<(Option<i64>, String)>, MetaError> {
        if ino == ROOT_INODE {
            return Ok(vec![(None, "/".to_string())]);
        }

        let operation = "get_names";
        self.read_txn(operation, |store, txn| {
            Box::pin(async move {
                let Some(node) = store.txn_get_node(txn, ino, false, operation).await? else {
                    return Ok(Vec::new());
                };
                if node.deleted || node.nlink == 0 {
                    return Ok(Vec::new());
                }
                if node.kind == StoredNodeKind::Dir || node.nlink <= 1 {
                    return Ok(vec![(Some(node.parent), node.name)]);
                }

                let mut out: Vec<_> = store
                    .txn_get_link_parents(txn, ino, false, operation)
                    .await?
                    .into_iter()
                    .map(|link| (Some(link.parent), link.name))
                    .collect();
                out.sort();
                out.dedup();
                Ok(out)
            })
        })
        .await
    }

    async fn get_paths(&self, ino: i64) -> Result<Vec<String>, MetaError> {
        if ino == ROOT_INODE {
            return Ok(vec!["/".to_string()]);
        }

        let operation = "get_paths";
        self.read_txn(operation, |store, txn| {
            Box::pin(async move {
                let Some(node) = store.txn_get_node(txn, ino, false, operation).await? else {
                    return Ok(Vec::new());
                };
                if node.deleted || node.nlink == 0 {
                    return Ok(Vec::new());
                }

                let bindings = if node.kind == StoredNodeKind::Dir || node.nlink <= 1 {
                    vec![StoredLinkParent {
                        parent: node.parent,
                        name: node.name,
                    }]
                } else {
                    store
                        .txn_get_link_parents(txn, ino, false, operation)
                        .await?
                };

                let mut out = Vec::with_capacity(bindings.len());
                for binding in bindings {
                    let mut parts = vec![binding.name];
                    let mut current_parent = binding.parent;
                    while current_parent != ROOT_INODE {
                        let Some(parent) = store
                            .txn_get_node(txn, current_parent, false, operation)
                            .await?
                        else {
                            parts.clear();
                            break;
                        };
                        if parent.deleted || parent.nlink == 0 {
                            parts.clear();
                            break;
                        }
                        parts.push(parent.name);
                        current_parent = parent.parent;
                    }
                    if !parts.is_empty() {
                        parts.reverse();
                        out.push(format!("/{}", parts.join("/")));
                    }
                }
                out.sort();
                out.dedup();
                Ok(out)
            })
        })
        .await
    }

    fn root_ino(&self) -> i64 {
        ROOT_INODE
    }

    async fn initialize(&self) -> Result<(), MetaError> {
        let operation = "initialize";
        let (root_exists, counter_exists) = self
            .read_txn(operation, |store, txn| {
                Box::pin(async move {
                    let root_exists = store
                        .txn_get_node(txn, ROOT_INODE, false, operation)
                        .await?
                        .is_some();
                    let counter_exists =
                        Self::txn_get_raw(txn, store.counter_key(INODE_ID_KEY), false, operation)
                            .await?
                            .is_some();
                    Ok((root_exists, counter_exists))
                })
            })
            .await?;

        if root_exists && counter_exists {
            return Ok(());
        }

        self.write_txn(operation, |store, txn| {
            Box::pin(async move {
                if store
                    .txn_get_node(txn, ROOT_INODE, true, operation)
                    .await?
                    .is_none()
                {
                    store
                        .txn_put_node(txn, &Self::root_node(), operation)
                        .await?;
                }

                let counter_key = store.counter_key(INODE_ID_KEY);
                if Self::txn_get_raw(txn, counter_key.clone(), true, operation)
                    .await?
                    .is_none()
                {
                    Self::txn_put_raw(
                        txn,
                        counter_key,
                        Self::encode(&FIRST_ALLOCATED_INODE)?,
                        operation,
                    )
                    .await?;
                }
                Ok(())
            })
        })
        .await
    }

    async fn get_deleted_files(&self) -> Result<Vec<i64>, MetaError> {
        let operation = "get_deleted_files";
        self.read_txn(operation, |store, txn| {
            Box::pin(async move {
                let pairs =
                    Self::txn_scan_prefix(txn, store.inode_prefix(), None, operation).await?;
                let mut out = Vec::new();
                for pair in pairs {
                    let node = Self::decode_node(pair.value())?;
                    if node.deleted && node.kind != StoredNodeKind::Dir {
                        out.push(node.ino);
                    }
                }
                Ok(out)
            })
        })
        .await
    }

    async fn remove_file_metadata(&self, ino: i64) -> Result<(), MetaError> {
        let operation = "remove_file_metadata";
        self.write_txn(operation, |store, txn| {
            Box::pin(async move {
                Self::txn_delete_raw(txn, store.inode_key(ino), operation).await?;
                store.txn_delete_link_parents(txn, ino, operation).await
            })
        })
        .await
    }

    async fn get_slices(&self, chunk_id: u64) -> Result<Vec<SliceDesc>, MetaError> {
        let operation = "get_slices";
        self.read_txn(operation, |store, txn| {
            Box::pin(async move { store.txn_get_slices(txn, chunk_id, false, operation).await })
        })
        .await
    }

    async fn list_chunk_ids(&self, limit: usize) -> Result<Vec<u64>, MetaError> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let operation = "list_chunk_ids";
        self.read_txn(operation, |store, txn| {
            Box::pin(async move {
                let prefix = store.chunk_prefix();
                let pairs =
                    Self::txn_scan_prefix(txn, prefix.clone(), Some(limit), operation).await?;
                let mut out = Vec::with_capacity(pairs.len());
                for pair in pairs {
                    let key: Vec<u8> = pair.into_key().into();
                    let suffix = std::str::from_utf8(&key[prefix.len()..]).map_err(|e| {
                        MetaError::Serialization(format!("TiKV chunk key is not UTF-8: {e}"))
                    })?;
                    if let Ok(chunk_id) = suffix.parse::<u64>() {
                        out.push(chunk_id);
                    }
                }
                Ok(out)
            })
        })
        .await
    }

    async fn replace_slices_for_compact(
        &self,
        chunk_id: u64,
        new_slices: &[SliceDesc],
        old_slices_to_delay: &[u8],
    ) -> Result<(), MetaError> {
        if !old_slices_to_delay.is_empty() && !old_slices_to_delay.len().is_multiple_of(20) {
            return Err(MetaError::Internal(
                "Invalid delayed data length".to_string(),
            ));
        }

        let delayed_slices = SliceDesc::decode_delayed_data(old_slices_to_delay)
            .ok_or_else(|| MetaError::Internal("Invalid delayed data length".to_string()))?;
        let delayed_ids: HashSet<u64> = delayed_slices
            .iter()
            .map(|(slice_id, _, _)| *slice_id)
            .collect();
        let new_slices = new_slices.to_vec();
        let now = Self::now_secs();
        let operation = "replace_slices_for_compact";

        self.write_txn(operation, |store, txn| {
            let delayed_slices = delayed_slices.clone();
            let delayed_ids = delayed_ids.clone();
            let new_slices = new_slices.clone();
            Box::pin(async move {
                let mut updated = store.txn_get_slices(txn, chunk_id, true, operation).await?;
                if !delayed_ids.is_empty() {
                    updated.retain(|slice| !delayed_ids.contains(&slice.slice_id));
                }
                updated.extend(new_slices);
                store
                    .txn_put_slices_or_delete(txn, chunk_id, &updated, operation)
                    .await?;
                store
                    .txn_stage_delayed_slice_records(txn, chunk_id, &delayed_slices, now, operation)
                    .await
            })
        })
        .await
    }

    async fn replace_slices_for_compact_with_version(
        &self,
        chunk_id: u64,
        new_slices: &[SliceDesc],
        old_slices_to_delay: &[u8],
        expected_slices: &[SliceDesc],
    ) -> Result<(), MetaError> {
        if !old_slices_to_delay.is_empty() && !old_slices_to_delay.len().is_multiple_of(20) {
            return Err(MetaError::Internal(
                "Invalid delayed data length".to_string(),
            ));
        }

        let delayed_slices = SliceDesc::decode_delayed_data(old_slices_to_delay)
            .ok_or_else(|| MetaError::Internal("Invalid delayed data length".to_string()))?;
        let new_slices = new_slices.to_vec();
        let expected_slices = expected_slices.to_vec();
        let now = Self::now_secs();
        let operation = "replace_slices_for_compact_with_version";

        self.write_txn(operation, |store, txn| {
            let delayed_slices = delayed_slices.clone();
            let new_slices = new_slices.clone();
            let expected_slices = expected_slices.clone();
            Box::pin(async move {
                let current = store.txn_get_slices(txn, chunk_id, true, operation).await?;
                if current != expected_slices {
                    return Err(MetaError::ContinueRetry(RetryReason::CompactConflict));
                }

                store
                    .txn_put_slices_or_delete(txn, chunk_id, &new_slices, operation)
                    .await?;

                for slice in &new_slices {
                    Self::txn_delete_raw(
                        txn,
                        store.uncommitted_pending_key(slice.slice_id),
                        operation,
                    )
                    .await?;
                    Self::txn_delete_raw(
                        txn,
                        store.uncommitted_orphan_key(slice.slice_id),
                        operation,
                    )
                    .await?;
                }

                store
                    .txn_stage_delayed_slice_records(txn, chunk_id, &delayed_slices, now, operation)
                    .await
            })
        })
        .await
    }

    async fn record_uncommitted_slice(
        &self,
        slice_id: u64,
        chunk_id: u64,
        size: u64,
        operation_name: &str,
    ) -> Result<i64, MetaError> {
        let operation = "record_uncommitted_slice";
        let operation_name = operation_name.to_string();
        let now = Self::now_secs();

        self.write_txn(operation, |store, txn| {
            let operation_name = operation_name.clone();
            Box::pin(async move {
                let pending_key = store.uncommitted_pending_key(slice_id);
                if let Some(existing) =
                    Self::txn_get_raw(txn, pending_key.clone(), true, operation).await?
                {
                    return Ok(Self::decode_uncommitted_slice(&existing)?.id);
                }

                let orphan_key = store.uncommitted_orphan_key(slice_id);
                let existing_orphan =
                    Self::txn_get_raw(txn, orphan_key.clone(), true, operation).await?;

                let id = match existing_orphan.as_deref() {
                    Some(bytes) => Self::decode_uncommitted_slice(bytes)?.id,
                    None => {
                        let counter_key = store.counter_key(UNCOMMITTED_ID_COUNTER);
                        let next_id = Self::txn_get_raw(txn, counter_key.clone(), true, operation)
                            .await?
                            .as_deref()
                            .map(Self::decode_counter)
                            .transpose()?
                            .unwrap_or(0)
                            .checked_add(1)
                            .ok_or_else(|| {
                                MetaError::Internal(
                                    "TiKV uncommitted slice id overflow".to_string(),
                                )
                            })?;
                        Self::txn_put_raw(txn, counter_key, Self::encode(&next_id)?, operation)
                            .await?;
                        next_id
                    }
                };

                let record = StoredUncommittedSliceRecord {
                    id,
                    slice_id,
                    chunk_id,
                    size,
                    created_at: now,
                    operation: operation_name,
                    status: "pending".to_string(),
                };

                Self::txn_delete_raw(txn, orphan_key, operation).await?;
                Self::txn_put_raw(txn, pending_key, Self::encode(&record)?, operation).await?;
                Ok(id)
            })
        })
        .await
    }

    async fn confirm_slice_committed(&self, slice_id: u64) -> Result<(), MetaError> {
        let operation = "confirm_slice_committed";
        self.write_txn(operation, |store, txn| {
            Box::pin(async move {
                Self::txn_delete_raw(txn, store.uncommitted_pending_key(slice_id), operation)
                    .await?;
                Self::txn_delete_raw(txn, store.uncommitted_orphan_key(slice_id), operation).await
            })
        })
        .await
    }

    async fn process_delayed_slices(
        &self,
        batch_size: usize,
        max_age_secs: i64,
    ) -> Result<Vec<(u64, u64, u64, i64)>, MetaError> {
        if batch_size == 0 {
            return Ok(Vec::new());
        }

        let cutoff_time = Self::now_secs() - max_age_secs;
        let operation = "process_delayed_slices";
        let mut records = self
            .read_txn(operation, |store, txn| {
                Box::pin(async move {
                    let mut records = Vec::new();
                    for prefix in [
                        store.delayed_pending_prefix(),
                        store.delayed_meta_deleted_prefix(),
                    ] {
                        let pairs = Self::txn_scan_prefix(txn, prefix, None, operation).await?;
                        for pair in pairs {
                            let record = Self::decode_delayed_slice(pair.value())?;
                            if (record.status == "pending" || record.status == "meta_deleted")
                                && record.created_at <= cutoff_time
                            {
                                records.push(record);
                            }
                        }
                    }
                    records.sort_by_key(|record| record.id);
                    records.truncate(batch_size);
                    Ok(records)
                })
            })
            .await?;

        if records.is_empty() {
            return Ok(Vec::new());
        }

        let mut ready = Vec::new();
        for record in records.drain(..) {
            if record.status == "meta_deleted" {
                ready.push((record.slice_id, record.offset, record.size, record.id));
                continue;
            }

            let processed = self
                .write_txn(operation, |store, txn| {
                    let record = record.clone();
                    Box::pin(async move {
                        let pending_key = store.delayed_pending_key(record.id);
                        let meta_deleted_key = store.delayed_meta_deleted_key(record.id);
                        let Some(current_bytes) =
                            Self::txn_get_raw(txn, pending_key.clone(), true, operation).await?
                        else {
                            return Ok(None);
                        };
                        let current = Self::decode_delayed_slice(&current_bytes)?;
                        if current.status != "pending" || current.created_at > cutoff_time {
                            return Ok(None);
                        }

                        let mut slices = store
                            .txn_get_slices(txn, current.chunk_id, true, operation)
                            .await?;
                        slices.retain(|slice| slice.slice_id != current.slice_id);
                        store
                            .txn_put_slices_or_delete(txn, current.chunk_id, &slices, operation)
                            .await?;

                        let mut updated = current.clone();
                        updated.status = "meta_deleted".to_string();
                        Self::txn_delete_raw(txn, pending_key, operation).await?;
                        Self::txn_put_raw(
                            txn,
                            meta_deleted_key,
                            Self::encode(&updated)?,
                            operation,
                        )
                        .await?;
                        Ok(Some((
                            current.slice_id,
                            current.offset,
                            current.size,
                            current.id,
                        )))
                    })
                })
                .await?;

            if let Some(entry) = processed {
                ready.push(entry);
            }
        }

        Ok(ready)
    }

    async fn confirm_delayed_deleted(&self, delayed_ids: &[i64]) -> Result<(), MetaError> {
        if delayed_ids.is_empty() {
            return Ok(());
        }

        let operation = "confirm_delayed_deleted";
        let delayed_ids = delayed_ids.to_vec();
        self.write_txn(operation, |store, txn| {
            let delayed_ids = delayed_ids.clone();
            Box::pin(async move {
                for delayed_id in delayed_ids {
                    Self::txn_delete_raw(txn, store.delayed_pending_key(delayed_id), operation)
                        .await?;
                    Self::txn_delete_raw(
                        txn,
                        store.delayed_meta_deleted_key(delayed_id),
                        operation,
                    )
                    .await?;
                }
                Ok(())
            })
        })
        .await
    }

    async fn cleanup_orphan_uncommitted_slices(
        &self,
        max_age_secs: i64,
        batch_size: usize,
    ) -> Result<Vec<(u64, u64)>, MetaError> {
        if batch_size == 0 {
            return Ok(Vec::new());
        }

        let cutoff_time = Self::now_secs() - max_age_secs;
        let operation = "cleanup_orphan_uncommitted_slices";
        let (pending_records, orphan_records) = self
            .read_txn(operation, |store, txn| {
                Box::pin(async move {
                    let mut pending_records = Vec::new();
                    let pending_pairs = Self::txn_scan_prefix(
                        txn,
                        store.uncommitted_pending_prefix(),
                        None,
                        operation,
                    )
                    .await?;
                    for pair in pending_pairs {
                        let record = Self::decode_uncommitted_slice(pair.value())?;
                        if record.status == "pending" && record.created_at < cutoff_time {
                            pending_records.push(record);
                        }
                    }
                    pending_records.sort_by_key(|record| record.id);
                    pending_records.truncate(batch_size);

                    let mut orphan_records = Vec::new();
                    let orphan_pairs = Self::txn_scan_prefix(
                        txn,
                        store.uncommitted_orphan_prefix(),
                        None,
                        operation,
                    )
                    .await?;
                    for pair in orphan_pairs {
                        let record = Self::decode_uncommitted_slice(pair.value())?;
                        if record.status == "orphan" {
                            orphan_records.push(record);
                        }
                    }
                    orphan_records.sort_by_key(|record| record.id);
                    orphan_records.truncate(batch_size);

                    Ok((pending_records, orphan_records))
                })
            })
            .await?;

        if pending_records.is_empty() && orphan_records.is_empty() {
            return Ok(Vec::new());
        }

        let mut cleaned = Vec::new();
        let mut seen = HashSet::new();
        for record in pending_records {
            let transition = self
                .write_txn(operation, |store, txn| {
                    let record = record.clone();
                    Box::pin(async move {
                        let pending_key = store.uncommitted_pending_key(record.slice_id);
                        let orphan_key = store.uncommitted_orphan_key(record.slice_id);
                        let Some(current_bytes) =
                            Self::txn_get_raw(txn, pending_key.clone(), true, operation).await?
                        else {
                            return Ok(None);
                        };
                        let current = Self::decode_uncommitted_slice(&current_bytes)?;
                        if current.status != "pending" || current.created_at >= cutoff_time {
                            return Ok(None);
                        }

                        let slices = store
                            .txn_get_slices(txn, current.chunk_id, true, operation)
                            .await?;
                        if slices
                            .iter()
                            .any(|slice| slice.slice_id == current.slice_id)
                        {
                            Self::txn_delete_raw(txn, pending_key, operation).await?;
                            return Ok(None);
                        }

                        let mut orphan = current.clone();
                        orphan.status = "orphan".to_string();
                        Self::txn_delete_raw(txn, pending_key, operation).await?;
                        Self::txn_put_raw(txn, orphan_key, Self::encode(&orphan)?, operation)
                            .await?;
                        Ok(Some((current.slice_id, current.size)))
                    })
                })
                .await?;

            if let Some((slice_id, size)) = transition
                && seen.insert(slice_id)
            {
                cleaned.push((slice_id, size));
            }
        }

        for record in orphan_records {
            if seen.insert(record.slice_id) {
                cleaned.push((record.slice_id, record.size));
            }
        }

        Ok(cleaned)
    }

    async fn delete_uncommitted_slices(&self, slice_ids: &[u64]) -> Result<(), MetaError> {
        if slice_ids.is_empty() {
            return Ok(());
        }

        let operation = "delete_uncommitted_slices";
        let slice_ids = slice_ids.to_vec();
        self.write_txn(operation, |store, txn| {
            let slice_ids = slice_ids.clone();
            Box::pin(async move {
                for slice_id in slice_ids {
                    Self::txn_delete_raw(txn, store.uncommitted_pending_key(slice_id), operation)
                        .await?;
                    Self::txn_delete_raw(txn, store.uncommitted_orphan_key(slice_id), operation)
                        .await?;
                }
                Ok(())
            })
        })
        .await
    }

    async fn append_slice(&self, chunk_id: u64, slice: SliceDesc) -> Result<(), MetaError> {
        let operation = "append_slice";
        self.write_txn(operation, |store, txn| {
            Box::pin(async move {
                let mut slices = store.txn_get_slices(txn, chunk_id, true, operation).await?;
                slices.push(slice);
                store
                    .txn_put_slices_or_delete(txn, chunk_id, &slices, operation)
                    .await
            })
        })
        .await
    }

    async fn write(
        &self,
        ino: i64,
        chunk_id: u64,
        slice: SliceDesc,
        new_size: u64,
    ) -> Result<(), MetaError> {
        let operation = "write";
        self.write_txn(operation, |store, txn| {
            Box::pin(async move {
                let mut node = store
                    .txn_get_node(txn, ino, true, operation)
                    .await?
                    .ok_or(MetaError::NotFound(ino))?;
                if node.kind != StoredNodeKind::File {
                    return Err(MetaError::NotSupported(
                        "TiKV write currently supports only regular files".to_string(),
                    ));
                }

                let mut slices = store.txn_get_slices(txn, chunk_id, true, operation).await?;
                slices.push(slice);
                store
                    .txn_put_slices_or_delete(txn, chunk_id, &slices, operation)
                    .await?;

                if new_size > node.size {
                    let now = Self::now();
                    node.size = new_size;
                    node.blocks = new_size.div_ceil(512);
                    node.mtime = now;
                    node.ctime = now;
                    store.txn_put_node(txn, &node, operation).await?;
                }
                Ok(())
            })
        })
        .await
    }

    async fn get_plock(
        &self,
        inode: i64,
        query: &FileLockQuery,
    ) -> Result<FileLockInfo, MetaError> {
        let operation = "get_plock";
        let sid = self.lock_session;
        let query = *query;
        self.read_txn(operation, |store, txn| {
            Box::pin(async move {
                let plocks = store.txn_get_plocks(txn, inode, false, operation).await?;

                if let Some(current) = plocks
                    .iter()
                    .find(|plock| plock.sid == sid && plock.owner == query.owner)
                    && let Some(info) =
                        PlockRecord::get_plock(&current.records, &query, &sid, &current.sid)
                {
                    return Ok(info);
                }

                for plock in &plocks {
                    if plock.sid == sid && plock.owner == query.owner {
                        continue;
                    }
                    if let Some(info) =
                        PlockRecord::get_plock(&plock.records, &query, &sid, &plock.sid)
                    {
                        return Ok(info);
                    }
                }

                Ok(FileLockInfo {
                    lock_type: FileLockType::UnLock,
                    range: FileLockRange { start: 0, end: 0 },
                    pid: 0,
                })
            })
        })
        .await
    }

    async fn set_plock(
        &self,
        inode: i64,
        owner: i64,
        block: bool,
        lock_type: FileLockType,
        range: FileLockRange,
        pid: u32,
    ) -> Result<(), MetaError> {
        let new_lock = PlockRecord::new(lock_type, pid, range.start, range.end);
        let mut attempt = 0usize;

        loop {
            match self
                .try_set_plock(inode, owner, new_lock, lock_type, range)
                .await
            {
                Ok(()) => return Ok(()),
                Err(MetaError::LockConflict { .. }) if block => {
                    Self::retry_delay(attempt).await;
                    attempt = attempt.saturating_add(1);
                }
                Err(err) => return Err(err),
            }
        }
    }

    async fn next_id(&self, key: &str) -> Result<i64, MetaError> {
        if key == SLICE_ID_KEY {
            return self.next_leased_slice_id().await;
        }

        let operation = "next_id";
        let key = key.to_string();
        self.write_txn(operation, |store, txn| {
            let key = key.clone();
            Box::pin(async move { store.txn_next_counter(txn, &key, 1, operation).await })
        })
        .await
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

fn normalize_namespace(namespace: &str) -> String {
    let namespace = namespace.trim_matches('/');
    if namespace.is_empty() {
        default_tikv_namespace()
    } else {
        namespace.to_string()
    }
}

fn prefix_range_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    for idx in (0..end.len()).rev() {
        if end[idx] != 0xff {
            end[idx] += 1;
            end.truncate(idx + 1);
            return Some(end);
        }
    }
    None
}

fn apply_nlink_delta(nlink: u32, delta: i32) -> u32 {
    if delta >= 0 {
        nlink.saturating_add(delta as u32)
    } else {
        nlink.saturating_sub(delta.unsigned_abs())
    }
}

#[cfg(test)]
mod tests;
