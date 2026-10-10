//! Immutable native TiKV namespace over the exact data layout of a pinned PM10.
//! Import may read packed namespace pages; mounted native lookup never does.
//! Matched experiments require inline-off snapshots: copying inline payloads
//! from packed metadata to TiKV would change their physical data placement.

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU8, AtomicU64, Ordering};
use tikv_client::{CheckLevel, Transaction, TransactionClient, TransactionOptions};

use super::budget::V3Owned;
use super::manifest::V3ReadPlacementInput;
use super::{
    AuthenticatedV3Snapshot, V3BudgetPool, V3IndexReader, V3ObjectKind, V3ObjectRef, V3OwnedPermit,
    V3Placement, V3RootKind,
};
use crate::cadapter::client::{ObjectBackend, ObjectClient};
use crate::chunk::read_plan::{PreparedUnifiedRead, ResolvedReadPlan, WorkspaceReadPlanProvider};
use crate::chunk::{BlockKey, BlockStore, SliceDesc};
use crate::meta::client::session::SessionInfo;
use crate::meta::file_lock::{FileLockInfo, FileLockQuery, FileLockRange, FileLockType};
use crate::meta::layer::MetaLayer;
use crate::meta::store::{
    AclRule, CreateEntryResult, DirEntry, FileAttr, FileType, MetaError, OpenFlags, SetAttrFlags,
    SetAttrRequest, StatFsSnapshot, stat_fs_snapshot_from_usage,
};
use crate::vfs::handles::{DirHandle, DirectoryPageSource, OwnedDirectoryPage, RawDirEntry};
use crate::workspace_overlay::packed_v3::wire::{PackedResult, PackedWireError, Reader, Writer};
use crate::workspace_overlay::packed_v3::{GroupMetaEntry, GroupMetaExtent};

const MAX_RECORD: usize = 1 << 20;
const PAGE: usize = 64;

fn invalid(message: &str) -> PackedWireError {
    PackedWireError::Invalid(format!("native placement: {message}"))
}
fn backend(error: impl std::fmt::Display) -> PackedWireError {
    PackedWireError::Backend(format!("native TiKV: {error}"))
}
fn meta_error(error: PackedWireError) -> MetaError {
    if matches!(error, PackedWireError::LimitExceeded(_)) {
        MetaError::Io(std::io::Error::from_raw_os_error(libc::ENOMEM))
    } else {
        MetaError::Internal(error.to_string())
    }
}
fn validate_inline_off(entry: &GroupMetaEntry) -> PackedResult<()> {
    entry.validate_placement()?;
    if entry.flags != 0 || !entry.inline_data.is_empty() {
        return Err(PackedWireError::UnsupportedFormat(
            "native matched-data arm requires inline-off snapshot".into(),
        ));
    }
    Ok(())
}
fn prefix_end(prefix: &[u8]) -> PackedResult<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last != 255 {
            end.push(last + 1);
            return Ok(end);
        }
    }
    Err(invalid("namespace has no upper fence"))
}

/// The import write API is create-only and refuses writes after ready exists.
/// Tests supply an in-memory implementation; production uses TiKV transactions.
#[async_trait]
trait NativePlacementKv: Send + Sync {
    async fn get(&self, key: Vec<u8>) -> PackedResult<Option<Vec<u8>>>;
    async fn scan(
        &self,
        start: Vec<u8>,
        end: Vec<u8>,
        limit: u32,
    ) -> PackedResult<Vec<(Vec<u8>, Vec<u8>)>>;
    async fn put_import(&self, ready: Vec<u8>, key: Vec<u8>, value: Vec<u8>) -> PackedResult<()>;
    async fn publish(&self, ready: Vec<u8>, value: Vec<u8>) -> PackedResult<()>;
}

struct TiKvPlacementKv {
    client: TransactionClient,
}
impl TiKvPlacementKv {
    async fn begin(&self) -> PackedResult<Transaction> {
        // Backend errors and dropped read futures must remain recoverable;
        // tikv-client's default panic-on-unfinished-transaction is unsuitable.
        self.client
            .begin_with_options(TransactionOptions::new_optimistic().drop_check(CheckLevel::Warn))
            .await
            .map_err(backend)
    }
}
#[async_trait]
impl NativePlacementKv for TiKvPlacementKv {
    async fn get(&self, key: Vec<u8>) -> PackedResult<Option<Vec<u8>>> {
        let mut txn = self.begin().await?;
        let value = txn.get(key).await.map_err(backend)?;
        txn.commit().await.map_err(backend)?;
        Ok(value)
    }
    async fn scan(
        &self,
        start: Vec<u8>,
        end: Vec<u8>,
        limit: u32,
    ) -> PackedResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut txn = self.begin().await?;
        let rows = txn.scan(start..end, limit).await.map_err(backend)?;
        txn.commit().await.map_err(backend)?;
        Ok(rows
            .into_iter()
            .map(|row| {
                let (key, value): (tikv_client::Key, Vec<u8>) = row.into();
                (key.into(), value)
            })
            .collect())
    }
    async fn put_import(&self, ready: Vec<u8>, key: Vec<u8>, value: Vec<u8>) -> PackedResult<()> {
        let mut txn = self.begin().await?;
        // Read-only ready checks alone allow optimistic write skew: publish
        // could commit between this check and an import record's commit.
        txn.lock_keys(vec![ready.clone()]).await.map_err(backend)?;
        if txn.get(ready).await.map_err(backend)?.is_some() {
            return Err(invalid("published namespace is immutable"));
        }
        match txn.get(key.clone()).await.map_err(backend)? {
            Some(existing) if existing != value => return Err(invalid("import record changed")),
            Some(_) => {}
            None => txn.put(key, value).await.map_err(backend)?,
        }
        txn.commit().await.map_err(backend)?;
        Ok(())
    }
    async fn publish(&self, ready: Vec<u8>, value: Vec<u8>) -> PackedResult<()> {
        let mut txn = self.begin().await?;
        match txn.get(ready.clone()).await.map_err(backend)? {
            Some(existing) if existing != value => return Err(invalid("ready binding changed")),
            Some(_) => {}
            None => txn.put(ready, value).await.map_err(backend)?,
        }
        txn.commit().await.map_err(backend)?;
        Ok(())
    }
}

#[derive(Default)]
struct KvCounters {
    started: AtomicU64,
    succeeded: AtomicU64,
    failed: AtomicU64,
    cancelled: AtomicU64,
    received: AtomicU64,
    validation_failed: AtomicU64,
}
struct KvOperation {
    counters: Arc<KvCounters>,
    terminal: bool,
}
impl KvOperation {
    fn start(counters: &Arc<KvCounters>) -> Self {
        counters.started.fetch_add(1, Ordering::Relaxed);
        Self {
            counters: counters.clone(),
            terminal: false,
        }
    }
    fn finish<T>(&mut self, result: &PackedResult<T>, received: usize) {
        self.terminal = true;
        if result.is_ok() {
            self.counters.succeeded.fetch_add(1, Ordering::Relaxed);
        } else {
            self.counters.failed.fetch_add(1, Ordering::Relaxed);
        }
        self.counters
            .received
            .fetch_add(received as u64, Ordering::Relaxed);
    }
}
impl Drop for KvOperation {
    fn drop(&mut self) {
        if !self.terminal {
            self.counters.cancelled.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Separate immutable namespace for each caller-selected manifest digest.
/// Every value authenticates its namespace key and snapshot binding, rather
/// than accepting a self-selected data descriptor from the native record.
pub struct NativePlacementRepository {
    kv: Arc<dyn NativePlacementKv>,
    prefix: Vec<u8>,
    binding: [u8; 32],
    budget: Arc<super::V3MountBudget>,
    counters: [Arc<KvCounters>; 2],
    phase: AtomicU8,
    _roots: V3OwnedPermit,
}
// Pathnames come from this immutable bound namespace. Retain its actual
// repository and Roots authority through the final ancestor permission use.
struct NativePathsOwner {
    _permit: V3OwnedPermit,
    _repository: Arc<NativePlacementRepository>,
}
impl std::fmt::Debug for NativePathsOwner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativePathsOwner").finish_non_exhaustive()
    }
}

impl NativePlacementRepository {
    pub async fn connect(
        endpoints: Vec<String>,
        namespace: &str,
        binding: [u8; 32],
        budget: Arc<super::V3MountBudget>,
    ) -> PackedResult<Arc<Self>> {
        if endpoints.is_empty()
            || namespace.is_empty()
            || namespace.len() > 512
            || binding == [0; 32]
        {
            return Err(invalid("endpoints/namespace/snapshot binding are invalid"));
        }
        let roots = budget.admit(&[(V3BudgetPool::Roots, 128 << 10)])?;
        let client = TransactionClient::new(endpoints).await.map_err(backend)?;
        Ok(Self::from_kv(
            Arc::new(TiKvPlacementKv { client }),
            namespace,
            binding,
            budget,
            roots,
        ))
    }
    #[cfg(test)]
    fn with_kv(
        kv: Arc<dyn NativePlacementKv>,
        namespace: &str,
        binding: [u8; 32],
        budget: Arc<super::V3MountBudget>,
    ) -> Arc<Self> {
        let roots = budget
            .admit(&[(V3BudgetPool::Roots, 128 << 10)])
            .expect("test repository root admission");
        Self::from_kv(kv, namespace, binding, budget, roots)
    }
    fn from_kv(
        kv: Arc<dyn NativePlacementKv>,
        namespace: &str,
        binding: [u8; 32],
        budget: Arc<super::V3MountBudget>,
        roots: V3OwnedPermit,
    ) -> Arc<Self> {
        Arc::new(Self {
            kv,
            prefix: format!("{namespace}/packed-native/{}/", hex::encode(binding)).into_bytes(),
            binding,
            budget,
            counters: std::array::from_fn(|_| Arc::new(KvCounters::default())),
            phase: AtomicU8::new(0),
            _roots: roots,
        })
    }
    fn active_counters(&self) -> Arc<KvCounters> {
        self.counters[usize::from(self.phase.load(Ordering::Acquire) != 0)].clone()
    }
    pub fn start_runtime(&self) {
        self.phase.store(1, Ordering::Release);
    }
    fn key(&self, suffix: &[u8]) -> Vec<u8> {
        [self.prefix.as_slice(), suffix].concat()
    }
    fn ready_key(&self) -> Vec<u8> {
        self.key(b"ready")
    }
    fn record_key(&self, kind: u8, inode: u64) -> Vec<u8> {
        self.key(&[b"r/".as_slice(), &[kind, b'/'], &inode.to_be_bytes()].concat())
    }
    fn envelope(&self, key: &[u8], body: &[u8]) -> PackedResult<Vec<u8>> {
        if body.len() > MAX_RECORD {
            return Err(invalid("record exceeds byte bound"));
        }
        let mut hash = Sha256::new();
        hash.update(self.binding);
        hash.update(key);
        hash.update(body);
        let mut result = Vec::with_capacity(body.len() + 68);
        result.extend_from_slice(b"NM01");
        result.extend_from_slice(&self.binding);
        result.extend_from_slice(&hash.finalize());
        result.extend_from_slice(body);
        Ok(result)
    }
    fn body<'a>(&self, key: &[u8], bytes: &'a [u8]) -> PackedResult<&'a [u8]> {
        if bytes.len() < 68
            || bytes.len() > MAX_RECORD + 68
            || &bytes[..4] != b"NM01"
            || bytes[4..36] != self.binding
        {
            self.active_counters()
                .validation_failed
                .fetch_add(1, Ordering::Relaxed);
            return Err(invalid("record snapshot/version/size mismatch"));
        }
        let mut hash = Sha256::new();
        hash.update(self.binding);
        hash.update(key);
        hash.update(&bytes[68..]);
        if hash.finalize().as_slice() != &bytes[36..68] {
            self.active_counters()
                .validation_failed
                .fetch_add(1, Ordering::Relaxed);
            return Err(invalid("record key/content digest mismatch"));
        }
        Ok(&bytes[68..])
    }
    async fn get_body(&self, key: Vec<u8>, bound: usize) -> PackedResult<Option<V3Owned<Vec<u8>>>> {
        let permit = self
            .budget
            .admit(&[(V3BudgetPool::Metadata, (bound * 3 + 4096) as u64)])?;
        let mut operation = KvOperation::start(&self.active_counters());
        let result = self.kv.get(key.clone()).await;
        let received = result
            .as_ref()
            .ok()
            .and_then(|value| value.as_ref())
            .map_or(0, Vec::len);
        operation.finish(&result, received);
        let Some(bytes) = result? else {
            return Ok(None);
        };
        if bytes.len() > bound + 68 {
            return Err(invalid("backend value exceeds admitted limit"));
        }
        let body = self.body(&key, &bytes)?.to_vec();
        Ok(Some(V3Owned::new(body, permit)))
    }
    async fn put_body(&self, key: Vec<u8>, body: &[u8]) -> PackedResult<()> {
        let _permit = self
            .budget
            .admit(&[(V3BudgetPool::Workspace, (2 * MAX_RECORD + 4096) as u64)])?;
        let value = self.envelope(&key, body)?;
        let mut operation = KvOperation::start(&self.active_counters());
        let result = self.kv.put_import(self.ready_key(), key, value).await;
        operation.finish(&result, 0);
        result
    }
    async fn rows(
        &self,
        prefix: Vec<u8>,
        after: Option<&[u8]>,
        limit: usize,
        bound: usize,
    ) -> PackedResult<V3Owned<Vec<(Vec<u8>, Vec<u8>)>>> {
        if limit == 0 || limit > PAGE {
            return Err(invalid("scan page exceeds row bound"));
        }
        let permit = self
            .budget
            .admit(&[(V3BudgetPool::Metadata, (limit * (bound + 2048) * 3) as u64)])?;
        let start = after.map_or_else(
            || prefix.clone(),
            |key| {
                let mut next = key.to_vec();
                next.push(0);
                next
            },
        );
        let end = prefix_end(&prefix)?;
        let mut operation = KvOperation::start(&self.active_counters());
        let result = self.kv.scan(start, end, limit as u32).await;
        let received = result
            .as_ref()
            .map_or(0, |rows| rows.iter().map(|(k, v)| k.len() + v.len()).sum());
        operation.finish(&result, received);
        let rows = result?;
        if rows.len() > limit {
            return Err(invalid("backend scan exceeded limit"));
        }
        let mut prior = after;
        for (key, value) in &rows {
            if !key.starts_with(&prefix)
                || prior.is_some_and(|prior| prior >= key.as_slice())
                || value.len() > bound + 68
            {
                return Err(invalid("scan fence/order/byte bound mismatch"));
            }
            self.body(key, value)?;
            prior = Some(key);
        }
        Ok(V3Owned::new(rows, permit))
    }
    async fn inode(&self, inode: u64) -> PackedResult<Option<Arc<V3Owned<NativeInode>>>> {
        let Some(body) = self
            .get_body(self.record_key(b'i', inode), MAX_RECORD)
            .await?
        else {
            return Ok(None);
        };
        let permit = self
            .budget
            .admit(&[(V3BudgetPool::Metadata, (MAX_RECORD * 2) as u64)])?;
        let record = NativeInode::decode(&body, inode).inspect_err(|_| {
            self.active_counters()
                .validation_failed
                .fetch_add(1, Ordering::Relaxed);
        })?;
        Ok(Some(Arc::new(V3Owned::new(record, permit))))
    }
    async fn ready(&self) -> PackedResult<Ready> {
        let body = self
            .get_body(self.ready_key(), 1024)
            .await?
            .ok_or_else(|| invalid("import is incomplete; ready binding is absent"))?;
        Ready::decode(&body)
    }
    /// Hash every bounded record page before mount/publication. This verifies
    /// ready closure without holding the full namespace in memory.
    async fn closure(&self) -> PackedResult<([u8; 32], u64)> {
        let prefix = self.key(b"r/");
        let mut after = None;
        let mut hash = Sha256::new();
        let mut count = 0u64;
        loop {
            // A page of two maximum-size inode records stays below Metadata.
            let rows = self
                .rows(prefix.clone(), after.as_deref(), 2, MAX_RECORD)
                .await?;
            if rows.is_empty() {
                break;
            }
            for (key, value) in rows.iter() {
                hash.update((key.len() as u64).to_le_bytes());
                hash.update(key);
                hash.update((value.len() as u64).to_le_bytes());
                hash.update(value);
                count = count
                    .checked_add(1)
                    .ok_or_else(|| invalid("record count overflow"))?;
            }
            after = rows.last().map(|(key, _)| key.clone());
        }
        Ok((hash.finalize().into(), count))
    }
    async fn verify_ready(&self, root: u64, expected_dentries: u64) -> PackedResult<Ready> {
        let ready = self.ready().await?;
        let (digest, records) = self.closure().await?;
        if ready.root != root
            || ready.dentries != expected_dentries
            || ready.digest != digest
            || ready.records != records
            || ready.inodes == 0
        {
            return Err(invalid("ready namespace closure disagrees with snapshot"));
        }
        if ready.records
            != ready
                .inodes
                .checked_add(
                    ready
                        .dentries
                        .checked_mul(3)
                        .ok_or_else(|| invalid("ready count overflow"))?,
                )
                .ok_or_else(|| invalid("ready count overflow"))?
        {
            return Err(invalid("ready record inventory does not close"));
        }
        self.validate_edges(root, ready.inodes, ready.dentries, ready.selectors)
            .await?;
        Ok(ready)
    }
    pub async fn import_from<B: ObjectBackend + Clone + 'static>(
        &self,
        client: ObjectClient<B>,
        snapshot: AuthenticatedV3Snapshot,
        chunk_size: u64,
        metadata_cache: u64,
    ) -> PackedResult<()> {
        if snapshot.read_generation().lower_snapshot != self.binding {
            return Err(invalid("import bound to another manifest"));
        }
        if self.get_body(self.ready_key(), 1024).await?.is_some() {
            self.verify_ready(
                snapshot.manifest().root_inode,
                snapshot.manifest().group_dentry_count,
            )
            .await?;
            return Ok(());
        }
        let source = crate::workspace_overlay::packed_v3::PackedV3ReadonlyMeta::from_v3_budget(
            client.clone(),
            snapshot.clone(),
            chunk_size,
            metadata_cache,
            self.budget.clone(),
        )?;
        let reader =
            V3IndexReader::with_budget(client.clone(), metadata_cache, self.budget.clone());
        let _root_workspace = self.budget.admit(&[(V3BudgetPool::Workspace, 2 << 20)])?;
        let root = snapshot.manifest().root_inode;
        let root_attr = source
            .stat_fresh(root as i64)
            .await
            .map_err(backend)?
            .ok_or_else(|| invalid("source root is absent"))?;
        let mut root_record = NativeInode::root(root_attr);
        root_record.cold = snapshot
            .cold_attributes_owned(&client, &reader, root)
            .await?
            .map(|cold| cold.encode())
            .transpose()?;
        self.put_body(self.record_key(b'i', root), &root_record.encode()?)
            .await?;
        let mut inodes = 1u64;
        let mut selectors = 0u64;
        let mut dentries = self.import_directory(&source, root).await?;
        let mut after = None;
        loop {
            let _page = self.budget.admit(&[(V3BudgetPool::Workspace, 2 << 20)])?;
            let rows = reader
                .scan_page(
                    snapshot.root(V3RootKind::Inodes),
                    &[0],
                    &[255],
                    after.as_deref(),
                    16,
                )
                .await?;
            if rows.is_empty() {
                break;
            }
            for row in &rows {
                let inode = u64::from_be_bytes(
                    row.first_key
                        .as_slice()
                        .try_into()
                        .map_err(|_| invalid("source inode key length"))?,
                );
                if inode == root {
                    return Err(invalid("source inode index duplicates root"));
                }
                let location = snapshot
                    .lookup_inode(&reader, inode)
                    .await?
                    .ok_or_else(|| invalid("source inode vanished"))?;
                let container = snapshot
                    .container_ref(&reader, location.group.container_ordinal)
                    .await?;
                let metadata = location
                    .group
                    .read_metadata_owned(&client, &container, 512 << 10, &self.budget)
                    .await?;
                let entry = metadata
                    .entries()
                    .get(location.hot.entry_ordinal as usize)
                    .ok_or_else(|| invalid("source inode ordinal"))?;
                location.validate_entry(entry)?;
                // Reject before serializing any inline data into TiKV. Every
                // accepted regular payload retains its packed extent route.
                validate_inline_off(entry)?;
                let placement = if entry.kind == 1 {
                    snapshot.placement(&reader, inode, entry.size).await?
                } else {
                    None
                };
                let cold = snapshot
                    .cold_attributes_owned(&client, &reader, inode)
                    .await?
                    .map(|cold| cold.encode())
                    .transpose()?;
                let blocks = source
                    .stat_fresh(inode as i64)
                    .await
                    .map_err(backend)?
                    .ok_or_else(|| invalid("source attr absent"))?
                    .blocks;
                let record = NativeInode {
                    group_id: location.group.group_id,
                    container_ordinal: location.group.container_ordinal,
                    entry: entry.clone(),
                    placement,
                    blocks,
                    cold,
                };
                if record.entry.kind == 1 {
                    selectors += 1;
                }
                self.put_body(self.record_key(b'i', inode), &record.encode()?)
                    .await?;
                if record.entry.kind == 2 {
                    dentries = dentries
                        .checked_add(self.import_directory(&source, inode).await?)
                        .ok_or_else(|| invalid("dentry count overflow"))?;
                }
                inodes = inodes
                    .checked_add(1)
                    .ok_or_else(|| invalid("inode count overflow"))?;
            }
            after = rows.last().map(|row| row.first_key.clone());
        }
        if dentries != snapshot.manifest().group_dentry_count {
            return Err(invalid("import dentry count differs from manifest"));
        }
        // Check every forward/ordinal/reverse edge and visible hardlink count
        // after all records exist, before publishing the immutable ready key.
        self.validate_edges(root, inodes, dentries, selectors)
            .await?;
        let (digest, records) = self.closure().await?;
        if records
            != inodes
                .checked_add(
                    dentries
                        .checked_mul(3)
                        .ok_or_else(|| invalid("closure count overflow"))?,
                )
                .ok_or_else(|| invalid("closure count overflow"))?
        {
            return Err(invalid(
                "inode/lookup/ordinal/reverse record count does not close",
            ));
        }
        let ready = Ready {
            root,
            inodes,
            dentries,
            selectors,
            records,
            digest,
        };
        let ready_key = self.ready_key();
        let value = self.envelope(&ready_key, &ready.encode())?;
        let mut operation = KvOperation::start(&self.active_counters());
        let result = self.kv.publish(ready_key, value).await;
        operation.finish(&result, 0);
        result?;
        self.verify_ready(root, dentries).await?;
        Ok(())
    }
    async fn import_directory<B: ObjectBackend + Clone + 'static>(
        &self,
        source: &crate::workspace_overlay::packed_v3::PackedV3ReadonlyMeta<B>,
        parent: u64,
    ) -> PackedResult<u64> {
        let handle = source.opendir(parent as i64).await.map_err(backend)?;
        let mut offset = 0u64;
        loop {
            let _permit = self.budget.admit(&[(V3BudgetPool::Workspace, 2 << 20)])?;
            let page = handle
                .get_entries_page_raw(offset, PAGE)
                .await
                .map_err(backend)?;
            if page.is_empty() {
                break;
            }
            for entry in page {
                let child =
                    u64::try_from(entry.ino).map_err(|_| invalid("negative dentry inode"))?;
                let mut w = Writer::default();
                w.u64(parent);
                w.u64(child);
                w.u16(entry.name.len() as u16);
                w.bytes(&entry.name);
                let body = w.finish();
                let lookup = [self.record_key(b'l', parent), entry.name.clone()].concat();
                let ordinal =
                    [self.record_key(b'd', parent), offset.to_be_bytes().to_vec()].concat();
                let reverse = [
                    self.record_key(b'n', child),
                    parent.to_be_bytes().to_vec(),
                    entry.name,
                ]
                .concat();
                self.put_body(lookup, &body).await?;
                self.put_body(ordinal, &body).await?;
                self.put_body(reverse, &body).await?;
                offset = offset
                    .checked_add(1)
                    .ok_or_else(|| invalid("directory ordinal overflow"))?;
            }
        }
        Ok(offset)
    }
    async fn validate_edges(
        &self,
        root: u64,
        expected_inodes: u64,
        expected_dentries: u64,
        expected_selectors: u64,
    ) -> PackedResult<()> {
        let mut after = None;
        let mut inodes = 0;
        let mut selectors = 0;
        let mut edges = 0;
        loop {
            let rows = self
                .rows(self.key(b"r/i/"), after.as_deref(), 2, MAX_RECORD)
                .await?;
            if rows.is_empty() {
                break;
            }
            for (key, value) in rows.iter() {
                let inode = u64::from_be_bytes(key[key.len() - 8..].try_into().unwrap());
                let entry = NativeInode::decode(self.body(key, value)?, inode)?;
                inodes += 1;
                if entry.entry.kind == 1 {
                    selectors += 1;
                }
                if entry.entry.kind == 2 {
                    let prefix = self.record_key(b'd', inode);
                    let mut ordinal_after = None;
                    let mut ordinal = 0u64;
                    loop {
                        let rows = self
                            .rows(prefix.clone(), ordinal_after.as_deref(), PAGE, 4096)
                            .await?;
                        if rows.is_empty() {
                            break;
                        }
                        for (key, value) in rows.iter() {
                            if key.len() != prefix.len() + 8
                                || u64::from_be_bytes(key[key.len() - 8..].try_into().unwrap())
                                    != ordinal
                            {
                                return Err(invalid("directory ordinals do not close"));
                            }
                            let (parent, child, name) = decode_dentry(self.body(key, value)?)?;
                            if parent != inode {
                                return Err(invalid("ordinal parent mismatch"));
                            }
                            let child_record = self
                                .inode(child)
                                .await?
                                .ok_or_else(|| invalid("ordinal child absent"))?;
                            let forward = self
                                .get_body(
                                    [self.record_key(b'l', inode), name.to_vec()].concat(),
                                    4096,
                                )
                                .await?
                                .ok_or_else(|| invalid("ordinal forward edge absent"))?;
                            if forward.as_slice() != self.body(key, value)? {
                                return Err(invalid("ordinal/forward edge disagreement"));
                            }
                            if child_record.entry.kind == 2 {
                                let mut parent = inode;
                                let mut reached_root = false;
                                for _ in 0..=4096 {
                                    if parent == root {
                                        reached_root = true;
                                        break;
                                    }
                                    let names = self
                                        .rows(self.record_key(b'n', parent), None, 2, 4096)
                                        .await?;
                                    if names.len() != 1 {
                                        return Err(invalid(
                                            "directory parent missing or ambiguous",
                                        ));
                                    }
                                    let (next, _, _) =
                                        decode_dentry(self.body(&names[0].0, &names[0].1)?)?;
                                    parent = next;
                                }
                                if !reached_root {
                                    return Err(invalid(
                                        "directory parent cycle/depth exceeds bound",
                                    ));
                                }
                            }
                            ordinal = ordinal
                                .checked_add(1)
                                .ok_or_else(|| invalid("ordinal overflow"))?;
                        }
                        ordinal_after = rows.last().map(|(key, _)| key.clone());
                    }
                }
                let mut names_after = None;
                let mut links = 0;
                loop {
                    let names = self
                        .rows(
                            self.record_key(b'n', inode),
                            names_after.as_deref(),
                            PAGE,
                            4096,
                        )
                        .await?;
                    if names.is_empty() {
                        break;
                    }
                    for (name_key, name_value) in names.iter() {
                        let (parent, child, name) =
                            decode_dentry(self.body(name_key, name_value)?)?;
                        if child != inode || inode == root {
                            return Err(invalid("reverse edge child/root mismatch"));
                        }
                        let parent_record = self
                            .inode(parent)
                            .await?
                            .ok_or_else(|| invalid("parent inode missing"))?;
                        if parent_record.entry.kind != 2 {
                            return Err(invalid("parent is not directory"));
                        }
                        let forward = self
                            .get_body(
                                [self.record_key(b'l', parent), name.to_vec()].concat(),
                                4096,
                            )
                            .await?
                            .ok_or_else(|| invalid("forward edge missing"))?;
                        if forward.as_slice() != self.body(name_key, name_value)? {
                            return Err(invalid("forward/reverse edge disagreement"));
                        }
                        links += 1;
                        edges += 1;
                    }
                    names_after = names.last().map(|(key, _)| key.clone());
                }
                if inode != root
                    && ((entry.entry.kind != 2 && links != u64::from(entry.entry.nlink))
                        || (entry.entry.kind == 2 && links != 1))
                {
                    return Err(invalid(
                        "visible hardlink/directory membership does not close",
                    ));
                }
            }
            after = rows.last().map(|(key, _)| key.clone());
        }
        if (inodes, edges, selectors) != (expected_inodes, expected_dentries, expected_selectors) {
            return Err(invalid("namespace inventory counts do not close"));
        }
        Ok(())
    }
}

struct NativeInode {
    group_id: u64,
    container_ordinal: u32,
    entry: GroupMetaEntry,
    placement: Option<V3Placement>,
    blocks: u64,
    cold: Option<Vec<u8>>,
}
impl NativeInode {
    fn root(attr: FileAttr) -> Self {
        Self {
            group_id: 0,
            container_ordinal: 0,
            blocks: attr.blocks,
            placement: None,
            cold: None,
            entry: GroupMetaEntry {
                name: b"root".to_vec(),
                inode: attr.ino as u64,
                kind: 2,
                mode: attr.mode,
                uid: attr.uid,
                gid: attr.gid,
                rdev: attr.rdev as u64,
                nlink: attr.nlink,
                atime_ns: attr.atime,
                mtime_ns: attr.mtime,
                ctime_ns: attr.ctime,
                size: attr.size,
                flags: 0,
                inline_data: Arc::from([]),
                extents: vec![],
            },
        }
    }
    fn encode(&self) -> PackedResult<Vec<u8>> {
        validate_inline_off(&self.entry)?;
        let e = &self.entry;
        let mut w = Writer::default();
        w.bytes(b"NI01");
        w.u64(self.group_id);
        w.u32(self.container_ordinal);
        w.u64(self.blocks);
        w.u16(e.name.len() as u16);
        w.bytes(&e.name);
        w.u64(e.inode);
        w.u8(e.kind);
        w.u32(e.mode);
        w.u32(e.uid);
        w.u32(e.gid);
        w.u64(e.rdev);
        w.u32(e.nlink);
        w.i64(e.atime_ns);
        w.i64(e.mtime_ns);
        w.i64(e.ctime_ns);
        w.u64(e.size);
        w.u8(e.flags);
        w.u32(e.inline_data.len() as u32);
        w.bytes(&e.inline_data);
        w.u16(e.extents.len() as u16);
        for x in &e.extents {
            w.u64(x.file_offset);
            w.u32(x.logical_len);
            w.u32(x.frame_ordinal);
            w.u32(x.raw_offset);
            w.u32(x.raw_len);
        }
        let placement = self
            .placement
            .as_ref()
            .map(V3Placement::encode)
            .transpose()?
            .unwrap_or_default();
        w.u32(placement.len() as u32);
        w.bytes(&placement);
        let cold = self.cold.as_deref().unwrap_or_default();
        w.u32(cold.len() as u32);
        w.bytes(cold);
        let body = w.finish();
        if body.len() > MAX_RECORD {
            return Err(invalid("plain inode exceeds record bound"));
        }
        Ok(body)
    }
    fn decode(body: &[u8], expected: u64) -> PackedResult<Self> {
        if body.len() > MAX_RECORD {
            return Err(invalid("plain inode exceeds record bound"));
        }
        let mut r = Reader::new(body);
        if r.take(4)? != b"NI01" {
            return Err(invalid("plain inode version"));
        }
        let group_id = r.u64()?;
        let container_ordinal = r.u32()?;
        let blocks = r.u64()?;
        let name_len = r.u16()? as usize;
        if name_len > 1024 {
            return Err(invalid("inode name exceeds bound"));
        }
        let name = r.take(name_len)?.to_vec();
        let inode = r.u64()?;
        let kind = r.u8()?;
        let mode = r.u32()?;
        let uid = r.u32()?;
        let gid = r.u32()?;
        let rdev = r.u64()?;
        let nlink = r.u32()?;
        let atime_ns = r.i64()?;
        let mtime_ns = r.i64()?;
        let ctime_ns = r.i64()?;
        let size = r.u64()?;
        let flags = r.u8()?;
        let inline_len = r.u32()? as usize;
        if inline_len >= 256 << 10 {
            return Err(invalid("inline length exceeds bound"));
        }
        let inline_data = Arc::from(r.take(inline_len)?);
        let count = r.u16()? as usize;
        if count > 1024 {
            return Err(invalid("extent count exceeds bound"));
        }
        let mut extents = Vec::with_capacity(count);
        for _ in 0..count {
            extents.push(GroupMetaExtent {
                file_offset: r.u64()?,
                logical_len: r.u32()?,
                frame_ordinal: r.u32()?,
                raw_offset: r.u32()?,
                raw_len: r.u32()?,
            });
        }
        let selector_len = r.u32()? as usize;
        if selector_len > 8192 {
            return Err(invalid("selector exceeds bound"));
        }
        let placement = if selector_len == 0 {
            None
        } else {
            Some(V3Placement::decode(r.take(selector_len)?, inode, size)?)
        };
        let cold_len = r.u32()? as usize;
        if cold_len > 320 << 10 {
            return Err(invalid("cold payload exceeds bound"));
        }
        let cold = (cold_len != 0)
            .then(|| r.take(cold_len).map(Vec::from))
            .transpose()?;
        if inode != expected || !r.is_empty() {
            return Err(invalid("inode key/trailing bytes mismatch"));
        }
        let result = Self {
            group_id,
            container_ordinal,
            blocks,
            placement,
            cold,
            entry: GroupMetaEntry {
                name,
                inode,
                kind,
                mode,
                uid,
                gid,
                rdev,
                nlink,
                atime_ns,
                mtime_ns,
                ctime_ns,
                size,
                flags,
                inline_data,
                extents,
            },
        };
        // Also enforce policy when opening an already published namespace.
        validate_inline_off(&result.entry)?;
        Ok(result)
    }
    fn attr(&self) -> FileAttr {
        let e = &self.entry;
        FileAttr {
            ino: e.inode as i64,
            size: e.size,
            blocks: self.blocks,
            kind: FileType::from_mode(e.mode),
            mode: e.mode,
            rdev: e.rdev as u32,
            uid: e.uid,
            gid: e.gid,
            atime: e.atime_ns,
            mtime: e.mtime_ns,
            ctime: e.ctime_ns,
            nlink: e.nlink,
        }
    }
    fn cold(&self) -> PackedResult<Option<super::V3ColdAttributes>> {
        self.cold
            .as_ref()
            .map(|bytes| {
                let reference = V3ObjectRef::from_bytes(
                    "native-inline-cold".into(),
                    V3ObjectKind::ColdAttributes,
                    bytes,
                )?;
                let attrs = super::V3ColdAttributes::decode(&reference, bytes, self.entry.inode)?;
                attrs.validate_for_inode(self.entry.kind, self.entry.mode)?;
                if self.entry.kind == 3
                    && attrs
                        .symlink_target
                        .as_ref()
                        .is_none_or(|target| target.len() as u64 != self.entry.size)
                {
                    return Err(invalid("symlink target size disagrees with inode"));
                }
                Ok(attrs)
            })
            .transpose()
    }
}
fn decode_dentry(body: &[u8]) -> PackedResult<(u64, u64, &[u8])> {
    let mut r = Reader::new(body);
    let parent = r.u64()?;
    let child = r.u64()?;
    let len = r.u16()? as usize;
    if len == 0 || len > 1024 {
        return Err(invalid("dentry name bound"));
    }
    let name = r.take(len)?;
    if name.contains(&0) || name.contains(&b'/') || name == b"." || name == b".." || !r.is_empty() {
        return Err(invalid("dentry name/trailing bytes"));
    }
    Ok((parent, child, name))
}
struct Ready {
    root: u64,
    inodes: u64,
    dentries: u64,
    selectors: u64,
    records: u64,
    digest: [u8; 32],
}
impl Ready {
    fn encode(&self) -> Vec<u8> {
        let mut w = Writer::default();
        w.bytes(b"NR01");
        for n in [
            self.root,
            self.inodes,
            self.dentries,
            self.selectors,
            self.records,
        ] {
            w.u64(n);
        }
        w.bytes(&self.digest);
        w.finish()
    }
    fn decode(body: &[u8]) -> PackedResult<Self> {
        let mut r = Reader::new(body);
        if r.take(4)? != b"NR01" {
            return Err(invalid("ready version"));
        }
        let result = Self {
            root: r.u64()?,
            inodes: r.u64()?,
            dentries: r.u64()?,
            selectors: r.u64()?,
            records: r.u64()?,
            digest: r.array()?,
        };
        if !r.is_empty() {
            return Err(invalid("ready trailing bytes"));
        }
        Ok(result)
    }
}

/// A sentinel for the legacy block path. Prepared reads must always be selected.
#[derive(Clone)]
pub struct NativePlacementBlockStore;
#[async_trait]
impl BlockStore for NativePlacementBlockStore {
    async fn write_fresh_range(
        &self,
        _key: BlockKey,
        _offset: u64,
        _bytes: &[u8],
    ) -> anyhow::Result<u64> {
        anyhow::bail!("immutable native placement forbids writes")
    }
    async fn read_range(
        &self,
        _key: BlockKey,
        _offset: u64,
        _buf: &mut [u8],
    ) -> anyhow::Result<()> {
        anyhow::bail!("native placement must use shared prepared executor")
    }
    async fn delete_range(&self, _key: BlockKey, _count: u64) -> anyhow::Result<()> {
        anyhow::bail!("immutable native placement forbids deletes")
    }
}

pub struct NativePackedPlacementProvider<B: ObjectBackend + Clone + 'static> {
    repository: Arc<NativePlacementRepository>,
    client: ObjectClient<crate::workspace_overlay::packed_v3::readonly::V3ReadonlyBackend<B>>,
    snapshot: AuthenticatedV3Snapshot,
    reader: V3IndexReader<crate::workspace_overlay::packed_v3::readonly::V3ReadonlyBackend<B>>,
    root: AtomicI64,
    chunk_size: u64,
    _roots: V3OwnedPermit,
    phase: Arc<AtomicU8>,
    data_metrics: Arc<crate::workspace_overlay::packed_v3::readonly::V3ReadonlyMetrics>,
}
impl<B: ObjectBackend + Clone + 'static> NativePackedPlacementProvider<B> {
    pub async fn open(
        repository: Arc<NativePlacementRepository>,
        client: ObjectClient<B>,
        snapshot: AuthenticatedV3Snapshot,
        chunk_size: u64,
        metadata_cache: u64,
    ) -> PackedResult<Arc<Self>> {
        if chunk_size == 0 || snapshot.read_generation().lower_snapshot != repository.binding {
            return Err(invalid("provider snapshot/chunk binding"));
        }
        let classes = snapshot.manifest().size_classes;
        repository.budget.validate_frame_capability(
            classes
                .max_random_frame_raw_bytes
                .max(classes.max_sequential_frame_raw_bytes) as usize,
        )?;
        let observer = client
            .read_observer()
            .ok_or_else(|| invalid("native arm requires mount-owned observer"))?;
        if !observer.owned_by_budget(Arc::as_ptr(&repository.budget) as usize) {
            return Err(invalid("observer belongs to another budget"));
        }
        repository
            .verify_ready(
                snapshot.manifest().root_inode,
                snapshot.manifest().group_dentry_count,
            )
            .await?;
        let roots = repository
            .budget
            .admit(&[(V3BudgetPool::Roots, 128 << 10)])?;
        let (client, data_metrics) =
            crate::workspace_overlay::packed_v3::readonly::shared_v3_readonly_client(
                client,
                observer,
                crate::cadapter::read_observer::Engine::Native,
                crate::cadapter::read_observer::Phase::Startup,
                repository.budget.clone(),
            );
        let phase = Arc::new(AtomicU8::new(0));
        let client = client.with_phase_control(phase.clone());
        let reader =
            V3IndexReader::with_budget(client.clone(), metadata_cache, repository.budget.clone());
        let root = AtomicI64::new(snapshot.manifest().root_inode as i64);
        Ok(Arc::new(Self {
            repository,
            client,
            snapshot,
            reader,
            root,
            chunk_size,
            _roots: roots,
            phase,
            data_metrics,
        }))
    }
    pub fn block_store(&self) -> NativePlacementBlockStore {
        NativePlacementBlockStore
    }
    pub fn start_runtime(&self) {
        self.repository.start_runtime();
        self.phase.store(1, Ordering::Release);
    }
    async fn entry(&self, ino: i64) -> Result<Option<Arc<V3Owned<NativeInode>>>, MetaError> {
        let inode = u64::try_from(ino).map_err(|_| MetaError::NotFound(ino))?;
        self.repository.inode(inode).await.map_err(meta_error)
    }
    async fn readonly<T>() -> Result<T, MetaError> {
        Err(MetaError::Io(std::io::Error::from_raw_os_error(
            libc::EROFS,
        )))
    }
    pub fn install_stats(&self, stats: &crate::vfs::stats::FsStats) -> bool {
        stats.set_extension(Arc::new(NativePlacementStats {
            repository: self.repository.clone(),
            observer: self
                .client
                .read_observer()
                .expect("observer checked at open"),
        }))
    }
}
struct NativePlacementStats {
    repository: Arc<NativePlacementRepository>,
    observer: Arc<crate::cadapter::read_observer::ReadObserver>,
}
impl std::fmt::Debug for NativePlacementStats {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NativePlacementStats")
            .finish_non_exhaustive()
    }
}
impl crate::vfs::stats::FsStatsExtension for NativePlacementStats {
    fn render_max_bytes(&self) -> usize {
        super::V3MountBudget::STATS_EXTENSION_RENDER_MAX_BYTES
            .saturating_add(self.observer.render_max_bytes())
    }
    fn begin_stats_observation(&self) -> Option<crate::cadapter::read_observer::TerminalGuard> {
        use crate::cadapter::read_observer::*;
        Some(self.observer.start(
            Ledger::LogicalOperation,
            ReadContext {
                engine: Engine::Native,
                phase: Phase::Runtime,
                origin: Origin::StatsObserver,
                class: ReadClass::StatsSnapshot,
            },
            0,
        ))
    }
    fn render_into(&self, output: &mut dyn std::fmt::Write) {
        let _ = writeln!(
            output,
            "brewfs_native_placement_inline_payload_capability 0"
        );
        for (phase, c) in ["startup", "runtime"]
            .into_iter()
            .zip(&self.repository.counters)
        {
            for (name, counter) in [
                ("started", &c.started),
                ("succeeded", &c.succeeded),
                ("failed", &c.failed),
                ("cancelled", &c.cancelled),
                ("received_bytes", &c.received),
                ("validation_failed", &c.validation_failed),
            ] {
                let _ = writeln!(
                    output,
                    "brewfs_native_placement_kv_operations_{name}_total{{phase=\"{phase}\"}} {}",
                    counter.load(Ordering::Relaxed)
                );
            }
        }
        // These count real client transactions; TiKV transport attempts/retries
        // are not claimed by the object-store HTTP-attempt capability.
        self.repository.budget.render_into(output);
        self.observer.render_into(output);
    }
}

#[async_trait]
impl<B: ObjectBackend + Clone + 'static> WorkspaceReadPlanProvider
    for NativePackedPlacementProvider<B>
{
    fn max_read_bytes(&self) -> Option<usize> {
        Some(self.repository.budget.max_read_bytes())
    }
    fn reserve_read_output(
        &self,
        length: usize,
    ) -> Result<Option<Box<dyn Send + Sync>>, MetaError> {
        Ok(Some(Box::new(
            self.repository.budget.output(length).map_err(meta_error)?,
        )))
    }
    fn supports_prepared_unified_read(&self) -> bool {
        true
    }
    fn record_unified_read_success(&self, bytes: u64) {
        self.data_metrics.logical_success(bytes);
    }
    async fn read_plan(
        &self,
        _ino: i64,
        _chunk: u64,
        _offset: u64,
        _len: u64,
    ) -> Result<ResolvedReadPlan, MetaError> {
        Err(MetaError::Internal(
            "native matched arm forbids legacy read plan".into(),
        ))
    }
    async fn prepare_unified_read(
        &self,
        ino: i64,
        chunk: u64,
        offset: u64,
        len: u64,
    ) -> Result<Option<PreparedUnifiedRead>, MetaError> {
        self.prepare_unified_read_observed(ino, chunk, offset, len, None)
            .await
    }
    async fn prepare_unified_read_observed(
        &self,
        ino: i64,
        chunk: u64,
        offset: u64,
        len: u64,
        delivery: Option<Arc<crate::cadapter::read_observer::OperationDelivery>>,
    ) -> Result<Option<PreparedUnifiedRead>, MetaError> {
        let record = self.entry(ino).await?.ok_or(MetaError::NotFound(ino))?;
        let base = chunk
            .checked_mul(self.chunk_size)
            .ok_or_else(|| meta_error(invalid("chunk base overflows")))?;
        let absolute = base
            .checked_add(offset)
            .ok_or_else(|| meta_error(invalid("read offset overflows")))?;
        let length =
            usize::try_from(len).map_err(|_| meta_error(invalid("read length exceeds usize")))?;
        let _clone = self
            .repository
            .budget
            .admit(&[(V3BudgetPool::Workspace, 1 << 20)])
            .map_err(meta_error)?;
        let input = V3ReadPlacementInput {
            group_id: record.group_id,
            container_ordinal: record.container_ordinal,
            entry: record.entry.clone(),
            placement: record.placement.clone(),
            owner: record,
        };
        let mut prepared = self
            .snapshot
            .prepare_placement_read_observed(
                &self.client,
                &self.reader,
                super::V3ReadRange {
                    offset: absolute,
                    length,
                },
                32 << 20,
                delivery,
                input,
            )
            .await
            .map_err(meta_error)?;
        for segment in &mut prepared.plan.segments {
            segment.logical_offset = segment
                .logical_offset
                .checked_sub(base)
                .ok_or_else(|| meta_error(invalid("plan precedes chunk")))?;
        }
        prepared.plan.logical_size = prepared
            .plan
            .logical_size
            .saturating_sub(base)
            .min(self.chunk_size);
        prepared
            .plan
            .validate(offset, len)
            .map_err(|error| MetaError::Internal(error.to_string()))?;
        Ok(Some(prepared))
    }
    fn begin_unified_read_operation(
        &self,
        requested: u64,
    ) -> Option<crate::cadapter::read_observer::TerminalGuard> {
        use crate::cadapter::read_observer::*;
        Some(self.client.read_observer()?.start(
            Ledger::LogicalOperation,
            self.client.read_context(ReadClass::LogicalRead)?,
            requested,
        ))
    }
    async fn range_has_data(&self, ino: i64, offset: u64, len: u64) -> Result<bool, MetaError> {
        let record = self.entry(ino).await?.ok_or(MetaError::NotFound(ino))?;
        if !record.entry.inline_data.is_empty() {
            return Ok(offset < record.entry.size && len != 0);
        }
        let prepared = self
            .snapshot
            .prepare_placement_read_observed(
                &self.client,
                &self.reader,
                super::V3ReadRange {
                    offset,
                    length: usize::try_from(len)
                        .map_err(|_| meta_error(invalid("range length")))?,
                },
                32 << 20,
                None,
                V3ReadPlacementInput {
                    group_id: record.group_id,
                    container_ordinal: record.container_ordinal,
                    entry: record.entry.clone(),
                    placement: record.placement.clone(),
                    owner: record,
                },
            )
            .await
            .map_err(meta_error)?;
        Ok(!prepared.plan.segments.is_empty())
    }
}

struct NativeDirectorySource {
    repository: Arc<NativePlacementRepository>,
}
#[async_trait]
impl DirectoryPageSource for NativeDirectorySource {
    async fn read_page_owned(
        &self,
        ino: i64,
        offset: u64,
        max_entries: usize,
    ) -> Result<OwnedDirectoryPage, MetaError> {
        let guard = self
            .repository
            .budget
            .admit(&[
                (V3BudgetPool::Output, 1 << 20),
                (V3BudgetPool::Control, 4096),
            ])
            .map_err(meta_error)?;
        let entries = self.read_page(ino, offset, max_entries).await?;
        Ok(OwnedDirectoryPage {
            entries,
            guard: Some(Arc::new(guard)),
        })
    }
    async fn read_page(
        &self,
        ino: i64,
        offset: u64,
        max_entries: usize,
    ) -> Result<Vec<RawDirEntry>, MetaError> {
        let parent = u64::try_from(ino).map_err(|_| MetaError::NotFound(ino))?;
        let record = self
            .repository
            .inode(parent)
            .await
            .map_err(meta_error)?
            .ok_or(MetaError::NotFound(ino))?;
        if record.entry.kind != 2 {
            return Err(MetaError::NotDirectory(ino));
        }
        if max_entries == 0 {
            return Ok(vec![]);
        }
        let prefix = self.repository.record_key(b'd', parent);
        let after = offset
            .checked_sub(1)
            .map(|previous| [prefix.clone(), previous.to_be_bytes().to_vec()].concat());
        let rows = self
            .repository
            .rows(
                prefix.clone(),
                after.as_deref(),
                max_entries.min(PAGE),
                4096,
            )
            .await
            .map_err(meta_error)?;
        let _permit = self
            .repository
            .budget
            .admit(&[(V3BudgetPool::Workspace, 1 << 20)])
            .map_err(meta_error)?;
        let mut result = Vec::with_capacity(rows.len());
        for (index, (key, value)) in rows.iter().enumerate() {
            if key.len() != prefix.len() + 8
                || u64::from_be_bytes(key[key.len() - 8..].try_into().unwrap())
                    != offset + index as u64
            {
                return Err(meta_error(invalid("directory ordinal gap")));
            }
            let (actual_parent, child, name) =
                decode_dentry(self.repository.body(key, value).map_err(meta_error)?)
                    .map_err(meta_error)?;
            if actual_parent != parent {
                return Err(meta_error(invalid("directory parent mismatch")));
            }
            let child_record = self
                .repository
                .inode(child)
                .await
                .map_err(meta_error)?
                .ok_or_else(|| meta_error(invalid("directory child missing")))?;
            result.push(RawDirEntry {
                name: name.to_vec(),
                ino: child as i64,
                kind: child_record.attr().kind,
            });
        }
        Ok(result)
    }
}

impl<B: ObjectBackend + Clone + 'static> NativePackedPlacementProvider<B> {
    async fn names_raw(&self, ino: i64) -> Result<Vec<(i64, Vec<u8>)>, MetaError> {
        let permit = self
            .repository
            .budget
            .admit(&[(V3BudgetPool::Workspace, 1 << 20)])
            .map_err(meta_error)?;
        self.names_raw_in_workspace(ino, &permit).await
    }
    async fn names_raw_in_workspace(
        &self,
        ino: i64,
        _workspace: &super::V3OwnedPermit,
    ) -> Result<Vec<(i64, Vec<u8>)>, MetaError> {
        let inode = u64::try_from(ino).map_err(|_| MetaError::NotFound(ino))?;
        self.entry(ino).await?.ok_or(MetaError::NotFound(ino))?;
        let mut result = Vec::new();
        let mut after = None;
        let mut bytes = 0usize;
        loop {
            let rows = self
                .repository
                .rows(
                    self.repository.record_key(b'n', inode),
                    after.as_deref(),
                    PAGE,
                    4096,
                )
                .await
                .map_err(meta_error)?;
            if rows.is_empty() {
                break;
            }
            for (key, value) in rows.iter() {
                let (parent, child, name) =
                    decode_dentry(self.repository.body(key, value).map_err(meta_error)?)
                        .map_err(meta_error)?;
                if child != inode {
                    return Err(meta_error(invalid("reverse child mismatch")));
                }
                bytes += name.len() + 32;
                if result.len() >= 4096 || bytes > 256 << 10 {
                    return Err(meta_error(PackedWireError::LimitExceeded(
                        "use paged native reverse index for large hardlink set".into(),
                    )));
                }
                result.push((parent as i64, name.to_vec()));
            }
            after = rows.last().map(|(key, _)| key.clone());
        }
        Ok(result)
    }
}

#[async_trait]
impl<B: ObjectBackend + Clone + 'static> MetaLayer for NativePackedPlacementProvider<B> {
    fn reserve_inline_roots(
        &self,
        bytes: u64,
    ) -> Result<Option<asyncfuse::raw::reply::InlineRootPermit>, MetaError> {
        let permit = self
            .repository
            .budget
            .admit(&[(V3BudgetPool::Roots, bytes)])
            .map_err(meta_error)?;
        let permit = asyncfuse::raw::reply::InlineRootPermit::try_new(permit).map_err(|_| {
            meta_error(PackedWireError::LimitExceeded(
                "inline Roots permit layout".into(),
            ))
        })?;
        Ok(Some(permit))
    }
    fn reserve_memory(
        &self,
        kind: crate::meta::layer::MetadataMemoryKind,
        bytes: u64,
    ) -> Result<Option<crate::meta::layer::MetadataMemoryGuard>, MetaError> {
        use crate::meta::layer::MetadataMemoryKind;
        let checked = |extra| {
            bytes.checked_add(extra).ok_or_else(|| {
                meta_error(PackedWireError::LimitExceeded(
                    "FUSE memory size overflow".into(),
                ))
            })
        };
        let charges = match kind {
            MetadataMemoryKind::Roots => vec![(V3BudgetPool::Roots, bytes)],
            MetadataMemoryKind::Request => vec![
                (V3BudgetPool::Control, 8192),
                (V3BudgetPool::Metadata, checked(2 << 20)?),
            ],
            MetadataMemoryKind::Reply => vec![
                (V3BudgetPool::Output, checked(1 << 20)?),
                (V3BudgetPool::Control, 4096),
            ],
            // The persistent handle retains attributes and reader state after
            // request completion; its unchanged guard follows the last owner.
            MetadataMemoryKind::Handle => vec![(V3BudgetPool::Metadata, bytes.max(8192))],
            MetadataMemoryKind::Control => vec![(V3BudgetPool::Control, bytes)],
        };
        Ok(Some(Arc::new(
            self.repository.budget.admit(&charges).map_err(meta_error)?,
        )))
    }
    fn name(&self) -> &'static str {
        "native-tikv-matched-placement-readonly"
    }
    fn supports_fuse_read_cancellation(&self) -> bool {
        // Namespace transactions and the shared executor are read-only; a
        // dropped adapter future releases its waiter and owned reservations.
        true
    }
    fn posix_acl_capability(&self) -> crate::meta::layer::PosixAclCapability {
        crate::meta::layer::PosixAclCapability::ReadOnly
    }
    fn root_ino(&self) -> i64 {
        self.root.load(Ordering::Acquire)
    }
    fn chroot(&self, inode: i64) {
        self.root.store(inode, Ordering::Release);
    }
    async fn initialize(&self) -> Result<(), MetaError> {
        self.entry(self.root_ino())
            .await?
            .ok_or(MetaError::NotFound(self.root_ino()))?;
        Ok(())
    }
    async fn stat_fs(&self) -> Result<StatFsSnapshot, MetaError> {
        Ok(stat_fs_snapshot_from_usage(0, 0))
    }
    async fn stat(&self, ino: i64) -> Result<Option<FileAttr>, MetaError> {
        self.stat_fresh(ino).await
    }
    async fn stat_fresh(&self, ino: i64) -> Result<Option<FileAttr>, MetaError> {
        Ok(self.entry(ino).await?.map(|record| record.attr()))
    }
    async fn lookup(&self, parent: i64, name: &str) -> Result<Option<i64>, MetaError> {
        Ok(self
            .lookup_with_attr_bytes(parent, name.as_bytes())
            .await?
            .map(|(ino, _)| ino))
    }
    async fn lookup_with_attr_bytes(
        &self,
        parent: i64,
        name: &[u8],
    ) -> Result<Option<(i64, FileAttr)>, MetaError> {
        let parent_record = self
            .entry(parent)
            .await?
            .ok_or(MetaError::NotFound(parent))?;
        if parent_record.entry.kind != 2 {
            return Err(MetaError::NotDirectory(parent));
        }
        if name == b"." {
            return Ok(Some((parent, parent_record.attr())));
        }
        if name == b".." {
            let ino = self.get_dir_parent(parent).await?.unwrap_or(parent);
            return Ok(self.stat_fresh(ino).await?.map(|attr| (ino, attr)));
        }
        if name.is_empty() || name.len() > 1024 || name.contains(&0) || name.contains(&b'/') {
            return Err(MetaError::InvalidFilename);
        }
        let key = [
            self.repository.record_key(b'l', parent as u64),
            name.to_vec(),
        ]
        .concat();
        let Some(body) = self
            .repository
            .get_body(key, 4096)
            .await
            .map_err(meta_error)?
        else {
            return Ok(None);
        };
        let (actual_parent, inode, actual_name) = decode_dentry(&body).map_err(meta_error)?;
        if actual_parent != parent as u64 || actual_name != name {
            return Err(meta_error(invalid("lookup key/value disagreement")));
        }
        let attr = self
            .stat_fresh(inode as i64)
            .await?
            .ok_or_else(|| meta_error(invalid("lookup inode absent")))?;
        Ok(Some((inode as i64, attr)))
    }
    async fn lookup_path(&self, path: &str) -> Result<Option<(i64, FileType)>, MetaError> {
        let mut inode = self.root_ino();
        for name in path.split('/').filter(|name| !name.is_empty()) {
            let Some((next, _)) = self.lookup_with_attr_bytes(inode, name.as_bytes()).await? else {
                return Ok(None);
            };
            inode = next;
        }
        Ok(self.stat_fresh(inode).await?.map(|attr| (inode, attr.kind)))
    }
    async fn readdir(&self, ino: i64) -> Result<Vec<DirEntry>, MetaError> {
        let _permit = self
            .repository
            .budget
            .admit(&[(V3BudgetPool::Workspace, 1 << 20)])
            .map_err(meta_error)?;
        let handle = self.opendir(ino).await?;
        let mut offset = 0;
        let mut result = Vec::new();
        let mut bytes = 0;
        loop {
            let page = handle.get_entries_page_raw(offset, PAGE).await?;
            if page.is_empty() {
                break;
            }
            offset += page.len() as u64;
            for entry in page {
                bytes += entry.name.len() + 64;
                if bytes > 256 << 10 || result.len() >= 4096 {
                    return Err(meta_error(PackedWireError::LimitExceeded(
                        "use paged native directory API".into(),
                    )));
                }
                result.push(DirEntry {
                    name: String::from_utf8(entry.name).map_err(|_| MetaError::InvalidFilename)?,
                    ino: entry.ino,
                    kind: entry.kind,
                });
            }
        }
        Ok(result)
    }
    async fn opendir(&self, ino: i64) -> Result<DirHandle, MetaError> {
        let record = self.entry(ino).await?.ok_or(MetaError::NotFound(ino))?;
        if record.entry.kind != 2 {
            return Err(MetaError::NotDirectory(ino));
        }
        Ok(DirHandle::new_paged(
            ino,
            Arc::new(NativeDirectorySource {
                repository: self.repository.clone(),
            }),
        ))
    }
    async fn mkdir(&self, _parent: i64, _name: String) -> Result<i64, MetaError> {
        Self::readonly().await
    }
    async fn rmdir(&self, _parent: i64, _name: &str) -> Result<(), MetaError> {
        Self::readonly().await
    }
    async fn create_file(&self, _parent: i64, _name: String) -> Result<i64, MetaError> {
        Self::readonly().await
    }
    async fn create_file_with_attr(
        &self,
        _parent: i64,
        _name: String,
    ) -> Result<CreateEntryResult, MetaError> {
        Self::readonly().await
    }
    async fn create_node(
        &self,
        _parent: i64,
        _name: String,
        _kind: FileType,
        _mode: u32,
        _uid: u32,
        _gid: u32,
        _rdev: u32,
    ) -> Result<i64, MetaError> {
        Self::readonly().await
    }
    async fn link(&self, _ino: i64, _parent: i64, _name: &str) -> Result<FileAttr, MetaError> {
        Self::readonly().await
    }
    async fn symlink(
        &self,
        _parent: i64,
        _name: &str,
        _target: &str,
    ) -> Result<(i64, FileAttr), MetaError> {
        Self::readonly().await
    }
    async fn unlink(&self, _parent: i64, _name: &str) -> Result<(), MetaError> {
        Self::readonly().await
    }
    async fn rename(
        &self,
        _old_parent: i64,
        _old_name: &str,
        _new_parent: i64,
        _new_name: String,
    ) -> Result<(), MetaError> {
        Self::readonly().await
    }
    async fn rename_noreplace(
        &self,
        _old_parent: i64,
        _old_name: &str,
        _new_parent: i64,
        _new_name: String,
    ) -> Result<(), MetaError> {
        Self::readonly().await
    }
    async fn rename_exchange(
        &self,
        _old_parent: i64,
        _old_name: &str,
        _new_parent: i64,
        _new_name: &str,
    ) -> Result<(), MetaError> {
        Self::readonly().await
    }
    async fn set_file_size(&self, _ino: i64, _size: u64) -> Result<(), MetaError> {
        Self::readonly().await
    }
    async fn extend_file_size(&self, _ino: i64, _size: u64) -> Result<(), MetaError> {
        Self::readonly().await
    }
    async fn truncate(&self, _ino: i64, _size: u64, _chunk: u64) -> Result<(), MetaError> {
        Self::readonly().await
    }
    async fn get_names(&self, ino: i64) -> Result<Vec<(Option<i64>, String)>, MetaError> {
        self.names_raw(ino)
            .await?
            .into_iter()
            .map(|(parent, name)| {
                Ok((
                    Some(parent),
                    String::from_utf8(name).map_err(|_| MetaError::InvalidFilename)?,
                ))
            })
            .collect()
    }
    async fn get_dentries(&self, ino: i64) -> Result<Vec<(i64, String)>, MetaError> {
        self.names_raw(ino)
            .await?
            .into_iter()
            .map(|(parent, name)| {
                Ok((
                    parent,
                    String::from_utf8(name).map_err(|_| MetaError::InvalidFilename)?,
                ))
            })
            .collect()
    }
    async fn get_dir_parent(&self, ino: i64) -> Result<Option<i64>, MetaError> {
        if ino == self.root_ino() {
            return Ok(Some(ino));
        }
        let record = self.entry(ino).await?.ok_or(MetaError::NotFound(ino))?;
        if record.entry.kind != 2 {
            return Err(MetaError::NotDirectory(ino));
        }
        Ok(self
            .names_raw(ino)
            .await?
            .first()
            .map(|(parent, _)| *parent))
    }
    async fn get_paths(&self, ino: i64) -> Result<Vec<String>, MetaError> {
        self.get_paths_bytes(ino)
            .await?
            .into_iter()
            .map(|path| String::from_utf8(path).map_err(|_| MetaError::InvalidFilename))
            .collect()
    }
    async fn get_paths_bytes(&self, ino: i64) -> Result<Vec<Vec<u8>>, MetaError> {
        Ok(self.get_paths_bytes_owned(ino).await?.paths)
    }
    async fn get_paths_bytes_owned(
        &self,
        ino: i64,
    ) -> Result<crate::meta::layer::OwnedPaths, MetaError> {
        // <=1 MiB paths plus Vec headers/ancestor components and allocation
        // overhead. Reserve before constructing any returned paths.
        let permit = self
            .repository
            .budget
            .admit(&[(V3BudgetPool::Output, 2 << 20)])
            .map_err(meta_error)?;
        let guard: crate::meta::layer::MetadataMemoryGuard = Arc::new(NativePathsOwner {
            _permit: permit,
            _repository: self.repository.clone(),
        });
        self.entry(ino).await?.ok_or(MetaError::NotFound(ino))?;
        // One 8 MiB arena covers the <=4 MiB pending paths, <=1 MiB results,
        // reverse-name scratch and bounded Vec/path-copy overhead. Nested
        // reverse lookups borrow it instead of reserving the same scratch twice.
        let workspace = self
            .repository
            .budget
            .admit(&[(V3BudgetPool::Workspace, 8 << 20)])
            .map_err(meta_error)?;
        let root = self.root_ino();
        let mut pending = vec![(ino, Vec::<u8>::new(), 0usize)];
        let mut result = Vec::new();
        let mut bytes = 0usize;
        let mut pending_bytes = 64usize;
        while let Some((inode, suffix, depth)) = pending.pop() {
            pending_bytes = pending_bytes.saturating_sub(suffix.len() + 64);
            if depth > 4096 {
                return Err(meta_error(invalid("namespace path cycle/depth limit")));
            }
            if inode == root {
                let path = [b"/".as_slice(), suffix.as_slice()].concat();
                bytes += path.len();
                if bytes > 1 << 20 || result.len() >= 4096 {
                    return Err(meta_error(PackedWireError::LimitExceeded(
                        "native path materialization exceeds bound".into(),
                    )));
                }
                result.push(path);
                continue;
            }
            for (parent, name) in self.names_raw_in_workspace(inode, &workspace).await? {
                let path = if suffix.is_empty() {
                    name
                } else {
                    [name.as_slice(), b"/", suffix.as_slice()].concat()
                };
                pending_bytes = pending_bytes
                    .checked_add(path.len() + 64)
                    .ok_or_else(|| meta_error(invalid("path work count overflows")))?;
                if path.len() > 128 << 10 || pending.len() >= 4096 || pending_bytes > 4 << 20 {
                    return Err(meta_error(PackedWireError::LimitExceeded(
                        "native path work exceeds bound".into(),
                    )));
                }
                pending.push((parent, path, depth + 1));
            }
        }
        self.repository.budget.admit(&[]).map_err(meta_error)?;
        Ok(crate::meta::layer::OwnedPaths {
            paths: result,
            guard: Some(guard),
        })
    }
    async fn read_symlink(&self, ino: i64) -> Result<String, MetaError> {
        String::from_utf8(self.read_symlink_bytes(ino).await?)
            .map_err(|_| MetaError::InvalidFilename)
    }
    async fn read_symlink_bytes(&self, ino: i64) -> Result<Vec<u8>, MetaError> {
        let record = self.entry(ino).await?.ok_or(MetaError::NotFound(ino))?;
        if record.entry.kind != 3 {
            return Err(MetaError::NotSupported("readlink requires symlink".into()));
        }
        record
            .cold()
            .map_err(meta_error)?
            .and_then(|attrs| attrs.symlink_target)
            .ok_or_else(|| meta_error(invalid("symlink target absent")))
    }
    async fn set_attr(
        &self,
        _ino: i64,
        _req: &SetAttrRequest,
        _flags: SetAttrFlags,
    ) -> Result<FileAttr, MetaError> {
        Self::readonly().await
    }
    async fn open(&self, ino: i64, flags: OpenFlags) -> Result<FileAttr, MetaError> {
        if flags.contains(OpenFlags::WRONLY)
            || flags.intersects(OpenFlags::APPEND | OpenFlags::TRUNC | OpenFlags::CREATE)
        {
            return Self::readonly().await;
        }
        self.stat_fresh(ino).await?.ok_or(MetaError::NotFound(ino))
    }
    async fn close(&self, _ino: i64) -> Result<(), MetaError> {
        Ok(())
    }
    async fn record_open(
        &self,
        _ino: i64,
        _attr: FileAttr,
        _read: bool,
        write: bool,
        append: bool,
    ) -> Result<(), MetaError> {
        if write || append {
            return Self::readonly().await;
        }
        Ok(())
    }
    async fn write(
        &self,
        _ino: i64,
        _chunk: u64,
        _slice: SliceDesc,
        _size: u64,
    ) -> Result<(), MetaError> {
        Self::readonly().await
    }
    async fn get_deleted_files(&self) -> Result<Vec<i64>, MetaError> {
        Ok(vec![])
    }
    async fn remove_file_metadata(&self, _ino: i64) -> Result<(), MetaError> {
        Self::readonly().await
    }
    async fn get_slices(&self, _chunk: u64) -> Result<Vec<SliceDesc>, MetaError> {
        Err(meta_error(invalid(
            "matched native arm forbids legacy slices",
        )))
    }
    async fn append_slice(&self, _chunk: u64, _slice: SliceDesc) -> Result<(), MetaError> {
        Self::readonly().await
    }
    async fn next_id(&self, _key: &str) -> Result<i64, MetaError> {
        Self::readonly().await
    }
    async fn start_session(&self, _session: SessionInfo) -> Result<(), MetaError> {
        Ok(())
    }
    async fn shutdown_session(&self) -> Result<(), MetaError> {
        self.data_metrics.transport.stop_admission();
        self.reader.close().await;
        self.data_metrics
            .transport
            .drain()
            .await
            .map_err(MetaError::Anyhow)?;
        self.repository.budget.close();
        Ok(())
    }
    async fn get_plock(
        &self,
        _inode: i64,
        _query: &FileLockQuery,
    ) -> Result<FileLockInfo, MetaError> {
        Self::readonly().await
    }
    async fn set_plock(
        &self,
        _inode: i64,
        _owner: i64,
        _block: bool,
        _kind: FileLockType,
        _range: FileLockRange,
        _pid: u32,
    ) -> Result<(), MetaError> {
        Self::readonly().await
    }
    async fn get_flock(&self, _inode: i64, _owner: i64) -> Result<FileLockType, MetaError> {
        Self::readonly().await
    }
    async fn set_flock(
        &self,
        _inode: i64,
        _owner: i64,
        _block: bool,
        _kind: FileLockType,
    ) -> Result<(), MetaError> {
        Self::readonly().await
    }
    async fn set_xattr(
        &self,
        _inode: i64,
        _name: &str,
        _value: &[u8],
        _flags: u32,
    ) -> Result<(), MetaError> {
        Self::readonly().await
    }
    async fn set_xattr_bytes(
        &self,
        _inode: i64,
        _name: &[u8],
        _value: &[u8],
        _flags: u32,
    ) -> Result<(), MetaError> {
        Self::readonly().await
    }
    async fn get_xattr(&self, inode: i64, name: &str) -> Result<Option<Vec<u8>>, MetaError> {
        self.get_xattr_bytes(inode, name.as_bytes()).await
    }
    async fn get_xattr_bytes(&self, inode: i64, name: &[u8]) -> Result<Option<Vec<u8>>, MetaError> {
        let record = self.entry(inode).await?.ok_or(MetaError::NotFound(inode))?;
        Ok(record.cold().map_err(meta_error)?.and_then(|attrs| {
            attrs
                .xattrs
                .into_iter()
                .find(|attr| attr.name == name)
                .map(|attr| attr.value)
        }))
    }
    async fn list_xattr(&self, inode: i64) -> Result<Vec<String>, MetaError> {
        self.list_xattr_bytes(inode)
            .await?
            .into_iter()
            .map(|name| String::from_utf8(name).map_err(|_| MetaError::InvalidFilename))
            .collect()
    }
    async fn list_xattr_bytes(&self, inode: i64) -> Result<Vec<Vec<u8>>, MetaError> {
        Ok(self.list_xattr_bytes_owned(inode).await?.names)
    }
    async fn list_xattr_bytes_owned(
        &self,
        inode: i64,
    ) -> Result<crate::meta::layer::OwnedXattrNames, MetaError> {
        let permit = self
            .repository
            .budget
            .admit(&[(V3BudgetPool::Output, 2 << 20)])
            .map_err(meta_error)?;
        let record = self.entry(inode).await?.ok_or(MetaError::NotFound(inode))?;
        let names: Vec<Vec<u8>> = record
            .cold()
            .map_err(meta_error)?
            .map_or_else(Vec::new, |attrs| {
                attrs.xattrs.into_iter().map(|attr| attr.name).collect()
            });
        if names.iter().map(|name| name.len() + 1).sum::<usize>() > 65536 {
            return Err(MetaError::Io(std::io::Error::from_raw_os_error(
                libc::E2BIG,
            )));
        }
        Ok(crate::meta::layer::OwnedXattrNames {
            names,
            guard: Some(Arc::new(permit)),
        })
    }
    async fn remove_xattr(&self, _inode: i64, _name: &str) -> Result<(), MetaError> {
        Self::readonly().await
    }
    async fn remove_xattr_bytes(&self, _inode: i64, _name: &[u8]) -> Result<(), MetaError> {
        Self::readonly().await
    }
    async fn set_acl(&self, _inode: i64, _rule: AclRule) -> Result<(), MetaError> {
        Self::readonly().await
    }
    async fn get_acl(
        &self,
        inode: i64,
        acl_type: u8,
        acl_id: u32,
    ) -> Result<Option<AclRule>, MetaError> {
        let record = self.entry(inode).await?.ok_or(MetaError::NotFound(inode))?;
        Ok(record.cold().map_err(meta_error)?.and_then(|attrs| {
            attrs
                .acl
                .into_iter()
                .find(|rule| rule.acl_type == acl_type && rule.qualifier == acl_id)
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::super::{V3ProducerOptions, V3SnapshotProducer};
    use super::*;
    use crate::cadapter::localfs::LocalFsBackend;
    use crate::cadapter::read_observer::{Engine, Origin, Phase, ReadObserver};
    use crate::chunk::read_plan::execute_unified_into;
    use crate::workspace_overlay::packed_v3::{
        AccessProfile, GroupMeta, PackedCodec, PackedFrameInput, PackedGroupInput,
        PackedV3ReadonlyMeta, SizeClass, SizeClassTable,
    };
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    #[tokio::test]
    async fn direct_paths_owned_packed_context_and_output_survive_adapter_drop() {
        let (_temp, client, snapshot, _reads) = fixture().await;
        let budget = super::super::V3MountBudget::defaults();
        let packed = PackedV3ReadonlyMeta::from_v3_budget(
            observed(client, &budget, Engine::PackedV3),
            snapshot,
            4096,
            0,
            budget.clone(),
        )
        .unwrap();
        let output = V3BudgetPool::Output as usize;
        let roots = V3BudgetPool::Roots as usize;
        let baseline = budget.state().used[output];
        for (target, expected) in [
            (7, vec![b"/".to_vec()]),
            (8, vec![b"/a".to_vec(), b"/z".to_vec()]),
            (999, Vec::new()),
        ] {
            let paths = packed.get_paths_bytes_owned(target).await.unwrap();
            assert_eq!(paths.paths, expected);
            assert_eq!(budget.state().used[output], baseline + (1 << 20));
            drop(paths);
            assert_eq!(budget.state().used[output], baseline);
        }
        let paths = packed.get_paths_bytes_owned(8).await.unwrap();
        let retained_roots = budget.state().used[roots];
        let owner = paths.guard.clone().unwrap();
        drop(packed);
        assert_eq!(budget.state().used[roots], retained_roots);
        drop(paths);
        assert_eq!(budget.state().used[output], baseline + (1 << 20));
        drop(owner);
        assert_eq!(budget.state().used[output], baseline);
        assert!(budget.state().used[roots] < retained_roots);
    }

    #[tokio::test]
    async fn direct_paths_owned_native_repository_and_output_survive_adapter_drop() {
        let (_temp, client, snapshot, _reads) = fixture().await;
        let (_kv, repository, native) = imported(client, snapshot).await;
        let budget = repository.budget.clone();
        let output = V3BudgetPool::Output as usize;
        let baseline = budget.state().used[output];
        for (target, expected) in [
            (7, vec![b"/".to_vec()]),
            (8, vec![b"/z".to_vec(), b"/a".to_vec()]),
        ] {
            let paths = native.get_paths_bytes_owned(target).await.unwrap();
            assert_eq!(paths.paths, expected);
            assert_eq!(budget.state().used[output], baseline + (2 << 20));
            drop(paths);
            assert_eq!(budget.state().used[output], baseline);
        }
        assert!(native.get_paths_bytes_owned(999).await.is_err());
        assert_eq!(budget.state().used[output], baseline);
        let paths = native.get_paths_bytes_owned(8).await.unwrap();
        let retained = Arc::downgrade(&repository);
        drop(native);
        drop(repository);
        assert!(retained.upgrade().is_some());
        assert_eq!(budget.state().used[output], baseline + (2 << 20));
        drop(paths);
        assert!(retained.upgrade().is_none());
        assert_eq!(budget.state().used[output], baseline);
    }

    #[tokio::test]
    async fn direct_paths_owned_closed_budget_rejects_root_before_backend_read() {
        let (_temp, client, snapshot, _reads) = fixture().await;
        let (_kv, repository, native) = imported(client.clone(), snapshot.clone()).await;
        let native_before = repository.counters[1].started.load(Ordering::Relaxed);
        repository.budget.close();
        assert!(native.get_paths_bytes_owned(7).await.is_err());
        assert_eq!(
            repository.counters[1].started.load(Ordering::Relaxed),
            native_before
        );
        let budget = super::super::V3MountBudget::defaults();
        let packed = PackedV3ReadonlyMeta::from_v3_budget(
            observed(client, &budget, Engine::PackedV3),
            snapshot,
            4096,
            0,
            budget.clone(),
        )
        .unwrap();
        budget.close();
        assert!(packed.get_paths_bytes_owned(7).await.is_err());
        assert_eq!(budget.state().used[V3BudgetPool::Output as usize], 0);
    }

    fn assert_request_payload_budget<M: MetaLayer>(
        adapter: &M,
        budget: &Arc<super::super::V3MountBudget>,
    ) {
        use crate::meta::layer::MetadataMemoryKind;

        let control = V3BudgetPool::Control as usize;
        let metadata = V3BudgetPool::Metadata as usize;
        assert_eq!(budget.capacity(V3BudgetPool::Control), 1 << 20);
        assert_eq!(budget.capacity(V3BudgetPool::Metadata), 256 << 20);
        let baseline = budget.state().used;
        // A maximum 4MiB FUSE WRITE also carries its 40-byte input header.
        let bytes = (4u64 << 20) + 40;
        let guard = adapter
            .reserve_memory(MetadataMemoryKind::Request, bytes)
            .expect("maximum advertised WRITE body must fit default request pools")
            .unwrap();
        let mut expected = baseline;
        expected[control] += 8192;
        expected[metadata] += (2 << 20) + bytes;
        assert_eq!(budget.state().used, expected);
        let last_body_owner = guard.clone();
        drop(guard);
        assert_eq!(budget.state().used, expected);
        drop(last_body_owner);
        assert_eq!(budget.state().used, baseline);

        // Payload capacity cannot substitute for exhausted control state.
        let held = budget
            .admit(&[(
                V3BudgetPool::Control,
                budget.capacity(V3BudgetPool::Control) - baseline[control],
            )])
            .unwrap();
        let full = budget.state().used;
        assert!(
            adapter
                .reserve_memory(MetadataMemoryKind::Request, bytes)
                .is_err()
        );
        assert_eq!(budget.state().used, full, "failed admission is atomic");
        drop(held);
        assert_eq!(budget.state().used, baseline);

        let held = budget
            .admit(&[(
                V3BudgetPool::Metadata,
                budget.capacity(V3BudgetPool::Metadata) - baseline[metadata],
            )])
            .unwrap();
        let full = budget.state().used;
        assert!(
            adapter
                .reserve_memory(MetadataMemoryKind::Request, bytes)
                .is_err()
        );
        assert_eq!(
            budget.state().used,
            full,
            "payload refusal must not charge Control"
        );
        drop(held);
        assert_eq!(budget.state().used, baseline);

        for bytes in [u64::MAX, u64::MAX - (2 << 20) + 1] {
            assert!(
                adapter
                    .reserve_memory(MetadataMemoryKind::Request, bytes)
                    .is_err()
            );
            assert_eq!(
                budget.state().used,
                baseline,
                "overflow must not partly charge"
            );
        }
    }

    #[tokio::test]
    async fn request_payload_admission_uses_metadata_and_bounded_control_in_both_adapters() {
        let (_temp, client, snapshot, _reads) = fixture().await;
        let (_kv, repository, native) = imported(client.clone(), snapshot.clone()).await;
        assert_request_payload_budget(native.as_ref(), &repository.budget);

        let budget = super::super::V3MountBudget::defaults();
        let packed = PackedV3ReadonlyMeta::from_v3_budget(
            observed(client, &budget, Engine::PackedV3),
            snapshot,
            4096,
            0,
            budget.clone(),
        )
        .unwrap();
        assert_request_payload_budget(&packed, &budget);
    }

    #[tokio::test]
    async fn native_existing_handle_guards_charge_full_metadata_until_last_consumer() {
        use crate::meta::layer::MetadataMemoryKind;
        let (_temp, client, snapshot, _reads) = fixture().await;
        let mut limits = super::super::V3BudgetLimits::default();
        limits.bytes[V3BudgetPool::Control as usize] = 32 << 10;
        limits.bytes[V3BudgetPool::Metadata as usize] = 64 << 20;
        let budget = super::super::V3MountBudget::new(limits).unwrap();
        budget.validate_frame_capability(8 << 20).unwrap();
        let repository = NativePlacementRepository::with_kv(
            Arc::new(MemoryKv::default()),
            "minimum-control-handle",
            snapshot.read_generation().lower_snapshot,
            budget.clone(),
        );
        let client = observed(client, &budget, Engine::Native);
        repository
            .import_from(client.clone(), snapshot.clone(), 4096, 0)
            .await
            .unwrap();
        let native = NativePackedPlacementProvider::open(repository, client, snapshot, 4096, 0)
            .await
            .unwrap();
        let baseline = budget.state().used;
        for bytes in [1u64, 8192, 16384] {
            let guard = native
                .reserve_memory(MetadataMemoryKind::Handle, bytes)
                .unwrap()
                .unwrap();
            let mut expected = baseline;
            expected[V3BudgetPool::Metadata as usize] += bytes.max(8192);
            assert_eq!(budget.state().used, expected);
            let last_consumer = guard.clone();
            drop(guard);
            assert_eq!(budget.state().used, expected);
            drop(last_consumer);
            assert_eq!(budget.state().used, baseline);
        }
    }

    #[tokio::test]
    async fn native_metadata_exhaustion_rejects_handles_and_preserves_other_kind_charges() {
        use crate::meta::layer::MetadataMemoryKind;
        let (_temp, client, snapshot, _reads) = fixture().await;
        let (_kv, repository, native) = imported(client, snapshot).await;
        let budget = &repository.budget;
        let baseline = budget.state().used;
        let held = budget
            .admit(&[(
                V3BudgetPool::Metadata,
                budget.capacity(V3BudgetPool::Metadata) - baseline[V3BudgetPool::Metadata as usize],
            )])
            .unwrap();
        let full = budget.state().used;
        assert!(
            native
                .reserve_memory(MetadataMemoryKind::Handle, 16384)
                .is_err()
        );
        assert_eq!(budget.state().used, full);
        drop(held);
        assert_eq!(budget.state().used, baseline);
        for (kind, charges) in [
            (MetadataMemoryKind::Roots, vec![(V3BudgetPool::Roots, 513)]),
            (
                MetadataMemoryKind::Request,
                vec![
                    (V3BudgetPool::Control, 8192),
                    (V3BudgetPool::Metadata, (2 << 20) + 513),
                ],
            ),
            (
                MetadataMemoryKind::Reply,
                vec![
                    (V3BudgetPool::Output, 513 + (1 << 20)),
                    (V3BudgetPool::Control, 4096),
                ],
            ),
            (
                MetadataMemoryKind::Control,
                vec![(V3BudgetPool::Control, 513)],
            ),
        ] {
            let guard = native.reserve_memory(kind, 513).unwrap().unwrap();
            let mut expected = baseline;
            for (pool, bytes) in charges {
                expected[pool as usize] += bytes;
            }
            assert_eq!(budget.state().used, expected);
            let consumer = guard.clone();
            drop(guard);
            assert_eq!(budget.state().used, expected);
            drop(consumer);
            assert_eq!(budget.state().used, baseline);
        }
    }

    #[derive(Default)]
    struct MemoryKv {
        values: Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
        pause_put: std::sync::atomic::AtomicBool,
        entered: tokio::sync::Notify,
        resume: tokio::sync::Notify,
    }
    #[async_trait]
    impl NativePlacementKv for MemoryKv {
        async fn get(&self, key: Vec<u8>) -> PackedResult<Option<Vec<u8>>> {
            Ok(self.values.lock().unwrap().get(&key).cloned())
        }
        async fn scan(
            &self,
            start: Vec<u8>,
            end: Vec<u8>,
            limit: u32,
        ) -> PackedResult<Vec<(Vec<u8>, Vec<u8>)>> {
            Ok(self
                .values
                .lock()
                .unwrap()
                .range(start..end)
                .take(limit as usize)
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect())
        }
        async fn put_import(
            &self,
            ready: Vec<u8>,
            key: Vec<u8>,
            value: Vec<u8>,
        ) -> PackedResult<()> {
            if self.pause_put.load(Ordering::Acquire) {
                self.entered.notify_one();
                self.resume.notified().await;
            }
            let mut values = self.values.lock().unwrap();
            if values.contains_key(&ready) {
                return Err(invalid("published namespace is immutable"));
            }
            if values.get(&key).is_some_and(|existing| existing != &value) {
                return Err(invalid("import record changed"));
            }
            values.insert(key, value);
            Ok(())
        }
        async fn publish(&self, ready: Vec<u8>, value: Vec<u8>) -> PackedResult<()> {
            let mut values = self.values.lock().unwrap();
            if values
                .get(&ready)
                .is_some_and(|existing| existing != &value)
            {
                return Err(invalid("ready binding changed"));
            }
            values.insert(ready, value);
            Ok(())
        }
    }
    #[derive(Clone)]
    struct RecordingBackend {
        local: LocalFsBackend,
        reads: Arc<Mutex<Vec<(String, u64, usize)>>>,
    }
    #[async_trait]
    impl ObjectBackend for RecordingBackend {
        async fn put_object(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
            self.local.put_object(key, data).await
        }
        async fn put_object_create_only(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
            self.local.put_object_create_only(key, data).await
        }
        async fn get_object(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
            self.reads.lock().unwrap().push((key.into(), u64::MAX, 0));
            self.local.get_object(key).await
        }
        async fn get_object_range(
            &self,
            key: &str,
            offset: u64,
            bytes: &mut [u8],
        ) -> anyhow::Result<usize> {
            self.reads
                .lock()
                .unwrap()
                .push((key.into(), offset, bytes.len()));
            self.local.get_object_range(key, offset, bytes).await
        }
        async fn get_etag(&self, key: &str) -> anyhow::Result<String> {
            self.local.get_etag(key).await
        }
        async fn delete_object(&self, key: &str) -> anyhow::Result<()> {
            self.local.delete_object(key).await
        }
    }
    async fn fixture() -> (
        tempfile::TempDir,
        ObjectClient<RecordingBackend>,
        AuthenticatedV3Snapshot,
        Arc<Mutex<Vec<(String, u64, usize)>>>,
    ) {
        fixture_with_inline(false).await
    }
    async fn fixture_with_inline(
        inline: bool,
    ) -> (
        tempfile::TempDir,
        ObjectClient<RecordingBackend>,
        AuthenticatedV3Snapshot,
        Arc<Mutex<Vec<(String, u64, usize)>>>,
    ) {
        let temp = tempfile::tempdir().unwrap();
        let backend = RecordingBackend {
            local: LocalFsBackend::new(temp.path().join("objects")),
            reads: Arc::new(Mutex::new(vec![])),
        };
        let reads = backend.reads.clone();
        let client = ObjectClient::new(backend);
        let mut entry = GroupMetaEntry {
            name: b"a".to_vec(),
            inode: 8,
            kind: 1,
            mode: 0o100644,
            uid: 17,
            gid: 23,
            rdev: 0,
            nlink: 2,
            atime_ns: 1,
            mtime_ns: 2,
            ctime_ns: 3,
            size: 12,
            flags: 0,
            inline_data: Arc::from([]),
            extents: vec![
                GroupMetaExtent {
                    file_offset: 0,
                    logical_len: 4,
                    frame_ordinal: 0,
                    raw_offset: 0,
                    raw_len: 4,
                },
                GroupMetaExtent {
                    file_offset: 8,
                    logical_len: 4,
                    frame_ordinal: 1,
                    raw_offset: 0,
                    raw_len: 4,
                },
            ],
        };
        if inline {
            entry.size = 4;
            entry.flags = crate::workspace_overlay::packed_v3::meta::INLINE_DATA_FLAG;
            entry.inline_data = Arc::from(b"abcd".as_slice());
            entry.extents.clear();
        }
        let mut alias = entry.clone();
        alias.name = b"z".to_vec();
        let metadata = GroupMeta::new(vec![entry, alias])
            .unwrap()
            .encode()
            .unwrap();
        let group = PackedGroupInput {
            group_id: 1,
            parent_dir_key: [7; 32],
            metadata,
            frame_ordinals: if inline { vec![] } else { vec![0, 1] },
            entry_count: 2,
            file_count: 2,
            layout_profile: AccessProfile::RandomSmallFile,
        };
        let mut frames = [b"abcd", b"wxyz"]
            .into_iter()
            .map(|raw| PackedFrameInput {
                raw: raw.to_vec(),
                size_class: SizeClass::Tiny,
                codec: 0,
                first_file_slot: 0,
                last_file_slot: 1,
            })
            .collect::<Vec<_>>();
        if inline {
            frames.clear();
        }
        let mut producer = V3SnapshotProducer::new(
            client.clone(),
            temp.path(),
            "matched-native".into(),
            V3ProducerOptions {
                snapshot_id: [9; 32],
                root_dir_key: [7; 32],
                root_inode: 7,
                profile: AccessProfile::RandomSmallFile,
                size_classes: SizeClassTable::default(),
                build_policy: super::super::V3BuildPolicy {
                    frames: Default::default(),
                    inline_data: inline,
                    p90: None,
                },
                metadata_codec: PackedCodec::Raw,
                data_codec: PackedCodec::Raw,
            },
        )
        .await
        .unwrap();
        producer
            .add_container(1, &[group], &frames, &[7])
            .await
            .unwrap();
        let reference = producer.finish().await.unwrap();
        let snapshot = AuthenticatedV3Snapshot::open(&client, &reference)
            .await
            .unwrap();
        (temp, client, snapshot, reads)
    }
    fn observed(
        client: ObjectClient<RecordingBackend>,
        budget: &Arc<super::super::V3MountBudget>,
        engine: Engine,
    ) -> ObjectClient<RecordingBackend> {
        client.with_read_observer(
            Arc::new(ReadObserver::with_mount_budget(budget).unwrap()),
            engine,
            Phase::Runtime,
            Origin::Demand,
        )
    }
    async fn imported(
        client: ObjectClient<RecordingBackend>,
        snapshot: AuthenticatedV3Snapshot,
    ) -> (
        Arc<MemoryKv>,
        Arc<NativePlacementRepository>,
        Arc<NativePackedPlacementProvider<RecordingBackend>>,
    ) {
        let budget = super::super::V3MountBudget::defaults();
        let kv = Arc::new(MemoryKv::default());
        let repo = NativePlacementRepository::with_kv(
            kv.clone(),
            "test",
            snapshot.read_generation().lower_snapshot,
            budget.clone(),
        );
        let client = observed(client, &budget, Engine::Native);
        repo.import_from(client.clone(), snapshot.clone(), 4096, 0)
            .await
            .unwrap();
        let provider = NativePackedPlacementProvider::open(repo.clone(), client, snapshot, 4096, 0)
            .await
            .unwrap();
        provider.start_runtime();
        (kv, repo, provider)
    }
    #[tokio::test]
    async fn native_tikv_same_snapshot_plan_and_data_match_packed_without_namespace_pages() {
        let (_temp, client, snapshot, reads) = fixture().await;
        let namespace_keys = [
            snapshot.root(V3RootKind::Inodes).key.clone(),
            snapshot.root(V3RootKind::Groups).key.clone(),
            snapshot.root(V3RootKind::ReverseNames).key.clone(),
        ];
        let (_kv, repo, native) = imported(client.clone(), snapshot.clone()).await;
        assert_eq!(native.root_ino(), 7);
        assert_eq!(native.lookup(7, "a").await.unwrap(), Some(8));
        assert_eq!(native.lookup(7, "z").await.unwrap(), Some(8));
        assert_eq!(native.stat_fresh(8).await.unwrap().unwrap().nlink, 2);
        let budget = super::super::V3MountBudget::defaults();
        let packed = PackedV3ReadonlyMeta::from_v3_budget(
            observed(client.clone(), &budget, Engine::PackedV3),
            snapshot,
            4096,
            0,
            budget,
        )
        .unwrap();
        assert!(native.supports_fuse_read_cancellation());
        assert!(packed.supports_fuse_read_cancellation());
        let expected = b"abcd\0\0\0\0wxyz";
        for (offset, len) in [(0, 12), (1, 2), (2, 9), (4, 4), (12, 0)] {
            reads.lock().unwrap().clear();
            let native_plan = native
                .prepare_unified_read(8, 0, offset, len)
                .await
                .unwrap()
                .unwrap();
            let native_requests = reads.lock().unwrap().clone();
            assert!(
                native_requests
                    .iter()
                    .all(|(key, _, _)| !namespace_keys.contains(key)),
                "native lookup accessed packed namespace"
            );
            let packed_plan = packed
                .prepare_unified_read(8, 0, offset, len)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                native_plan.plan, packed_plan.plan,
                "both metadata arms must share refs, descriptor fields and generation"
            );
            let mut left = vec![0; len as usize];
            let mut right = vec![0; len as usize];
            execute_unified_into(
                native_plan.fetcher.as_ref(),
                offset,
                &native_plan.plan,
                &mut left,
            )
            .await
            .unwrap();
            execute_unified_into(
                packed_plan.fetcher.as_ref(),
                offset,
                &packed_plan.plan,
                &mut right,
            )
            .await
            .unwrap();
            assert_eq!(left, right);
            assert_eq!(left, &expected[offset as usize..(offset + len) as usize]);
        }
        let dir = native.opendir(7).await.unwrap();
        assert_eq!(dir.get_entries_page_raw(1, 1).await.unwrap()[0].name, b"z");
        assert_eq!(
            native.get_paths_bytes(8).await.unwrap(),
            vec![b"/z".to_vec(), b"/a".to_vec()]
        );
        assert!(
            repo.counters
                .iter()
                .map(|c| c.started.load(Ordering::Relaxed))
                .sum::<u64>()
                > 0,
            "native metadata work must be real and observable"
        );
        let mut output = [0; 1];
        assert!(
            native
                .block_store()
                .read_range((1, 0), 0, &mut output)
                .await
                .is_err()
        );
        assert!(native.open(8, OpenFlags::RDONLY).await.is_ok());
        assert!(native.open(8, OpenFlags::TRUNC).await.is_err());
        assert!(
            native
                .record_open(
                    8,
                    native.stat_fresh(8).await.unwrap().unwrap(),
                    false,
                    true,
                    false
                )
                .await
                .is_err()
        );
    }
    #[tokio::test]
    async fn native_ready_rejects_incomplete_foreign_tampered_and_mutable_records() {
        let (_temp, client, snapshot, _reads) = fixture().await;
        let budget = super::super::V3MountBudget::defaults();
        let kv = Arc::new(MemoryKv::default());
        let repo = NativePlacementRepository::with_kv(
            kv.clone(),
            "test",
            snapshot.read_generation().lower_snapshot,
            budget.clone(),
        );
        let client = observed(client, &budget, Engine::Native);
        assert!(
            NativePackedPlacementProvider::open(
                repo.clone(),
                client.clone(),
                snapshot.clone(),
                4096,
                0
            )
            .await
            .is_err()
        );
        repo.import_from(client.clone(), snapshot.clone(), 4096, 0)
            .await
            .unwrap();
        let record = repo.inode(8).await.unwrap().unwrap();
        assert!(
            repo.put_body(repo.record_key(b'i', 8), &record.encode().unwrap())
                .await
                .is_err()
        );
        let foreign =
            NativePlacementRepository::with_kv(kv.clone(), "test", [1; 32], budget.clone());
        assert!(
            NativePackedPlacementProvider::open(foreign, client.clone(), snapshot.clone(), 4096, 0)
                .await
                .is_err()
        );
        let key = repo.record_key(b'i', 8);
        if let Some(byte) = kv.values.lock().unwrap().get_mut(&key).unwrap().last_mut() {
            *byte ^= 1;
        }
        assert!(repo.inode(8).await.is_err());
        assert!(
            NativePackedPlacementProvider::open(repo, client, snapshot, 4096, 0)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn native_and_packed_reject_the_same_tampered_frame_without_flat_fallback() {
        let (temp, client, snapshot, _reads) = fixture().await;
        let (_kv, _repo, native) = imported(client.clone(), snapshot.clone()).await;
        let budget = super::super::V3MountBudget::defaults();
        let packed = PackedV3ReadonlyMeta::from_v3_budget(
            observed(client, &budget, Engine::PackedV3),
            snapshot,
            4096,
            0,
            budget,
        )
        .unwrap();
        let left = native
            .prepare_unified_read(8, 0, 0, 12)
            .await
            .unwrap()
            .unwrap();
        let right = packed
            .prepare_unified_read(8, 0, 0, 12)
            .await
            .unwrap()
            .unwrap();
        let crate::chunk::read_plan::ReadSource::PackedFrame {
            container_ordinal,
            object_offset,
            ..
        } = left.plan.segments[0].source
        else {
            panic!("expected shared frame source");
        };
        let reference = native
            .snapshot
            .container_ref(&native.reader, container_ordinal)
            .await
            .unwrap();
        let path = temp.path().join("objects").join(&reference.key);
        let mut bytes = tokio::fs::read(&path).await.unwrap();
        bytes[object_offset as usize] ^= 0x20;
        tokio::fs::write(&path, bytes).await.unwrap();
        for prepared in [left, right] {
            let mut output = [0; 12];
            assert!(
                execute_unified_into(prepared.fetcher.as_ref(), 0, &prepared.plan, &mut output)
                    .await
                    .is_err()
            );
        }
    }
    #[tokio::test]
    async fn native_import_cancellation_never_publishes_ready_and_releases_workspace() {
        let (_temp, client, snapshot, _reads) = fixture().await;
        let budget = super::super::V3MountBudget::defaults();
        let kv = Arc::new(MemoryKv::default());
        kv.pause_put.store(true, Ordering::Release);
        let repo = NativePlacementRepository::with_kv(
            kv.clone(),
            "cancel",
            snapshot.read_generation().lower_snapshot,
            budget.clone(),
        );
        let client = observed(client, &budget, Engine::Native);
        let copy = repo.clone();
        let task = tokio::spawn(async move { copy.import_from(client, snapshot, 4096, 0).await });
        kv.entered.notified().await;
        task.abort();
        let _ = task.await;
        assert!(!kv.values.lock().unwrap().contains_key(&repo.ready_key()));
        assert_eq!(budget.state().used[V3BudgetPool::Workspace as usize], 0);
        assert_eq!(repo.counters[0].cancelled.load(Ordering::Relaxed), 1);
    }
    #[tokio::test]
    async fn native_matched_import_rejects_inline_payload_before_copy_or_ready() {
        let (_temp, client, snapshot, _reads) = fixture_with_inline(true).await;
        let budget = super::super::V3MountBudget::defaults();
        let kv = Arc::new(MemoryKv::default());
        let repo = NativePlacementRepository::with_kv(
            kv.clone(),
            "inline-off",
            snapshot.read_generation().lower_snapshot,
            budget.clone(),
        );
        let error = repo
            .import_from(observed(client, &budget, Engine::Native), snapshot, 4096, 0)
            .await
            .unwrap_err();
        assert!(
            matches!(error, PackedWireError::UnsupportedFormat(message) if message.contains("inline-off"))
        );
        let values = kv.values.lock().unwrap();
        assert!(!values.contains_key(&repo.ready_key()));
        assert!(!values.contains_key(&repo.record_key(b'i', 8)));
        assert_eq!(budget.state().used[V3BudgetPool::Workspace as usize], 0);
    }
    #[tokio::test]
    async fn native_low_budget_fails_before_kv_request_and_has_no_flat_fallback() {
        let (_temp, client, snapshot, _reads) = fixture().await;
        let (_kv, repo, native) = imported(client, snapshot).await;
        let used = repo.budget.state().used[V3BudgetPool::Metadata as usize];
        let capacity = repo.budget.capacity(V3BudgetPool::Metadata);
        let held = repo
            .budget
            .admit(&[(V3BudgetPool::Metadata, capacity - used - 1)])
            .unwrap();
        let starts = repo
            .counters
            .iter()
            .map(|c| c.started.load(Ordering::Relaxed))
            .sum::<u64>();
        let error = native.stat_fresh(8).await.unwrap_err();
        assert!(matches!(error,MetaError::Io(error)if error.raw_os_error()==Some(libc::ENOMEM)));
        assert_eq!(
            repo.counters
                .iter()
                .map(|c| c.started.load(Ordering::Relaxed))
                .sum::<u64>(),
            starts
        );
        drop(held);
    }

    // Existing APIs over the real Native provider and MemoryKv fixture. This
    // exercises its actual metadata path, but is not a live TiKV/kernel proof.
    mod minimum_roots_existing_api_tests {
        use super::super::super::{V3BudgetLimits, V3MountBudget};
        use super::*;
        use crate::VFS;
        use crate::vfs::config::VFSConfig;
        use asyncfuse::raw::{Filesystem, Request};

        // Ask the existing validator for its smallest supported Roots capacity. Other
        // pools keep their existing defaults; Control remains the existing 32768.
        // No copy of the validator's internal formula or new admission constant.
        fn minimum_supported_roots_budget() -> Arc<V3MountBudget> {
            let mut limits = V3BudgetLimits::default();
            limits.bytes[V3BudgetPool::Control as usize] = 32 << 10;
            let mut lower = 1u64;
            let mut upper = limits.bytes[V3BudgetPool::Roots as usize];
            while lower < upper {
                let middle = lower + (upper - lower) / 2;
                limits.bytes[V3BudgetPool::Roots as usize] = middle;
                if V3MountBudget::new(limits.clone())
                    .unwrap()
                    .validate_frame_capability(8 << 20)
                    .is_ok()
                {
                    upper = middle;
                } else {
                    lower = middle + 1;
                }
            }
            limits.bytes[V3BudgetPool::Roots as usize] = lower;
            let budget = V3MountBudget::new(limits.clone()).unwrap();
            budget.validate_frame_capability(8 << 20).unwrap();
            limits.bytes[V3BudgetPool::Roots as usize] = lower - 1;
            assert!(
                V3MountBudget::new(limits)
                    .unwrap()
                    .validate_frame_capability(8 << 20)
                    .is_err(),
                "one below the existing supported Roots floor"
            );
            eprintln!(
                "BREWFS_EXISTING_MINIMUM_ROOTS capacity={lower} control_capacity=32768 validator=existing_frame_capability"
            );
            budget
        }

        type NativeVfs =
            VFS<NativePlacementBlockStore, NativePackedPlacementProvider<RecordingBackend>>;
        fn request(unique: u64) -> Request {
            Request {
                unique,
                uid: 0,
                gid: 0,
                pid: std::process::id(),
            }
        }
        async fn minimum_fixture() -> (
            tempfile::TempDir,
            NativeVfs,
            Arc<V3MountBudget>,
            Arc<NativePlacementRepository>,
        ) {
            let (temp, client, snapshot, _reads) = fixture().await;
            let budget = minimum_supported_roots_budget();
            let repository = NativePlacementRepository::with_kv(
                Arc::new(MemoryKv::default()),
                "minimum-roots-provider",
                snapshot.read_generation().lower_snapshot,
                budget.clone(),
            );
            let client = observed(client, &budget, Engine::Native);
            repository
                .import_from(client.clone(), snapshot.clone(), 4096, 0)
                .await
                .unwrap();
            let native =
                NativePackedPlacementProvider::open(repository.clone(), client, snapshot, 4096, 0)
                    .await
                    .unwrap();
            native.start_runtime();
            let layout = crate::chunk::ChunkLayout {
                chunk_size: 4096,
                block_size: 4096,
            };
            let fs = VFS::from_workspace_components(
                VFSConfig::new(layout),
                Arc::new(native.block_store()),
                native,
            )
            .unwrap();
            (temp, fs, budget, repository)
        }
        #[tokio::test]
        async fn native_minimum_roots_inline_hooks_deny_before_kv_and_refund_and_recover() {
            let (_temp, fs, budget, repository) = minimum_fixture().await;
            let baseline = budget.state().used;
            let future =
                asyncfuse::raw::Session::<NativeVfs>::readonly_ordinary_worker_future_layout();
            let prepare = asyncfuse::raw::Session::<NativeVfs>::readonly_prepare_future_layout();
            eprintln!(
                "BREWFS_NATIVE_MINIMUM_ROOTS_PROVIDER_LAYOUT ordinary_bytes={} ordinary_align={} prepare_child_bytes={} prepare_holder_bytes={} prepare_outer_bytes={}",
                future.0, future.1, prepare.0.0, prepare.1.0, prepare.2.0
            );
            for bytes in [513u64, future.0 as u64, (prepare.0.0 + prepare.1.0) as u64] {
                let before_arcs = Arc::strong_count(&budget);
                let permit = Filesystem::reserve_inline_root_memory(&fs, bytes)
                    .unwrap()
                    .unwrap();
                let mut expected = baseline;
                expected[V3BudgetPool::Roots as usize] += bytes;
                assert_eq!(budget.state().used, expected);
                assert_eq!(
                    Arc::strong_count(&budget),
                    before_arcs + 1,
                    "only the existing budget Arc is retained"
                );
                let moved = Some(permit);
                assert_eq!(budget.state().used, expected);
                drop(moved);
                assert_eq!(budget.state().used, baseline);
                assert_eq!(Arc::strong_count(&budget), before_arcs);
                let prepare_permit = Filesystem::reserve_inline_prepare_memory(&fs, bytes)
                    .unwrap()
                    .unwrap();
                assert_eq!(budget.state().used, expected);
                drop(prepare_permit);
                assert_eq!(budget.state().used, baseline);
            }
            let starts = || {
                repository
                    .counters
                    .iter()
                    .map(|row| row.started.load(Ordering::Acquire))
                    .sum::<u64>()
            };
            let before_starts = starts();
            let remaining =
                budget.capacity(V3BudgetPool::Roots) - baseline[V3BudgetPool::Roots as usize];
            let held = budget.admit(&[(V3BudgetPool::Roots, remaining)]).unwrap();
            let full = budget.state().used;
            assert_eq!(
                Filesystem::reserve_inline_root_memory(&fs, 1).unwrap_err(),
                asyncfuse::Errno::from(libc::ENOMEM)
            );
            assert_eq!(
                Filesystem::reserve_inline_prepare_memory(&fs, 1).unwrap_err(),
                asyncfuse::Errno::from(libc::ENOMEM)
            );
            assert_eq!(budget.state().used, full);
            assert_eq!(
                starts(),
                before_starts,
                "denied Roots must not submit native metadata requests"
            );
            drop(held);
            assert_eq!(budget.state().used, baseline);
            let recovered = Filesystem::reserve_inline_root_memory(&fs, 1)
                .unwrap()
                .unwrap();
            drop(recovered);
            assert_eq!(budget.state().used, baseline);
        }
        #[tokio::test]
        async fn native_minimum_roots_first_valid_adapter_read_and_real_release_fit_original_control()
         {
            let (_temp, fs, budget, _repository) = minimum_fixture().await;
            let opened = Filesystem::open(&fs, request(3911), 8, libc::O_RDONLY as u32)
                .await
                .unwrap();
            let request_owner = Filesystem::reserve_request_memory(&fs, 40)
                .unwrap()
                .unwrap();
            let reply = Filesystem::read(&fs, request(3912), 8, opened.fh, 0, 12).await
                .expect("existing minimum supported Roots profile cannot serve the first native adapter read");
            assert_eq!(reply.data.as_ref(), b"abcd\0\0\0\0wxyz");
            drop(reply);
            drop(request_owner);
            Filesystem::release(&fs, request(3913), 8, opened.fh, 0, 0, false)
                .await
                .unwrap();
            assert_eq!(fs.open_file_handle_count(), 0);
            assert_eq!(budget.state().used[V3BudgetPool::Control as usize], 0);
            assert!(budget.state().peak[V3BudgetPool::Control as usize] <= 32768);
            assert!(
                budget.state().peak[V3BudgetPool::Roots as usize]
                    <= budget.capacity(V3BudgetPool::Roots)
            );
            Filesystem::prepare_unmount(&fs).await.unwrap();
        }
    }
}
