use crate::meta::store::MetaError;
use etcd_client::{
    Client as EtcdClient, Compare, CompareOp, GetOptions, GetResponse, PutOptions, Txn, TxnOp,
    TxnResponse,
};
use rand::{RngCore, rng};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::OnceLock;

const ETCD_TXN_LOCK_STRIPES: usize = 1024;
const ETCD_READ_MAX_ATTEMPTS: u64 = 4;

static ETCD_TXN_LOCKS: OnceLock<Vec<tokio::sync::Mutex<()>>> = OnceLock::new();

/// Retry an idempotent etcd range request after transient transport failures.
///
/// A write transaction is deliberately not retried here: if its response is lost,
/// the caller cannot know whether etcd committed it. Reads are safe to replay and
/// need this protection because an HTTP/2 GOAWAY otherwise leaks out as FUSE EIO.
pub(crate) async fn get_with_retry<F>(
    client: &EtcdClient,
    key: &str,
    mut options: F,
) -> Result<GetResponse, etcd_client::Error>
where
    F: FnMut() -> Option<GetOptions>,
{
    for attempt in 1..=ETCD_READ_MAX_ATTEMPTS {
        let mut client = client.clone();
        match client.get(key, options()).await {
            Ok(response) => return Ok(response),
            Err(error) if attempt < ETCD_READ_MAX_ATTEMPTS => {
                let delay_ms = 10_u64 << (attempt - 1);
                tracing::warn!(
                    key,
                    attempt,
                    delay_ms,
                    error = %error,
                    "retrying etcd read after request failure"
                );
                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
            }
            Err(error) => return Err(error),
        }
    }

    unreachable!("etcd read retry loop always returns")
}

/// Retry a transaction containing only read operations.
pub(crate) async fn read_txn_with_retry<F>(
    client: &EtcdClient,
    mut build: F,
) -> Result<TxnResponse, etcd_client::Error>
where
    F: FnMut() -> Txn,
{
    for attempt in 1..=ETCD_READ_MAX_ATTEMPTS {
        let mut client = client.clone();
        match client.txn(build()).await {
            Ok(response) => return Ok(response),
            Err(error) if attempt < ETCD_READ_MAX_ATTEMPTS => {
                let delay_ms = 10_u64 << (attempt - 1);
                tracing::warn!(
                    attempt,
                    delay_ms,
                    error = %error,
                    "retrying read-only etcd transaction after request failure"
                );
                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
            }
            Err(error) => return Err(error),
        }
    }

    unreachable!("etcd read transaction retry loop always returns")
}

enum EtcdTxnWriteOp {
    Put {
        value: Vec<u8>,
        options: Option<PutOptions>,
    },
    Delete,
}

/// Snapshot of a key captured during one transaction attempt.
///
/// `mod_revision == 0` means the key did not exist when it was read or
/// baselined. Commit turns that into `Compare::version(key, Equal, 0)`.
struct EtcdTxnReadSlot {
    value: Option<Vec<u8>>,
    mod_revision: i64,
}

/// In-memory optimistic transaction context for a single `EtcdTxn::run` attempt.
///
/// Typical usage is:
/// - read with `get`/`exists` or the typed helpers;
/// - stage mutations with `set`/`delete`;
/// - return `Ok(..)` from the closure and let `EtcdTxn` commit atomically.
///
/// Important semantics:
/// - read-your-own-write: a staged `set` is immediately visible to later `get`;
/// - no partial persistence: writes stay local until `commit`;
/// - blind writes are protected too: keys written without a prior read are baselined
///   before commit so CAS still guards them.
///
/// Prefer `get_typed`/`set_typed` for values serialized through
/// `crate::meta::serialization`, and `get_typed_json`/`set_typed_json` for the
/// legacy JSON-encoded etcd records still used in this store.
pub(crate) struct EtcdTxnCtx<'a> {
    client: &'a EtcdClient,
    reads: HashMap<String, EtcdTxnReadSlot>,
    writes: BTreeMap<String, EtcdTxnWriteOp>,
}

impl<'a> EtcdTxnCtx<'a> {
    fn new(client: &'a EtcdClient) -> Self {
        Self {
            client,
            reads: HashMap::new(),
            writes: BTreeMap::new(),
        }
    }

    async fn fetch_slot(&self, key: &str) -> Result<EtcdTxnReadSlot, MetaError> {
        let resp = get_with_retry(self.client, key, || None)
            .await
            .map_err(|e| MetaError::Internal(format!("Failed to get key {key}: {e}")))?;

        let slot = resp
            .kvs()
            .first()
            .map(|kv| EtcdTxnReadSlot {
                value: Some(kv.value().to_vec()),
                mod_revision: kv.mod_revision(),
            })
            .unwrap_or(EtcdTxnReadSlot {
                value: None,
                mod_revision: 0,
            });

        Ok(slot)
    }

    /// Reads a key using the transaction snapshot.
    ///
    /// If the key has already been written in this attempt, this returns the staged
    /// value instead of going back to etcd.
    pub async fn get(&mut self, key: impl AsRef<str>) -> Result<Option<Vec<u8>>, MetaError> {
        let key = key.as_ref();

        if let Some(op) = self.writes.get(key) {
            return Ok(match op {
                EtcdTxnWriteOp::Put { value, .. } => Some(value.clone()),
                EtcdTxnWriteOp::Delete => None,
            });
        }

        if let Some(slot) = self.reads.get(key) {
            return Ok(slot.value.clone());
        }

        let slot = self.fetch_slot(key).await?;
        let out = slot.value.clone();
        self.reads.insert(key.to_string(), slot);

        Ok(out)
    }

    pub async fn exists(&mut self, key: impl AsRef<str>) -> Result<bool, MetaError> {
        Ok(self.get(key).await?.is_some())
    }

    pub fn set(&mut self, key: impl Into<String>, value: Vec<u8>) {
        self.set_with_options(key, value, None);
    }

    /// Stages a put operation with optional etcd put options (for example lease
    /// attachment for session-scoped keys).
    pub fn set_with_options(
        &mut self,
        key: impl Into<String>,
        value: Vec<u8>,
        options: Option<PutOptions>,
    ) {
        self.writes
            .insert(key.into(), EtcdTxnWriteOp::Put { value, options });
    }

    pub fn delete(&mut self, key: impl Into<String>) {
        self.writes.insert(key.into(), EtcdTxnWriteOp::Delete);
    }

    #[cfg(feature = "rkyv-serialization")]
    pub async fn get_typed<T>(&mut self, key: impl AsRef<str>) -> Result<Option<T>, MetaError>
    where
        T: rkyv::Archive,
        T::Archived:
            rkyv::Deserialize<T, rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>>,
        for<'de> T: DeserializeOwned,
    {
        let raw = self.get(key).await?;

        raw.map(|raw| crate::meta::serialization::deserialize_meta::<T>(&raw))
            .transpose()
    }

    #[cfg(not(feature = "rkyv-serialization"))]
    pub async fn get_typed<T>(&mut self, key: impl AsRef<str>) -> Result<Option<T>, MetaError>
    where
        T: DeserializeOwned,
    {
        let raw = self.get(key).await?;

        raw.map(|raw| crate::meta::serialization::deserialize_meta::<T>(&raw))
            .transpose()
    }

    #[cfg(feature = "rkyv-serialization")]
    pub fn set_typed<T>(&mut self, key: impl Into<String>, value: &T) -> Result<(), MetaError>
    where
        T: rkyv::Archive,
        for<'ser> T: rkyv::Serialize<
                rkyv::rancor::Strategy<
                    rkyv::ser::Serializer<
                        rkyv::util::AlignedVec,
                        rkyv::ser::allocator::ArenaHandle<'ser>,
                        rkyv::ser::sharing::Share,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
        T: serde::Serialize,
    {
        let raw = crate::meta::serialization::serialize_meta(value)?;
        self.set(key, raw);

        Ok(())
    }

    #[cfg(not(feature = "rkyv-serialization"))]
    pub fn set_typed<T: Serialize>(
        &mut self,
        key: impl Into<String>,
        value: &T,
    ) -> Result<(), MetaError> {
        let raw = crate::meta::serialization::serialize_meta(value)?;
        self.set(key, raw);

        Ok(())
    }

    pub async fn get_typed_json<T: DeserializeOwned>(
        &mut self,
        key: impl AsRef<str>,
    ) -> Result<Option<T>, MetaError> {
        let key = key.as_ref();
        let raw = self.get(key).await?;

        raw.map(|raw| {
            serde_json::from_slice::<T>(&raw)
                .map_err(|e| MetaError::Internal(format!("Failed to parse {key}: {e}")))
        })
        .transpose()
    }

    pub fn set_typed_json<T: Serialize>(
        &mut self,
        key: impl Into<String>,
        value: &T,
    ) -> Result<(), MetaError> {
        let raw = serde_json::to_vec(value).map_err(|e| MetaError::Internal(e.to_string()))?;
        self.set(key, raw);

        Ok(())
    }

    async fn ensure_baselines_for_blind_writes(&mut self) -> Result<(), MetaError> {
        let missing_keys: Vec<String> = self
            .writes
            .keys()
            .filter(|key| !self.reads.contains_key(*key))
            .cloned()
            .collect();

        for key in missing_keys {
            let slot = self.fetch_slot(&key).await?;
            self.reads.insert(key, slot);
        }

        Ok(())
    }

    async fn writes_match_etcd(&self) -> Result<bool, MetaError> {
        for (key, op) in &self.writes {
            let response = get_with_retry(self.client, key, || None)
                .await
                .map_err(|error| {
                    MetaError::Internal(format!(
                        "Failed to verify indeterminate transaction key {key}: {error}"
                    ))
                })?;
            let actual = response.kvs().first().map(|kv| kv.value());
            let matches = match op {
                EtcdTxnWriteOp::Put { value, .. } => actual == Some(value.as_slice()),
                EtcdTxnWriteOp::Delete => actual.is_none(),
            };
            if !matches {
                return Ok(false);
            }
        }

        Ok(true)
    }

    /// Builds a single etcd transaction from the recorded read set and write set.
    ///
    /// Returns `Ok(true)` when the CAS succeeds, `Ok(false)` when another writer won
    /// the race and the caller should retry, and `Err(..)` for real execution errors.
    async fn commit(&mut self) -> Result<bool, MetaError> {
        self.ensure_baselines_for_blind_writes().await?;

        if self.writes.is_empty() {
            return Ok(true);
        }

        let mut compares = Vec::with_capacity(self.reads.len());
        for (key, slot) in &self.reads {
            if slot.mod_revision == 0 {
                compares.push(Compare::version(key.as_str(), CompareOp::Equal, 0));
            } else {
                compares.push(Compare::mod_revision(
                    key.as_str(),
                    CompareOp::Equal,
                    slot.mod_revision,
                ));
            }
        }

        let mut ops = Vec::with_capacity(self.writes.len());
        for (key, op) in &self.writes {
            match op {
                EtcdTxnWriteOp::Put { value, options } => {
                    ops.push(TxnOp::put(key.as_str(), value.clone(), options.clone()));
                }
                EtcdTxnWriteOp::Delete => {
                    ops.push(TxnOp::delete(key.as_str(), None));
                }
            }
        }

        let mut request_failed = false;
        for attempt in 1..=ETCD_READ_MAX_ATTEMPTS {
            let txn = Txn::new().when(compares.clone()).and_then(ops.clone());
            let mut client = self.client.clone();
            match client.txn(txn).await {
                Ok(response) if response.succeeded() => return Ok(true),
                Ok(_) if request_failed => {
                    let applied = self.writes_match_etcd().await?;
                    tracing::warn!(
                        attempt,
                        applied,
                        "etcd transaction CAS changed after retry; verified the staged write set"
                    );
                    return Ok(applied);
                }
                Ok(_) => return Ok(false),
                Err(error) if attempt < ETCD_READ_MAX_ATTEMPTS => {
                    request_failed = true;
                    let delay_ms = 10_u64 << (attempt - 1);
                    tracing::warn!(
                        attempt,
                        delay_ms,
                        error = %error,
                        "retrying etcd transaction after request failure"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                }
                Err(error) => {
                    return Err(MetaError::Internal(format!(
                        "Failed to execute transaction: {error}"
                    )));
                }
            }
        }

        unreachable!("etcd transaction retry loop always returns")
    }
}

/// Retryable etcd transaction runner built on optimistic concurrency control.
///
/// New write-side code should generally use this instead of hand-writing
/// `Txn::new()...when(...).and_then(...)` blocks. Put all reads and staged writes in
/// the closure, and let `run` retry the whole closure on CAS failure.
///
/// Example:
/// ```ignore
/// let out = EtcdTxn::new(&self.client)
///     .max_retries(10)
///     .run(|tx| {
///         Box::pin(async move {
///             let mut entry: MyType = tx.get_typed_json(&key).await?
///                 .ok_or(MetaError::NotFound(ino))?;
///
///             entry.counter += 1;
///             tx.set_typed_json(&key, &entry)?;
///
///             Ok(entry.counter)
///         })
///     })
///     .await?;
/// ```
pub(crate) struct EtcdTxn<'a> {
    client: &'a EtcdClient,
    max_retries: u64,
    lock_key: Option<String>,
}

impl<'a> EtcdTxn<'a> {
    pub(crate) fn new(client: &'a EtcdClient) -> Self {
        Self {
            client,
            max_retries: 10,
            lock_key: None,
        }
    }

    pub(crate) fn max_retries(mut self, max_retries: u64) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Serializes this process's transactions that share the same primary key.
    ///
    /// This mirrors JuiceFS's local txlock phase: it does not provide distributed
    /// exclusion, but it prevents same-process writers from stampeding into CAS
    /// failures for the same metadata key.
    pub(crate) fn lock_key(mut self, key: impl Into<String>) -> Self {
        self.lock_key = Some(key.into());
        self
    }

    fn local_locks() -> &'static [tokio::sync::Mutex<()>] {
        ETCD_TXN_LOCKS.get_or_init(|| {
            (0..ETCD_TXN_LOCK_STRIPES)
                .map(|_| tokio::sync::Mutex::new(()))
                .collect()
        })
    }

    pub(crate) fn local_lock_slot_for_key(key: &str) -> usize {
        let mut hash = 2166136261u32;
        for byte in key.as_bytes() {
            hash ^= u32::from(*byte);
            hash = hash.wrapping_mul(16777619);
        }
        hash as usize % ETCD_TXN_LOCK_STRIPES
    }

    fn local_lock_for_key(key: &str) -> &'static tokio::sync::Mutex<()> {
        &Self::local_locks()[Self::local_lock_slot_for_key(key)]
    }

    #[cfg(test)]
    pub(crate) async fn with_local_lock_for_key<Fut>(key: &str, fut: Fut) -> Fut::Output
    where
        Fut: Future,
    {
        let _guard = Self::local_lock_for_key(key).lock().await;
        fut.await
    }

    /// Executes the transaction closure with automatic retry on CAS conflicts.
    ///
    /// The closure must be self-contained and retry-safe: it may run multiple times,
    /// so avoid irreversible side effects inside it. Only etcd reads via `tx` and
    /// in-memory staging should happen in the closure body.
    pub(crate) async fn run<R, F>(&self, mut task: F) -> Result<R, MetaError>
    where
        F: for<'task> FnMut(
            &'task mut EtcdTxnCtx<'a>,
        )
            -> Pin<Box<dyn Future<Output = Result<R, MetaError>> + Send + 'task>>,
    {
        if let Some(lock_key) = &self.lock_key {
            let _guard = Self::local_lock_for_key(lock_key).lock().await;
            self.run_inner(&mut task).await
        } else {
            self.run_inner(&mut task).await
        }
    }

    async fn run_inner<R, F>(&self, task: &mut F) -> Result<R, MetaError>
    where
        F: for<'task> FnMut(
            &'task mut EtcdTxnCtx<'a>,
        )
            -> Pin<Box<dyn Future<Output = Result<R, MetaError>> + Send + 'task>>,
    {
        for attempt in 0..self.max_retries {
            let mut tx = EtcdTxnCtx::new(self.client);
            let out = task(&mut tx).await?;

            if tx.commit().await? {
                return Ok(out);
            }

            if attempt + 1 < self.max_retries {
                let jitter_bound = (attempt + 1).saturating_mul(attempt + 1).max(1);
                let backoff_ms = 20 + (rng().next_u64() % jitter_bound);
                tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
            }
        }

        Err(MetaError::MaxRetriesExceeded)
    }
}

#[cfg(test)]
mod tests {
    use super::EtcdTxn;
    use crate::meta::entities::EntryType;
    use crate::meta::entities::etcd::EtcdForwardEntry;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::{Duration, sleep};

    #[test]
    fn typed_json_helpers_roundtrip() {
        let value = EtcdForwardEntry {
            parent_inode: 1,
            name: "file".to_string(),
            inode: 2,
            is_file: true,
            entry_type: Some(EntryType::File),
        };

        let encoded = serde_json::to_vec(&value).expect("serialize forward entry");
        let decoded: EtcdForwardEntry = serde_json::from_slice(&encoded).expect("decode");

        assert_eq!(decoded.inode, 2);
    }

    #[test]
    fn local_lock_slot_is_stable_for_same_key() {
        assert_eq!(
            EtcdTxn::local_lock_slot_for_key("slices/42"),
            EtcdTxn::local_lock_slot_for_key("slices/42")
        );
    }

    #[tokio::test]
    async fn local_lock_serializes_same_primary_key() {
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();

        for _ in 0..8 {
            let in_flight = in_flight.clone();
            let max_seen = max_seen.clone();
            tasks.push(tokio::spawn(async move {
                EtcdTxn::with_local_lock_for_key("slices/serialized", async move {
                    let current = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                    max_seen.fetch_max(current, Ordering::SeqCst);
                    sleep(Duration::from_millis(5)).await;
                    in_flight.fetch_sub(1, Ordering::SeqCst);
                })
                .await;
            }));
        }

        for task in tasks {
            task.await.unwrap();
        }

        assert_eq!(max_seen.load(Ordering::SeqCst), 1);
    }
}
