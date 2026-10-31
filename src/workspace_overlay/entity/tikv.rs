//! TiKV current-entity transactions. The legacy workspace store is not used
//! for upper rows: keys retain the entity volume domain and wire format.
//!
//! Mutations lock exact predicates and writes in byte order, one key per
//! locking request so a wait retains the acquired prefix. Read-only
//! validation uses one fresh MVCC snapshot without locking shared lease rows.
//! A fresh PD timestamp fences the lease before either result. No commit error
//! is retried or converted into a definite conflict.

use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Bound,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use tikv_client::transaction::Mutation;
use tikv_client::{
    BoundRange, CheckLevel, Key, Transaction, TransactionClient, TransactionOptions,
};
use tokio::sync::{OwnedRwLockReadGuard, RwLock};
use uuid::Uuid;

use super::{EntityError, Result, backend::*, codec::MAX_ROW_BYTES, keys::EntityKeys};

const POINT_BATCH: usize = 16;
const MESSAGE_BYTES: usize = MAX_TRANSACTION_BYTES + (1 << 20);
const MAX_PRECOMMIT_ATTEMPTS: usize = 128;

#[cfg(test)]
#[path = "tikv_tests.rs"]
mod tests;

struct Inner {
    client: TransactionClient,
    closed: Arc<RwLock<bool>>,
    #[cfg(test)]
    control: tests::Control,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.client.cancel_background_tasks();
    }
}

#[derive(Clone)]
pub struct TiKvEntityBackend {
    inner: Arc<Inner>,
    volume_prefix: Vec<u8>,
}

enum Allocation {
    Inode {
        prepared: PreparedCreate,
        lower_max_ino: i64,
    },
    Slices(PreparedSlices),
}

impl Allocation {
    fn counter_key(&self) -> &[u8] {
        match self {
            Self::Inode { prepared, .. } => &prepared.counter_key,
            Self::Slices(prepared) => &prepared.counter_key,
        }
    }

    fn extend_plan(
        &self,
        current: &[u8],
        plan: &mut EntityPlan,
    ) -> Result<(Vec<u64>, BTreeSet<Vec<u8>>)> {
        let (ids, writes, existing_dentry) = match self {
            Self::Inode {
                prepared,
                lower_max_ino,
            } => {
                let ino = next_counter(current)? as i64;
                if ino <= *lower_max_ino {
                    return Err(EntityError::Corrupt(
                        "inode allocator overlaps lower".into(),
                    ));
                }
                (
                    vec![ino as u64],
                    prepared.writes(ino),
                    Some(prepared.dentry_key.as_slice()),
                )
            }
            Self::Slices(prepared) => {
                let mut counter = current.to_vec();
                let mut ids = Vec::with_capacity(prepared.suffixes.len());
                for _ in &prepared.suffixes {
                    let id = next_counter(&counter)?;
                    counter = id.to_string().into_bytes();
                    ids.push(id);
                }
                let writes = prepared.writes(&ids);
                (ids, writes, None)
            }
        };
        let counter_key = self.counter_key();
        plan.checks.push(RowCheck {
            key: counter_key.to_vec(),
            expected: Some(current.to_vec()),
        });
        let mut collisions = BTreeSet::new();
        for write in &writes {
            // The counter is already locked. Derived identities must never
            // introduce an earlier key into the ordered lock sequence.
            if write.key() <= counter_key {
                return Err(EntityError::Corrupt(
                    "allocated row precedes entity counter".into(),
                ));
            }
            if Some(write.key()) != existing_dentry {
                collisions.insert(write.key().to_vec());
                plan.checks.push(RowCheck {
                    key: write.key().to_vec(),
                    expected: None,
                });
            }
        }
        if let Self::Slices(prepared) = self {
            for &id in &ids {
                let key = prepared.descriptor_key(id);
                if key.as_slice() <= counter_key {
                    return Err(EntityError::Corrupt(
                        "object descriptor precedes entity counter".into(),
                    ));
                }
                collisions.insert(key.clone());
                plan.checks.push(RowCheck {
                    key,
                    expected: None,
                });
            }
        }
        plan.writes.extend(writes);
        plan.writes.push(RowWrite::Put {
            key: counter_key.to_vec(),
            value: ids
                .last()
                .ok_or_else(|| EntityError::Invalid("empty allocation".into()))?
                .to_string()
                .into_bytes(),
        });
        Ok((ids, collisions))
    }
}

type LockedRows = BTreeMap<Vec<u8>, Option<Vec<u8>>>;

fn ordered_keys(authority: &TransactionAuthority, plan: &EntityPlan) -> Vec<Vec<u8>> {
    authority
        .checks
        .iter()
        .chain(&plan.checks)
        .map(|check| check.key.clone())
        .chain(plan.writes.iter().map(|write| write.key().to_vec()))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn backend(error: impl std::fmt::Display) -> EntityError {
    EntityError::Backend(format!("TiKV entity: {error}"))
}

fn time_ns(physical_ms: i64) -> Result<i64> {
    physical_ms
        .checked_mul(1_000_000)
        .filter(|n| *n >= 0)
        .ok_or(EntityError::Overflow)
}

// Used only for an operation which has not submitted commit. Its caller must
// successfully roll back before retrying a structured conflict or deadlock.
// No error-string classification, and no commit error is passed through this
// classifier, even when a server error would otherwise look retryable.
fn precommit_error(error: tikv_client::Error) -> EntityError {
    fn retryable(error: &tikv_client::Error) -> bool {
        use tikv_client::Error;
        match error {
            Error::PessimisticLockError { inner, .. } => retryable(inner),
            Error::MultipleKeyErrors(errors) | Error::ExtractedErrors(errors) => {
                !errors.is_empty() && errors.iter().all(retryable)
            }
            Error::KeyError(key) => {
                matches!(
                    (&key.conflict, &key.deadlock),
                    (Some(_), None) | (None, Some(_))
                ) && key.locked.is_none()
                    && key.abort.is_empty()
                    && key.already_exist.is_none()
                    && key.commit_ts_expired.is_none()
                    && key.txn_not_found.is_none()
                    && key.commit_ts_too_large.is_none()
                    && key.assertion_failed.is_none()
                    && key.primary_mismatch.is_none()
            }
            _ => false,
        }
    }
    if retryable(&error) {
        EntityError::Conflict
    } else {
        backend(error)
    }
}

impl TiKvEntityBackend {
    pub async fn connect(
        endpoints: Vec<String>,
        volume: Uuid,
        tls: Option<&crate::config::TiKvTlsFileConfig>,
    ) -> Result<Self> {
        if endpoints.is_empty() || endpoints.iter().any(|s| s.trim().is_empty()) || volume.is_nil()
        {
            return Err(EntityError::Invalid(
                "TiKV entity requires PD endpoints and a volume UUID".into(),
            ));
        }
        let mut config = tikv_client::Config::default()
            .with_timeout(Duration::from_secs(5))
            .with_grpc_max_decoding_message_size(MESSAGE_BYTES)
            .with_grpc_accept_gzip(false);
        // Restore the SDK's TTL-aware missing-primary resolution only for
        // this new entity client; legacy clients keep their existing policy.
        config.resolve_txn_not_found = true;
        config.pd_max_decoding_message_size = 1 << 20;
        config.max_cached_regions = 1024;
        config.max_cached_stores = 64;
        config.max_cached_kv_clients = 64;
        config.max_cache_entry_bytes = 64 << 10;
        config.tso_max_pending_groups = 1024;
        if let Some(tls) = tls {
            tls.validate()
                .map_err(|e| EntityError::Invalid(e.to_string()))?;
            config = config.with_security(&tls.ca_path, &tls.cert_path, &tls.key_path);
        }
        let client = TransactionClient::new_with_config(endpoints, config)
            .await
            .map_err(backend)?;
        Ok(Self {
            inner: Arc::new(Inner {
                client,
                closed: Arc::new(RwLock::new(false)),
                #[cfg(test)]
                control: tests::Control::default(),
            }),
            volume_prefix: EntityKeys::new(volume, Uuid::nil())
                .volume_prefix()
                .to_vec(),
        })
    }

    async fn enter(&self) -> Result<OwnedRwLockReadGuard<bool>> {
        let guard = self.inner.closed.clone().read_owned().await;
        if *guard {
            return Err(backend("backend is shut down"));
        }
        Ok(guard)
    }

    /// Wait for admitted operations and their owned commits, then join SDK
    /// tasks. Keeping the closed flag set makes cancelled shutdown resumable.
    pub async fn shutdown(&self) -> Result<()> {
        let mut closed = self.inner.closed.write().await;
        *closed = true;
        self.inner.client.shutdown().await.map_err(backend)
    }

    fn scope(&self, key: &[u8]) -> Result<()> {
        validate_key(key)?;
        if !key.starts_with(&self.volume_prefix) {
            return Err(EntityError::Invalid(
                "key is outside the connected entity volume".into(),
            ));
        }
        Ok(())
    }

    async fn read_transaction(&self) -> Result<Transaction> {
        self.inner
            .client
            .begin_with_options(
                TransactionOptions::new_optimistic()
                    .read_only()
                    .drop_check(CheckLevel::None),
            )
            .await
            .map_err(backend)
    }

    async fn read_rows(
        &self,
        transaction: &mut Transaction,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>> {
        let mut result = Vec::with_capacity(keys.len());
        let mut total = 0;
        for chunk in keys.chunks(POINT_BATCH) {
            let mut found = BTreeMap::new();
            for pair in transaction
                .batch_get(chunk.iter().cloned())
                .await
                .map_err(backend)?
            {
                let (key, value): (Vec<u8>, Vec<u8>) =
                    (pair.key().clone().into(), pair.value().clone());
                if !chunk.contains(&key) || found.contains_key(&key) {
                    return Err(EntityError::Corrupt("unexpected TiKV batch key".into()));
                }
                if value.len() > MAX_ROW_BYTES {
                    return Err(EntityError::Invalid("entity row exceeds size bound".into()));
                }
                found.insert(key, value);
            }
            for key in chunk {
                let value = found.get(key);
                add_bytes(&mut total, key, value.map(Vec::as_slice))?;
                result.push(value.cloned());
            }
        }
        Ok(result)
    }

    async fn run(
        &self,
        authority: TransactionAuthority,
        plan: EntityPlan,
        allocation: Option<Allocation>,
    ) -> Result<(CommitOutcome, Vec<u64>)> {
        validate_plan(&authority, &plan)?;
        for key in authority
            .checks
            .iter()
            .chain(&plan.checks)
            .map(|c| c.key.as_slice())
            .chain(plan.writes.iter().map(RowWrite::key))
        {
            self.scope(key)?;
        }
        if let Some(allocation) = &allocation {
            self.scope(allocation.counter_key())?;
        }
        let admitted = self.enter().await?;
        let this = self.clone();
        // Dropping the caller cannot abandon a submitted mutation or let a
        // backend shutdown overtake it. Unknown results remain unknown.
        tokio::spawn(async move {
            let _admitted = admitted;
            if plan.writes.is_empty() && allocation.is_none() {
                return Ok((this.validate_read(&authority, &plan).await?, Vec::new()));
            }
            let operation = match &allocation {
                None => "commit",
                Some(Allocation::Inode { .. }) => "inode_allocation",
                Some(Allocation::Slices(_)) => "slice_allocation",
            };
            for _ in 0..MAX_PRECOMMIT_ATTEMPTS {
                let mut transaction = this
                    .inner
                    .client
                    .begin_with_options(
                        TransactionOptions::new_pessimistic()
                            .wait_for_secondary_commit()
                            .drop_check(CheckLevel::Warn),
                    )
                    .await
                    .map_err(backend)?;
                let prepared = async {
                    let mut actual = plan.clone();
                    let mut held = LockedRows::new();
                    let (ids, collisions) = if let Some(allocation) = &allocation {
                        let counter = allocation.counter_key();
                        let mut prefix = ordered_keys(&authority, &actual)
                            .into_iter()
                            .filter(|key| key.as_slice() <= counter)
                            .collect::<BTreeSet<_>>();
                        prefix.insert(counter.to_vec());
                        this.lock_rows(&mut transaction, prefix.into_iter().collect(), &mut held)
                            .await?;
                        #[cfg(test)]
                        this.inner.control.after_allocation_read().await;
                        // IDs are derived from the latest locked counter, not
                        // from a separate MVCC read that can lose every CAS.
                        let generated = held
                            .get(counter)
                            .and_then(Option::as_deref)
                            .ok_or_else(|| {
                                EntityError::Corrupt("uninitialized entity counter".into())
                            })
                            .and_then(|current| allocation.extend_plan(current, &mut actual));
                        match generated {
                            Ok(generated) => generated,
                            Err(error) => {
                                // Preserve fencing/predicate precedence over
                                // an exhausted or corrupt counter. No generated
                                // keys or writes are needed on this failure path.
                                let predicates = EntityPlan {
                                    checks: plan.checks.clone(),
                                    writes: Vec::new(),
                                };
                                let outcome = this
                                    .prepare_locked(
                                        &mut transaction,
                                        &authority,
                                        &predicates,
                                        &BTreeSet::new(),
                                        held,
                                    )
                                    .await?;
                                return if outcome == CommitOutcome::Conflict {
                                    Ok((outcome, Vec::new()))
                                } else {
                                    Err(error)
                                };
                            }
                        }
                    } else {
                        (Vec::new(), BTreeSet::new())
                    };
                    validate_plan(&authority, &actual)?;
                    for key in ordered_keys(&authority, &actual) {
                        this.scope(&key)?;
                    }
                    let outcome = this
                        .prepare_locked(&mut transaction, &authority, &actual, &collisions, held)
                        .await?;
                    Ok::<_, EntityError>((outcome, ids))
                }
                .await;
                let ids = match prepared {
                    Ok((CommitOutcome::Committed, ids)) => ids,
                    Err(EntityError::Conflict) => {
                        transaction.rollback().await.map_err(backend)?;
                        tokio::task::yield_now().await;
                        continue;
                    }
                    other => {
                        transaction.rollback().await.map_err(backend)?;
                        return other;
                    }
                };
                // Never retry after submission, including apparently retryable
                // strings wrapped in a transport/undetermined error.
                #[cfg(test)]
                this.inner.control.submitting();
                transaction.commit().await.map_err(backend)?;
                #[cfg(test)]
                this.inner.control.after_commit()?;
                return Ok((CommitOutcome::Committed, ids));
            }
            tracing::warn!(
                operation,
                attempts = MAX_PRECOMMIT_ATTEMPTS,
                "TiKV entity retries exhausted"
            );
            Err(EntityError::Busy)
        })
        .await
        .map_err(backend)?
    }

    async fn validate_read(
        &self,
        authority: &TransactionAuthority,
        plan: &EntityPlan,
    ) -> Result<CommitOutcome> {
        let checks = authority
            .checks
            .iter()
            .chain(&plan.checks)
            .collect::<Vec<_>>();
        let keys = checks
            .iter()
            .map(|check| check.key.clone())
            .collect::<Vec<_>>();
        // All predicates share one newly acquired timestamp, including across
        // bounded batches. That timestamp is the read's linearization point.
        // Lock-only commits would serialize readers on the common authority
        // rows and can starve renewal under sustained reads. There is no write
        // to authorize here; mutations still use prepare_locked and 2PC.
        let mut snapshot = self.read_transaction().await?;
        let rows = self.read_rows(&mut snapshot, &keys).await?;
        #[cfg(test)]
        self.inner.control.before_clock().await;
        let now = time_ns(
            self.inner
                .client
                .current_timestamp()
                .await
                .map_err(backend)?
                .physical,
        )?;
        // Conservatively reject expiry during a slow read as well as expiry at
        // the snapshot timestamp. A later renewal cannot extend this authority.
        if !authority.allows_time(now) {
            return Err(EntityError::Fenced);
        }
        Ok(
            if checks
                .iter()
                .zip(rows)
                .all(|(check, row)| check.expected == row)
            {
                CommitOutcome::Committed
            } else {
                CommitOutcome::Conflict
            },
        )
    }

    async fn lock_rows(
        &self,
        transaction: &mut Transaction,
        keys: Vec<Vec<u8>>,
        found: &mut LockedRows,
    ) -> Result<()> {
        let mut total = 0;
        for (key, value) in found.iter() {
            add_bytes(&mut total, key, value.as_deref())?;
        }
        // A multi-key locking request may release its newly acquired prefix
        // when a later key waits. Finish each single-key request first. Record
        // absent rows too: they are held locks and must not be reacquired out
        // of order while completing an allocation plan.
        for key in keys {
            if found.contains_key(&key) {
                continue;
            }
            let mut value = None;
            for pair in transaction
                .batch_get_for_update([key.clone()])
                .await
                .map_err(precommit_error)?
            {
                let returned_key: Vec<u8> = pair.key().clone().into();
                if returned_key != key || value.is_some() {
                    return Err(EntityError::Corrupt(
                        "unexpected TiKV locking-read key".into(),
                    ));
                }
                value = Some(pair.value().clone());
            }
            add_bytes(&mut total, &key, value.as_deref())?;
            found.insert(key, value);
        }
        Ok(())
    }

    async fn prepare_locked(
        &self,
        transaction: &mut Transaction,
        authority: &TransactionAuthority,
        plan: &EntityPlan,
        collisions: &BTreeSet<Vec<u8>>,
        mut found: LockedRows,
    ) -> Result<CommitOutcome> {
        self.lock_rows(transaction, ordered_keys(authority, plan), &mut found)
            .await?;
        let mut stored_bytes = 0;
        for key in authority
            .checks
            .iter()
            .chain(&plan.checks)
            .map(|c| c.key.as_slice())
            .chain(plan.writes.iter().map(RowWrite::key))
        {
            add_bytes(
                &mut stored_bytes,
                key,
                found.get(key).and_then(Option::as_deref),
            )?;
        }
        for check in authority.checks.iter().chain(&plan.checks) {
            if found.get(&check.key).and_then(Option::as_ref) != check.expected.as_ref() {
                let now = time_ns(
                    self.inner
                        .client
                        .current_timestamp()
                        .await
                        .map_err(backend)?
                        .physical,
                )?;
                if !authority.allows_time(now) {
                    return Err(EntityError::Fenced);
                }
                // Allocator counter predicates precede generated absence
                // predicates: an intervening allocation is a conflict, not
                // persistent corruption. A real occupied new ID is corrupt.
                if collisions.contains(&check.key)
                    && found.get(&check.key).is_some_and(Option::is_some)
                {
                    return Err(EntityError::Corrupt("entity allocator collision".into()));
                }
                return Ok(CommitOutcome::Conflict);
            }
        }
        // The SDK reacquires locks when buffering mutations. Every write key
        // is already held above, so these bounded batches cannot introduce a
        // new lock order and avoid one redundant RPC per put/delete.
        for writes in plan.writes.chunks(POINT_BATCH) {
            transaction
                .batch_mutate(writes.iter().map(|write| match write {
                    RowWrite::Put { key, value } => {
                        Mutation::Put(key.clone().into(), value.clone())
                    }
                    RowWrite::Delete { key } => Mutation::Delete(key.clone().into()),
                }))
                .await
                .map_err(precommit_error)?;
        }
        #[cfg(test)]
        self.inner.control.before_clock().await;
        let now = time_ns(
            self.inner
                .client
                .current_timestamp()
                .await
                .map_err(backend)?
                .physical,
        )?;
        if !authority.allows_time(now) {
            return Err(EntityError::Fenced);
        }
        Ok(CommitOutcome::Committed)
    }

    async fn scan(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
        keys_only: bool,
        require_complete: bool,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        validate_scan_page(prefix, after, limit)?;
        self.scope(prefix)?;
        let _admitted = self.enter().await?;
        let mut transaction = self.read_transaction().await?;
        let lower = after.map_or_else(
            || Bound::Included(Key::from(prefix.to_vec())),
            |k| Bound::Excluded(Key::from(k.to_vec())),
        );
        let upper = Bound::Excluded(Key::from(prefix_end(prefix)?));
        let range = BoundRange::new(lower, upper);
        let mut rows = Vec::new();
        let mut total = 0;
        if keys_only {
            for key in transaction
                .scan_keys(range, limit as u32)
                .await
                .map_err(backend)?
            {
                let key: Vec<u8> = key.into();
                add_bytes(&mut total, &key, None)?;
                rows.push((key, Vec::new()));
            }
        } else {
            for pair in transaction
                .scan(range, limit as u32)
                .await
                .map_err(backend)?
            {
                let (key, value): (Vec<u8>, Vec<u8>) =
                    (pair.key().clone().into(), pair.value().clone());
                add_bytes(&mut total, &key, Some(&value))?;
                rows.push((key, value));
            }
        }
        if rows.len() > limit
            || rows
                .iter()
                .any(|(k, _)| !k.starts_with(prefix) || after.is_some_and(|a| k.as_slice() <= a))
            || rows.windows(2).any(|p| p[0].0 >= p[1].0)
        {
            return Err(EntityError::Corrupt("invalid TiKV scan page".into()));
        }
        if require_complete && rows.len() == limit {
            let range = BoundRange::new(
                Bound::Excluded(Key::from(rows.last().unwrap().0.clone())),
                Bound::Excluded(Key::from(prefix_end(prefix)?)),
            );
            // The overflow sentinel belongs to the same MVCC snapshot as the
            // returned rows, including when a concurrent writer deletes it.
            if transaction
                .scan_keys(range, 1)
                .await
                .map_err(backend)?
                .next()
                .is_some()
            {
                return Err(EntityError::Invalid("entity scan exceeds row bound".into()));
            }
        }
        Ok(rows)
    }
}

fn prefix_end(prefix: &[u8]) -> Result<Vec<u8>> {
    let mut end = prefix.to_vec();
    for i in (0..end.len()).rev() {
        if end[i] != 255 {
            end[i] += 1;
            end.truncate(i + 1);
            return Ok(end);
        }
    }
    Err(EntityError::Invalid("unbounded entity prefix".into()))
}

#[async_trait]
impl EntityBackend for TiKvEntityBackend {
    async fn shutdown(&self) -> Result<()> {
        TiKvEntityBackend::shutdown(self).await
    }

    async fn read(&self, keys: &[Vec<u8>]) -> Result<RowSnapshot> {
        validate_checked_read(keys, &[])?;
        for key in keys {
            self.scope(key)?;
        }
        let _admitted = self.enter().await?;
        let mut transaction = self.read_transaction().await?;
        let now_ns = time_ns(transaction.start_timestamp().physical)?;
        let rows = self.read_rows(&mut transaction, keys).await?;
        Ok(RowSnapshot { now_ns, rows })
    }

    async fn commit(
        &self,
        authority: &TransactionAuthority,
        plan: &EntityPlan,
    ) -> Result<CommitOutcome> {
        self.run(authority.clone(), plan.clone(), None)
            .await
            .map(|(outcome, _)| outcome)
    }

    async fn commit_with_inode_allocation(
        &self,
        authority: &TransactionAuthority,
        plan: &EntityPlan,
        create: &CreateAllocation,
    ) -> Result<Option<i64>> {
        let prepared = prepare_create(authority, plan, create)?;
        let (outcome, ids) = self
            .run(
                authority.clone(),
                plan.clone(),
                Some(Allocation::Inode {
                    prepared,
                    lower_max_ino: create.lower_max_ino,
                }),
            )
            .await?;
        Ok((outcome == CommitOutcome::Committed).then(|| ids[0] as i64))
    }

    async fn reserve_slices(
        &self,
        authority: &TransactionAuthority,
        allocation: &SliceAllocation,
    ) -> Result<Option<Vec<u64>>> {
        let prepared = prepare_slices(authority, allocation)?;
        let (outcome, ids) = self
            .run(
                authority.clone(),
                EntityPlan::default(),
                Some(Allocation::Slices(prepared)),
            )
            .await?;
        Ok((outcome == CommitOutcome::Committed).then_some(ids))
    }

    async fn scan_prefix(&self, prefix: &[u8], limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan(prefix, None, limit, false, true).await
    }
    async fn scan_prefix_batch(
        &self,
        prefix: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan(prefix, None, limit, false, false).await
    }
    async fn scan_keys_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<Vec<u8>>> {
        Ok(self
            .scan(prefix, after, limit, true, false)
            .await?
            .into_iter()
            .map(|r| r.0)
            .collect())
    }
    async fn scan_prefix_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan(prefix, after, limit, false, false).await
    }
}
