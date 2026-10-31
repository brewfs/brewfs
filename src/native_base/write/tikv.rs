//! TiKV `ControlStore`: one pessimistic transaction per [`Txn`].
//!
//! Every check runs as `get_for_update`, so the key is write-locked for the
//! transaction's lifetime — no other writer can interleave between the check
//! and the commit. All writes then apply and commit together, or the
//! transaction rolls back and nothing applies. A failed check returns
//! [`StoreError::Conflict`] directly; a commit-time lock expiry (PD
//! leadership change, long GC pauses) is detected and also surfaced as
//! `Conflict`, because the correct caller response is identical: re-read
//! and re-derive.
//!
//! Keys are used as-is: every key the pipeline builds already lives under
//! `nb2/{volume}/` (see [`super::keys`]), so a cluster shared with the
//! workspace catalog or other subsystems never collides.

use std::ops::Bound;

use async_trait::async_trait;
use tikv_client::{BoundRange, CheckLevel, Key, KvPair, TransactionClient, TransactionOptions};

use super::store::{ControlStore, StoreError, Txn, expect_matches};

const SCAN_BATCH_LIMIT: u32 = 1024;

/// The control plane on TiKV.
pub struct TiKvControlStore {
    client: TransactionClient,
}

impl TiKvControlStore {
    /// Connect through the given PD endpoints (e.g.
    /// `["127.0.0.1:2379"]`).
    pub async fn connect(pd_endpoints: &[String]) -> Result<Self, StoreError> {
        if pd_endpoints.is_empty() {
            return Err(StoreError::Backend(
                "TiKV control store requires at least one PD endpoint".into(),
            ));
        }
        let client = TransactionClient::new(pd_endpoints.to_vec())
            .await
            .map_err(backend)?;
        Ok(Self { client })
    }

    async fn run_transaction(&self, txn: Txn, before_ns: Option<i64>) -> Result<(), StoreError> {
        if before_ns.is_some_and(|deadline| deadline <= 0) {
            return Err(StoreError::Conflict);
        }
        let options = TransactionOptions::new_pessimistic().drop_check(CheckLevel::Warn);
        let options = if before_ns.is_some() {
            options.wait_for_secondary_commit()
        } else {
            options
        };
        let mut transaction = self
            .client
            .begin_with_options(options)
            .await
            .map_err(backend)?;
        // Every guard and business check is locked until commit. Use one key
        // order so a namespace lease renewal cannot introduce a lock cycle.
        let mut checks: Vec<_> = txn.checks.iter().collect();
        if before_ns.is_some() {
            checks.sort_by(|left, right| left.0.cmp(&right.0));
        }
        for (key, expect) in checks {
            let current = match transaction.get_for_update(key.clone()).await {
                Ok(current) => current,
                Err(error) => {
                    let _ = transaction.rollback().await;
                    return if is_retryable(&error.to_string()) {
                        Err(StoreError::Conflict)
                    } else {
                        Err(backend(error))
                    };
                }
            };
            if !expect_matches(expect, current.as_deref()) {
                let _ = transaction.rollback().await;
                return Err(StoreError::Conflict);
            }
        }
        for (key, value) in &txn.writes {
            let result = match value {
                Some(value) => transaction.put(key.clone(), value.clone()).await,
                None => transaction.delete(key.clone()).await,
            };
            if let Err(error) = result {
                let _ = transaction.rollback().await;
                return if is_retryable(&error.to_string()) {
                    Err(StoreError::Conflict)
                } else {
                    Err(backend(error))
                };
            }
        }
        if let Some(deadline) = before_ns {
            // A start timestamp can predate an arbitrarily long lock wait.
            // Read PD time after all checks/writes while retaining their locks.
            let now = match self.client.current_timestamp().await {
                Ok(timestamp) => timestamp.physical.checked_mul(1_000_000),
                Err(error) => {
                    let _ = transaction.rollback().await;
                    return Err(backend(error));
                }
            };
            if now.is_none_or(|now| now >= deadline) {
                let _ = transaction.rollback().await;
                return Err(StoreError::Conflict);
            }
        }
        match transaction.commit().await {
            Ok(_) => Ok(()),
            // Keep the existing unguarded caller's conflict behavior. Once a
            // guarded commit was submitted, an uncertain reply is never proof
            // that nothing happened: reconciliation must retain original IDs.
            Err(error) if before_ns.is_none() && is_retryable(&error.to_string()) => {
                Err(StoreError::Conflict)
            }
            Err(error) => Err(backend(error)),
        }
    }
}

fn backend(err: impl std::fmt::Display) -> StoreError {
    StoreError::Backend(format!("TiKV control store: {err}"))
}

/// A commit failed because a lock expired or a write conflicted underneath
/// us: retryable, and the retry response is the conflict path.
fn is_retryable(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("write conflict")
        || message.contains("writeconflict")
        || message.contains("pessimisticlock")
        || message.contains("lock conflict")
        || message.contains("txnlock")
}

/// Exclusive upper bound of the key range covering `prefix`: increment the
/// last non-`0xff` byte and truncate after it. An all-`0xff` prefix has no
/// finite bound.
fn prefix_range_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    for index in (0..end.len()).rev() {
        if end[index] != 0xff {
            end[index] += 1;
            end.truncate(index + 1);
            return Some(end);
        }
    }
    None
}

/// Keep the prefix invariant even when a prefix has no finite exclusive upper
/// bound (for example, a binary key ending in 0xff). TiKV scans are ordered,
/// so the first key outside the prefix terminates the useful range.
fn take_prefix_batch(prefix: &[u8], batch: Vec<KvPair>) -> (Vec<KvPair>, bool) {
    let mut accepted = Vec::with_capacity(batch.len());
    for pair in batch {
        let key: Vec<u8> = pair.key().clone().into();
        if !key.starts_with(prefix) {
            return (accepted, true);
        }
        accepted.push(pair);
    }
    (accepted, false)
}

#[async_trait]
impl ControlStore for TiKvControlStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        let options = TransactionOptions::new_optimistic().drop_check(CheckLevel::Warn);
        let mut transaction = self
            .client
            .begin_with_options(options)
            .await
            .map_err(backend)?;
        let value = match transaction.get(key.to_vec()).await {
            Ok(value) => value,
            Err(error) => {
                if let Err(rollback_error) = transaction.rollback().await {
                    log::debug!("TiKV control store get rollback failed: {rollback_error}");
                }
                return Err(backend(error));
            }
        };
        if let Err(rollback_error) = transaction.rollback().await {
            log::debug!("TiKV control store get rollback failed: {rollback_error}");
        }
        Ok(value)
    }

    async fn run(&self, txn: Txn) -> Result<(), StoreError> {
        self.run_transaction(txn, None).await
    }

    async fn run_before(&self, txn: Txn, before_ns: i64) -> Result<(), StoreError> {
        self.run_transaction(txn, Some(before_ns)).await
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StoreError> {
        let options = TransactionOptions::new_optimistic().drop_check(CheckLevel::Warn);
        let mut transaction = self
            .client
            .begin_with_options(options)
            .await
            .map_err(backend)?;
        let upper = match prefix_range_end(prefix) {
            Some(end) => Bound::Excluded(Key::from(end)),
            None => Bound::Unbounded,
        };
        let mut lower = Bound::Included(Key::from(prefix.to_vec()));
        let mut out = Vec::new();

        loop {
            let range = BoundRange::new(lower.clone(), upper.clone());
            let batch: Vec<KvPair> = match transaction.scan(range, SCAN_BATCH_LIMIT).await {
                Ok(batch) => batch.collect(),
                Err(error) => {
                    if let Err(rollback_error) = transaction.rollback().await {
                        log::debug!("TiKV control store scan rollback failed: {rollback_error}");
                    }
                    return Err(backend(error));
                }
            };
            let (batch, exhausted) = take_prefix_batch(prefix, batch);
            let batch_len = batch.len();
            for pair in batch {
                let key: Vec<u8> = pair.key().clone().into();
                lower = Bound::Excluded(Key::from(key.clone()));
                out.push((key, pair.value().to_vec()));
            }
            if exhausted || batch_len < SCAN_BATCH_LIMIT as usize {
                break;
            }
        }

        if let Err(rollback_error) = transaction.rollback().await {
            log::debug!("TiKV control store scan rollback failed: {rollback_error}");
        }
        // TiKV scans in key order already; the batched resume keeps it.
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "requires BREWFS_TEST_TIKV_PD_ENDPOINTS"]
    async fn real_tikv_native_deadline_and_owner_are_checked_with_the_write() {
        let endpoints = std::env::var("BREWFS_TEST_TIKV_PD_ENDPOINTS")
            .unwrap()
            .split(',')
            .map(|value| value.trim().to_owned())
            .collect::<Vec<_>>();
        let store = TiKvControlStore::connect(&endpoints).await.unwrap();
        let scope = uuid::Uuid::new_v4();
        let owner = format!("native-manager-{scope}/ws:v1/manager/controller").into_bytes();
        let target = format!("nb2/{scope}/manager-deadline-test").into_bytes();
        store
            .run(
                Txn::new()
                    .check_absent(owner.clone())
                    .put(owner.clone(), vec![1]),
            )
            .await
            .unwrap();
        let operation = || {
            Txn::new()
                .check_bytes(owner.clone(), vec![1])
                .put(target.clone(), vec![7])
        };
        assert!(matches!(
            store.run_before(operation(), 1).await,
            Err(StoreError::Conflict)
        ));
        assert_eq!(store.get(&target).await.unwrap(), None);
        store.run_before(operation(), i64::MAX).await.unwrap();
        assert_eq!(store.get(&target).await.unwrap(), Some(vec![7]));
        store
            .run(
                Txn::new()
                    .check_bytes(owner.clone(), vec![1])
                    .put(owner.clone(), vec![2])
                    .delete(target.clone()),
            )
            .await
            .unwrap();
        assert!(matches!(
            store.run_before(operation(), i64::MAX).await,
            Err(StoreError::Conflict)
        ));
        assert_eq!(store.get(&target).await.unwrap(), None);
        store
            .run(Txn::new().delete(owner).delete(target))
            .await
            .unwrap();
    }

    #[test]
    fn prefix_range_end_increments_the_last_free_byte() {
        assert_eq!(prefix_range_end(b"nb2/0909/"), Some(b"nb2/09090".to_vec()));
        assert_eq!(prefix_range_end(b"\xff\xff"), None);
        assert_eq!(prefix_range_end(b"\xa0\xff\xff"), Some(b"\xa1".to_vec()));
        assert_eq!(prefix_range_end(b""), None);
    }

    #[test]
    fn prefix_scan_stops_at_the_first_nonmatching_key() {
        let prefix = b"abc";
        let batch = vec![
            KvPair::new(b"abc\x00".to_vec(), b"inside".to_vec()),
            KvPair::new(b"abc\xff".to_vec(), b"inside".to_vec()),
            KvPair::new(b"abd".to_vec(), b"outside".to_vec()),
        ];
        let (accepted, exhausted) = take_prefix_batch(prefix, batch);
        assert!(exhausted);
        assert_eq!(accepted.len(), 2);
        assert!(
            accepted
                .iter()
                .all(|pair| Vec::<u8>::from(pair.key().clone()).starts_with(prefix))
        );
    }
}
