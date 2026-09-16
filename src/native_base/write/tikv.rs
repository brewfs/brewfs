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
        let options = TransactionOptions::new_pessimistic().drop_check(CheckLevel::Warn);
        let mut transaction = self
            .client
            .begin_with_options(options)
            .await
            .map_err(backend)?;

        // Lock-then-verify: every checked key is read under a write lock,
        // so the verified state cannot change before the commit.
        for (key, expect) in &txn.checks {
            let current = match transaction.get_for_update(key.clone()).await {
                Ok(current) => current,
                Err(error) => {
                    if let Err(rollback_error) = transaction.rollback().await {
                        log::debug!("TiKV control store check rollback failed: {rollback_error}");
                    }
                    // Losing the pessimistic-lock race is the same outcome
                    // as a commit-time conflict: nothing applied, re-derive.
                    let message = error.to_string();
                    if is_retryable(&message) {
                        return Err(StoreError::Conflict);
                    }
                    return Err(backend(error));
                }
            };
            if !expect_matches(expect, current.as_deref()) {
                if let Err(rollback_error) = transaction.rollback().await {
                    log::debug!(
                        "TiKV control store check mismatch rollback failed: {rollback_error}"
                    );
                }
                return Err(StoreError::Conflict);
            }
        }
        for (key, value) in &txn.writes {
            let result = match value {
                Some(v) => transaction.put(key.clone(), v.clone()).await,
                None => transaction.delete(key.clone()).await,
            };
            if let Err(error) = result {
                if let Err(rollback_error) = transaction.rollback().await {
                    log::debug!("TiKV control store write rollback failed: {rollback_error}");
                }
                let message = error.to_string();
                if is_retryable(&message) {
                    return Err(StoreError::Conflict);
                }
                return Err(backend(error));
            }
        }

        match transaction.commit().await {
            Ok(_) => Ok(()),
            // A pessimistic lock can expire under a PD disruption; the
            // transaction applied nothing, and the caller must re-derive.
            Err(error) if is_retryable(&error.to_string()) => Err(StoreError::Conflict),
            Err(error) => Err(backend(error)),
        }
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
            let batch_len = batch.len();
            for pair in batch {
                let key: Vec<u8> = pair.key().clone().into();
                lower = Bound::Excluded(Key::from(key.clone()));
                out.push((key, pair.value().to_vec()));
            }
            if batch_len < SCAN_BATCH_LIMIT as usize {
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

    #[test]
    fn prefix_range_end_increments_the_last_free_byte() {
        assert_eq!(prefix_range_end(b"nb2/0909/"), Some(b"nb2/09090".to_vec()));
        assert_eq!(prefix_range_end(b"\xff\xff"), None);
        assert_eq!(prefix_range_end(b"\xa0\xff\xff"), Some(b"\xa1".to_vec()));
        assert_eq!(prefix_range_end(b""), None);
    }
}
