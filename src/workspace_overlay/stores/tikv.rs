//! TiKV transactional substrate for the workspace catalog.

use std::ops::Bound;
use std::time::Duration;

use async_trait::async_trait;
use rand::{RngCore, rng};
use tikv_client::{BoundRange, CheckLevel, Key, KvPair, TransactionClient, TransactionOptions};

use super::kv_backend::{KvCheck, KvEntry, KvWrite, WorkspaceKvBackend};
use crate::workspace_overlay::error::WorkspaceError;

const SCAN_BATCH_LIMIT: u32 = 1024;
const TXN_MAX_RETRIES: usize = 10;

#[derive(Clone)]
pub struct TiKvWorkspaceBackend {
    client: TransactionClient,
    prefix: Vec<u8>,
}

impl TiKvWorkspaceBackend {
    pub async fn connect(
        pd_endpoints: Vec<String>,
        namespace: &str,
    ) -> Result<Self, WorkspaceError> {
        if pd_endpoints.is_empty() {
            return Err(WorkspaceError::Backend(
                "TiKV workspace catalog requires at least one PD endpoint".into(),
            ));
        }
        validate_namespace(namespace)?;
        let client = TransactionClient::new(pd_endpoints)
            .await
            .map_err(backend)?;
        let prefix = format!("{namespace}/ws:v1/").into_bytes();
        Ok(Self { client, prefix })
    }

    fn scoped(&self, key: &[u8]) -> Vec<u8> {
        let mut scoped = Vec::with_capacity(self.prefix.len() + key.len());
        scoped.extend_from_slice(&self.prefix);
        scoped.extend_from_slice(key);
        scoped
    }

    async fn retry_delay(attempt: usize) {
        let bound = ((attempt + 1) * (attempt + 1)).max(1) as u64;
        let jitter = rng().next_u64() % bound;
        tokio::time::sleep(Duration::from_millis(20 + jitter)).await;
    }
}

#[async_trait]
impl WorkspaceKvBackend for TiKvWorkspaceBackend {
    fn name(&self) -> &'static str {
        "workspace-tikv"
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, WorkspaceError> {
        let options = TransactionOptions::new_optimistic().drop_check(CheckLevel::Warn);
        let mut transaction = self
            .client
            .begin_with_options(options)
            .await
            .map_err(backend)?;
        let value = transaction.get(self.scoped(key)).await.map_err(backend)?;
        if let Err(error) = transaction.rollback().await {
            log::debug!("TiKV workspace get rollback failed: {error}");
        }
        Ok(value)
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<KvEntry>, WorkspaceError> {
        let options = TransactionOptions::new_optimistic().drop_check(CheckLevel::Warn);
        let mut transaction = self
            .client
            .begin_with_options(options)
            .await
            .map_err(backend)?;
        let scoped_prefix = self.scoped(prefix);
        let upper = prefix_range_end(&scoped_prefix)
            .map(Key::from)
            .map(Bound::Excluded)
            .unwrap_or(Bound::Unbounded);
        let mut lower = Bound::Included(Key::from(scoped_prefix));
        let mut entries = Vec::new();

        loop {
            let range = BoundRange::new(lower.clone(), upper.clone());
            let batch: Vec<KvPair> = transaction
                .scan(range, SCAN_BATCH_LIMIT)
                .await
                .map_err(backend)?
                .collect();
            let batch_len = batch.len();
            if batch_len == 0 {
                break;
            }
            for pair in batch {
                let last_key: Vec<u8> = pair.key().clone().into();
                lower = Bound::Excluded(Key::from(last_key.clone()));
                let logical = last_key
                    .strip_prefix(self.prefix.as_slice())
                    .ok_or_else(|| {
                        WorkspaceError::Backend(
                            "TiKV workspace scan returned an out-of-namespace key".into(),
                        )
                    })?;
                entries.push(KvEntry {
                    key: logical.to_vec(),
                    value: pair.value().to_vec(),
                });
            }
            if batch_len < SCAN_BATCH_LIMIT as usize {
                break;
            }
        }

        if let Err(error) = transaction.rollback().await {
            log::debug!("TiKV workspace scan rollback failed: {error}");
        }
        Ok(entries)
    }

    async fn compare_and_swap(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
    ) -> Result<bool, WorkspaceError> {
        'attempt: for attempt in 0..TXN_MAX_RETRIES {
            let options = TransactionOptions::new_pessimistic().drop_check(CheckLevel::Warn);
            let mut transaction = self
                .client
                .begin_with_options(options)
                .await
                .map_err(backend)?;

            let mut matched = true;
            for check in checks {
                let current = match transaction.get_for_update(self.scoped(&check.key)).await {
                    Ok(current) => current,
                    Err(error) => {
                        let retryable = is_retryable(&error.to_string());
                        if let Err(rollback_error) = transaction.rollback().await {
                            log::debug!(
                                "TiKV workspace CAS check rollback failed: {rollback_error}"
                            );
                        }
                        if retryable && attempt + 1 < TXN_MAX_RETRIES {
                            Self::retry_delay(attempt).await;
                            continue 'attempt;
                        }
                        return Err(backend(error));
                    }
                };
                if current.as_deref() != check.expected.as_deref() {
                    matched = false;
                    break;
                }
            }
            if !matched {
                if let Err(error) = transaction.rollback().await {
                    log::debug!("TiKV workspace CAS mismatch rollback failed: {error}");
                }
                return Ok(false);
            }

            for write in writes {
                let result = match write {
                    KvWrite::Put { key, value } => {
                        transaction.put(self.scoped(key), value.clone()).await
                    }
                    KvWrite::Delete { key } => transaction.delete(self.scoped(key)).await,
                };
                if let Err(error) = result {
                    let retryable = is_retryable(&error.to_string());
                    if let Err(rollback_error) = transaction.rollback().await {
                        log::debug!("TiKV workspace CAS rollback failed: {rollback_error}");
                    }
                    if retryable && attempt + 1 < TXN_MAX_RETRIES {
                        Self::retry_delay(attempt).await;
                        continue 'attempt;
                    }
                    return Err(backend(error));
                }
            }

            match transaction.commit().await {
                Ok(_) => return Ok(true),
                Err(error) if is_retryable(&error.to_string()) && attempt + 1 < TXN_MAX_RETRIES => {
                    Self::retry_delay(attempt).await;
                }
                Err(error) => return Err(backend(error)),
            }
        }
        Err(WorkspaceError::Backend(
            "TiKV workspace catalog exhausted transaction retries".into(),
        ))
    }

    async fn server_time_ns(&self) -> Result<i64, WorkspaceError> {
        let timestamp = self.client.current_timestamp().await.map_err(backend)?;
        pd_physical_ms_to_ns(timestamp.physical)
    }
}

fn pd_physical_ms_to_ns(physical_ms: i64) -> Result<i64, WorkspaceError> {
    physical_ms
        .checked_mul(1_000_000)
        .ok_or_else(|| WorkspaceError::Backend("PD TSO physical time overflows nanoseconds".into()))
}

fn validate_namespace(namespace: &str) -> Result<(), WorkspaceError> {
    if namespace.is_empty()
        || !namespace
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(WorkspaceError::Backend(
            "TiKV workspace namespace must contain only ASCII letters, digits, '-', '_' or '.'"
                .into(),
        ));
    }
    Ok(())
}

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

fn is_retryable(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("write conflict")
        || message.contains("pessimisticlock")
        || message.contains("lock conflict")
        || message.contains("txnlock")
}

fn backend(error: impl std::fmt::Display) -> WorkspaceError {
    WorkspaceError::Backend(format!("TiKV workspace catalog: {error}"))
}

#[cfg(test)]
mod tests {
    use super::pd_physical_ms_to_ns;

    #[test]
    fn pd_tso_physical_milliseconds_are_converted_to_nanoseconds() {
        assert_eq!(
            pd_physical_ms_to_ns(1_725_000_000_123).unwrap(),
            1_725_000_000_123_000_000
        );
        assert!(pd_physical_ms_to_ns(i64::MAX).is_err());
    }
}
