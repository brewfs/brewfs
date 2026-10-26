//! Transactional test backend with a controlled clock and a paused mutation.
use async_trait::async_trait;
use brewfs::workspace_overlay::error::WorkspaceError;
use brewfs::workspace_overlay::stores::kv_backend::{
    KvCheck, KvEntry, KvReadLimits, KvWrite, WorkspaceKvBackend,
};
use std::collections::BTreeMap;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicI64, Ordering},
};
use tokio::sync::{Mutex, Notify};

#[derive(Clone, Default)]
pub struct MemoryBackend {
    records: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
    pub clock: Arc<AtomicI64>,
    pub pause_mutation: Arc<AtomicBool>,
    pub mutation_arrived: Arc<Notify>,
    pub mutation_continue: Arc<Notify>,
    pub deadline_first: Arc<AtomicBool>,
    pub fail_bounded_read_busy_once: Arc<AtomicBool>,
}

impl MemoryBackend {
    async fn cas(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        lower: Option<i64>,
        upper: Option<i64>,
    ) -> Result<bool, WorkspaceError> {
        if writes.iter().any(|write| {
            matches!(write, KvWrite::Put { key, .. }
            if key.starts_with(b"delta/inode/"))
        }) && self.pause_mutation.swap(false, Ordering::SeqCst)
        {
            self.mutation_arrived.notify_one();
            self.mutation_continue.notified().await;
        }
        let mut records = self.records.lock().await;
        let now = self.clock.load(Ordering::SeqCst);
        let time = || {
            if lower.is_some_and(|bound| now < bound) {
                return Err(WorkspaceError::Busy);
            }
            if upper.is_some_and(|bound| now >= bound) {
                return Err(WorkspaceError::Fenced);
            }
            Ok(())
        };
        // Both orders satisfy the temporal CAS contract. A rejected deadline
        // proves this packet did not commit, not that its lease is still old.
        if self.deadline_first.load(Ordering::SeqCst) {
            time()?;
        }
        if checks
            .iter()
            .any(|check| records.get(&check.key) != check.expected.as_ref())
        {
            return Ok(false);
        }
        time()?;
        for write in writes {
            match write {
                KvWrite::Put { key, value } => {
                    records.insert(key.clone(), value.clone());
                }
                KvWrite::Delete { key } => {
                    records.remove(key);
                }
            }
        }
        Ok(true)
    }

    async fn page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limits: KvReadLimits,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        limits.validate()?;
        let records = self.records.lock().await;
        let mut bytes = 0;
        let mut result = Vec::new();
        for (key, value) in records
            .range(prefix.to_vec()..)
            .take_while(|(key, _)| key.starts_with(prefix))
            .filter(|(key, _)| after.is_none_or(|after| key.as_slice() > after))
            .take(limits.max_records)
        {
            bytes += key.len() + value.len();
            if key.len() > limits.max_key_bytes
                || value.len() > limits.max_value_bytes
                || bytes > limits.max_total_bytes
                || bytes + 64 * (result.len() + 1) > limits.max_response_bytes
            {
                return Err(WorkspaceError::InvalidReadPlan("test scan limit".into()));
            }
            result.push(KvEntry {
                key: key.clone(),
                value: value.clone(),
            });
        }
        Ok(result)
    }
}

#[async_trait]
impl WorkspaceKvBackend for MemoryBackend {
    fn name(&self) -> &'static str {
        "lease-retry-test"
    }
    fn supports_consistent_reads(&self) -> bool {
        true
    }
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, WorkspaceError> {
        Ok(self.records.lock().await.get(key).cloned())
    }
    async fn get_many(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>, WorkspaceError> {
        let records = self.records.lock().await;
        Ok(keys.iter().map(|key| records.get(key).cloned()).collect())
    }
    async fn get_many_consistent(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>, WorkspaceError> {
        self.get_many(keys).await
    }
    async fn get_many_with_time(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        self.get_many_consistent_with_time(keys).await
    }
    async fn get_many_consistent_with_time(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        let records = self.records.lock().await;
        Ok((
            keys.iter().map(|key| records.get(key).cloned()).collect(),
            self.clock.load(Ordering::SeqCst),
        ))
    }
    async fn get_many_consistent_with_time_bounded(
        &self,
        keys: &[Vec<u8>],
        limits: KvReadLimits,
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        limits.validate_keys(keys)?;
        if self
            .fail_bounded_read_busy_once
            .swap(false, Ordering::SeqCst)
        {
            return Err(WorkspaceError::Busy);
        }
        let records = self.records.lock().await;
        let mut bytes = 0;
        for key in keys {
            let size = records.get(key).map_or(0, Vec::len);
            bytes += key.len() + size;
            if size > limits.max_value_bytes
                || bytes > limits.max_total_bytes
                || bytes + 64 * keys.len() > limits.max_response_bytes
            {
                return Err(WorkspaceError::InvalidReadPlan("test point limit".into()));
            }
        }
        Ok((
            keys.iter().map(|key| records.get(key).cloned()).collect(),
            self.clock.load(Ordering::SeqCst),
        ))
    }
    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<KvEntry>, WorkspaceError> {
        Ok(self
            .records
            .lock()
            .await
            .range(prefix.to_vec()..)
            .take_while(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| KvEntry {
                key: key.clone(),
                value: value.clone(),
            })
            .collect())
    }
    async fn scan_prefix_bounded(
        &self,
        prefix: &[u8],
        max_records: usize,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        Ok(self
            .records
            .lock()
            .await
            .range(prefix.to_vec()..)
            .take_while(|(key, _)| key.starts_with(prefix))
            .take(max_records)
            .map(|(key, value)| KvEntry {
                key: key.clone(),
                value: value.clone(),
            })
            .collect())
    }
    async fn scan_prefix_with_byte_limits(
        &self,
        prefix: &[u8],
        limits: KvReadLimits,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        self.page(prefix, None, limits).await
    }
    async fn scan_prefix_page_with_byte_limits(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limits: KvReadLimits,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        self.page(prefix, after, limits).await
    }
    async fn compare_and_swap(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
    ) -> Result<bool, WorkspaceError> {
        self.cas(checks, writes, None, None).await
    }
    async fn compare_and_swap_before(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        expires_at_ns: i64,
    ) -> Result<bool, WorkspaceError> {
        self.cas(checks, writes, None, Some(expires_at_ns)).await
    }
    async fn compare_and_swap_in_time_window(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        not_before_ns: Option<i64>,
        before_ns: Option<i64>,
    ) -> Result<bool, WorkspaceError> {
        self.cas(checks, writes, not_before_ns, before_ns).await
    }
    async fn authenticate_checks_before_bounded(
        &self,
        checks: &[KvCheck],
        expires_at_ns: i64,
        limits: KvReadLimits,
    ) -> Result<bool, WorkspaceError> {
        limits.validate_keys(
            &checks
                .iter()
                .map(|check| check.key.clone())
                .collect::<Vec<_>>(),
        )?;
        self.cas(checks, &[], None, Some(expires_at_ns)).await
    }
    async fn server_time_ns(&self) -> Result<i64, WorkspaceError> {
        Ok(self.clock.load(Ordering::SeqCst))
    }
}
