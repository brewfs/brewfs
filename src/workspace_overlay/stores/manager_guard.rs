//! Durable single-controller ownership for shared Manager catalogs.
//!
//! Journal ownership is permanent. A per-process session may be replaced only
//! after backend-clock expiry (or explicit release), with a greater epoch.
//! This is fencing for one durable journal, not replicated Manager storage.

use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::kv_backend::{KvCheck, KvEntry, KvReadLimits, KvWrite, WorkspaceKvBackend};
use crate::workspace_overlay::error::{ConflictDetail, WorkspaceError};

pub const MANAGER_CONTROLLER_KEY: &[u8] = b"manager/controller";
const MAX_RECORD_BYTES: usize = 1024;
const CLAIM_ATTEMPTS: usize = 8;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Controller {
    version: u32,
    journal_id: Uuid,
    session_id: Uuid,
    epoch: u64,
    expires_at_ns: i64,
}

fn decode(raw: &[u8]) -> Result<Controller, WorkspaceError> {
    if raw.len() > MAX_RECORD_BYTES {
        return Err(WorkspaceError::Fenced);
    }
    let record: Controller = serde_json::from_slice(raw).map_err(|_| WorkspaceError::Fenced)?;
    if record.version != 1
        || record.journal_id.is_nil()
        || record.session_id.is_nil()
        || record.epoch == 0
        || record.expires_at_ns <= 0
    {
        return Err(WorkspaceError::Fenced);
    }
    Ok(record)
}

fn encode(record: &Controller) -> Result<Vec<u8>, WorkspaceError> {
    serde_json::to_vec(record).map_err(|_| WorkspaceError::Fenced)
}

fn expiry(now: i64, ttl: u64) -> Result<i64, WorkspaceError> {
    if now <= 0 || ttl == 0 {
        return Err(WorkspaceError::Fenced);
    }
    now.checked_add(i64::try_from(ttl).map_err(|_| WorkspaceError::Fenced)?)
        .ok_or(WorkspaceError::Fenced)
}

#[derive(Clone, Debug)]
pub struct ManagerFence {
    pub check: KvCheck,
    pub expires_at_ns: i64,
}

pub struct ManagerGuard {
    raw: Arc<dyn WorkspaceKvBackend>,
    record: RwLock<Controller>,
    renewal: tokio::sync::Mutex<()>,
    ttl_ns: u64,
}

impl ManagerGuard {
    /// Authenticate a journal's permanent authority without acquiring or
    /// renewing a session. Resource inspection may outlive the active lease,
    /// but a copied catalog header alone is never proof of this ownership.
    pub async fn verify_owner(
        raw: &dyn WorkspaceKvBackend,
        journal_id: Uuid,
    ) -> Result<(), WorkspaceError> {
        let value = raw
            .get(MANAGER_CONTROLLER_KEY)
            .await?
            .ok_or(WorkspaceError::Fenced)?;
        if decode(&value)?.journal_id != journal_id {
            return Err(WorkspaceError::Fenced);
        }
        Ok(())
    }

    pub async fn acquire(
        raw: Arc<dyn WorkspaceKvBackend>,
        journal_id: Uuid,
        session_id: Uuid,
        ttl_ns: u64,
        require_empty: bool,
    ) -> Result<Arc<Self>, WorkspaceError> {
        Self::acquire_inner(raw, journal_id, session_id, ttl_ns, require_empty, true).await
    }

    /// Reopen a scope whose ownership was already checkpointed in the local
    /// durable journal. Missing authority must never be silently recreated.
    pub async fn acquire_existing(
        raw: Arc<dyn WorkspaceKvBackend>,
        journal_id: Uuid,
        session_id: Uuid,
        ttl_ns: u64,
    ) -> Result<Arc<Self>, WorkspaceError> {
        Self::acquire_inner(raw, journal_id, session_id, ttl_ns, false, false).await
    }

    async fn acquire_inner(
        raw: Arc<dyn WorkspaceKvBackend>,
        journal_id: Uuid,
        session_id: Uuid,
        ttl_ns: u64,
        require_empty: bool,
        allow_new_owner: bool,
    ) -> Result<Arc<Self>, WorkspaceError> {
        if journal_id.is_nil() || session_id.is_nil() || ttl_ns == 0 {
            return Err(WorkspaceError::Fenced);
        }
        let key = MANAGER_CONTROLLER_KEY.to_vec();
        for _ in 0..CLAIM_ATTEMPTS {
            let (mut values, now) = raw
                .get_many_consistent_with_time(std::slice::from_ref(&key))
                .await?;
            if values.len() != 1 {
                return Err(WorkspaceError::Fenced);
            }
            let previous = values.pop().flatten();
            let record = previous.as_deref().map(decode).transpose()?;
            if record.is_none() && !allow_new_owner {
                return Err(WorkspaceError::Fenced);
            }
            if let Some(record) = &record {
                if record.journal_id != journal_id {
                    return Err(WorkspaceError::Fenced);
                }
                if record.session_id == session_id && record.expires_at_ns > now {
                    let check = KvCheck {
                        key: key.clone(),
                        expected: previous.clone(),
                    };
                    if raw
                        .compare_and_swap_before(&[check], &[], record.expires_at_ns)
                        .await?
                    {
                        return Ok(Arc::new(Self {
                            raw,
                            record: RwLock::new(record.clone()),
                            renewal: tokio::sync::Mutex::new(()),
                            ttl_ns,
                        }));
                    }
                    continue;
                }
                if record.expires_at_ns > now {
                    return Err(WorkspaceError::Busy);
                }
            }
            let next = Controller {
                version: 1,
                journal_id,
                session_id,
                epoch: record
                    .as_ref()
                    .map_or(Some(1), |record| record.epoch.checked_add(1))
                    .ok_or(WorkspaceError::Fenced)?,
                expires_at_ns: expiry(now, ttl_ns)?,
            };
            let encoded = encode(&next)?;
            let checks = [KvCheck {
                key: key.clone(),
                expected: previous,
            }];
            let writes = [KvWrite::Put {
                key: key.clone(),
                value: encoded.clone(),
            }];
            let result = if require_empty && record.is_none() {
                raw.compare_and_swap_in_empty_namespace(&checks, &writes, next.expires_at_ns)
                    .await
            } else if let Some(record) = &record {
                raw.compare_and_swap_in_time_window(
                    &checks,
                    &writes,
                    Some(record.expires_at_ns),
                    Some(next.expires_at_ns),
                )
                .await
            } else {
                raw.compare_and_swap_before(&checks, &writes, next.expires_at_ns)
                    .await
            };
            let committed = match result {
                Ok(committed) => committed,
                Err(error) => {
                    if !Self::confirm(&*raw, encoded, next.expires_at_ns)
                        .await
                        .unwrap_or(false)
                    {
                        return Err(error);
                    }
                    true
                }
            };
            if committed {
                return Ok(Arc::new(Self {
                    raw,
                    record: RwLock::new(next),
                    renewal: tokio::sync::Mutex::new(()),
                    ttl_ns,
                }));
            }
            if require_empty && record.is_none() && raw.get(&key).await?.is_none() {
                // A successful competing claim always leaves this row. With
                // no claim present, strict CAS rejected a nonempty namespace,
                // not a live session that may become available after expiry.
                return Err(WorkspaceError::Conflict(ConflictDetail {
                    path: Vec::new(),
                    reason: "initialize requires an empty shared namespace".into(),
                }));
            }
            tokio::task::yield_now().await;
        }
        Err(WorkspaceError::Busy)
    }

    async fn confirm(
        raw: &dyn WorkspaceKvBackend,
        expected: Vec<u8>,
        before: i64,
    ) -> Result<bool, WorkspaceError> {
        raw.compare_and_swap_before(
            &[KvCheck {
                key: MANAGER_CONTROLLER_KEY.to_vec(),
                expected: Some(expected),
            }],
            &[],
            before,
        )
        .await
    }

    pub fn fence(&self) -> Result<ManagerFence, WorkspaceError> {
        let record = self.record.read().map_err(|_| WorkspaceError::Fenced)?;
        Ok(ManagerFence {
            check: KvCheck {
                key: MANAGER_CONTROLLER_KEY.to_vec(),
                expected: Some(encode(&record)?),
            },
            expires_at_ns: record.expires_at_ns,
        })
    }

    pub fn guarded_backend(self: &Arc<Self>) -> ManagerGuardedBackend {
        ManagerGuardedBackend {
            guard: self.clone(),
        }
    }

    /// Keep one Manager mutation's fence stable through its backend reply.
    /// Renewal and release use the same lock. This does not extend authority:
    /// every write must still check the fence's backend-clock deadline.
    pub async fn lock_mutations(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.renewal.lock().await
    }

    pub async fn renew(&self) -> Result<(), WorkspaceError> {
        let _lock = self.renewal.lock().await;
        let before = self
            .record
            .read()
            .map_err(|_| WorkspaceError::Fenced)?
            .clone();
        let now = self.raw.server_time_ns().await?;
        if now >= before.expires_at_ns {
            return Err(WorkspaceError::Fenced);
        }
        let mut next = before.clone();
        next.expires_at_ns = expiry(now, self.ttl_ns)?.max(before.expires_at_ns);
        let encoded = encode(&next)?;
        let result = self
            .raw
            .compare_and_swap_before(
                &[KvCheck {
                    key: MANAGER_CONTROLLER_KEY.to_vec(),
                    expected: Some(encode(&before)?),
                }],
                &[KvWrite::Put {
                    key: MANAGER_CONTROLLER_KEY.to_vec(),
                    value: encoded.clone(),
                }],
                before.expires_at_ns,
            )
            .await;
        match result {
            Ok(true) => {}
            Ok(false) => return Err(WorkspaceError::Fenced),
            Err(error) => {
                if !Self::confirm(&*self.raw, encoded, next.expires_at_ns)
                    .await
                    .unwrap_or(false)
                {
                    return Err(error);
                }
            }
        }
        *self.record.write().map_err(|_| WorkspaceError::Fenced)? = next;
        Ok(())
    }

    pub async fn release(&self) -> Result<(), WorkspaceError> {
        let _lock = self.renewal.lock().await;
        let before = self
            .record
            .read()
            .map_err(|_| WorkspaceError::Fenced)?
            .clone();
        let now = self.raw.server_time_ns().await?;
        let mut next = before.clone();
        next.expires_at_ns = now.max(1).min(before.expires_at_ns);
        if !self
            .raw
            .compare_and_swap(
                &[KvCheck {
                    key: MANAGER_CONTROLLER_KEY.to_vec(),
                    expected: Some(encode(&before)?),
                }],
                &[KvWrite::Put {
                    key: MANAGER_CONTROLLER_KEY.to_vec(),
                    value: encode(&next)?,
                }],
            )
            .await?
        {
            return Err(WorkspaceError::Fenced);
        }
        *self.record.write().map_err(|_| WorkspaceError::Fenced)? = next;
        Ok(())
    }
}

#[derive(Clone)]
pub struct ManagerGuardedBackend {
    guard: Arc<ManagerGuard>,
}

impl ManagerGuardedBackend {
    fn plan(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        before: Option<i64>,
    ) -> Result<(Vec<KvCheck>, i64), WorkspaceError> {
        if writes.iter().any(|write| match write {
            KvWrite::Put { key, .. } | KvWrite::Delete { key } => {
                key.as_slice() == MANAGER_CONTROLLER_KEY
            }
        }) {
            return Err(WorkspaceError::Fenced);
        }
        let fence = self.guard.fence()?;
        let mut checks = checks.to_vec();
        checks.push(fence.check);
        Ok((
            checks,
            before.map_or(fence.expires_at_ns, |value| value.min(fence.expires_at_ns)),
        ))
    }

    async fn check_result(
        &self,
        result: Result<bool, WorkspaceError>,
    ) -> Result<bool, WorkspaceError> {
        if matches!(result, Ok(false)) {
            // Callers hold lock_mutations from plan through this check, so
            // self-renewal cannot be mistaken for target contention. Confirm
            // the current fence before reporting an ordinary failed check.
            let fence = self.guard.fence()?;
            if !self
                .guard
                .raw
                .compare_and_swap_before(&[fence.check], &[], fence.expires_at_ns)
                .await?
            {
                return Err(WorkspaceError::Fenced);
            }
        }
        result
    }
}

#[async_trait]
impl WorkspaceKvBackend for ManagerGuardedBackend {
    fn name(&self) -> &'static str {
        self.guard.raw.name()
    }
    fn supports_consistent_reads(&self) -> bool {
        self.guard.raw.supports_consistent_reads()
    }
    fn short_scan_page_is_end(&self) -> bool {
        self.guard.raw.short_scan_page_is_end()
    }
    fn native_gc_metadata_page_quota(&self) -> Option<usize> {
        self.guard.raw.native_gc_metadata_page_quota()
    }
    async fn authenticate_gc_admin(&self) -> Result<(), WorkspaceError> {
        self.guard.raw.authenticate_gc_admin().await
    }
    async fn shutdown_metadata_backend(&self) -> Result<(), WorkspaceError> {
        self.guard.raw.shutdown_metadata_backend().await
    }
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, WorkspaceError> {
        self.guard.raw.get(key).await
    }
    async fn get_many(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>, WorkspaceError> {
        self.guard.raw.get_many(keys).await
    }
    async fn get_many_consistent(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>, WorkspaceError> {
        self.guard.raw.get_many_consistent(keys).await
    }
    async fn get_many_consistent_with_time(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        self.guard.raw.get_many_consistent_with_time(keys).await
    }
    async fn get_many_consistent_with_time_bounded(
        &self,
        keys: &[Vec<u8>],
        limits: KvReadLimits,
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        self.guard
            .raw
            .get_many_consistent_with_time_bounded(keys, limits)
            .await
    }
    async fn get_publication_packet_consistent_with_time_bounded(
        &self,
        keys: &[Vec<u8>],
        limits: KvReadLimits,
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        self.guard
            .raw
            .get_publication_packet_consistent_with_time_bounded(keys, limits)
            .await
    }
    async fn get_many_with_time(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        self.guard.raw.get_many_with_time(keys).await
    }
    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<KvEntry>, WorkspaceError> {
        self.guard.raw.scan_prefix(prefix).await
    }
    async fn scan_prefix_bounded(
        &self,
        prefix: &[u8],
        max_records: usize,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        self.guard
            .raw
            .scan_prefix_bounded(prefix, max_records)
            .await
    }
    async fn scan_prefix_with_byte_limits(
        &self,
        prefix: &[u8],
        limits: KvReadLimits,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        self.guard
            .raw
            .scan_prefix_with_byte_limits(prefix, limits)
            .await
    }
    async fn scan_prefix_page_with_byte_limits(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limits: KvReadLimits,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        self.guard
            .raw
            .scan_prefix_page_with_byte_limits(prefix, after, limits)
            .await
    }
    async fn compare_and_swap(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
    ) -> Result<bool, WorkspaceError> {
        self.compare_and_swap_before(checks, writes, i64::MAX).await
    }
    async fn compare_and_swap_before(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        before: i64,
    ) -> Result<bool, WorkspaceError> {
        let _mutation = self.guard.lock_mutations().await;
        let (checks, deadline) = self.plan(checks, writes, Some(before))?;
        self.check_result(
            self.guard
                .raw
                .compare_and_swap_before(&checks, writes, deadline)
                .await,
        )
        .await
    }
    async fn compare_and_swap_in_time_window(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        lower: Option<i64>,
        upper: Option<i64>,
    ) -> Result<bool, WorkspaceError> {
        super::kv_backend::validate_cas_time_window(lower, upper)?;
        let _mutation = self.guard.lock_mutations().await;
        let (checks, deadline) = self.plan(checks, writes, upper)?;
        self.check_result(
            self.guard
                .raw
                .compare_and_swap_in_time_window(&checks, writes, lower, Some(deadline))
                .await,
        )
        .await
    }
    async fn compare_and_swap_in_empty_namespace(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        before: i64,
    ) -> Result<bool, WorkspaceError> {
        let _mutation = self.guard.lock_mutations().await;
        let (checks, deadline) = self.plan(checks, writes, Some(before))?;
        self.check_result(
            self.guard
                .raw
                .compare_and_swap_in_empty_namespace(&checks, writes, deadline)
                .await,
        )
        .await
    }
    async fn authenticate_checks_before_bounded(
        &self,
        checks: &[KvCheck],
        before: i64,
        limits: KvReadLimits,
    ) -> Result<bool, WorkspaceError> {
        let _mutation = self.guard.lock_mutations().await;
        let (checks, deadline) = self.plan(checks, &[], Some(before))?;
        self.guard
            .raw
            .authenticate_checks_before_bounded(&checks, deadline, limits)
            .await
    }
    async fn server_time_ns(&self) -> Result<i64, WorkspaceError> {
        self.guard.raw.server_time_ns().await
    }
}

#[cfg(test)]
#[path = "manager_guard_tests.rs"]
pub(crate) mod tests;
