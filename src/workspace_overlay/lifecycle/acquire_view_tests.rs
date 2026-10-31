//! Deterministic out-of-band head change between mount preflight and lease CAS.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::{Mutex, Notify};
use uuid::Uuid;

use super::*;
use crate::meta::MetaLayer;
use crate::workspace_overlay::catalog::CreateVolumeRoot;
use crate::workspace_overlay::meta_layer::WorkspaceMetaLayer;
use crate::workspace_overlay::model::LeaseState;
use crate::workspace_overlay::stores::kv_backend::{
    KvCheck, KvEntry, KvReadLimits, KvWrite, WorkspaceKvBackend,
};
use crate::workspace_overlay::stores::kv_store::KvWorkspaceStore;

#[derive(Clone, Default)]
struct PausedClockBackend {
    records: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
    pause_next_lease_cas: Arc<AtomicBool>,
    lease_cas_arrived: Arc<Notify>,
    lease_cas_continue: Arc<Notify>,
}

impl PausedClockBackend {
    const NOW: i64 = 1_000_000_000;

    /// Bump the stored workspace head epoch in place, keeping its envelope.
    async fn rotate_head(&self, store: &KvWorkspaceStore<Self>, id: WorkspaceId) -> u64 {
        let mut record = store.load_workspace(id).await.unwrap();
        let old_payload = bincode::serialize(&record).unwrap();
        record.head_epoch += 1;
        let new_payload = bincode::serialize(&record).unwrap();
        let mut records = self.records.lock().await;
        let (_, value) = records
            .iter_mut()
            .find(|(_, value)| value.ends_with(&old_payload))
            .expect("workspace record is stored");
        let envelope = value.len() - old_payload.len();
        value.truncate(envelope);
        value.extend_from_slice(&new_payload);
        record.head_epoch
    }

    async fn cas(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        before: Option<i64>,
    ) -> Result<bool, WorkspaceError> {
        let grants_lease = writes.iter().any(|write| {
            matches!(write, KvWrite::Put { key, .. }
                if key.starts_with(b"lease/")
                    && checks.iter().any(|check| check.key == *key && check.expected.is_none()))
        });
        if grants_lease && self.pause_next_lease_cas.swap(false, Ordering::SeqCst) {
            self.lease_cas_arrived.notify_one();
            self.lease_cas_continue.notified().await;
        }
        let mut records = self.records.lock().await;
        if before.is_some_and(|deadline| Self::NOW >= deadline) {
            return Err(WorkspaceError::Fenced);
        }
        if checks
            .iter()
            .any(|check| records.get(&check.key) != check.expected.as_ref())
        {
            return Ok(false);
        }
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
}

#[async_trait]
impl WorkspaceKvBackend for PausedClockBackend {
    fn name(&self) -> &'static str {
        "paused-clock-test"
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

    async fn get_many_consistent_with_time(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        let now = self.server_time_ns().await?;
        Ok((self.get_many(keys).await?, now))
    }

    async fn get_many_consistent_with_time_bounded(
        &self,
        keys: &[Vec<u8>],
        limits: KvReadLimits,
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        limits.validate_keys(keys)?;
        let now = self.server_time_ns().await?;
        let records = self.records.lock().await;
        let mut total = 0usize;
        let mut values = Vec::with_capacity(keys.len());
        for key in keys {
            let value = records.get(key);
            let bytes = value.map_or(0, Vec::len);
            total += key.len() + bytes;
            let response = total.checked_add((values.len() + 1) * 16);
            if bytes > limits.max_value_bytes
                || total > limits.max_total_bytes
                || response.is_none_or(|bytes| bytes > limits.max_response_bytes)
            {
                return Err(WorkspaceError::CorruptMetadata(
                    "bounded test read exceeded".into(),
                ));
            }
            values.push(value.cloned());
        }
        Ok((values, now))
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

    async fn compare_and_swap(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
    ) -> Result<bool, WorkspaceError> {
        self.cas(checks, writes, None).await
    }

    async fn scan_prefix_bounded(
        &self,
        prefix: &[u8],
        max_records: usize,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        if max_records == 0 {
            return Err(WorkspaceError::InvalidReadPlan(
                "zero test scan limit".into(),
            ));
        }
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
        self.scan_prefix_page_with_byte_limits(prefix, None, limits)
            .await
    }

    async fn scan_prefix_page_with_byte_limits(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limits: KvReadLimits,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        limits.validate_scan_page(prefix, after)?;
        let records = self.records.lock().await;
        let mut total = 0usize;
        let mut rows = Vec::new();
        for (key, value) in records
            .range(prefix.to_vec()..)
            .take_while(|(key, _)| key.starts_with(prefix))
            .filter(|(key, _)| after.is_none_or(|after| key.as_slice() > after))
            .take(limits.max_records)
        {
            let bytes = key
                .len()
                .checked_add(value.len())
                .ok_or_else(|| WorkspaceError::InvalidReadPlan("test scan overflow".into()))?;
            total = total
                .checked_add(bytes)
                .ok_or_else(|| WorkspaceError::InvalidReadPlan("test scan overflow".into()))?;
            let response = total.checked_add((rows.len() + 1) * 16);
            if key.len() > limits.max_key_bytes
                || value.len() > limits.max_value_bytes
                || total > limits.max_total_bytes
                || response.is_none_or(|bytes| bytes > limits.max_response_bytes)
            {
                return Err(WorkspaceError::InvalidReadPlan(
                    "bounded test scan exceeded".into(),
                ));
            }
            rows.push(KvEntry {
                key: key.clone(),
                value: value.clone(),
            });
        }
        Ok(rows)
    }

    async fn compare_and_swap_before(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        expires_at_ns: i64,
    ) -> Result<bool, WorkspaceError> {
        self.cas(checks, writes, Some(expires_at_ns)).await
    }

    async fn server_time_ns(&self) -> Result<i64, WorkspaceError> {
        Ok(Self::NOW)
    }
}

#[tokio::test]
async fn switched_head_during_acquire_releases_lease_and_rejects_stale_view() {
    let backend = PausedClockBackend::default();
    let store = Arc::new(KvWorkspaceStore::new(backend.clone()));
    store.initialize_workspace_schema().await.unwrap();
    let workspace = store
        .create_volume_root(CreateVolumeRoot {
            upper_bucket: None,
            volume_format: "workspace-v1".into(),
            schema_version: super::super::model::WORKSPACE_SCHEMA_VERSION,
            volume_id: Uuid::new_v4(),
            workspace_id: WorkspaceId::new(),
            root_layer_id: LayerId::new(),
            writable_layer_id: LayerId::new(),
            owner_id: None,
        })
        .await
        .unwrap();
    backend.pause_next_lease_cas.store(true, Ordering::SeqCst);
    let mount_store = store.clone();
    let workspace_id = workspace.workspace_id;
    let mount = tokio::spawn(async move {
        WorkspaceMountSession::acquire(
            mount_store,
            workspace_id,
            1,
            DEFAULT_LEASE_TTL,
            Duration::from_secs(3600),
        )
        .await
    });
    tokio::time::timeout(
        Duration::from_secs(10),
        backend.lease_cas_arrived.notified(),
    )
    .await
    .expect("mount must reach lease acquisition after the preflight view read");
    // An out-of-band head rotation lands between preflight and lease CAS.
    let switched = backend.rotate_head(&store, workspace_id).await;
    backend.lease_cas_continue.notify_one();
    let result = tokio::time::timeout(Duration::from_secs(10), mount)
        .await
        .expect("mount task must settle")
        .unwrap();
    assert!(matches!(result, Err(WorkspaceError::Fenced)));
    assert!(
        store
            .list_leases(workspace_id)
            .await
            .unwrap()
            .iter()
            .all(|lease| { lease.state != LeaseState::Active })
    );
    assert_eq!(
        store
            .load_workspace(workspace_id)
            .await
            .unwrap()
            .active_lease,
        None
    );
    let fresh = WorkspaceMountSession::acquire(
        store.clone(),
        workspace_id,
        2,
        DEFAULT_LEASE_TTL,
        Duration::from_secs(3600),
    )
    .await
    .unwrap();
    assert_eq!(fresh.view.head_epoch, switched);
    let meta = WorkspaceMetaLayer::new(store.clone(), fresh.view.clone());
    meta.create_file(meta.root_ino(), "after-acquire-retry".into())
        .await
        .unwrap();
    fresh.release().await.unwrap();
}
