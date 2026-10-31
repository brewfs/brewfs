use super::*;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

#[derive(Default)]
pub(crate) struct ClockStore {
    rows: std::sync::Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
    pub(crate) now: AtomicI64,
    lose_reply: AtomicBool,
    pub(crate) pause_reply: AtomicBool,
    pub(crate) reply_committed: tokio::sync::Notify,
    pub(crate) resume_reply: tokio::sync::Notify,
}

impl ClockStore {
    fn cas(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        lower: Option<i64>,
        upper: Option<i64>,
        empty: bool,
    ) -> Result<bool, WorkspaceError> {
        let mut rows = self.rows.lock().unwrap();
        let now = self.now.load(Ordering::SeqCst);
        if lower.is_some_and(|limit| now < limit) {
            return Err(WorkspaceError::Busy);
        }
        if upper.is_some_and(|limit| now >= limit) {
            return Err(WorkspaceError::Fenced);
        }
        if checks
            .iter()
            .any(|check| rows.get(&check.key) != check.expected.as_ref())
        {
            return Ok(false);
        }
        if empty
            && rows
                .keys()
                .any(|key| key.as_slice() != MANAGER_CONTROLLER_KEY)
        {
            return Ok(false);
        }
        for write in writes {
            match write {
                KvWrite::Put { key, value } => {
                    rows.insert(key.clone(), value.clone());
                }
                KvWrite::Delete { key } => {
                    rows.remove(key);
                }
            }
        }
        if self.lose_reply.swap(false, Ordering::SeqCst) {
            return Err(WorkspaceError::Backend("committed reply lost".into()));
        }
        Ok(true)
    }
}

#[async_trait]
impl WorkspaceKvBackend for ClockStore {
    fn name(&self) -> &'static str {
        "clock-store"
    }
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, WorkspaceError> {
        Ok(self.rows.lock().unwrap().get(key).cloned())
    }
    async fn get_many_consistent_with_time(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        let rows = self.rows.lock().unwrap();
        Ok((
            keys.iter().map(|key| rows.get(key).cloned()).collect(),
            self.now.load(Ordering::SeqCst),
        ))
    }
    async fn get_many_consistent_with_time_bounded(
        &self,
        keys: &[Vec<u8>],
        limits: KvReadLimits,
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        limits.validate_keys(keys)?;
        let rows = self.rows.lock().unwrap();
        // TopologyTxn uses this bounded read before submitting its strict CAS.
        // Admit the in-memory response under the same lock before cloning it;
        // otherwise the renewal test would stop at UnsupportedCapability.
        let mut total = 0usize;
        for key in keys {
            let bytes = rows.get(key).map_or(0, Vec::len);
            total = total
                .checked_add(key.len())
                .and_then(|sum| sum.checked_add(bytes))
                .ok_or_else(|| {
                    WorkspaceError::InvalidReadPlan("clock fixture read overflow".into())
                })?;
            if bytes > limits.max_value_bytes
                || total > limits.max_total_bytes
                || total > limits.max_response_bytes
            {
                return Err(WorkspaceError::InvalidReadPlan(
                    "clock fixture bounded point read exceeded its byte limits".into(),
                ));
            }
        }
        Ok((
            keys.iter().map(|key| rows.get(key).cloned()).collect(),
            self.now.load(Ordering::SeqCst),
        ))
    }
    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<KvEntry>, WorkspaceError> {
        Ok(self
            .rows
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
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
        self.cas(checks, writes, None, None, false)
    }
    async fn compare_and_swap_before(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        before: i64,
    ) -> Result<bool, WorkspaceError> {
        let outcome = self.cas(checks, writes, None, Some(before), false);
        if matches!(outcome, Ok(true))
            && !writes.is_empty()
            && self.pause_reply.swap(false, Ordering::SeqCst)
        {
            self.reply_committed.notify_one();
            self.resume_reply.notified().await;
        }
        outcome
    }
    async fn compare_and_swap_in_time_window(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        lower: Option<i64>,
        upper: Option<i64>,
    ) -> Result<bool, WorkspaceError> {
        self.cas(checks, writes, lower, upper, false)
    }
    async fn compare_and_swap_in_empty_namespace(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        before: i64,
    ) -> Result<bool, WorkspaceError> {
        self.cas(checks, writes, None, Some(before), true)
    }
    async fn server_time_ns(&self) -> Result<i64, WorkspaceError> {
        Ok(self.now.load(Ordering::SeqCst))
    }
}

pub(crate) fn fixture() -> Arc<ClockStore> {
    let raw = Arc::new(ClockStore::default());
    raw.now.store(1_000, Ordering::SeqCst);
    raw
}

#[tokio::test]
async fn a_foreign_namespace_is_not_modified_by_an_initial_claim() {
    let raw = fixture();
    raw.rows
        .lock()
        .unwrap()
        .insert(b"foreign".to_vec(), b"preserve".to_vec());
    assert!(matches!(
        ManagerGuard::acquire(raw.clone(), Uuid::new_v4(), Uuid::new_v4(), 100, true).await,
        Err(WorkspaceError::Conflict(_))
    ));
    assert_eq!(raw.rows.lock().unwrap().len(), 1);
    assert!(raw.get(MANAGER_CONTROLLER_KEY).await.unwrap().is_none());
}

#[tokio::test]
async fn another_journal_cannot_take_over_even_after_session_expiry() {
    let raw = fixture();
    ManagerGuard::acquire(raw.clone(), Uuid::new_v4(), Uuid::new_v4(), 100, true)
        .await
        .unwrap();
    raw.now.store(1_200, Ordering::SeqCst);
    assert!(
        ManagerGuard::acquire(raw, Uuid::new_v4(), Uuid::new_v4(), 100, false)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn copied_journal_sessions_exclude_each_other_and_fence_old_writes() {
    let raw = fixture();
    let journal = Uuid::new_v4();
    let old = ManagerGuard::acquire(raw.clone(), journal, Uuid::new_v4(), 100, true)
        .await
        .unwrap();
    assert!(
        ManagerGuard::acquire(raw.clone(), journal, Uuid::new_v4(), 100, false)
            .await
            .is_err()
    );
    raw.now.store(1_100, Ordering::SeqCst);
    let new = ManagerGuard::acquire(raw.clone(), journal, Uuid::new_v4(), 100, false)
        .await
        .unwrap();
    let write = [KvWrite::Put {
        key: b"effect".to_vec(),
        value: b"new".to_vec(),
    }];
    assert!(
        old.guarded_backend()
            .compare_and_swap(&[], &write)
            .await
            .is_err()
    );
    assert!(raw.get(b"effect").await.unwrap().is_none());
    assert!(
        new.guarded_backend()
            .compare_and_swap(&[], &write)
            .await
            .unwrap()
    );
    assert!(old.renew().await.is_err());
}

#[tokio::test]
async fn renewal_and_clean_release_preserve_the_permanent_owner() {
    let raw = fixture();
    let journal = Uuid::new_v4();
    let owner = ManagerGuard::acquire(raw.clone(), journal, Uuid::new_v4(), 100, true)
        .await
        .unwrap();
    raw.now.store(1_050, Ordering::SeqCst);
    owner.renew().await.unwrap();
    assert_eq!(owner.fence().unwrap().expires_at_ns, 1_150);
    owner.release().await.unwrap();
    assert!(raw.get(MANAGER_CONTROLLER_KEY).await.unwrap().is_some());
    assert!(
        ManagerGuard::acquire(raw.clone(), journal, Uuid::new_v4(), 100, false)
            .await
            .is_ok()
    );
    assert!(
        ManagerGuard::acquire(raw, Uuid::new_v4(), Uuid::new_v4(), 100, false)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn a_lost_claim_reply_reconciles_only_the_exact_session() {
    let raw = fixture();
    raw.lose_reply.store(true, Ordering::SeqCst);
    let owner = ManagerGuard::acquire(raw.clone(), Uuid::new_v4(), Uuid::new_v4(), 100, true)
        .await
        .unwrap();
    assert!(
        owner
            .guarded_backend()
            .compare_and_swap(&[], &[])
            .await
            .unwrap()
    );
    raw.now.store(1_050, Ordering::SeqCst);
    raw.lose_reply.store(true, Ordering::SeqCst);
    owner.renew().await.unwrap();
    assert_eq!(owner.fence().unwrap().expires_at_ns, 1_150);
}

#[tokio::test]
async fn callers_cannot_change_the_claim_through_the_guarded_backend() {
    let raw = fixture();
    let owner = ManagerGuard::acquire(raw.clone(), Uuid::new_v4(), Uuid::new_v4(), 100, true)
        .await
        .unwrap();
    let before = raw.get(MANAGER_CONTROLLER_KEY).await.unwrap();
    assert!(
        owner
            .guarded_backend()
            .compare_and_swap(
                &[],
                &[KvWrite::Delete {
                    key: MANAGER_CONTROLLER_KEY.to_vec()
                }]
            )
            .await
            .is_err()
    );
    assert_eq!(raw.get(MANAGER_CONTROLLER_KEY).await.unwrap(), before);
}

#[tokio::test]
async fn reopening_a_bound_scope_never_recreates_missing_authority() {
    let raw = fixture();
    let journal = Uuid::new_v4();
    let owner = ManagerGuard::acquire(raw.clone(), journal, Uuid::new_v4(), 100, true)
        .await
        .unwrap();
    raw.rows.lock().unwrap().remove(MANAGER_CONTROLLER_KEY);
    assert!(
        ManagerGuard::acquire_existing(raw.clone(), journal, Uuid::new_v4(), 100)
            .await
            .is_err()
    );
    assert!(raw.get(MANAGER_CONTROLLER_KEY).await.unwrap().is_none());
    assert!(
        owner
            .guarded_backend()
            .compare_and_swap(
                &[],
                &[KvWrite::Put {
                    key: b"must-not-appear".to_vec(),
                    value: vec![1]
                }]
            )
            .await
            .is_err()
    );
    assert!(raw.get(b"must-not-appear").await.unwrap().is_none());
}

#[tokio::test]
async fn an_inflight_renewal_is_not_misclassified_as_lost_authority() {
    let raw = fixture();
    let owner = ManagerGuard::acquire(raw.clone(), Uuid::new_v4(), Uuid::new_v4(), 100, true)
        .await
        .unwrap();
    raw.now.store(1_050, Ordering::SeqCst);
    raw.pause_reply.store(true, Ordering::SeqCst);
    let renewing = {
        let owner = owner.clone();
        tokio::spawn(async move { owner.renew().await })
    };
    raw.reply_committed.notified().await;
    let mut mutation = {
        let guarded = owner.guarded_backend();
        tokio::spawn(async move {
            guarded
                .compare_and_swap(
                    &[],
                    &[KvWrite::Put {
                        key: b"effect".to_vec(),
                        value: vec![1],
                    }],
                )
                .await
        })
    };
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), &mut mutation)
            .await
            .is_err(),
        "a renewal whose reply is pending must settle before classifying authority"
    );
    raw.resume_reply.notify_one();
    renewing.await.unwrap().unwrap();
    assert!(mutation.await.unwrap().unwrap());
    assert_eq!(raw.get(b"effect").await.unwrap(), Some(vec![1]));
}

#[tokio::test]
async fn strict_root_initialization_waits_for_renewal_instead_of_reporting_foreign_rows() {
    use crate::workspace_overlay::{
        catalog::CreateVolumeRoot,
        ids::{LayerId, WorkspaceId},
        stores::kv_store::KvWorkspaceStore,
    };

    let raw = fixture();
    let owner = ManagerGuard::acquire(raw.clone(), Uuid::new_v4(), Uuid::new_v4(), 100, true)
        .await
        .unwrap();
    raw.now.store(1_050, Ordering::SeqCst);
    raw.pause_reply.store(true, Ordering::SeqCst);
    let renewing = {
        let owner = owner.clone();
        tokio::spawn(async move { owner.renew().await })
    };
    raw.reply_committed.notified().await;
    let workspace_id = WorkspaceId::from_uuid(Uuid::new_v4());
    assert_eq!(owner.fence().unwrap().expires_at_ns, 1_100);
    assert_eq!(
        decode(&raw.get(MANAGER_CONTROLLER_KEY).await.unwrap().unwrap())
            .unwrap()
            .expires_at_ns,
        1_150,
        "the backend renewal must be committed while its local reply is paused"
    );
    let mut initializing = tokio::spawn(async move {
        KvWorkspaceStore::new(owner.guarded_backend())
            .create_volume_root_in_empty_namespace(CreateVolumeRoot {
                upper_bucket: None,
                volume_format: "workspace-v1".into(),
                schema_version: 1,
                volume_id: Uuid::new_v4(),
                workspace_id,
                root_layer_id: LayerId::from_uuid(Uuid::new_v4()),
                writable_layer_id: LayerId::from_uuid(Uuid::new_v4()),
                owner_id: Some("renewal-test".into()),
            })
            .await
    });
    let waiting =
        tokio::time::timeout(std::time::Duration::from_millis(20), &mut initializing).await;
    assert!(
        waiting.is_err(),
        "the strict root packet must wait for the committed renewal reply; completed early: {waiting:?}"
    );
    assert_eq!(
        raw.rows.lock().unwrap().len(),
        1,
        "only the controller row may exist before the renewal reply settles"
    );
    raw.resume_reply.notify_one();
    renewing.await.unwrap().unwrap();
    let workspace = initializing.await.unwrap().unwrap();
    assert_eq!(workspace.workspace_id, workspace_id);
    assert!(raw.get(b"control").await.unwrap().is_some());
}

#[tokio::test]
async fn read_only_owner_verification_accepts_expiry_but_never_missing_or_foreign_authority() {
    let raw = fixture();
    let journal = Uuid::new_v4();
    let owner = ManagerGuard::acquire(raw.clone(), journal, Uuid::new_v4(), 100, true)
        .await
        .unwrap();
    let before = raw.get(MANAGER_CONTROLLER_KEY).await.unwrap();
    raw.now.store(2_000, Ordering::SeqCst);
    ManagerGuard::verify_owner(raw.as_ref(), journal)
        .await
        .unwrap();
    assert!(
        ManagerGuard::verify_owner(raw.as_ref(), Uuid::new_v4())
            .await
            .is_err()
    );
    assert_eq!(raw.get(MANAGER_CONTROLLER_KEY).await.unwrap(), before);
    raw.rows.lock().unwrap().remove(MANAGER_CONTROLLER_KEY);
    assert!(
        ManagerGuard::verify_owner(raw.as_ref(), journal)
            .await
            .is_err()
    );
    assert!(raw.get(MANAGER_CONTROLLER_KEY).await.unwrap().is_none());
    drop(owner);
}
