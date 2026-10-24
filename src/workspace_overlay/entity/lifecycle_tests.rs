use super::*;
use crate::workspace_overlay::entity::backend::MemoryEntityBackend;
use crate::workspace_overlay::entity::backend::{
    CreateAllocation, EntityBackend, RowSnapshot, SliceAllocation,
};
use crate::workspace_overlay::entity::model::{ChunkMap, ChunkSource, EntityKind, UpperInode};
use crate::workspace_overlay::entity::ownership::{
    Ownership, SliceOwnership, SliceState, plan_chunk_membership, plan_chunk_membership_from_rows,
};
use crate::workspace_overlay::entity::resolver::test_support::inode_record;
use std::sync::atomic::{AtomicBool, Ordering};

fn lower() -> LowerBinding {
    LowerBinding {
        manifest_key: b"verified-reference-manifest".to_vec(),
        manifest_digest: [7; 32],
        root_ino: 1,
        max_ino: 100,
    }
}

async fn fixture() -> (
    Arc<MemoryEntityBackend>,
    Arc<Lifecycle<MemoryEntityBackend>>,
    WriterSession,
) {
    let backend = Arc::new(MemoryEntityBackend::new(10));
    let lifecycle = Arc::new(Lifecycle::new(
        backend.clone(),
        EntityKeys::new(Uuid::new_v4(), Uuid::new_v4()),
    ));
    lifecycle.provision(&lower()).await.unwrap();
    let session = lifecycle.acquire(Uuid::new_v4(), 100).await.unwrap();
    (backend, lifecycle, session)
}

async fn reserve_slice_history(
    backend: &Arc<MemoryEntityBackend>,
    lifecycle: &Lifecycle<MemoryEntityBackend>,
    session: &WriterSession,
) {
    let ownership = Ownership::new(backend.clone(), lifecycle.keys().clone());
    let authority = lifecycle.authority(session).await.unwrap();
    for _ in 0..64 {
        drop(
            ownership
                .reserve_uploads(session, &authority, &[1; 64])
                .await
                .unwrap(),
        );
    }
    drop(
        ownership
            .reserve_uploads(session, &authority, &[1])
            .await
            .unwrap(),
    );
}

async fn foreign_slice_history() -> (
    Arc<MemoryEntityBackend>,
    Arc<Lifecycle<MemoryEntityBackend>>,
    WriterSession,
) {
    let (backend, lifecycle, session) = fixture().await;
    let foreign = Lifecycle::new(
        backend.clone(),
        EntityKeys::new(lifecycle.keys().volume_id(), Uuid::new_v4()),
    );
    foreign.provision(&lower()).await.unwrap();
    let writer = foreign.acquire(Uuid::new_v4(), 100).await.unwrap();
    reserve_slice_history(&backend, &foreign, &writer).await;
    // Establish the old failure without any object PUTs: the complete volume
    // scan rejects the 4,097th row before a namespace filter can run.
    let prefix = [lifecycle.keys().volume_prefix(), b"slice/"].concat();
    assert!(matches!(
        backend.scan_prefix(&prefix, 4096).await,
        Err(EntityError::Invalid(_))
    ));
    (backend, lifecycle, session)
}

#[tokio::test]
async fn discard_empty_ignores_4097_foreign_slices() {
    let (_, lifecycle, session) = foreign_slice_history().await;
    let maintenance = lifecycle.enter_maintenance(&session).await.unwrap();
    lifecycle.discard_empty(&maintenance).await.unwrap();
    assert!(lifecycle.discard_completed(&maintenance).await.unwrap());
}

#[tokio::test]
async fn resume_ignores_4097_foreign_slices() {
    let (_, lifecycle, session) = foreign_slice_history().await;
    let maintenance = lifecycle.enter_maintenance(&session).await.unwrap();
    let resumed = lifecycle
        .resume_after_maintenance(&maintenance, Uuid::new_v4(), 100)
        .await
        .unwrap();
    lifecycle.authority(&resumed).await.unwrap();
    assert!(matches!(
        lifecycle.maintenance_authority(&maintenance).await,
        Err(EntityError::Fenced)
    ));
}

#[tokio::test]
async fn discard_step_ignores_4097_foreign_slices() {
    use crate::workspace_overlay::entity::recovery::{DiscardProgress, Recovery};

    let (backend, lifecycle, session) = foreign_slice_history().await;
    let maintenance = lifecycle.enter_maintenance(&session).await.unwrap();
    let recovery = Recovery::new(backend, lifecycle.keys().clone());
    assert!(matches!(
        recovery.discard_step(&maintenance).await.unwrap(),
        DiscardProgress::Complete
    ));
}

#[tokio::test]
async fn discard_finds_own_pending_slice_after_4097_foreign_slices() {
    use crate::workspace_overlay::entity::recovery::{DiscardProgress, Recovery};

    let (backend, lifecycle, session) = foreign_slice_history().await;
    let keys = lifecycle.keys();
    let ownership = Ownership::new(backend.clone(), keys.clone());
    let authority = lifecycle.authority(&session).await.unwrap();
    drop(
        ownership
            .reserve_uploads(&session, &authority, &[1])
            .await
            .unwrap(),
    );
    let maintenance = lifecycle.enter_maintenance(&session).await.unwrap();
    assert!(matches!(
        lifecycle.discard_empty(&maintenance).await,
        Err(EntityError::Busy)
    ));
    assert!(matches!(
        lifecycle
            .resume_after_maintenance(&maintenance, Uuid::new_v4(), 100)
            .await,
        Err(EntityError::Busy)
    ));
    let recovery = Recovery::new(backend, keys.clone());
    assert!(matches!(
        recovery.discard_step(&maintenance).await.unwrap(),
        DiscardProgress::Pending
    ));
    let DiscardProgress::ObjectDeletion(deletion) =
        recovery.discard_step(&maintenance).await.unwrap()
    else {
        panic!("own slice was skipped after foreign pages");
    };
    assert_eq!(deletion.slice_id(), 4098);
    // No PUT was issued; acknowledge the idempotent deletion of an absent object.
    ownership
        .complete_delete(&maintenance, &deletion)
        .await
        .unwrap();
    assert!(matches!(
        recovery.discard_step(&maintenance).await.unwrap(),
        DiscardProgress::Complete
    ));
}

#[tokio::test]
async fn resume_and_discard_preserve_4097_slice_tombstones() {
    let (backend, lifecycle, session) = fixture().await;
    reserve_slice_history(&backend, &lifecycle, &session).await;
    let keys = lifecycle.keys();
    let ownership = Ownership::new(backend.clone(), keys.clone());
    let bounded = Lifecycle::new(
        Arc::new(CensusBackend {
            inner: backend.clone(),
            keys: keys.clone(),
            maintenance: Mutex::new(None),
        }),
        keys.clone(),
    );
    let maintenance = lifecycle.enter_maintenance(&session).await.unwrap();
    for id in 1..=4097 {
        ownership.abandon_upload(&maintenance, id).await.unwrap();
    }
    // All 4,097 Available rows are eligible, but cannot fit in one final CAS.
    let resumed = bounded
        .resume_after_maintenance(&maintenance, Uuid::new_v4(), 100)
        .await
        .unwrap();
    let maintenance = lifecycle.enter_maintenance(&resumed).await.unwrap();
    for id in 1..=4097 {
        let deletion = ownership.reserve_delete(&maintenance, id).await.unwrap();
        ownership
            .complete_delete(&maintenance, &deletion)
            .await
            .unwrap();
    }
    let resumed = bounded
        .resume_after_maintenance(&maintenance, Uuid::new_v4(), 100)
        .await
        .unwrap();
    let maintenance = lifecycle.enter_maintenance(&resumed).await.unwrap();
    lifecycle.discard_empty(&maintenance).await.unwrap();
    for id in 1..=4097 {
        let snapshot = backend.read(&[keys.slice(id)]).await.unwrap();
        let row: SliceOwnership = decode(snapshot.rows[0].as_deref().unwrap()).unwrap();
        assert!(matches!(row.state, SliceState::Deleted { .. }));
    }
    assert!(
        backend
            .read(&[keys.slice_delete_fence()])
            .await
            .unwrap()
            .rows[0]
            .is_none()
    );

    let foreign = Lifecycle::new(
        backend.clone(),
        EntityKeys::new(keys.volume_id(), Uuid::new_v4()),
    );
    foreign.provision(&lower()).await.unwrap();
    let writer = foreign.acquire(Uuid::new_v4(), 100).await.unwrap();
    let maintenance = foreign.enter_maintenance(&writer).await.unwrap();
    // Historical tombstones in another namespace also permit resume.
    let writer = foreign
        .resume_after_maintenance(&maintenance, Uuid::new_v4(), 100)
        .await
        .unwrap();
    let foreign_ownership = Ownership::new(backend, foreign.keys().clone());
    let authority = foreign.authority(&writer).await.unwrap();
    let uploads = foreign_ownership
        .reserve_uploads(&writer, &authority, &[1])
        .await
        .unwrap();
    assert_eq!(uploads[0].slice_id(), 4098, "SIDs must never be reused");
}

/// Check bounded reads/commits and optionally introduce Deleting after returning
/// an Available row, modeling a concurrent worker without timing dependencies.
struct CensusBackend {
    inner: Arc<MemoryEntityBackend>,
    keys: EntityKeys,
    maintenance: Mutex<Option<MaintenanceToken>>,
}

#[async_trait::async_trait]
impl EntityBackend for CensusBackend {
    async fn read(&self, keys: &[Vec<u8>]) -> Result<RowSnapshot> {
        assert!(keys.len() <= 128, "census reads must remain bounded");
        let snapshot = self.inner.read(keys).await?;
        if keys.contains(&self.keys.slice(1)) {
            let maintenance = self.maintenance.lock().unwrap().take();
            if let Some(maintenance) = maintenance {
                Ownership::new(self.inner.clone(), self.keys.clone())
                    .reserve_delete(&maintenance, 1)
                    .await?;
            }
        }
        Ok(snapshot)
    }

    async fn commit(
        &self,
        authority: &TransactionAuthority,
        plan: &EntityPlan,
    ) -> Result<CommitOutcome> {
        assert!(authority.checks.len() + plan.checks.len() + plan.writes.len() < 32);
        self.inner.commit(authority, plan).await
    }

    async fn scan_keys_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<Vec<u8>>> {
        self.inner.scan_keys_page(prefix, after, limit).await
    }

    async fn scan_prefix(&self, prefix: &[u8], limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.inner.scan_prefix(prefix, limit).await
    }
    async fn scan_prefix_batch(
        &self,
        prefix: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.inner.scan_prefix_batch(prefix, limit).await
    }
}

#[tokio::test]
async fn resume_is_fenced_by_deletion_started_during_census() {
    let (backend, lifecycle, session) = fixture().await;
    let keys = lifecycle.keys().clone();
    let ownership = Ownership::new(backend.clone(), keys.clone());
    let authority = lifecycle.authority(&session).await.unwrap();
    drop(
        ownership
            .reserve_uploads(&session, &authority, &[1])
            .await
            .unwrap(),
    );
    let maintenance = lifecycle.enter_maintenance(&session).await.unwrap();
    ownership.abandon_upload(&maintenance, 1).await.unwrap();
    let racing = Lifecycle::new(
        Arc::new(CensusBackend {
            inner: backend,
            keys: keys.clone(),
            maintenance: Mutex::new(Some(maintenance.clone())),
        }),
        keys,
    );
    assert!(matches!(
        racing
            .resume_after_maintenance(&maintenance, Uuid::new_v4(), 100)
            .await,
        Err(EntityError::Conflict)
    ));
    assert_eq!(
        lifecycle.descriptor().await.unwrap().state,
        NamespaceState::Quiescent
    );
    assert!(matches!(
        lifecycle
            .resume_after_maintenance(&maintenance, Uuid::new_v4(), 100)
            .await,
        Err(EntityError::Busy)
    ));
}

#[tokio::test]
async fn live_or_expired_writer_cannot_be_silently_replaced() {
    let (backend, lifecycle, session) = fixture().await;
    assert!(matches!(
        lifecycle.acquire(Uuid::new_v4(), 100).await,
        Err(EntityError::Busy)
    ));
    backend.set_time(110).unwrap();
    assert!(matches!(
        lifecycle.authority(&session).await,
        Err(EntityError::Fenced)
    ));
    assert!(matches!(
        lifecycle.renew(&session, 100).await,
        Err(EntityError::Fenced)
    ));
    assert!(matches!(
        lifecycle.acquire(Uuid::new_v4(), 100).await,
        Err(EntityError::Busy)
    ));
    assert!(matches!(
        lifecycle.enter_maintenance(&session).await,
        Err(EntityError::Fenced)
    ));
}

#[tokio::test]
async fn backend_time_expiry_fences_prepared_writes_without_partial_mutation() {
    let (backend, lifecycle, session) = fixture().await;
    let authority = lifecycle.authority(&session).await.unwrap();
    let key = lifecycle.keys().xattr(1, b"user.test");
    let plan = EntityPlan {
        checks: vec![RowCheck {
            key: key.clone(),
            expected: None,
        }],
        writes: vec![RowWrite::Put {
            key: key.clone(),
            value: b"must-not-appear".to_vec(),
        }],
    };
    backend.set_time(110).unwrap();
    assert_eq!(
        backend.commit(&authority, &plan).await.unwrap_err(),
        EntityError::Fenced
    );
    assert_eq!(backend.read(&[key]).await.unwrap().rows, vec![None]);
}

#[tokio::test]
async fn renewal_keeps_identity_and_invalidates_old_exact_authority() {
    let (backend, lifecycle, session) = fixture().await;
    let old = lifecycle.authority(&session).await.unwrap();
    let identity = session.identity();
    backend.set_time(50).unwrap();
    lifecycle.renew(&session, 200).await.unwrap();
    assert_eq!(session.identity(), identity);
    assert_eq!(
        backend.commit(&old, &EntityPlan::default()).await.unwrap(),
        CommitOutcome::Conflict
    );
    backend.set_time(200).unwrap();
    lifecycle.authority(&session).await.unwrap();
    let foreign = Lifecycle::new(
        backend.clone(),
        EntityKeys::new(Uuid::new_v4(), Uuid::new_v4()),
    );
    foreign.provision(&lower()).await.unwrap();
    assert!(matches!(
        foreign.renew(&session, 100).await,
        Err(EntityError::Fenced)
    ));
}

#[tokio::test]
async fn acquired_and_resumed_sessions_reject_consistent_lower_rebinding() {
    for resume in [false, true] {
        for component in 0..4 {
            let (backend, lifecycle, acquired) = fixture().await;
            let session = if resume {
                let maintenance = lifecycle.enter_maintenance(&acquired).await.unwrap();
                lifecycle
                    .resume_after_maintenance(&maintenance, Uuid::new_v4(), 100)
                    .await
                    .unwrap()
            } else {
                acquired
            };
            let authority = lifecycle.authority(&session).await.unwrap();
            let mut descriptor = lifecycle.descriptor().await.unwrap();
            match component {
                0 => descriptor.lower.manifest_key = b"another-manifest-key".to_vec(),
                1 => descriptor.lower.manifest_digest = [8; 32],
                2 => descriptor.lower.root_ino = 2,
                3 => descriptor.lower.max_ino = 101,
                _ => unreachable!(),
            }
            let owner = LowerOwner {
                namespace_id: descriptor.namespace_id,
                binding: descriptor.lower.clone(),
            };
            let registry = LowerRegistry {
                binding: descriptor.lower.clone(),
                owners: 1,
            };
            // Replacing only the descriptor would already fail pin validation.
            // Even a self-consistent replacement cannot retarget this session's
            // retained lower objects, handles or namespace inode boundary.
            let keys = lifecycle.keys();
            let plan = EntityPlan {
                checks: Vec::new(),
                writes: vec![
                    put(keys.descriptor(), &descriptor).unwrap(),
                    put(keys.lower_pin(), &owner).unwrap(),
                    put(
                        keys.lower_owner(&descriptor.lower.manifest_digest, keys.namespace_id()),
                        &owner,
                    )
                    .unwrap(),
                    put(
                        keys.lower_registry(&descriptor.lower.manifest_digest),
                        &registry,
                    )
                    .unwrap(),
                ],
            };
            assert_eq!(
                backend.commit(&authority, &plan).await.unwrap(),
                CommitOutcome::Committed
            );
            assert!(matches!(
                lifecycle.authority(&session).await,
                Err(EntityError::Fenced)
            ));
            assert!(matches!(
                lifecycle.renew(&session, 100).await,
                Err(EntityError::Fenced)
            ));
        }
    }
}

#[tokio::test]
async fn missing_lower_pin_prevents_runtime_admission() {
    let (backend, lifecycle, session) = fixture().await;
    let authority = lifecycle.authority(&session).await.unwrap();
    let plan = EntityPlan {
        checks: Vec::new(),
        writes: vec![RowWrite::Delete {
            key: lifecycle.keys().lower_pin(),
        }],
    };
    assert_eq!(
        backend.commit(&authority, &plan).await.unwrap(),
        CommitOutcome::Committed
    );
    assert!(matches!(
        lifecycle.authority(&session).await,
        Err(EntityError::Fenced)
    ));
}

#[tokio::test]
async fn maintenance_waits_for_retained_read_or_handle_guards() {
    let (backend, lifecycle, session) = fixture().await;
    let read_plan = session.admit().unwrap();
    let writer = session.clone();
    let maintenance_lifecycle = lifecycle.clone();
    let task = tokio::spawn(async move { maintenance_lifecycle.enter_maintenance(&writer).await });
    for _ in 0..100 {
        if session.is_closed() {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(session.is_closed());
    assert!(!task.is_finished());
    assert!(matches!(session.admit(), Err(EntityError::Fenced)));
    drop(read_plan);
    let token = task.await.unwrap().unwrap();
    assert_eq!(
        lifecycle.descriptor().await.unwrap().state,
        NamespaceState::Quiescent
    );
    backend.set_time(10_000).unwrap();
    lifecycle.maintenance_authority(&token).await.unwrap();
    // Same locally drained session can recover an uncertain successful reply.
    let recovered = lifecycle.enter_maintenance(&session).await.unwrap();
    lifecycle.maintenance_authority(&recovered).await.unwrap();
    assert!(matches!(
        lifecycle.authority(&session).await,
        Err(EntityError::Fenced)
    ));
}

#[tokio::test]
async fn one_hundred_lower_owners_minus_ninety_nine_preserves_the_last_owner() {
    let backend = Arc::new(MemoryEntityBackend::new(10));
    let volume = Uuid::new_v4();
    let mut namespaces = Vec::new();
    for _ in 0..100 {
        let lifecycle = Lifecycle::new(backend.clone(), EntityKeys::new(volume, Uuid::new_v4()));
        lifecycle.provision(&lower()).await.unwrap();
        namespaces.push(lifecycle);
    }
    let registry_key = namespaces[0]
        .keys()
        .lower_registry(&lower().manifest_digest);
    let read_count = |rows: Vec<Option<Vec<u8>>>| {
        decode::<LowerRegistry>(rows[0].as_deref().unwrap())
            .unwrap()
            .owners
    };
    assert_eq!(
        read_count(
            backend
                .read(std::slice::from_ref(&registry_key))
                .await
                .unwrap()
                .rows
        ),
        100
    );
    for lifecycle in namespaces.iter().take(99) {
        let writer = lifecycle.acquire(Uuid::new_v4(), 100).await.unwrap();
        let token = lifecycle.enter_maintenance(&writer).await.unwrap();
        lifecycle.discard_empty(&token).await.unwrap();
        lifecycle.discard_empty(&token).await.unwrap();
    }
    assert_eq!(
        read_count(backend.read(&[registry_key]).await.unwrap().rows),
        1
    );
    let last = &namespaces[99];
    let writer = last.acquire(Uuid::new_v4(), 100).await.unwrap();
    last.authority(&writer).await.unwrap();
    let owner_key = last
        .keys()
        .lower_owner(&lower().manifest_digest, last.keys().namespace_id());
    assert!(backend.read(&[owner_key]).await.unwrap().rows[0].is_some());
}

#[tokio::test]
async fn allocators_preserve_lower_inode_boundary_and_volume_slice_identity() {
    let (backend, lifecycle, session) = fixture().await;
    let authority = lifecycle.authority(&session).await.unwrap();
    let create = CreateAllocation {
        keys: lifecycle.keys().clone(),
        inode: inode_record(1, EntityKind::File, 0),
        name: b"allocated".to_vec(),
        xattrs: vec![],
        lower_max_ino: lower().max_ino,
    };
    assert_eq!(
        backend
            .commit_with_inode_allocation(
                &authority,
                &EntityPlan {
                    checks: vec![
                        RowCheck {
                            key: lifecycle.keys().inode(1),
                            expected: None
                        },
                        RowCheck {
                            key: lifecycle.keys().dentry(1, &create.name),
                            expected: None
                        }
                    ],
                    writes: vec![
                        put(
                            lifecycle.keys().inode(1),
                            &UpperInode::Present(inode_record(1, EntityKind::Directory, 0))
                        )
                        .unwrap()
                    ],
                },
                &create
            )
            .await
            .unwrap(),
        Some(101)
    );
    let ownership = Ownership::new(backend.clone(), lifecycle.keys().clone());
    assert_eq!(
        ownership
            .reserve_uploads(&session, &authority, &[16])
            .await
            .unwrap()[0]
            .slice_id(),
        1
    );
    // A second namespace in this volume cannot reset the shared slice counter.
    let volume_prefix = lifecycle.keys().volume_prefix();
    let volume_string = std::str::from_utf8(volume_prefix).unwrap();
    let volume_id = Uuid::parse_str(
        volume_string
            .strip_prefix("{brewfs-entity:")
            .unwrap()
            .strip_suffix("}/")
            .unwrap(),
    )
    .unwrap();
    let second = Lifecycle::new(backend.clone(), EntityKeys::new(volume_id, Uuid::new_v4()));
    second.provision(&lower()).await.unwrap();
    let second_session = second.acquire(Uuid::new_v4(), 100).await.unwrap();
    let second_authority = second.authority(&second_session).await.unwrap();
    let second_ownership = Ownership::new(backend.clone(), second.keys().clone());
    assert_eq!(
        second_ownership
            .reserve_uploads(&second_session, &second_authority, &[16])
            .await
            .unwrap()[0]
            .slice_id(),
        2
    );
}

#[tokio::test]
async fn equal_namespace_and_lease_uuids_in_different_volumes_cannot_share_authority() {
    let backend = Arc::new(MemoryEntityBackend::new(10));
    let namespace = Uuid::new_v4();
    let lease_id = Uuid::new_v4();
    let first = Lifecycle::new(backend.clone(), EntityKeys::new(Uuid::new_v4(), namespace));
    let second = Lifecycle::new(backend, EntityKeys::new(Uuid::new_v4(), namespace));
    first.provision(&lower()).await.unwrap();
    second.provision(&lower()).await.unwrap();
    let writer_a = first.acquire(lease_id, 100).await.unwrap();
    let writer_b = second.acquire(lease_id, 100).await.unwrap();
    assert_eq!(writer_a.identity(), writer_b.identity());
    let retained_b = writer_b.admit().unwrap();
    assert!(matches!(
        second.authority(&writer_a).await,
        Err(EntityError::Fenced)
    ));
    assert!(matches!(
        second.renew(&writer_a, 100).await,
        Err(EntityError::Fenced)
    ));
    assert!(matches!(
        second.enter_maintenance(&writer_a).await,
        Err(EntityError::Fenced)
    ));
    assert!(
        !writer_a.is_closed(),
        "wrong lifecycle must not close a foreign session"
    );
    let token_a = first.enter_maintenance(&writer_a).await.unwrap();
    assert!(matches!(
        second.maintenance_authority(&token_a).await,
        Err(EntityError::Fenced)
    ));
    assert!(!writer_b.is_closed());
    second.authority(&writer_b).await.unwrap();
    drop(retained_b);
}

/// Models the important uncertain outcome: server committed, caller lost reply.
struct LostReplyBackend {
    inner: MemoryEntityBackend,
    lose_next_commit: AtomicBool,
}

#[async_trait::async_trait]
impl EntityBackend for LostReplyBackend {
    async fn read(&self, keys: &[Vec<u8>]) -> Result<RowSnapshot> {
        self.inner.read(keys).await
    }
    async fn commit(
        &self,
        authority: &TransactionAuthority,
        plan: &EntityPlan,
    ) -> Result<CommitOutcome> {
        let outcome = self.inner.commit(authority, plan).await?;
        if outcome == CommitOutcome::Committed
            && self.lose_next_commit.swap(false, Ordering::SeqCst)
        {
            return Err(EntityError::Backend(
                "injected lost successful reply".into(),
            ));
        }
        Ok(outcome)
    }
    async fn commit_with_inode_allocation(
        &self,
        authority: &TransactionAuthority,
        plan: &EntityPlan,
        create: &CreateAllocation,
    ) -> Result<Option<i64>> {
        let inode = self
            .inner
            .commit_with_inode_allocation(authority, plan, create)
            .await?;
        if inode.is_some() && self.lose_next_commit.swap(false, Ordering::SeqCst) {
            return Err(EntityError::Backend(
                "injected lost successful reply".into(),
            ));
        }
        Ok(inode)
    }
    async fn reserve_slices(
        &self,
        authority: &TransactionAuthority,
        allocation: &SliceAllocation,
    ) -> Result<Option<Vec<u64>>> {
        let ids = self.inner.reserve_slices(authority, allocation).await?;
        if ids.is_some() && self.lose_next_commit.swap(false, Ordering::SeqCst) {
            return Err(EntityError::Backend(
                "injected lost successful reply".into(),
            ));
        }
        Ok(ids)
    }
    async fn scan_keys_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<Vec<u8>>> {
        self.inner.scan_keys_page(prefix, after, limit).await
    }

    async fn scan_prefix(&self, prefix: &[u8], limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.inner.scan_prefix(prefix, limit).await
    }
    async fn scan_prefix_batch(
        &self,
        prefix: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.inner.scan_prefix_batch(prefix, limit).await
    }
}

#[tokio::test]
async fn lost_maintenance_and_object_replies_remain_recoverable_without_owner_gaps() {
    let backend = Arc::new(LostReplyBackend {
        inner: MemoryEntityBackend::new(10),
        lose_next_commit: AtomicBool::new(false),
    });
    let keys = EntityKeys::new(Uuid::new_v4(), Uuid::new_v4());
    let lifecycle = Lifecycle::new(backend.clone(), keys.clone());
    let ownership = Ownership::new(backend.clone(), keys.clone());
    lifecycle.provision(&lower()).await.unwrap();
    let session = lifecycle.acquire(Uuid::new_v4(), 100).await.unwrap();
    let authority = lifecycle.authority(&session).await.unwrap();

    // No upload can start without a registration reply. Its uncertain Pending
    // row nevertheless remains durable and cannot be mistaken for garbage.
    backend.lose_next_commit.store(true, Ordering::SeqCst);
    assert!(matches!(
        ownership.reserve_uploads(&session, &authority, &[16]).await,
        Err(EntityError::Backend(_))
    ));
    let pending: SliceOwnership = decode(
        backend.read(&[keys.slice(1)]).await.unwrap().rows[0]
            .as_deref()
            .unwrap(),
    )
    .unwrap();
    assert!(matches!(pending.state, SliceState::Pending { .. }));
    assert!(pending.staging_hold);

    let upload = ownership
        .reserve_uploads(&session, &authority, &[16])
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(
        upload.slice_id(),
        2,
        "uncertain reservation was not replayed"
    );
    let receipt = upload.confirm_durable_put();
    let holes = ChunkMap::holes(16).unwrap();
    let map = holes
        .replace(
            0,
            16,
            ChunkSource::Data {
                slice_id: 2,
                slice_offset: 0,
            },
        )
        .unwrap();
    let mut publication = plan_chunk_membership_from_rows(
        &keys,
        101,
        0,
        &holes,
        &map,
        &BTreeMap::new(),
        Some(&receipt),
    )
    .unwrap();
    publication.checks.push(RowCheck {
        key: keys.chunk(101, 0),
        expected: None,
    });
    publication
        .writes
        .push(put(keys.chunk(101, 0), &map).unwrap());
    backend.lose_next_commit.store(true, Ordering::SeqCst);
    assert!(matches!(
        backend.commit(&authority, &publication).await,
        Err(EntityError::Backend(_))
    ));
    // Inspect the confirmed server state; an unknown publication is never
    // blindly replayed or mistaken for an unowned object.
    let published: SliceOwnership = decode(
        backend.read(&[keys.slice(2)]).await.unwrap().rows[0]
            .as_deref()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(published.state, SliceState::Available);
    assert_eq!(published.current_refs, 1);
    assert!(!published.staging_hold);
    assert_eq!(
        backend.read(&[keys.chunk(101, 0)]).await.unwrap().rows[0],
        Some(encode(&map).unwrap())
    );
    drop(receipt);

    backend.lose_next_commit.store(true, Ordering::SeqCst);
    assert!(matches!(
        lifecycle.enter_maintenance(&session).await,
        Err(EntityError::Backend(_))
    ));
    assert!(session.is_closed());
    assert_eq!(
        lifecycle.descriptor().await.unwrap().state,
        NamespaceState::Quiescent
    );
    let maintenance = lifecycle.enter_maintenance(&session).await.unwrap();

    for slice_id in [1, 2] {
        assert!(matches!(
            ownership.reserve_delete(&maintenance, slice_id).await,
            Err(EntityError::Busy)
        ));
        if slice_id == 1 {
            ownership
                .abandon_upload(&maintenance, slice_id)
                .await
                .unwrap();
        } else {
            let authority = lifecycle.maintenance_authority(&maintenance).await.unwrap();
            let mut removal = plan_chunk_membership(backend.as_ref(), &keys, 101, 0, &map, &holes)
                .await
                .unwrap();
            removal.checks.push(RowCheck {
                key: keys.chunk(101, 0),
                expected: Some(encode(&map).unwrap()),
            });
            removal.writes.push(RowWrite::Delete {
                key: keys.chunk(101, 0),
            });
            assert_eq!(
                backend.commit(&authority, &removal).await.unwrap(),
                CommitOutcome::Committed
            );
        }
        backend.lose_next_commit.store(true, Ordering::SeqCst);
        assert!(matches!(
            ownership.reserve_delete(&maintenance, slice_id).await,
            Err(EntityError::Backend(_))
        ));
        let reserved_bytes = backend.read(&[keys.slice(slice_id)]).await.unwrap().rows[0].clone();
        let deletion = ownership
            .reserve_delete(&maintenance, slice_id)
            .await
            .unwrap();
        assert_eq!(
            backend.read(&[keys.slice(slice_id)]).await.unwrap().rows[0],
            reserved_bytes,
            "reservation retry keeps the same durable exclusion identity"
        );
        // The transport's idempotent DELETE would occur before this completion.
        backend.lose_next_commit.store(true, Ordering::SeqCst);
        assert!(matches!(
            ownership.complete_delete(&maintenance, &deletion).await,
            Err(EntityError::Backend(_))
        ));
        ownership
            .complete_delete(&maintenance, &deletion)
            .await
            .unwrap();
        let row: SliceOwnership = decode(
            backend.read(&[keys.slice(slice_id)]).await.unwrap().rows[0]
                .as_deref()
                .unwrap(),
        )
        .unwrap();
        assert!(matches!(row.state, SliceState::Deleted { .. }));
        assert_eq!(row.current_refs, 0);
    }
    lifecycle.discard_empty(&maintenance).await.unwrap();
}

#[tokio::test]
async fn provision_refuses_stray_upper_rows_without_a_descriptor() {
    let backend = Arc::new(MemoryEntityBackend::new(10));
    let keys = EntityKeys::new(Uuid::new_v4(), Uuid::new_v4());
    let bootstrap = TransactionAuthority::from_validated(
        vec![RowCheck {
            key: keys.descriptor(),
            expected: None,
        }],
        None,
    )
    .unwrap();
    backend
        .commit(
            &bootstrap,
            &EntityPlan {
                checks: Vec::new(),
                writes: vec![RowWrite::Put {
                    key: keys.inode(101),
                    value: b"stray entity after partial restore".to_vec(),
                }],
            },
        )
        .await
        .unwrap();
    let lifecycle = Lifecycle::new(backend.clone(), keys.clone());
    assert!(matches!(
        lifecycle.provision(&lower()).await,
        Err(EntityError::Corrupt(_))
    ));
    assert!(backend.read(&[keys.descriptor()]).await.unwrap().rows[0].is_none());
}

#[tokio::test]
async fn lost_resume_reply_never_reconstructs_a_second_admission_session() {
    let backend = Arc::new(LostReplyBackend {
        inner: MemoryEntityBackend::new(10),
        lose_next_commit: AtomicBool::new(false),
    });
    let keys = EntityKeys::new(Uuid::new_v4(), Uuid::new_v4());
    let lifecycle = Lifecycle::new(backend.clone(), keys);
    lifecycle.provision(&lower()).await.unwrap();
    let old = lifecycle.acquire(Uuid::new_v4(), 100).await.unwrap();
    let maintenance = lifecycle.enter_maintenance(&old).await.unwrap();
    let new_id = Uuid::new_v4();
    backend.lose_next_commit.store(true, Ordering::SeqCst);
    assert!(matches!(
        lifecycle
            .resume_after_maintenance(&maintenance, new_id, 100)
            .await,
        Err(EntityError::Backend(_))
    ));
    assert_eq!(
        lifecycle.descriptor().await.unwrap().active_lease,
        Some(LeaseIdentity {
            lease_id: new_id,
            holder_generation: old.identity().holder_generation + 1,
        })
    );
    assert!(old.is_closed());
    assert!(matches!(
        lifecycle.authority(&old).await,
        Err(EntityError::Fenced)
    ));
    assert!(matches!(
        lifecycle
            .resume_after_maintenance(&maintenance, new_id, 100)
            .await,
        Err(EntityError::Fenced)
    ));
}
