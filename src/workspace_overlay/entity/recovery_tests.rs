use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU8, Ordering};

use super::*;
use crate::workspace_overlay::entity::backend::{
    CommitOutcome, MemoryEntityBackend, RowSnapshot, SliceAllocation,
};
use crate::workspace_overlay::entity::codec::encode;
use crate::workspace_overlay::entity::lifecycle::{LowerRegistry, WriterSession};
use crate::workspace_overlay::entity::ownership::{
    SliceOwnership, chunk_membership_keys, plan_chunk_membership_from_rows, plan_orphan,
};
use crate::workspace_overlay::entity::redis::RedisEntityBackend;
use crate::workspace_overlay::entity::resolver::{self, test_support::MemoryLower};

struct FaultBackend<B> {
    inner: Arc<B>,
    /// 1: fail before mutation, 2: lose a successful committed reply.
    fault: AtomicU8,
}

#[async_trait::async_trait]
impl<B: EntityBackend> EntityBackend for FaultBackend<B> {
    async fn read(&self, keys: &[Vec<u8>]) -> Result<RowSnapshot> {
        self.inner.read(keys).await
    }
    async fn commit(
        &self,
        authority: &TransactionAuthority,
        plan: &EntityPlan,
    ) -> Result<CommitOutcome> {
        let fault = self.fault.swap(0, Ordering::SeqCst);
        if fault == 1 {
            return Err(EntityError::Backend("injected before commit".into()));
        }
        let outcome = self.inner.commit(authority, plan).await?;
        if fault == 2 && outcome == CommitOutcome::Committed {
            return Err(EntityError::Backend("injected lost committed reply".into()));
        }
        Ok(outcome)
    }
    async fn reserve_slices(
        &self,
        authority: &TransactionAuthority,
        allocation: &SliceAllocation,
    ) -> Result<Option<Vec<u64>>> {
        self.inner.reserve_slices(authority, allocation).await
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

struct Fixture<B> {
    backend: Arc<FaultBackend<B>>,
    keys: EntityKeys,
    lifecycle: Arc<Lifecycle<FaultBackend<B>>>,
    ownership: Ownership<FaultBackend<B>>,
    session: WriterSession,
    lower: MemoryLower,
    lower_bytes: Vec<u8>,
    objects: BTreeMap<u64, Vec<u8>>,
}

impl<B: EntityBackend> Fixture<B> {
    async fn new(backend: Arc<B>, keys: EntityKeys) -> Self {
        let backend = Arc::new(FaultBackend {
            inner: backend,
            fault: AtomicU8::new(0),
        });
        let mut lower = MemoryLower::empty();
        lower.add_file(1, b"file", 2, 48);
        let lifecycle = Arc::new(Lifecycle::new(backend.clone(), keys.clone()));
        lifecycle.provision(&lower.binding).await.unwrap();
        let session = lifecycle
            .acquire(Uuid::new_v4(), 60_000_000_000)
            .await
            .unwrap();
        let ownership = Ownership::new(backend.clone(), keys.clone());
        Self {
            backend,
            keys,
            lifecycle,
            ownership,
            session,
            lower,
            lower_bytes: (0..48).map(|byte| b'a' + byte % 26).collect(),
            objects: BTreeMap::new(),
        }
    }

    fn recovery(&self) -> Recovery<FaultBackend<B>> {
        Recovery::new(self.backend.clone(), self.keys.clone())
    }
    async fn authority(&self) -> TransactionAuthority {
        self.lifecycle.authority(&self.session).await.unwrap()
    }
    async fn inode(&self) -> (Option<Vec<u8>>, EntityInode) {
        let snapshot = self.backend.read(&[self.keys.inode(2)]).await.unwrap();
        let raw = snapshot.rows[0].clone();
        let upper = raw.as_deref().map(decode).transpose().unwrap();
        let inode = resolver::resolve_inode(&self.lower, &self.lower.binding, 2, upper.as_ref())
            .await
            .unwrap()
            .unwrap();
        (raw, inode)
    }
    async fn commit(&self, plan: &EntityPlan) {
        assert_eq!(
            self.backend
                .commit(&self.authority().await, plan)
                .await
                .unwrap(),
            CommitOutcome::Committed
        );
    }
    async fn resize(&self, size: u64) {
        let (raw, inode) = self.inode().await;
        self.commit(&prepare_resize(&self.keys, 2, raw, &inode, size, 16, 10).unwrap())
            .await;
    }
    async fn upper(&mut self, index: u64, offset: u64, bytes: &[u8]) -> u64 {
        let authority = self.authority().await;
        let upload = self
            .ownership
            .reserve_uploads(&self.session, &authority, &[bytes.len() as u64])
            .await
            .unwrap()
            .pop()
            .unwrap();
        let id = upload.slice_id();
        self.objects.insert(id, bytes.to_vec());
        let receipt = upload.confirm_durable_put();
        let (raw_inode, inode) = self.inode().await;
        let key = self.keys.chunk(2, index);
        let snapshot = self.backend.read(std::slice::from_ref(&key)).await.unwrap();
        let upper: Option<ChunkMap> = snapshot.rows[0].as_deref().map(decode).transpose().unwrap();
        let old = resolver::resolve_chunk(
            &self.lower,
            &self.lower.binding,
            &inode,
            index,
            16,
            upper.as_ref(),
        )
        .await
        .unwrap();
        let next = old
            .replace(
                offset,
                bytes.len() as u64,
                ChunkSource::Data {
                    slice_id: id,
                    slice_offset: 0,
                },
            )
            .unwrap();
        let owner_keys = chunk_membership_keys(&self.keys, 2, index, &old, &next).unwrap();
        let owners = self.backend.read(&owner_keys).await.unwrap();
        let owners = owner_keys.into_iter().zip(owners.rows).collect();
        let mut plan = plan_chunk_membership_from_rows(
            &self.keys,
            2,
            index,
            &old,
            &next,
            &owners,
            Some(&receipt),
        )
        .unwrap();
        let mut updated = inode;
        updated.data_version += 1;
        plan.checks.extend([
            RowCheck {
                key: self.keys.inode(2),
                expected: raw_inode,
            },
            RowCheck {
                key: key.clone(),
                expected: snapshot.rows[0].clone(),
            },
            absent_intent_check(&self.keys, 2),
        ]);
        plan.writes.extend([
            put(key, &next).unwrap(),
            put(self.keys.inode(2), &UpperInode::Present(updated)).unwrap(),
        ]);
        self.commit(&plan).await;
        id
    }
    async fn bytes(&self) -> Vec<u8> {
        let (_, inode) = self.inode().await;
        let mut bytes = vec![0; inode.size as usize];
        for index in 0..inode.size.div_ceil(16) {
            let snapshot = self
                .backend
                .read(&[self.keys.chunk(2, index)])
                .await
                .unwrap();
            let upper = snapshot.rows[0].as_deref().map(decode).transpose().unwrap();
            let map = resolver::resolve_chunk(
                &self.lower,
                &self.lower.binding,
                &inode,
                index,
                16,
                upper.as_ref(),
            )
            .await
            .unwrap();
            for span in map.clip(0, (inode.size - index * 16).min(16)).unwrap() {
                let destination = (index * 16 + span.offset) as usize;
                let len = span.length as usize;
                let source = match span.source {
                    ChunkSource::Data {
                        slice_id,
                        slice_offset,
                    } => Some((&self.objects[&slice_id][..], slice_offset as usize)),
                    ChunkSource::LowerRange { file_offset, .. } => {
                        Some((&self.lower_bytes[..], file_offset as usize))
                    }
                    ChunkSource::Hole => None,
                };
                if let Some((source, offset)) = source {
                    bytes[destination..destination + len]
                        .copy_from_slice(&source[offset..offset + len]);
                }
            }
        }
        bytes
    }
    async fn slice(&self, id: u64) -> SliceOwnership {
        decode(
            self.backend
                .read(&[self.keys.slice(id)])
                .await
                .unwrap()
                .rows[0]
                .as_deref()
                .unwrap(),
        )
        .unwrap()
    }
}

async fn truncate_contract<B: EntityBackend>(backend: Arc<B>, keys: EntityKeys) {
    let mut fixture = Fixture::new(backend, keys).await;
    let boundary = fixture.upper(0, 2, b"BOUNDARY").await;
    let tail = fixture.upper(1, 0, b"TAIL-CHUNK-DATA!").await;
    fixture.upper(2, 4, b"TAIL").await;
    let (raw, mut privileged) = fixture.inode().await;
    privileged.mode |= 0o6000;
    privileged.data_version += 1;
    fixture
        .commit(&EntityPlan {
            checks: vec![RowCheck {
                key: fixture.keys.inode(2),
                expected: raw,
            }],
            writes: vec![put(fixture.keys.inode(2), &UpperInode::Present(privileged)).unwrap()],
        })
        .await;
    let before = fixture.bytes().await;
    let (old_raw, old_inode) = fixture.inode().await;
    let stale_extension =
        prepare_resize(&fixture.keys, 2, old_raw.clone(), &old_inode, 64, 16, 10).unwrap();
    let cut = prepare_resize(&fixture.keys, 2, old_raw, &old_inode, 5, 16, 10).unwrap();
    let authority = fixture.authority().await;
    let before_rows = fixture
        .backend
        .scan_prefix(fixture.keys.volume_prefix(), 4096)
        .await
        .unwrap();
    fixture.backend.fault.store(1, Ordering::SeqCst);
    assert!(matches!(
        fixture.backend.commit(&authority, &cut).await,
        Err(EntityError::Backend(_))
    ));
    assert_eq!(
        fixture
            .backend
            .scan_prefix(fixture.keys.volume_prefix(), 4096)
            .await
            .unwrap(),
        before_rows
    );
    fixture.backend.fault.store(2, Ordering::SeqCst);
    assert!(matches!(
        fixture.backend.commit(&authority, &cut).await,
        Err(EntityError::Backend(_))
    ));
    assert_eq!(fixture.bytes().await, before[..5]);
    assert_eq!(fixture.inode().await.1.mode & 0o6000, 0);
    assert_eq!(
        fixture
            .backend
            .commit(&authority, &stale_extension)
            .await
            .unwrap(),
        CommitOutcome::Conflict
    );
    let (cut_raw, cut_inode) = fixture.inode().await;
    let blocked_extension =
        prepare_resize(&fixture.keys, 2, cut_raw, &cut_inode, 48, 16, 10).unwrap();
    assert_eq!(
        fixture
            .backend
            .commit(&authority, &blocked_extension)
            .await
            .unwrap(),
        CommitOutcome::Conflict
    );

    let maintenance = fixture
        .lifecycle
        .enter_maintenance(&fixture.session)
        .await
        .unwrap();
    assert!(matches!(
        fixture
            .lifecycle
            .resume_after_maintenance(&maintenance, Uuid::new_v4(), 60_000_000_000)
            .await,
        Err(EntityError::Busy)
    ));
    let authority = fixture
        .lifecycle
        .maintenance_authority(&maintenance)
        .await
        .unwrap();

    // Reconstructing only the recovery service simulates lost local operation
    // state. It does not claim an expired/crashed process may acquire a lease.
    for _ in 0..8 {
        let recovery = fixture.recovery();
        let snapshot = fixture
            .backend
            .read(&[fixture.keys.truncate(2)])
            .await
            .unwrap();
        if snapshot.rows[0].is_none() {
            break;
        }
        let before_rows = fixture
            .backend
            .scan_prefix(fixture.keys.volume_prefix(), 4096)
            .await
            .unwrap();
        fixture.backend.fault.store(1, Ordering::SeqCst);
        assert!(matches!(
            recovery.truncate_step(&authority, 2).await,
            Err(EntityError::Backend(_))
        ));
        assert_eq!(
            fixture
                .backend
                .scan_prefix(fixture.keys.volume_prefix(), 4096)
                .await
                .unwrap(),
            before_rows
        );
        fixture.backend.fault.store(2, Ordering::SeqCst);
        assert!(matches!(
            recovery.truncate_step(&authority, 2).await,
            Err(EntityError::Backend(_))
        ));
        assert_eq!(fixture.bytes().await, before[..5]);
    }
    assert!(
        fixture
            .backend
            .read(&[fixture.keys.truncate(2)])
            .await
            .unwrap()
            .rows[0]
            .is_none()
    );
    assert_eq!(fixture.slice(boundary).await.current_refs, 1);
    assert_eq!(fixture.slice(tail).await.current_refs, 0);
    assert!(fixture.slice(tail).await.retirement_hold);
    let deletion = fixture
        .ownership
        .reserve_delete(&maintenance, tail)
        .await
        .unwrap();
    assert!(matches!(
        fixture
            .lifecycle
            .resume_after_maintenance(&maintenance, Uuid::new_v4(), 60_000_000_000)
            .await,
        Err(EntityError::Busy)
    ));
    fixture
        .ownership
        .validate_delete(&maintenance, &deletion)
        .await
        .unwrap();
    fixture
        .ownership
        .complete_delete(&maintenance, &deletion)
        .await
        .unwrap();
    let next_session = fixture
        .lifecycle
        .resume_after_maintenance(&maintenance, Uuid::new_v4(), 60_000_000_000)
        .await
        .unwrap();
    assert_eq!(
        next_session.identity().holder_generation,
        fixture.session.identity().holder_generation + 1
    );
    assert!(fixture.session.is_closed());
    assert!(matches!(
        fixture.lifecycle.authority(&fixture.session).await,
        Err(EntityError::Fenced)
    ));
    assert!(matches!(
        fixture.lifecycle.maintenance_authority(&maintenance).await,
        Err(EntityError::Fenced)
    ));
    fixture.session = next_session;
    fixture.resize(48).await;
    let mut expected = before[..5].to_vec();
    expected.resize(48, 0);
    assert_eq!(
        fixture.bytes().await,
        expected,
        "upper and lower tails must not resurrect after extension"
    );
    assert_eq!(fixture.inode().await.1.lower_visible_size, 5);

    // Aligned and zero cuts also remove whole boundary chunks.
    for size in [16, 0] {
        fixture.resize(size).await;
        for _ in 0..8 {
            if fixture
                .recovery()
                .truncate_step(&fixture.authority().await, 2)
                .await
                .unwrap()
            {
                break;
            }
        }
        fixture.resize(48).await;
        expected.truncate(size as usize);
        expected.resize(48, 0);
        assert_eq!(fixture.bytes().await, expected);
    }
}

#[tokio::test]
async fn truncate_cut_cursor_crashes_and_extension_never_resurrect_upper_or_lower_bytes() {
    truncate_contract(
        Arc::new(MemoryEntityBackend::new(10)),
        EntityKeys::new(Uuid::new_v4(), Uuid::new_v4()),
    )
    .await;
}

#[tokio::test]
#[ignore = "requires BREWFS_TEST_REDIS_URL pointing to an isolated local Redis"]
async fn redis_truncate_orphan_underflow_and_discard_crash_contracts() {
    let url = std::env::var("BREWFS_TEST_REDIS_URL").expect("BREWFS_TEST_REDIS_URL");
    let client = redis::Client::open(url.as_str()).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    for case in 0..4 {
        let volume = Uuid::new_v4();
        let keys = EntityKeys::new(volume, Uuid::new_v4());
        let backend = Arc::new(RedisEntityBackend::connect(&url, volume).await.unwrap());
        match case {
            0 => truncate_contract(backend.clone(), keys.clone()).await,
            1 => underflow_contract(backend.clone(), keys.clone()).await,
            2 => orphan_contract(backend.clone(), keys.clone()).await,
            _ => discard_contract(backend.clone(), keys.clone()).await,
        }
        for (key, _) in backend
            .scan_prefix(keys.volume_prefix(), 4096)
            .await
            .unwrap()
        {
            redis::cmd("DEL")
                .arg(key)
                .query_async::<()>(&mut connection)
                .await
                .unwrap();
        }
    }
}

#[tokio::test]
async fn truncate_refcount_underflow_keeps_map_and_cursor_unchanged() {
    underflow_contract(
        Arc::new(MemoryEntityBackend::new(10)),
        EntityKeys::new(Uuid::new_v4(), Uuid::new_v4()),
    )
    .await;
}

async fn underflow_contract<B: EntityBackend>(backend: Arc<B>, keys: EntityKeys) {
    let mut fixture = Fixture::new(backend, keys).await;
    let id = fixture.upper(1, 0, b"TAIL-CHUNK-DATA!").await;
    fixture.resize(0).await;
    let mut broken = fixture.slice(id).await;
    broken.current_refs = 0;
    fixture
        .commit(&EntityPlan {
            checks: Vec::new(),
            writes: vec![put(fixture.keys.slice(id), &broken).unwrap()],
        })
        .await;
    let before = fixture
        .backend
        .scan_prefix(fixture.keys.volume_prefix(), 4096)
        .await
        .unwrap();
    assert!(matches!(
        fixture
            .recovery()
            .truncate_step(&fixture.authority().await, 2)
            .await,
        Err(EntityError::Corrupt(_))
    ));
    assert_eq!(
        fixture
            .backend
            .scan_prefix(fixture.keys.volume_prefix(), 4096)
            .await
            .unwrap(),
        before
    );
}

#[tokio::test]
async fn orphan_recovery_finishes_pending_cut_before_releasing_memberships() {
    orphan_contract(
        Arc::new(MemoryEntityBackend::new(10)),
        EntityKeys::new(Uuid::new_v4(), Uuid::new_v4()),
    )
    .await;
}

async fn orphan_contract<B: EntityBackend>(backend: Arc<B>, keys: EntityKeys) {
    let mut fixture = Fixture::new(backend, keys).await;
    let id = fixture.upper(1, 0, b"TAIL-CHUNK-DATA!").await;
    let (raw, mut inode) = fixture.inode().await;
    inode.nlink = 0;
    let mut orphan = plan_orphan(&fixture.keys, &fixture.session, 2, None).unwrap();
    orphan.checks.push(RowCheck {
        key: fixture.keys.inode(2),
        expected: raw,
    });
    orphan
        .writes
        .push(put(fixture.keys.inode(2), &UpperInode::Present(inode)).unwrap());
    orphan.writes.push(
        put(
            fixture.keys.dentry(1, b"file"),
            &super::super::model::EntityDentry::Whiteout,
        )
        .unwrap(),
    );
    fixture.commit(&orphan).await;
    fixture.resize(5).await;
    let maintenance = fixture
        .lifecycle
        .enter_maintenance(&fixture.session)
        .await
        .unwrap();
    let recovery = fixture.recovery();
    let authority = fixture
        .lifecycle
        .maintenance_authority(&maintenance)
        .await
        .unwrap();
    assert!(matches!(
        recovery.orphan_step(&authority, 2).await,
        Err(EntityError::Busy)
    ));
    assert!(matches!(
        fixture
            .lifecycle
            .resume_after_maintenance(&maintenance, Uuid::new_v4(), 60_000_000_000)
            .await,
        Err(EntityError::Busy)
    ));
    for _ in 0..8 {
        if fixture
            .backend
            .read(&[fixture.keys.orphan(2)])
            .await
            .unwrap()
            .rows[0]
            .is_none()
        {
            break;
        }
        let before = fixture
            .backend
            .scan_prefix(fixture.keys.volume_prefix(), 4096)
            .await
            .unwrap();
        fixture.backend.fault.store(1, Ordering::SeqCst);
        assert!(matches!(
            recovery.recover_orphan_step(&maintenance, 2).await,
            Err(EntityError::Backend(_))
        ));
        assert_eq!(
            fixture
                .backend
                .scan_prefix(fixture.keys.volume_prefix(), 4096)
                .await
                .unwrap(),
            before
        );
        fixture.backend.fault.store(2, Ordering::SeqCst);
        assert!(matches!(
            recovery.recover_orphan_step(&maintenance, 2).await,
            Err(EntityError::Backend(_))
        ));
    }
    assert!(recovery.recover_orphan_step(&maintenance, 2).await.unwrap());
    assert_eq!(fixture.slice(id).await.current_refs, 0);
    assert!(fixture.slice(id).await.retirement_hold);
    assert_eq!(
        fixture
            .backend
            .read(&[fixture.keys.inode(2)])
            .await
            .unwrap()
            .rows[0],
        Some(encode(&UpperInode::Deleted).unwrap())
    );
}

#[tokio::test]
async fn discard_progresses_above_4096_namespace_rows() {
    large_discard_contract(
        Arc::new(MemoryEntityBackend::new(10)),
        EntityKeys::new(Uuid::new_v4(), Uuid::new_v4()),
    )
    .await;
}

#[tokio::test]
#[ignore = "requires BREWFS_TEST_REDIS_URL pointing to an isolated local Redis"]
async fn discard_progresses_above_4096_namespace_rows_redis() {
    let url = std::env::var("BREWFS_TEST_REDIS_URL").expect("BREWFS_TEST_REDIS_URL");
    let volume = Uuid::new_v4();
    let backend = Arc::new(RedisEntityBackend::connect(&url, volume).await.unwrap());
    let keys = EntityKeys::new(volume, Uuid::new_v4());
    large_discard_contract(backend.clone(), keys.clone()).await;
    let client = redis::Client::open(url.as_str()).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    let mut remaining: Vec<_> = backend
        .scan_prefix(keys.volume_prefix(), 4096)
        .await
        .unwrap()
        .into_iter()
        .map(|(key, _)| key)
        .collect();
    remaining.push(keys.key_index());
    redis::cmd("DEL")
        .arg(remaining)
        .query_async::<()>(&mut connection)
        .await
        .unwrap();
}

async fn large_discard_contract<B: EntityBackend + 'static>(backend: Arc<B>, keys: EntityKeys) {
    use crate::workspace_overlay::entity::namespace::{
        CreateRequest, Credentials, EntityNamespace,
    };

    let lower = Arc::new(MemoryLower::empty());
    let lifecycle = Lifecycle::new(backend.clone(), keys.clone());
    lifecycle.provision(&lower.binding).await.unwrap();
    let session = lifecycle
        .acquire(Uuid::new_v4(), 60_000_000_000)
        .await
        .unwrap();
    let namespace = EntityNamespace::new(
        backend.clone(),
        lower.clone(),
        keys.clone(),
        session.clone(),
    );
    for index in 0..2049 {
        namespace
            .create(
                CreateRequest {
                    parent: 1,
                    name: format!("file-{index}").into_bytes(),
                    kind: EntityKind::File,
                    mode: 0o644,
                    rdev: 0,
                    symlink_target: None,
                },
                Credentials {
                    uid: 0,
                    gid: 0,
                    groups: vec![0],
                    umask: 0,
                },
            )
            .await
            .unwrap();
    }
    let mut metadata_keys = Vec::new();
    for suffix in [b"dentry/".as_slice(), b"inode/"] {
        let prefix = [keys.prefix(), suffix].concat();
        metadata_keys.extend(
            backend
                .scan_prefix(&prefix, 4096)
                .await
                .unwrap()
                .into_iter()
                .map(|(key, _)| key),
        );
    }
    assert!(metadata_keys.len() > 4096);
    // Complete observations must still reject overflow for other callers.
    assert!(matches!(
        backend.scan_prefix(keys.prefix(), 4096).await,
        Err(EntityError::Invalid(_))
    ));
    eprintln!(
        "created 2049 empty files: {} inode/dentry rows",
        metadata_keys.len()
    );
    let maintenance = lifecycle.enter_maintenance(&session).await.unwrap();
    let mut remaining = metadata_keys.len();
    let mut completed = false;
    for step in 0..64 {
        // Reconstruct recovery each time: progress must survive a caller restart.
        let result = Recovery::new(backend.clone(), keys.clone())
            .discard_step(&maintenance)
            .await;
        if let Err(error) = &result {
            for retry in 1..=2 {
                eprintln!(
                    "retry {retry}: {:?}",
                    Recovery::new(backend.clone(), keys.clone())
                        .discard_step(&maintenance)
                        .await
                        .err()
                );
            }
            eprintln!(
                "step {step}: {error:?}; lower-owner proof: {:?}",
                lifecycle.deleted_lower_owner_proof(&maintenance).await
            );
        }
        match result.unwrap() {
            DiscardProgress::Pending => {
                let mut next = 0;
                for batch in metadata_keys.chunks(128) {
                    next += backend
                        .read(batch)
                        .await
                        .unwrap()
                        .rows
                        .iter()
                        .filter(|row| row.is_some())
                        .count();
                }
                assert!(next < remaining, "discard did not remove metadata");
                assert!(remaining - next <= 128, "discard exceeded its batch bound");
                remaining = next;
                assert!(matches!(
                    lifecycle.deleted_lower_owner_proof(&maintenance).await,
                    Err(EntityError::Busy)
                ));
            }
            DiscardProgress::Complete => {
                assert_eq!(remaining, 0);
                eprintln!("discard complete after {} steps", step + 1);
                completed = true;
                break;
            }
            DiscardProgress::ObjectDeletion(_) => panic!("empty files have no upper objects"),
        }
    }
    assert!(
        completed,
        "discard failed to complete within bounded batches"
    );
    lifecycle
        .deleted_lower_owner_proof(&maintenance)
        .await
        .unwrap();
    assert!(
        backend
            .read(&[
                keys.lower_pin(),
                keys.lower_owner(&lower.binding.manifest_digest, keys.namespace_id()),
            ])
            .await
            .unwrap()
            .rows
            .iter()
            .all(Option::is_none)
    );
    let registry: LowerRegistry = decode(
        backend
            .read(&[keys.lower_registry(&lower.binding.manifest_digest)])
            .await
            .unwrap()
            .rows[0]
            .as_deref()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(registry.owners, 0);
}

#[tokio::test]
async fn discard_checks_chunks_and_owners_before_sweeping_metadata() {
    let mut fixture = Fixture::new(
        Arc::new(MemoryEntityBackend::new(10)),
        EntityKeys::new(Uuid::new_v4(), Uuid::new_v4()),
    )
    .await;
    let first = fixture.upper(1, 0, b"data").await;
    let second = fixture.upper(2, 0, b"more").await;
    let dentry_keys: Vec<_> = (0..129)
        .map(|i| fixture.keys.dentry(1, format!("whiteout-{i}").as_bytes()))
        .collect();
    fixture
        .commit(&EntityPlan {
            checks: vec![],
            writes: dentry_keys
                .iter()
                .map(|key| put(key.clone(), &EntityDentry::Whiteout).unwrap())
                .collect(),
        })
        .await;
    let mut metadata_keys = dentry_keys;
    metadata_keys.push(fixture.keys.inode(2));
    let before = fixture.backend.read(&metadata_keys).await.unwrap().rows;
    let maintenance = fixture
        .lifecycle
        .enter_maintenance(&fixture.session)
        .await
        .unwrap();
    for id in [first, second] {
        assert!(matches!(
            fixture.recovery().discard_step(&maintenance).await.unwrap(),
            DiscardProgress::Pending
        ));
        assert_eq!(fixture.slice(id).await.current_refs, 0);
        assert_eq!(
            fixture.backend.read(&metadata_keys).await.unwrap().rows,
            before
        );
    }
    // This owner sorts beyond the first metadata batch. Even there it must
    // block the sweep rather than being deleted as ordinary metadata.
    let authority = fixture
        .lifecycle
        .maintenance_authority(&maintenance)
        .await
        .unwrap();
    committed(
        fixture
            .backend
            .commit(
                &authority,
                &EntityPlan {
                    checks: vec![],
                    writes: vec![put(fixture.keys.owner(first, 2, 99), &true).unwrap()],
                },
            )
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(matches!(
        fixture.recovery().discard_step(&maintenance).await,
        Err(EntityError::Corrupt(message)) if message == "discard found membership without chunk"
    ));
    assert_eq!(
        fixture.backend.read(&metadata_keys).await.unwrap().rows,
        before
    );
    assert!(matches!(
        fixture
            .lifecycle
            .deleted_lower_owner_proof(&maintenance)
            .await,
        Err(EntityError::Busy)
    ));
}

#[tokio::test]
async fn discard_waits_for_read_pins_then_replays_pending_upload_and_delete_acknowledgments() {
    discard_contract(
        Arc::new(MemoryEntityBackend::new(10)),
        EntityKeys::new(Uuid::new_v4(), Uuid::new_v4()),
    )
    .await;
}

async fn discard_contract<B: EntityBackend + 'static>(backend: Arc<B>, keys: EntityKeys) {
    let mut fixture = Fixture::new(backend, keys).await;
    let live_id = fixture.upper(1, 0, b"TAIL-CHUNK-DATA!").await;
    let authority = fixture.authority().await;
    let pending = fixture
        .ownership
        .reserve_uploads(&fixture.session, &authority, &[16])
        .await
        .unwrap()
        .pop()
        .unwrap();
    let pending_id = pending.slice_id();
    drop(pending);
    let reader = fixture.session.admit().unwrap();
    let lifecycle = fixture.lifecycle.clone();
    let writer = fixture.session.clone();
    let draining = tokio::spawn(async move { lifecycle.enter_maintenance(&writer).await });
    for _ in 0..100 {
        if fixture.session.is_closed() {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(fixture.session.is_closed());
    assert!(!draining.is_finished());
    assert_eq!(fixture.slice(live_id).await.current_refs, 1);
    drop(reader);
    let maintenance = draining.await.unwrap().unwrap();
    assert!(matches!(
        fixture
            .lifecycle
            .resume_after_maintenance(&maintenance, Uuid::new_v4(), 60_000_000_000)
            .await,
        Err(EntityError::Busy)
    ));
    assert!(matches!(
        fixture
            .lifecycle
            .deleted_lower_owner_proof(&maintenance)
            .await,
        Err(EntityError::Busy)
    ));
    let mut deleted = Vec::new();
    for _ in 0..20 {
        // Lose each successful state-transition reply, then reconstruct the
        // recovery service using the same proven locally drained session.
        fixture.backend.fault.store(2, Ordering::SeqCst);
        let result = fixture.recovery().discard_step(&maintenance).await;
        match result {
            Err(EntityError::Backend(_)) | Ok(DiscardProgress::Pending) => {}
            Ok(DiscardProgress::ObjectDeletion(token)) => {
                fixture.backend.fault.store(0, Ordering::SeqCst);
                let same = fixture.recovery().discard_step(&maintenance).await.unwrap();
                let DiscardProgress::ObjectDeletion(same) = same else {
                    panic!("deletion reservation disappeared");
                };
                assert_eq!(same.slice_id(), token.slice_id());
                fixture
                    .ownership
                    .validate_delete(&maintenance, &token)
                    .await
                    .unwrap();
                // Simulated successful idempotent object DELETE acknowledgment.
                fixture.backend.fault.store(2, Ordering::SeqCst);
                assert!(matches!(
                    fixture
                        .ownership
                        .complete_delete(&maintenance, &token)
                        .await,
                    Err(EntityError::Backend(_))
                ));
                fixture
                    .ownership
                    .complete_delete(&maintenance, &token)
                    .await
                    .unwrap();
                deleted.push(token.slice_id());
            }
            Ok(DiscardProgress::Complete) => break,
            Err(error) => panic!("unexpected discard error: {error:?}"),
        }
    }
    fixture.backend.fault.store(0, Ordering::SeqCst);
    assert!(matches!(
        fixture.recovery().discard_step(&maintenance).await.unwrap(),
        DiscardProgress::Complete
    ));
    deleted.sort();
    assert_eq!(deleted, vec![live_id, pending_id]);
    let proof = fixture
        .lifecycle
        .deleted_lower_owner_proof(&maintenance)
        .await
        .unwrap();
    assert_eq!(proof.namespace_id(), fixture.keys.namespace_id());
    assert_eq!(proof.volume_id(), fixture.keys.volume_id());
    assert_eq!(proof.binding(), &fixture.lower.binding);
    let registry: LowerRegistry = decode(
        fixture
            .backend
            .read(&[fixture
                .keys
                .lower_registry(&fixture.lower.binding.manifest_digest)])
            .await
            .unwrap()
            .rows[0]
            .as_deref()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(registry.owners, 0);
    fixture.lifecycle.discard_empty(&maintenance).await.unwrap();
    assert!(
        fixture
            .lifecycle
            .acquire(Uuid::new_v4(), 1000)
            .await
            .is_err()
    );
}
