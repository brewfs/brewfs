use super::*;
use crate::workspace_overlay::entity::{
    backend::{CommitOutcome, MemoryEntityBackend},
    codec::encode,
    model::{ChunkSource, ChunkSpan, EntityInode, EntityKind, LowerBinding, UpperInode},
    recovery::Recovery,
};

thread_local! {
    pub(super) static RANGE_VALIDATION_SPAN_VISITS: std::cell::Cell<usize> = const {
        std::cell::Cell::new(0)
    };
}

struct Fixture {
    volume_id: Uuid,
    backend: Arc<MemoryEntityBackend>,
    lifecycle: Arc<Lifecycle<MemoryEntityBackend>>,
    ownership: Ownership<MemoryEntityBackend>,
    keys: EntityKeys,
    session: WriterSession,
}

impl Fixture {
    async fn new() -> Self {
        let backend = Arc::new(MemoryEntityBackend::new(10));
        let volume_id = Uuid::new_v4();
        let keys = EntityKeys::new(volume_id, Uuid::new_v4());
        let lifecycle = Arc::new(Lifecycle::new(backend.clone(), keys.clone()));
        lifecycle
            .provision(&LowerBinding {
                manifest_key: b"reference-lower".to_vec(),
                manifest_digest: [3; 32],
                root_ino: 1,
                max_ino: 2,
            })
            .await
            .unwrap();
        let session = lifecycle.acquire(Uuid::new_v4(), 1000).await.unwrap();
        let ownership = Ownership::new(backend.clone(), keys.clone());
        Self {
            volume_id,
            backend,
            lifecycle,
            ownership,
            keys,
            session,
        }
    }

    /// Typed starting state for membership/reclamation tests, independent of
    /// the upload protocol (tested with durable receipts below).
    async fn seed_available_slice(&self) -> u64 {
        let row = SliceOwnership {
            namespace_id: self.keys.namespace_id(),
            slice_id: 1,
            size: 16,
            state: SliceState::Available,
            current_refs: 0,
            staging_hold: true,
            retirement_hold: false,
            publication_refs: 0,
        };
        let authority = self.lifecycle.authority(&self.session).await.unwrap();
        assert_eq!(
            self.backend
                .commit(
                    &authority,
                    &EntityPlan {
                        checks: vec![RowCheck {
                            key: self.keys.slice(1),
                            expected: None
                        }],
                        writes: vec![
                            put(self.keys.slice(1), &row).unwrap(),
                            RowWrite::Put {
                                key: self.keys.counter("slice"),
                                value: b"1".to_vec(),
                            }
                        ],
                    }
                )
                .await
                .unwrap(),
            CommitOutcome::Committed
        );
        1
    }

    async fn row(&self, id: u64) -> SliceOwnership {
        let snapshot = self.backend.read(&[self.keys.slice(id)]).await.unwrap();
        decode(snapshot.rows[0].as_deref().unwrap()).unwrap()
    }

    async fn replacement(
        &self,
        ino: i64,
        index: u64,
        old: &ChunkMap,
        new: &ChunkMap,
    ) -> EntityPlan {
        let mut plan =
            plan_chunk_membership(self.backend.as_ref(), &self.keys, ino, index, old, new)
                .await
                .unwrap();
        let key = self.keys.chunk(ino, index);
        let snapshot = self.backend.read(std::slice::from_ref(&key)).await.unwrap();
        plan.checks.push(RowCheck {
            key: key.clone(),
            expected: snapshot.rows[0].clone(),
        });
        plan.writes.push(put(key, new).unwrap());
        plan
    }

    async fn publish(&self, ino: i64, index: u64, old: &ChunkMap, new: &ChunkMap) {
        let plan = self.replacement(ino, index, old, new).await;
        let authority = self.lifecycle.authority(&self.session).await.unwrap();
        assert_eq!(
            self.backend.commit(&authority, &plan).await.unwrap(),
            CommitOutcome::Committed
        );
    }
}

fn map(id: u64) -> ChunkMap {
    ChunkMap::holes(16)
        .unwrap()
        .replace(
            0,
            2,
            ChunkSource::Data {
                slice_id: id,
                slice_offset: 0,
            },
        )
        .unwrap()
        .replace(
            4,
            2,
            ChunkSource::Data {
                slice_id: id,
                slice_offset: 4,
            },
        )
        .unwrap()
}

#[test]
fn fragmented_membership_range_validation_visits_each_span_once() {
    let keys = EntityKeys::new(Uuid::new_v4(), Uuid::new_v4());
    let data = ChunkMap::new(
        1024 * 4096,
        (0..1024)
            .map(|index| ChunkSpan {
                offset: index * 4096,
                length: 4096,
                source: ChunkSource::Data {
                    slice_id: index + 1,
                    slice_offset: 0,
                },
            })
            .collect(),
    )
    .unwrap();
    assert_eq!(data.spans.len(), 1024);
    let mut rows = BTreeMap::new();
    for id in 1..=1024 {
        let row = SliceOwnership {
            namespace_id: keys.namespace_id(),
            slice_id: id,
            size: 4096,
            state: SliceState::Available,
            current_refs: 1,
            staging_hold: false,
            retirement_hold: false,
            publication_refs: 0,
        };
        let membership = SliceMembership {
            namespace_id: keys.namespace_id(),
            slice_id: id,
            ino: 3,
            chunk_index: 0,
        };
        rows.insert(keys.slice(id), Some(encode(&row).unwrap()));
        rows.insert(keys.owner(id, 3, 0), Some(encode(&membership).unwrap()));
    }
    let expected_checks: Vec<_> = rows
        .iter()
        .map(|(key, expected)| RowCheck {
            key: key.clone(),
            expected: expected.clone(),
        })
        .collect();
    let removed = data.replace(0, 4096, ChunkSource::Hole).unwrap();
    // Reads validate an unchanged full map; writes validate a replacement.
    for new in [&data, &removed] {
        RANGE_VALIDATION_SPAN_VISITS.with(|visits| visits.set(0));
        let plan = plan_chunk_membership_from_rows(&keys, 3, 0, &data, new, &rows, None).unwrap();
        assert_eq!(
            RANGE_VALIDATION_SPAN_VISITS.with(|visits| visits.get()),
            data.spans.len() + new.spans.len(),
            "range validation must not rescan both maps for every slice"
        );
        assert_eq!(plan.checks, expected_checks);
        if new == &data {
            assert!(plan.writes.is_empty());
        } else {
            let mut retired: SliceOwnership =
                decode(rows[&keys.slice(1)].as_deref().unwrap()).unwrap();
            retired.current_refs = 0;
            retired.retirement_hold = true;
            assert_eq!(
                plan.writes,
                vec![
                    RowWrite::Delete {
                        key: keys.owner(1, 3, 0),
                    },
                    put(keys.slice(1), &retired).unwrap(),
                ]
            );
        }
    }
}

#[tokio::test]
async fn membership_ranges_validate_removed_unchanged_and_new_fragments() {
    let fixture = Fixture::new().await;
    let id = fixture.seed_available_slice().await;
    let holes = ChunkMap::holes(16).unwrap();
    // The first fragment has the largest source end, exactly at the size bound.
    let data = map(id)
        .replace(
            0,
            2,
            ChunkSource::Data {
                slice_id: id,
                slice_offset: 14,
            },
        )
        .unwrap();
    fixture.publish(3, 0, &holes, &data).await;
    fixture.replacement(3, 0, &data, &data).await;
    for (slice_offset, expected) in [
        (
            15,
            EntityError::Corrupt("chunk extends beyond immutable slice".into()),
        ),
        (u64::MAX, EntityError::Overflow),
    ] {
        let mut invalid = data.clone();
        invalid.spans[0].source = ChunkSource::Data {
            slice_id: id,
            slice_offset,
        };
        for (old, new) in [
            (&invalid, &invalid),
            (&invalid, &holes),
            (&invalid, &data),
            (&data, &invalid),
        ] {
            assert_eq!(
                plan_chunk_membership(fixture.backend.as_ref(), &fixture.keys, 3, 0, old, new)
                    .await,
                Err(expected.clone())
            );
        }
    }
}

#[tokio::test]
async fn durable_receipt_publishes_pending_owner_and_membership_in_one_transaction() {
    let fixture = Fixture::new().await;
    let authority = fixture.lifecycle.authority(&fixture.session).await.unwrap();
    let token = fixture
        .ownership
        .reserve_uploads(&fixture.session, &authority, &[16])
        .await
        .unwrap()
        .pop()
        .unwrap();
    let id = token.slice_id();
    let pending = RowCheck {
        key: fixture.keys.slice(token.slice_id()),
        expected: Some(token.pending_bytes.clone()),
    };
    let receipt = token.confirm_durable_put();
    let holes = ChunkMap::holes(16).unwrap();
    let data = map(id);
    let mut plan = plan_chunk_membership_from_rows(
        &fixture.keys,
        3,
        0,
        &holes,
        &data,
        &BTreeMap::new(),
        Some(&receipt),
    )
    .unwrap();
    assert!(plan.checks.contains(&pending));
    assert!(plan.checks.contains(&RowCheck {
        key: fixture.keys.owner(id, 3, 0),
        expected: None
    }));
    assert!(matches!(
        fixture.row(id).await.state,
        SliceState::Pending { .. }
    ));
    plan.checks.push(RowCheck {
        key: fixture.keys.chunk(3, 0),
        expected: None,
    });
    plan.writes
        .push(put(fixture.keys.chunk(3, 0), &data).unwrap());
    assert_eq!(
        fixture.backend.commit(&authority, &plan).await.unwrap(),
        CommitOutcome::Committed
    );
    let row = fixture.row(id).await;
    assert_eq!(row.state, SliceState::Available);
    assert_eq!(row.current_refs, 1);
    assert!(!row.staging_hold);
    assert_eq!(
        fixture.backend.commit(&authority, &plan).await.unwrap(),
        CommitOutcome::Conflict
    );

    let changed = BTreeMap::from([(fixture.keys.slice(id), Some(encode(&row).unwrap()))]);
    assert_eq!(
        plan_chunk_membership_from_rows(
            &fixture.keys,
            3,
            0,
            &holes,
            &data,
            &changed,
            Some(&receipt),
        ),
        Err(EntityError::Conflict)
    );
    let foreign = EntityKeys::new(fixture.volume_id, Uuid::new_v4());
    assert!(matches!(
        plan_chunk_membership_from_rows(
            &foreign,
            3,
            0,
            &holes,
            &data,
            &BTreeMap::new(),
            Some(&receipt),
        ),
        Err(EntityError::Invalid(_))
    ));
    let duplicate = DurableUpload {
        token: receipt.token.clone(),
    };
    assert!(matches!(
        plan_chunk_memberships_from_rows(
            &fixture.keys,
            3,
            &[(0, holes, data)],
            &BTreeMap::new(),
            &[receipt, duplicate],
        ),
        Err(EntityError::Invalid(_))
    ));
}

#[tokio::test]
async fn batch_memberships_aggregate_shared_slices_and_keep_retirement_on_net_zero_change() {
    let fixture = Fixture::new().await;
    let id = fixture.seed_available_slice().await;
    let holes = ChunkMap::holes(16).unwrap();
    let data = map(id);
    fixture.publish(3, 0, &holes, &data).await;
    fixture.publish(3, 1, &holes, &data).await;
    let replacements = vec![
        (0, data.clone(), holes.clone()),
        (1, data.clone(), holes.clone()),
        (2, holes.clone(), data.clone()),
        (3, holes.clone(), data.clone()),
    ];
    let mut row_keys = BTreeSet::new();
    for (index, old, new) in &replacements {
        row_keys.extend(chunk_membership_keys(&fixture.keys, 3, *index, old, new).unwrap());
    }
    let row_keys: Vec<_> = row_keys.into_iter().collect();
    let snapshot = fixture.backend.read(&row_keys).await.unwrap();
    let rows: BTreeMap<_, _> = row_keys.into_iter().zip(snapshot.rows).collect();
    let mut plan =
        plan_chunk_memberships_from_rows(&fixture.keys, 3, &replacements, &rows, &[]).unwrap();
    assert_eq!(
        plan.writes
            .iter()
            .filter(|write| write.key() == fixture.keys.slice(id))
            .count(),
        1
    );
    for (index, _, new) in &replacements {
        let key = fixture.keys.chunk(3, *index);
        let current = fixture
            .backend
            .read(std::slice::from_ref(&key))
            .await
            .unwrap()
            .rows
            .pop()
            .unwrap();
        plan.checks.push(RowCheck {
            key: key.clone(),
            expected: current,
        });
        plan.writes.push(put(key, new).unwrap());
    }
    let authority = fixture.lifecycle.authority(&fixture.session).await.unwrap();
    assert_eq!(
        fixture.backend.commit(&authority, &plan).await.unwrap(),
        CommitOutcome::Committed
    );
    let row = fixture.row(id).await;
    assert_eq!(row.current_refs, 2);
    assert!(row.retirement_hold);

    let mut undercounted = rows;
    let mut row: SliceOwnership = decode(
        undercounted
            .get(&fixture.keys.slice(id))
            .unwrap()
            .as_deref()
            .unwrap(),
    )
    .unwrap();
    row.current_refs = 1;
    undercounted.insert(fixture.keys.slice(id), Some(encode(&row).unwrap()));
    assert!(matches!(
        plan_chunk_memberships_from_rows(&fixture.keys, 3, &replacements, &undercounted, &[],),
        Err(EntityError::Corrupt(_))
    ));
}

struct CountingBackend {
    inner: Arc<MemoryEntityBackend>,
    reads: std::sync::atomic::AtomicUsize,
    mutations: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl EntityBackend for CountingBackend {
    async fn read(&self, keys: &[Vec<u8>]) -> Result<super::super::backend::RowSnapshot> {
        self.reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.read(keys).await
    }
    async fn commit(
        &self,
        authority: &TransactionAuthority,
        plan: &EntityPlan,
    ) -> Result<CommitOutcome> {
        self.mutations
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.commit(authority, plan).await
    }
    async fn reserve_slices(
        &self,
        authority: &TransactionAuthority,
        allocation: &SliceAllocation,
    ) -> Result<Option<Vec<u64>>> {
        self.mutations
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.reserve_slices(authority, allocation).await
    }
    async fn scan_keys_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<Vec<u8>>> {
        self.reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.scan_keys_page(prefix, after, limit).await
    }

    async fn scan_prefix(&self, prefix: &[u8], limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.scan_prefix(prefix, limit).await
    }
    async fn scan_prefix_batch(
        &self,
        prefix: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.scan_prefix_batch(prefix, limit).await
    }
}

#[tokio::test]
async fn supplied_authority_reserves_all_pending_owners_in_one_backend_call() {
    let fixture = Fixture::new().await;
    let authority = fixture.lifecycle.authority(&fixture.session).await.unwrap();
    let backend = Arc::new(CountingBackend {
        inner: fixture.backend.clone(),
        reads: 0.into(),
        mutations: 0.into(),
    });
    let ownership = Ownership::new(backend.clone(), fixture.keys.clone());
    let uploads = ownership
        .reserve_uploads(&fixture.session, &authority, &[8, 16, 32])
        .await
        .unwrap();
    assert_eq!(uploads.len(), 3);
    assert_eq!(backend.reads.load(std::sync::atomic::Ordering::Relaxed), 0);
    assert_eq!(
        backend.mutations.load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    for upload in uploads {
        let row = fixture.row(upload.slice_id()).await;
        assert!(matches!(row.state, SliceState::Pending { .. }));
        assert!(row.staging_hold);
        assert_eq!(row.current_refs, 0);
    }
}

#[tokio::test]
async fn memberships_count_distinct_chunks_and_replay_cannot_double_release() {
    let fixture = Fixture::new().await;
    let id = fixture.seed_available_slice().await;
    let holes = ChunkMap::holes(16).unwrap();
    let data = map(id);
    fixture.publish(3, 0, &holes, &data).await;
    assert_eq!(
        fixture.row(id).await.current_refs,
        1,
        "two spans in one chunk count once"
    );
    assert!(!fixture.row(id).await.staging_hold);
    fixture.publish(3, 1, &holes, &data).await;
    assert_eq!(fixture.row(id).await.current_refs, 2);
    let plan = fixture.replacement(3, 0, &data, &holes).await;
    let authority = fixture.lifecycle.authority(&fixture.session).await.unwrap();
    assert_eq!(
        fixture.backend.commit(&authority, &plan).await.unwrap(),
        CommitOutcome::Committed
    );
    assert_eq!(
        fixture.backend.commit(&authority, &plan).await.unwrap(),
        CommitOutcome::Conflict
    );
    let row = fixture.row(id).await;
    assert_eq!(row.current_refs, 1);
    assert!(row.retirement_hold);
    fixture.publish(3, 1, &data, &holes).await;
    let row = fixture.row(id).await;
    assert_eq!(row.current_refs, 0);
    assert!(
        row.retirement_hold,
        "zero references cannot reclaim an old read plan"
    );
}

#[tokio::test]
async fn pending_and_unpublished_uploads_block_reclamation_and_upload_token_drains() {
    let fixture = Fixture::new().await;
    let authority = fixture.lifecycle.authority(&fixture.session).await.unwrap();
    let token = fixture
        .ownership
        .reserve_uploads(&fixture.session, &authority, &[16])
        .await
        .unwrap()
        .pop()
        .unwrap();
    let id = token.slice_id();
    let holes = ChunkMap::holes(16).unwrap();
    assert!(matches!(
        plan_chunk_membership(
            fixture.backend.as_ref(),
            &fixture.keys,
            3,
            0,
            &holes,
            &map(id)
        )
        .await,
        Err(EntityError::Busy)
    ));
    let cloned_token = token.clone();
    let lifecycle = fixture.lifecycle.clone();
    let session = fixture.session.clone();
    let task = tokio::spawn(async move { lifecycle.enter_maintenance(&session).await });
    for _ in 0..100 {
        if fixture.session.is_closed() {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(fixture.session.is_closed());
    assert!(!task.is_finished());
    drop(token);
    tokio::task::yield_now().await;
    assert!(
        !task.is_finished(),
        "cloned upload token retains its obligation"
    );
    drop(cloned_token);
    let maintenance = task.await.unwrap().unwrap();
    assert!(matches!(
        fixture.ownership.reserve_delete(&maintenance, id).await,
        Err(EntityError::Busy)
    ));
    assert!(matches!(
        fixture.lifecycle.discard_empty(&maintenance).await,
        Err(EntityError::Busy)
    ));
    fixture
        .ownership
        .abandon_upload(&maintenance, id)
        .await
        .unwrap();
    let deletion = fixture
        .ownership
        .reserve_delete(&maintenance, id)
        .await
        .unwrap();
    fixture
        .ownership
        .complete_delete(&maintenance, &deletion)
        .await
        .unwrap();
    fixture
        .ownership
        .complete_delete(&maintenance, &deletion)
        .await
        .unwrap();
    assert!(matches!(
        fixture.row(id).await.state,
        SliceState::Deleted { .. }
    ));
    fixture.lifecycle.discard_empty(&maintenance).await.unwrap();
}

#[tokio::test]
async fn uploaded_but_unpublished_object_keeps_staging_owner() {
    let fixture = Fixture::new().await;
    let authority = fixture.lifecycle.authority(&fixture.session).await.unwrap();
    let receipt = fixture
        .ownership
        .reserve_uploads(&fixture.session, &authority, &[16])
        .await
        .unwrap()
        .pop()
        .unwrap()
        .confirm_durable_put();
    let id = receipt.slice_id();
    drop(receipt);
    let maintenance = fixture
        .lifecycle
        .enter_maintenance(&fixture.session)
        .await
        .unwrap();
    assert!(matches!(
        fixture.ownership.reserve_delete(&maintenance, id).await,
        Err(EntityError::Busy)
    ));
    assert!(fixture.row(id).await.staging_hold);
}

#[tokio::test]
async fn retired_object_waits_for_reader_then_deletion_excludes_stale_publication() {
    let fixture = Fixture::new().await;
    let id = fixture.seed_available_slice().await;
    let holes = ChunkMap::holes(16).unwrap();
    let data = map(id);
    fixture.publish(3, 0, &holes, &data).await;
    let old_read_plan = fixture.session.admit().unwrap();
    fixture.publish(3, 0, &data, &holes).await;
    let stale_plan = fixture.replacement(3, 0, &holes, &data).await;
    let stale_authority = fixture.lifecycle.authority(&fixture.session).await.unwrap();
    let lifecycle = fixture.lifecycle.clone();
    let session = fixture.session.clone();
    let task = tokio::spawn(async move { lifecycle.enter_maintenance(&session).await });
    for _ in 0..100 {
        if fixture.session.is_closed() {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(!task.is_finished());
    assert!(fixture.row(id).await.retirement_hold);
    drop(old_read_plan);
    let maintenance = task.await.unwrap().unwrap();
    let deletion = fixture
        .ownership
        .reserve_delete(&maintenance, id)
        .await
        .unwrap();
    assert_eq!(
        fixture
            .backend
            .commit(&stale_authority, &stale_plan)
            .await
            .unwrap(),
        CommitOutcome::Conflict
    );
    assert!(matches!(
        plan_chunk_membership(fixture.backend.as_ref(), &fixture.keys, 3, 0, &holes, &data).await,
        Err(EntityError::Busy)
    ));
    fixture
        .ownership
        .complete_delete(&maintenance, &deletion)
        .await
        .unwrap();
    assert!(matches!(
        fixture.row(id).await.state,
        SliceState::Deleted { .. }
    ));
}

#[tokio::test]
async fn cross_namespace_missing_membership_and_underflow_fail_without_map_changes() {
    let fixture = Fixture::new().await;
    let id = fixture.seed_available_slice().await;
    let holes = ChunkMap::holes(16).unwrap();
    let data = map(id);
    let foreign_keys = EntityKeys::new(fixture.volume_id, Uuid::new_v4());
    assert!(
        plan_chunk_membership(fixture.backend.as_ref(), &foreign_keys, 3, 0, &holes, &data)
            .await
            .is_err()
    );
    // Pretending an unpublished slice already has a membership must fail closed.
    assert!(matches!(
        plan_chunk_membership(fixture.backend.as_ref(), &fixture.keys, 3, 0, &data, &holes).await,
        Err(EntityError::Corrupt(_))
    ));
    fixture.publish(3, 0, &holes, &data).await;
    let mut corrupt = fixture.row(id).await;
    corrupt.current_refs = 0;
    let authority = fixture.lifecycle.authority(&fixture.session).await.unwrap();
    fixture
        .backend
        .commit(
            &authority,
            &EntityPlan {
                checks: Vec::new(),
                writes: vec![put(fixture.keys.slice(id), &corrupt).unwrap()],
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        plan_chunk_membership(fixture.backend.as_ref(), &fixture.keys, 3, 0, &data, &holes).await,
        Err(EntityError::Corrupt(_))
    ));
    assert_eq!(
        fixture
            .backend
            .read(&[fixture.keys.chunk(3, 0)])
            .await
            .unwrap()
            .rows[0],
        Some(encode(&data).unwrap())
    );
}

#[tokio::test]
async fn orphan_cleanup_advances_with_memberships_and_is_idempotent() {
    let fixture = Fixture::new().await;
    let recovery = Recovery::new(fixture.backend.clone(), fixture.keys.clone());
    let id = fixture.seed_available_slice().await;
    let holes = ChunkMap::holes(16).unwrap();
    let data = map(id);
    fixture.publish(3, 0, &holes, &data).await;
    fixture.publish(3, 1, &holes, &data).await;
    let inode = EntityInode {
        ino: 3,
        kind: EntityKind::File,
        size: 32,
        mode: 0o600,
        uid: 1,
        gid: 1,
        rdev: 0,
        nlink: 0,
        atime_ns: 0,
        mtime_ns: 0,
        ctime_ns: 0,
        symlink_target: None,
        parent_hint: Some(1),
        lower_visible_size: 0,
        data_version: 1,
    };
    let mut orphan_plan = plan_orphan(&fixture.keys, &fixture.session, 3, None).unwrap();
    orphan_plan
        .writes
        .push(put(fixture.keys.inode(3), &UpperInode::Present(inode)).unwrap());
    let authority = fixture.lifecycle.authority(&fixture.session).await.unwrap();
    assert_eq!(
        fixture
            .backend
            .commit(&authority, &orphan_plan)
            .await
            .unwrap(),
        CommitOutcome::Committed
    );
    assert_eq!(
        fixture
            .backend
            .commit(&authority, &orphan_plan)
            .await
            .unwrap(),
        CommitOutcome::Conflict
    );
    let current = fixture
        .backend
        .read(&[fixture.keys.orphan(3)])
        .await
        .unwrap()
        .rows
        .remove(0);
    assert!(
        plan_orphan(&fixture.keys, &fixture.session, 3, current)
            .unwrap()
            .writes
            .is_empty()
    );
    let open_handle = fixture.session.admit().unwrap();
    let lifecycle = fixture.lifecycle.clone();
    let session = fixture.session.clone();
    let task = tokio::spawn(async move { lifecycle.enter_maintenance(&session).await });
    tokio::task::yield_now().await;
    assert!(!task.is_finished());
    assert_eq!(fixture.row(id).await.current_refs, 2);
    drop(open_handle);
    let maintenance = task.await.unwrap().unwrap();
    assert!(!recovery.recover_orphan_step(&maintenance, 3).await.unwrap());
    assert_eq!(fixture.row(id).await.current_refs, 1);
    let cursor: OrphanRecord = decode(
        fixture
            .backend
            .read(&[fixture.keys.orphan(3)])
            .await
            .unwrap()
            .rows[0]
            .as_deref()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(cursor.cleanup_chunk, 1);
    assert!(!recovery.recover_orphan_step(&maintenance, 3).await.unwrap());
    assert_eq!(fixture.row(id).await.current_refs, 0);
    assert!(fixture.row(id).await.retirement_hold);
    assert!(recovery.recover_orphan_step(&maintenance, 3).await.unwrap());
    assert!(recovery.recover_orphan_step(&maintenance, 3).await.unwrap());
    assert_eq!(
        fixture
            .backend
            .read(&[fixture.keys.inode(3)])
            .await
            .unwrap()
            .rows[0],
        Some(encode(&UpperInode::Deleted).unwrap())
    );
    let reservation = fixture
        .ownership
        .reserve_delete(&maintenance, id)
        .await
        .unwrap();
    fixture
        .ownership
        .complete_delete(&maintenance, &reservation)
        .await
        .unwrap();
}
