use std::{collections::BTreeSet, sync::Arc};

use uuid::Uuid;

use super::*;
use crate::workspace_overlay::entity::{keys::EntityKeys, redis::RedisEntityBackend};

fn check(key: Vec<u8>, expected: Option<&[u8]>) -> RowCheck {
    RowCheck {
        key,
        expected: expected.map(<[u8]>::to_vec),
    }
}

fn put(key: Vec<u8>, value: &[u8]) -> RowWrite {
    RowWrite::Put {
        key,
        value: value.to_vec(),
    }
}

async fn initialize<B: EntityBackend>(backend: &B, keys: &EntityKeys) -> TransactionAuthority {
    let initial =
        TransactionAuthority::from_validated(vec![check(keys.descriptor(), None)], None).unwrap();
    let plan = EntityPlan {
        checks: vec![check(keys.counter("inode"), None)],
        writes: vec![
            put(keys.descriptor(), b"active"),
            put(keys.lease(), b"lease-1"),
            put(keys.counter("inode"), b"9007199254740991"),
        ],
    };
    assert_eq!(
        backend.commit(&initial, &plan).await.unwrap(),
        CommitOutcome::Committed
    );
    TransactionAuthority::from_validated(
        vec![
            check(keys.descriptor(), Some(b"active")),
            check(keys.lease(), Some(b"lease-1")),
        ],
        None,
    )
    .unwrap()
}

async fn conformance<B: EntityBackend + 'static>(backend: Arc<B>, keys: EntityKeys) {
    let authority = initialize(backend.as_ref(), &keys).await;
    let protected = keys.inode(100);
    let foreign = EntityKeys::new(Uuid::new_v4(), Uuid::new_v4()).inode(100);
    assert!(matches!(
        backend
            .commit(
                &authority,
                &EntityPlan {
                    checks: vec![],
                    writes: vec![
                        put(protected.clone(), b"must-not-appear"),
                        put(foreign.clone(), b"foreign")
                    ],
                }
            )
            .await,
        Err(EntityError::Invalid(_))
    ));
    assert_eq!(backend.read(&[protected]).await.unwrap().rows, vec![None]);
    let contested = keys.dentry(1, b"exclusive");
    let mut tasks = tokio::task::JoinSet::new();
    for candidate in 1..=32 {
        let backend = backend.clone();
        let authority = authority.clone();
        let key = contested.clone();
        tasks.spawn(async move {
            backend
                .commit(
                    &authority,
                    &EntityPlan {
                        checks: vec![check(key.clone(), None)],
                        writes: vec![put(key, candidate.to_string().as_bytes())],
                    },
                )
                .await
                .unwrap()
        });
    }
    let mut committed = 0;
    while let Some(outcome) = tasks.join_next().await {
        committed += usize::from(outcome.unwrap() == CommitOutcome::Committed);
    }
    assert_eq!(committed, 1, "exclusive create has exactly one winner");

    let empty_key = keys.inode(2);
    let other_key = keys.inode(3);
    assert_eq!(
        backend
            .commit(
                &authority,
                &EntityPlan {
                    checks: vec![],
                    writes: vec![put(empty_key.clone(), b"")]
                }
            )
            .await
            .unwrap(),
        CommitOutcome::Committed
    );
    assert_eq!(
        backend
            .read(&[empty_key.clone(), other_key.clone()])
            .await
            .unwrap()
            .rows,
        vec![Some(vec![]), None]
    );
    // Checked rows are exact predicates, including empty values and absence.
    // They are not returned as part of the extension, even on a mismatch.
    for (expected_empty, expected_absent, matched) in [
        (Some(b"".as_slice()), None, true),
        (None, None, false),
        (Some(b"".as_slice()), Some(b"".as_slice()), false),
        (Some(b"changed".as_slice()), None, false),
    ] {
        let checks = [
            check(empty_key.clone(), expected_empty),
            check(other_key.clone(), expected_absent),
        ];
        let (snapshot, actual) = backend
            .read_checked(&[keys.descriptor()], &checks)
            .await
            .unwrap();
        assert_eq!(actual, matched);
        assert_eq!(snapshot.rows, vec![Some(b"active".to_vec())]);
        assert!(snapshot.now_ns >= 0);
        let (snapshot, actual) = backend.read_checked(&[], &checks).await.unwrap();
        assert_eq!(actual, matched);
        assert!(snapshot.rows.is_empty());
    }
    for checks in [
        vec![check(foreign, None)],
        vec![check(empty_key.clone(), None); MAX_TRANSACTION_ROWS],
        vec![RowCheck {
            key: empty_key.clone(),
            expected: Some(vec![0; MAX_ROW_BYTES + 1]),
        }],
    ] {
        assert!(matches!(
            backend.read_checked(&[keys.descriptor()], &checks).await,
            Err(EntityError::Invalid(_))
        ));
    }
    assert_eq!(
        backend
            .commit(
                &authority,
                &EntityPlan {
                    checks: vec![check(empty_key.clone(), None)],
                    writes: vec![put(other_key.clone(), b"must-not-appear")],
                }
            )
            .await
            .unwrap(),
        CommitOutcome::Conflict
    );
    assert_eq!(
        backend
            .read(std::slice::from_ref(&other_key))
            .await
            .unwrap()
            .rows,
        vec![None]
    );

    let prefix = keys.dentry_prefix(2);
    for name in [b"a", b"b", b"c"] {
        assert_eq!(
            backend
                .commit(
                    &authority,
                    &EntityPlan {
                        checks: vec![],
                        writes: vec![put(keys.dentry(2, name), name)]
                    }
                )
                .await
                .unwrap(),
            CommitOutcome::Committed
        );
    }
    assert!(matches!(
        backend.scan_prefix(&prefix, 2).await,
        Err(EntityError::Invalid(_))
    ));
    let rows = backend.scan_prefix(&prefix, 3).await.unwrap();
    assert_eq!(rows.len(), 3);
    assert!(rows.windows(2).all(|pair| pair[0].0 < pair[1].0));
    assert!(
        rows.iter()
            .all(|(key, value)| key.starts_with(&prefix) && value.len() == 1)
    );
    assert_eq!(
        backend.scan_prefix_batch(&prefix, 2).await.unwrap(),
        rows[..2]
    );
    assert_eq!(backend.scan_prefix_batch(&prefix, 4).await.unwrap(), rows);
    assert!(
        backend
            .scan_prefix_batch(&keys.dentry_prefix(3), 1)
            .await
            .unwrap()
            .is_empty()
    );
    for limit in [0, MAX_TRANSACTION_ROWS + 1] {
        assert!(matches!(
            backend.scan_prefix_batch(&prefix, limit).await,
            Err(EntityError::Invalid(_))
        ));
    }

    let page = backend.scan_prefix_page(&prefix, None, 2).await.unwrap();
    assert_eq!(page, rows[..2]);
    let after = page.last().unwrap().0.clone();
    assert_eq!(
        backend
            .commit(
                &authority,
                &EntityPlan {
                    checks: vec![],
                    writes: vec![RowWrite::Delete { key: after.clone() }],
                }
            )
            .await
            .unwrap(),
        CommitOutcome::Committed
    );
    assert_eq!(
        backend
            .scan_prefix_page(&prefix, Some(&after), 2)
            .await
            .unwrap(),
        rows[2..]
    );
    assert!(
        backend
            .scan_prefix_page(&prefix, Some(&rows[2].0), 2)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        backend.scan_prefix_page(&prefix, Some(&other_key), 2).await,
        Err(EntityError::Invalid(_))
    ));
    assert!(matches!(
        backend.scan_prefix_page(&prefix, None, 0).await,
        Err(EntityError::Invalid(_))
    ));

    let large_prefix = keys.dentry_prefix(3);
    for start in (0..4097).step_by(256) {
        let writes = (start..(start + 256).min(4097))
            .map(|index| {
                put(
                    keys.dentry(3, format!("entry-{index:04}").as_bytes()),
                    b"row",
                )
            })
            .collect();
        assert_eq!(
            backend
                .commit(
                    &authority,
                    &EntityPlan {
                        checks: vec![],
                        writes
                    }
                )
                .await
                .unwrap(),
            CommitOutcome::Committed
        );
    }
    assert!(matches!(
        backend.scan_prefix(&large_prefix, 4096).await,
        Err(EntityError::Invalid(_))
    ));
    let mut after = None;
    let mut observed = Vec::new();
    loop {
        let page = backend
            .scan_prefix_page(&large_prefix, after.as_deref(), 256)
            .await
            .unwrap();
        assert!(page.len() <= 256);
        if page.is_empty() {
            break;
        }
        assert!(after.as_ref().is_none_or(|after| &page[0].0 > after));
        after = Some(page.last().unwrap().0.clone());
        observed.extend(page);
    }
    assert_eq!(observed.len(), 4097);
    assert!(observed.windows(2).all(|pair| pair[0].0 < pair[1].0));
    assert_eq!(observed.last().unwrap().0, keys.dentry(3, b"entry-4096"));

    let snapshot = backend.read(&[]).await.unwrap();
    let expired =
        TransactionAuthority::from_validated(authority.checks.clone(), Some(snapshot.now_ns))
            .unwrap();
    assert_eq!(
        backend
            .commit(
                &expired,
                &EntityPlan {
                    checks: vec![],
                    writes: vec![put(other_key.clone(), b"must-not-appear")]
                }
            )
            .await,
        Err(EntityError::Fenced)
    );
    assert_eq!(
        backend
            .read(std::slice::from_ref(&other_key))
            .await
            .unwrap()
            .rows,
        vec![None]
    );

    assert_eq!(
        backend
            .commit(
                &authority,
                &EntityPlan {
                    checks: vec![],
                    writes: vec![put(keys.lease(), b"lease-2")]
                }
            )
            .await
            .unwrap(),
        CommitOutcome::Committed
    );
    assert_eq!(
        backend
            .commit(
                &authority,
                &EntityPlan {
                    checks: vec![],
                    writes: vec![put(other_key.clone(), b"stale-writer")]
                }
            )
            .await,
        Ok(CommitOutcome::Conflict)
    );
    // A heartbeat changes exact lease bytes without changing writer identity.
    // Only a fresh lifecycle read may create replacement authority; a caller
    // must never substitute a later deadline into the stale authority itself.
    let renewed = TransactionAuthority::from_validated(
        vec![
            check(keys.descriptor(), Some(b"active")),
            check(keys.lease(), Some(b"lease-2")),
        ],
        None,
    )
    .unwrap();
    assert_eq!(
        backend
            .commit(&renewed, &EntityPlan::default())
            .await
            .unwrap(),
        CommitOutcome::Committed
    );
    assert_eq!(
        backend
            .read(&[other_key, keys.descriptor()])
            .await
            .unwrap()
            .rows,
        vec![None, Some(b"active".to_vec())]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_backend_conformance() {
    conformance(
        Arc::new(MemoryEntityBackend::new(100)),
        EntityKeys::new(Uuid::new_v4(), Uuid::new_v4()),
    )
    .await;
}

fn create_request(keys: &EntityKeys, parent: i64, name: &[u8]) -> CreateAllocation {
    CreateAllocation {
        keys: keys.clone(),
        inode: EntityInode {
            ino: 1,
            kind: EntityKind::File,
            size: 0,
            mode: 0o640,
            uid: 123,
            gid: 456,
            rdev: 0,
            nlink: 1,
            atime_ns: 9007199254740993,
            mtime_ns: 9007199254740995,
            ctime_ns: 9007199254740997,
            symlink_target: None,
            parent_hint: Some(parent),
            lower_visible_size: 0,
            data_version: 0,
        },
        name: name.to_vec(),
        xattrs: vec![(b"user.identity".to_vec(), b"1 9007199254740993".to_vec())],
        lower_max_ino: 100,
    }
}

fn create_plan(create: &CreateAllocation, parent: &[u8], dentry: Option<&[u8]>) -> EntityPlan {
    let parent_key = create.keys.inode(create.inode.parent_hint.unwrap());
    EntityPlan {
        checks: vec![
            check(parent_key.clone(), Some(parent)),
            check(
                create
                    .keys
                    .dentry(create.inode.parent_hint.unwrap(), &create.name),
                dentry,
            ),
        ],
        writes: vec![put(parent_key, b"updated-parent")],
    }
}

async fn atomic_create_conformance<B: EntityBackend + 'static>(backend: Arc<B>, keys: EntityKeys) {
    let authority = initialize(backend.as_ref(), &keys).await;
    let setup = EntityPlan {
        checks: vec![],
        writes: (1001..=1032)
            .map(|ino| put(keys.inode(ino), b"parent"))
            .collect(),
    };
    assert_eq!(
        backend.commit(&authority, &setup).await.unwrap(),
        CommitOutcome::Committed
    );
    let mut tasks = tokio::task::JoinSet::new();
    for parent in 1001..=1032 {
        let backend = backend.clone();
        let authority = authority.clone();
        let create = create_request(&keys, parent, b"file-1");
        tasks.spawn(async move {
            let plan = create_plan(&create, b"parent", None);
            let ino = backend
                .commit_with_inode_allocation(&authority, &plan, &create)
                .await
                .unwrap()
                .expect("independent parents must not conflict on allocation");
            (ino, create)
        });
    }
    let mut allocated = BTreeSet::new();
    while let Some(result) = tasks.join_next().await {
        let (ino, mut create) = result.unwrap();
        assert!(allocated.insert(ino));
        let generated = [
            keys.inode(ino),
            keys.dentry(create.inode.parent_hint.unwrap(), &create.name),
            keys.xattr(ino, b"user.identity"),
        ];
        let rows = backend.read(&generated).await.unwrap().rows;
        for (key, value) in generated.into_iter().zip(&rows) {
            assert_eq!(
                backend.scan_prefix(&key, 1).await.unwrap(),
                vec![(key, value.clone().unwrap())],
                "allocator-generated rows must be indexed"
            );
        }
        create.inode.ino = ino;
        assert_eq!(
            codec::decode::<UpperInode>(rows[0].as_deref().unwrap()).unwrap(),
            UpperInode::Present(create.inode)
        );
        assert_eq!(
            codec::decode::<EntityDentry>(rows[1].as_deref().unwrap()).unwrap(),
            EntityDentry::Present {
                ino,
                kind: EntityKind::File
            }
        );
        assert_eq!(
            codec::decode::<EntityValue>(rows[2].as_deref().unwrap()).unwrap(),
            EntityValue::Present(b"1 9007199254740993".to_vec())
        );
    }
    assert_eq!(allocated, (9007199254740992..9007199254741024).collect());
    let counter = keys.counter("inode");
    let before = backend
        .read(std::slice::from_ref(&counter))
        .await
        .unwrap()
        .rows;
    let exclusive = create_request(&keys, 1001, b"file-1");
    assert_eq!(
        backend
            .commit_with_inode_allocation(
                &authority,
                &create_plan(&exclusive, b"updated-parent", None),
                &exclusive,
            )
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        backend
            .read(std::slice::from_ref(&counter))
            .await
            .unwrap()
            .rows,
        before
    );
    let existing = backend
        .read(&[keys.dentry(1001, &exclusive.name)])
        .await
        .unwrap()
        .rows
        .remove(0)
        .unwrap();
    assert_eq!(
        backend
            .commit_with_inode_allocation(
                &authority,
                &create_plan(&exclusive, b"updated-parent", Some(&existing)),
                &exclusive,
            )
            .await,
        Err(EntityError::Exists)
    );
    assert_eq!(
        backend
            .read(std::slice::from_ref(&counter))
            .await
            .unwrap()
            .rows,
        before
    );

    // Existing tombstones can be replaced only through their exact predicate.
    let name = keys.dentry(1001, b"tombstone");
    let tombstone = codec::encode(&EntityDentry::Whiteout).unwrap();
    backend
        .commit(
            &authority,
            &EntityPlan {
                checks: vec![],
                writes: vec![put(name, &tombstone)],
            },
        )
        .await
        .unwrap();
    let recreated = create_request(&keys, 1001, b"tombstone");
    assert!(
        backend
            .commit_with_inode_allocation(
                &authority,
                &create_plan(&recreated, b"updated-parent", Some(&tombstone)),
                &recreated,
            )
            .await
            .unwrap()
            .is_some()
    );

    // Collisions and exhausted counters are discovered before either allocator
    // advancement or publication of parent/name/inode rows.
    let fresh = create_request(&keys, 1001, b"must-not-appear");
    let fresh_plan = create_plan(&fresh, b"updated-parent", None);
    for invalid in [b"01".as_slice(), b"-1", b"9223372036854775808"] {
        backend
            .commit(
                &authority,
                &EntityPlan {
                    checks: vec![],
                    writes: vec![put(counter.clone(), invalid)],
                },
            )
            .await
            .unwrap();
        assert!(matches!(
            backend
                .commit_with_inode_allocation(&authority, &fresh_plan, &fresh)
                .await,
            Err(EntityError::Corrupt(_))
        ));
        assert_eq!(
            backend
                .read(&[counter.clone(), keys.dentry(1001, &fresh.name)])
                .await
                .unwrap()
                .rows,
            vec![Some(invalid.to_vec()), None]
        );
    }
    for (value, occupied) in [
        (b"100".as_slice(), keys.inode(101)),
        (b"101".as_slice(), keys.xattr(102, b"user.identity")),
    ] {
        backend
            .commit(
                &authority,
                &EntityPlan {
                    checks: vec![],
                    writes: vec![put(counter.clone(), value), put(occupied, b"occupied")],
                },
            )
            .await
            .unwrap();
        assert!(matches!(
            backend
                .commit_with_inode_allocation(&authority, &fresh_plan, &fresh)
                .await,
            Err(EntityError::Corrupt(_))
        ));
        assert_eq!(
            backend
                .read(&[counter.clone(), keys.dentry(1001, &fresh.name)])
                .await
                .unwrap()
                .rows,
            vec![Some(value.to_vec()), None]
        );
    }
    backend
        .commit(
            &authority,
            &EntityPlan {
                checks: vec![],
                writes: vec![put(counter.clone(), b"9223372036854775806")],
            },
        )
        .await
        .unwrap();
    let last = create_request(&keys, 1001, b"last-inode");
    assert_eq!(
        backend
            .commit_with_inode_allocation(
                &authority,
                &create_plan(&last, b"updated-parent", None),
                &last
            )
            .await
            .unwrap(),
        Some(i64::MAX)
    );
    assert_eq!(
        backend
            .commit_with_inode_allocation(&authority, &fresh_plan, &fresh)
            .await,
        Err(EntityError::Exhausted)
    );
    assert_eq!(
        backend
            .read(&[counter.clone(), keys.dentry(1001, &fresh.name)])
            .await
            .unwrap()
            .rows,
        vec![Some(b"9223372036854775807".to_vec()), None]
    );
    let now = backend.read(&[]).await.unwrap().now_ns;
    let expired =
        TransactionAuthority::from_validated(authority.checks.clone(), Some(now)).unwrap();
    assert_eq!(
        backend
            .commit_with_inode_allocation(&expired, &fresh_plan, &fresh)
            .await,
        Err(EntityError::Fenced)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_atomic_create_allocation_conformance() {
    atomic_create_conformance(
        Arc::new(MemoryEntityBackend::new(100)),
        EntityKeys::new(Uuid::new_v4(), Uuid::new_v4()),
    )
    .await;
}

async fn slice_reservation_conformance<B: EntityBackend + 'static>(
    backend: Arc<B>,
    keys: EntityKeys,
) {
    let authority = initialize(backend.as_ref(), &keys).await;
    let allocation = SliceAllocation {
        keys: keys.clone(),
        sizes: vec![8, 16, 32],
        upload_id: Uuid::new_v4(),
        lease: LeaseIdentity {
            lease_id: Uuid::new_v4(),
            holder_generation: 1,
        },
    };
    let mut foreign = allocation.clone();
    foreign.keys = EntityKeys::new(Uuid::new_v4(), Uuid::new_v4());
    assert!(matches!(
        backend.reserve_slices(&authority, &foreign).await,
        Err(EntityError::Invalid(_))
    ));
    assert_eq!(
        backend
            .commit(
                &authority,
                &EntityPlan {
                    checks: vec![],
                    writes: vec![put(keys.counter("slice"), b"9007199254740991")],
                }
            )
            .await
            .unwrap(),
        CommitOutcome::Committed,
    );
    let ids = backend
        .reserve_slices(&authority, &allocation)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        ids,
        vec![9007199254740992, 9007199254740993, 9007199254740994]
    );
    let rows = backend
        .read(&ids.iter().map(|id| keys.slice(*id)).collect::<Vec<_>>())
        .await
        .unwrap();
    for (position, (id, bytes)) in ids.iter().zip(rows.rows).enumerate() {
        let expected = allocation.pending(*id, position);
        assert_eq!(
            bytes,
            Some(codec::encode(&expected).unwrap()),
            "exact decimal identity in Pending row"
        );
        assert_eq!(
            backend.scan_prefix(&keys.slice(*id), 1).await.unwrap(),
            vec![(keys.slice(*id), bytes.unwrap())],
            "allocator-generated pending rows must be indexed"
        );
    }

    let mut reservations = tokio::task::JoinSet::new();
    for _ in 0..16 {
        let backend = backend.clone();
        let authority = authority.clone();
        let mut allocation = allocation.clone();
        allocation.upload_id = Uuid::new_v4();
        reservations.spawn(async move {
            backend
                .reserve_slices(&authority, &allocation)
                .await
                .unwrap()
                .unwrap()
        });
    }
    let mut concurrent_ids = BTreeSet::new();
    while let Some(batch) = reservations.join_next().await {
        let batch = batch.unwrap();
        assert!(batch.windows(2).all(|pair| pair[0] + 1 == pair[1]));
        for id in batch {
            assert!(concurrent_ids.insert(id));
        }
    }
    assert_eq!(
        concurrent_ids.len(),
        48,
        "no allocator CAS conflict or reused ID"
    );

    let checked_keys = vec![
        keys.counter("slice"),
        keys.slice(101),
        keys.slice(102),
        keys.slice(103),
        keys.object_descriptor(102),
    ];
    for collision in [keys.slice(102), keys.object_descriptor(102)] {
        assert_eq!(
            backend
                .commit(
                    &authority,
                    &EntityPlan {
                        checks: vec![],
                        writes: vec![
                            put(keys.counter("slice"), b"100"),
                            put(collision.clone(), b"occupied")
                        ],
                    }
                )
                .await
                .unwrap(),
            CommitOutcome::Committed
        );
        let before = backend.read(&checked_keys).await.unwrap().rows;
        assert!(matches!(
            backend.reserve_slices(&authority, &allocation).await,
            Err(EntityError::Corrupt(_))
        ));
        assert_eq!(
            backend.read(&checked_keys).await.unwrap().rows,
            before,
            "a later collision must not allocate or register any prefix"
        );
        assert_eq!(
            backend
                .commit(
                    &authority,
                    &EntityPlan {
                        checks: vec![],
                        writes: vec![RowWrite::Delete { key: collision }],
                    }
                )
                .await
                .unwrap(),
            CommitOutcome::Committed
        );
    }

    let counter = keys.counter("slice");
    let final_id = keys.slice(i64::MAX as u64);
    assert_eq!(
        backend
            .commit(
                &authority,
                &EntityPlan {
                    checks: vec![],
                    writes: vec![put(counter.clone(), b"9223372036854775806")],
                }
            )
            .await
            .unwrap(),
        CommitOutcome::Committed
    );
    let checked_keys = vec![counter.clone(), final_id];
    let before = backend.read(&checked_keys).await.unwrap().rows;
    assert_eq!(
        backend.reserve_slices(&authority, &allocation).await,
        Err(EntityError::Exhausted)
    );
    assert_eq!(
        backend.read(&checked_keys).await.unwrap().rows,
        before,
        "a later overflow must not allocate the valid first identity"
    );

    let conflict =
        TransactionAuthority::from_validated(vec![check(keys.lease(), Some(b"wrong-lease"))], None)
            .unwrap();
    assert_eq!(
        backend
            .reserve_slices(&conflict, &allocation)
            .await
            .unwrap(),
        None
    );
    let now = backend.read(&[]).await.unwrap().now_ns;
    let expired =
        TransactionAuthority::from_validated(authority.checks.clone(), Some(now)).unwrap();
    assert_eq!(
        backend.reserve_slices(&expired, &allocation).await,
        Err(EntityError::Fenced)
    );
    assert_eq!(backend.read(&checked_keys).await.unwrap().rows, before);
    for invalid in [b"01".as_slice(), b"-1", b"9223372036854775808"] {
        backend
            .commit(
                &authority,
                &EntityPlan {
                    checks: vec![],
                    writes: vec![put(counter.clone(), invalid)],
                },
            )
            .await
            .unwrap();
        assert!(matches!(
            backend.reserve_slices(&authority, &allocation).await,
            Err(EntityError::Corrupt(_))
        ));
        assert_eq!(
            backend.read(&checked_keys).await.unwrap().rows,
            vec![Some(invalid.to_vec()), None]
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_atomic_slice_reservation_conformance() {
    slice_reservation_conformance(
        Arc::new(MemoryEntityBackend::new(10)),
        EntityKeys::new(Uuid::new_v4(), Uuid::new_v4()),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires BREWFS_TEST_REDIS_URL pointing to an isolated local Redis"]
async fn redis_atomic_slice_reservation_conformance() {
    let url = std::env::var("BREWFS_TEST_REDIS_URL").expect("BREWFS_TEST_REDIS_URL");
    let volume = Uuid::new_v4();
    slice_reservation_conformance(
        Arc::new(RedisEntityBackend::connect(&url, volume).await.unwrap()),
        EntityKeys::new(volume, Uuid::new_v4()),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires BREWFS_TEST_REDIS_URL pointing to an isolated local Redis"]
async fn redis_atomic_create_allocation_conformance() {
    let url = std::env::var("BREWFS_TEST_REDIS_URL").expect("BREWFS_TEST_REDIS_URL");
    let volume = Uuid::new_v4();
    atomic_create_conformance(
        Arc::new(RedisEntityBackend::connect(&url, volume).await.unwrap()),
        EntityKeys::new(volume, Uuid::new_v4()),
    )
    .await;
}

#[tokio::test]
async fn memory_authority_uses_backend_deadline_and_rejects_plan_before_writes() {
    let backend = MemoryEntityBackend::new(10);
    let keys = EntityKeys::new(Uuid::new_v4(), Uuid::new_v4());
    let authority = initialize(&backend, &keys).await;
    let bounded = TransactionAuthority::from_validated(authority.checks.clone(), Some(30)).unwrap();
    assert_eq!(
        backend
            .commit(&bounded, &EntityPlan::default())
            .await
            .unwrap(),
        CommitOutcome::Committed
    );
    backend.set_time(29).unwrap();
    assert_eq!(
        backend
            .commit(&bounded, &EntityPlan::default())
            .await
            .unwrap(),
        CommitOutcome::Committed
    );
    backend.set_time(30).unwrap();
    assert_eq!(
        backend.commit(&bounded, &EntityPlan::default()).await,
        Err(EntityError::Fenced)
    );
    let row = keys.inode(2);
    let duplicate = EntityPlan {
        checks: vec![],
        writes: vec![put(row.clone(), b"first"), put(row.clone(), b"last")],
    };
    assert!(matches!(
        backend.commit(&authority, &duplicate).await,
        Err(EntityError::Invalid(_))
    ));
    assert_eq!(backend.read(&[row]).await.unwrap().rows, vec![None]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires BREWFS_TEST_REDIS_URL pointing to an isolated local Redis"]
async fn redis_entity_backend_conformance() {
    let url = std::env::var("BREWFS_TEST_REDIS_URL").expect("BREWFS_TEST_REDIS_URL");
    let volume = Uuid::new_v4();
    let backend = Arc::new(RedisEntityBackend::connect(&url, volume).await.unwrap());
    conformance(backend, EntityKeys::new(volume, Uuid::new_v4())).await;
}

#[tokio::test]
#[ignore = "requires BREWFS_TEST_REDIS_URL pointing to an isolated local Redis"]
async fn redis_entity_prefix_scan_does_not_traverse_unrelated_keys() {
    let url = std::env::var("BREWFS_TEST_REDIS_URL").expect("BREWFS_TEST_REDIS_URL");
    let volume = Uuid::new_v4();
    let keys = EntityKeys::new(volume, Uuid::new_v4());
    let client = redis::Client::open(url.as_str()).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    // Rows predating the index must remain visible after opening the backend.
    let legacy = keys.inode(42);
    redis::cmd("SET")
        .arg(&legacy)
        .arg("legacy")
        .query_async::<()>(&mut connection)
        .await
        .unwrap();
    let backend = RedisEntityBackend::connect(&url, volume).await.unwrap();
    let authority = initialize(&backend, &keys).await;
    let expected: Vec<_> = (0..10)
        .map(|i| (keys.dentry(1, format!("entry-{i}").as_bytes()), vec![b'x']))
        .collect();
    assert_eq!(
        backend
            .commit(
                &authority,
                &EntityPlan {
                    checks: vec![],
                    writes: expected
                        .iter()
                        .map(|(key, value)| put(key.clone(), value))
                        .collect(),
                },
            )
            .await
            .unwrap(),
        CommitOutcome::Committed
    );
    let user = format!("entity-no-scan-{volume}");
    let password = Uuid::new_v4().to_string();
    let mut restricted_url = url::Url::parse(&url).unwrap();
    restricted_url.set_username(&user).unwrap();
    restricted_url.set_password(Some(&password)).unwrap();
    redis::cmd("ACL")
        .arg("SETUSER")
        .arg(&user)
        .arg("reset")
        .arg("on")
        .arg(format!(">{password}"))
        .arg(format!(
            "~{}*",
            String::from_utf8_lossy(keys.volume_prefix())
        ))
        .arg("+@all")
        .arg("-scan")
        .query_async::<()>(&mut connection)
        .await
        .unwrap();
    let result = async {
        let restricted = RedisEntityBackend::connect(restricted_url.as_str(), volume).await?;
        assert_eq!(
            restricted.scan_prefix(&legacy, 1).await?,
            vec![(legacy, b"legacy".to_vec())]
        );
        // Reject traversal deterministically, without a wall-clock threshold.
        // Grow the same database while leaving the ten-entry directory fixed.
        for range in [0..1_000, 1_000..100_000] {
            for start in range.step_by(1_000) {
                let mut pipeline = redis::pipe();
                for i in start..start + 1_000 {
                    pipeline
                        .cmd("SET")
                        .arg(format!("unrelated-{volume}/{i}"))
                        .arg("x")
                        .ignore();
                }
                pipeline.query_async::<()>(&mut connection).await.unwrap();
            }
            assert_eq!(
                restricted.scan_prefix(&keys.dentry_prefix(1), 10).await?,
                expected
            );
            assert!(
                restricted
                    .scan_prefix(&keys.dentry_prefix(2), 10)
                    .await?
                    .is_empty()
            );
            assert!(matches!(
                restricted.scan_prefix(&keys.dentry_prefix(1), 9).await,
                Err(EntityError::Invalid(_))
            ));
        }
        let deletion = EntityPlan {
            checks: vec![],
            writes: vec![RowWrite::Delete {
                key: expected[0].0.clone(),
            }],
        };
        assert_eq!(
            restricted.commit(&authority, &deletion).await?,
            CommitOutcome::Committed
        );
        assert_eq!(
            restricted.scan_prefix(&keys.dentry_prefix(1), 10).await?,
            expected[1..]
        );
        Ok::<_, EntityError>(())
    }
    .await;
    redis::cmd("ACL")
        .arg("DELUSER")
        .arg(&user)
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    for start in (0..100_000).step_by(1_000) {
        let noise: Vec<_> = (start..start + 1_000)
            .map(|i| format!("unrelated-{volume}/{i}"))
            .collect();
        redis::cmd("DEL")
            .arg(noise)
            .query_async::<usize>(&mut connection)
            .await
            .unwrap();
    }
    result.unwrap();
}

#[tokio::test]
#[ignore = "requires BREWFS_TEST_REDIS_URL pointing to an isolated local Redis"]
async fn redis_entity_rejects_wrong_type_and_oversize_before_any_write_or_decode() {
    let url = std::env::var("BREWFS_TEST_REDIS_URL").expect("BREWFS_TEST_REDIS_URL");
    let volume = Uuid::new_v4();
    let keys = EntityKeys::new(volume, Uuid::new_v4());
    let backend = RedisEntityBackend::connect(&url, volume).await.unwrap();
    let authority = initialize(&backend, &keys).await;
    let client = redis::Client::open(url.as_str()).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    let first = keys.inode(10);
    let wrong_type = keys.inode(11);
    redis::cmd("LPUSH")
        .arg(&wrong_type)
        .arg("bad")
        .query_async::<()>(&mut connection)
        .await
        .unwrap();
    assert!(matches!(
        backend
            .commit(
                &authority,
                &EntityPlan {
                    checks: vec![],
                    writes: vec![
                        put(first.clone(), b"must-not-appear"),
                        put(wrong_type.clone(), b"overwrite")
                    ]
                }
            )
            .await,
        Err(EntityError::Corrupt(_))
    ));
    assert_eq!(
        backend
            .read(std::slice::from_ref(&first))
            .await
            .unwrap()
            .rows,
        vec![None]
    );
    assert!(matches!(
        backend.read(&[wrong_type]).await,
        Err(EntityError::Corrupt(_))
    ));

    let oversized = keys.inode(12);
    // Corrupt an indexed row, rather than bypassing the backend to create one.
    backend
        .commit(
            &authority,
            &EntityPlan {
                checks: vec![],
                writes: vec![put(oversized.clone(), b"valid")],
            },
        )
        .await
        .unwrap();
    redis::cmd("SET")
        .arg(&oversized)
        .arg(vec![b'x'; MAX_ROW_BYTES + 1])
        .query_async::<()>(&mut connection)
        .await
        .unwrap();
    assert!(matches!(
        backend.read(std::slice::from_ref(&oversized)).await,
        Err(EntityError::Invalid(_))
    ));
    assert!(matches!(
        backend.scan_prefix(&oversized, 1).await,
        Err(EntityError::Invalid(_))
    ));
    assert!(matches!(
        backend
            .commit(
                &authority,
                &EntityPlan {
                    checks: vec![],
                    writes: vec![
                        put(first.clone(), b"must-not-appear"),
                        RowWrite::Delete { key: oversized }
                    ]
                }
            )
            .await,
        Err(EntityError::Invalid(_))
    ));
    assert_eq!(backend.read(&[first]).await.unwrap().rows, vec![None]);
    assert!(matches!(
        backend
            .read(&[EntityKeys::new(Uuid::new_v4(), Uuid::new_v4()).inode(1)])
            .await,
        Err(EntityError::Invalid(_))
    ));

    // A permission error from STRLEN is not evidence of a corrupt row.
    // Keep the default user intact; this test runs in an owned Redis instance.
    let user = format!("entity-strlen-{}", Uuid::new_v4());
    let password = Uuid::new_v4().to_string();
    let mut restricted_url = url::Url::parse(&url).unwrap();
    restricted_url.set_username(&user).unwrap();
    restricted_url.set_password(Some(&password)).unwrap();
    let observed = [keys.descriptor(), keys.inode(13)];
    let before = backend.read(&observed).await.unwrap().rows;
    assert_eq!(before, vec![Some(b"active".to_vec()), None]);
    redis::cmd("ACL")
        .arg("SETUSER")
        .arg(&user)
        .arg("reset")
        .arg("on")
        .arg(format!(">{password}"))
        .arg(format!(
            "~{}*",
            String::from_utf8_lossy(keys.volume_prefix())
        ))
        .arg("+@all")
        .arg("-strlen")
        .query_async::<()>(&mut connection)
        .await
        .unwrap();
    let outcomes = async {
        let restricted = RedisEntityBackend::connect(restricted_url.as_str(), volume).await?;
        let read = restricted.read(&observed[..1]).await;
        let commit = restricted
            .commit(
                &authority,
                &EntityPlan {
                    checks: vec![],
                    writes: vec![put(observed[1].clone(), b"must-not-appear")],
                },
            )
            .await;
        Ok::<_, EntityError>((read, commit))
    }
    .await;
    // Clean up before asserting either result, including failed authentication.
    let deleted = redis::cmd("ACL")
        .arg("DELUSER")
        .arg(&user)
        .query_async::<usize>(&mut connection)
        .await;
    let after = backend.read(&observed).await;
    assert_eq!(deleted.unwrap(), 1);
    let (read, commit) = outcomes.unwrap();
    for outcome in [read.map(|_| ()), commit.map(|_| ())] {
        assert!(
            matches!(&outcome, Err(EntityError::Backend(message))
                if (message.contains("strlen")
                    && (message.contains("ACL") || message.contains("NOPERM")))
                    || message.contains("can't run this command or subcommand")),
            "{outcome:?}"
        );
    }
    assert_eq!(after.unwrap().rows, before);

    // Lua errors do not roll back earlier writes: validate the index first.
    redis::cmd("SET")
        .arg(keys.key_index())
        .arg("wrong-type")
        .query_async::<()>(&mut connection)
        .await
        .unwrap();
    assert!(matches!(
        backend
            .commit(
                &authority,
                &EntityPlan {
                    checks: vec![],
                    writes: vec![put(observed[1].clone(), b"must-not-appear")],
                },
            )
            .await,
        Err(EntityError::Corrupt(_))
    ));
    assert_eq!(backend.read(&observed).await.unwrap().rows, before);
    assert!(matches!(
        backend.scan_prefix(keys.prefix(), 4096).await,
        Err(EntityError::Corrupt(_))
    ));
}
