//! Deterministic cold-inode group commits through the actual packed KV store.

use super::*;
use std::sync::atomic::AtomicBool;

use crate::chunk::SliceDesc;
use crate::meta::store::MetaError;
use crate::workspace_overlay::lifecycle::LeaseValidity;
use crate::workspace_overlay::meta_layer::{MAX_GROUP_WRITES, PendingWrite};

pub(super) struct Control {
    pub(super) track: AtomicBool,
    pub(super) reads: AtomicUsize,
    pub(super) fail_pair_read_once: AtomicBool,
    pub(super) pause_namespace_commit: AtomicBool,
    active: AtomicUsize,
    peak: AtomicUsize,
    commits: AtomicUsize,
    pause_commit: AtomicBool,
    pub(super) reached: Semaphore,
    pub(super) resume: Semaphore,
}

impl Control {
    pub(super) fn new() -> Self {
        Self {
            track: AtomicBool::new(false),
            reads: AtomicUsize::new(0),
            fail_pair_read_once: AtomicBool::new(false),
            pause_namespace_commit: AtomicBool::new(false),
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            commits: AtomicUsize::new(0),
            pause_commit: AtomicBool::new(false),
            reached: Semaphore::new(0),
            resume: Semaphore::new(0),
        }
    }

    pub(super) async fn read_snapshot<B: WorkspaceKvBackend>(
        &self,
        backend: &B,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>, WorkspaceError> {
        // Two layer keys plus eight row keys per inode. Exclude commit-view
        // reads so the assertion measures only permission snapshots.
        let tracked = self.track.load(Ordering::SeqCst)
            && matches!(keys.len(), 10 | 18)
            && keys[0].starts_with(b"layer/")
            && keys[1].starts_with(b"layer/");
        if tracked {
            self.reads.fetch_add(1, Ordering::SeqCst);
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(active, Ordering::SeqCst);
            // A deterministic pending backend read, without timing thresholds.
            tokio::task::yield_now().await;
        }
        let result = if keys.len() == 18 && self.fail_pair_read_once.swap(false, Ordering::SeqCst) {
            Err(WorkspaceError::Backend(
                "injected speculative read failure".into(),
            ))
        } else {
            backend.get_many_consistent(keys).await
        };
        if tracked {
            self.active.fetch_sub(1, Ordering::SeqCst);
        }
        result
    }

    pub(super) async fn before_commit(&self, writes: &[KvWrite]) {
        if writes.iter().any(
            |write| matches!(write, KvWrite::Put { key, .. } if key.starts_with(b"delta/dentry/")),
        ) && self.pause_namespace_commit.swap(false, Ordering::SeqCst)
        {
            self.reached.add_permits(1);
            self.resume.acquire().await.unwrap().forget();
        }
        if self.track.load(Ordering::SeqCst)
            && writes.iter().any(|write| {
                matches!(write, KvWrite::Put { key, .. }
                    if key.starts_with(b"delta/extent/"))
            })
        {
            self.commits.fetch_add(1, Ordering::SeqCst);
            if self.pause_commit.swap(false, Ordering::SeqCst) {
                self.reached.add_permits(1);
                self.resume.acquire().await.unwrap().forget();
            }
        }
    }
}

// Preload a whole drained batch, independent of executor scheduling. These
// are the same valid PendingWrites that MetaLayer::write would enqueue.
fn enqueue<B: WorkspaceKvBackend>(
    f: &Fixture<B>,
    inodes: &[i64],
) -> Vec<tokio::sync::oneshot::Receiver<Result<(), MetaError>>> {
    let mut queue = f.meta.write_queue.lock().unwrap();
    assert!(queue.pending.is_empty());
    inodes
        .iter()
        .enumerate()
        .map(|(index, &ino)| {
            let chunk_id = crate::vfs::chunk_id_for(ino, 0).unwrap();
            let (reply, receiver) = tokio::sync::oneshot::channel();
            queue.pending.push(PendingWrite {
                ino,
                chunk_id,
                slice: SliceDesc {
                    slice_id: 50_000 + index as u64,
                    chunk_id,
                    offset: 0,
                    length: 64,
                },
                new_size: 64,
                reply,
            });
            receiver
        })
        .collect()
}

#[tokio::test]
async fn group_commit_leader_returns_before_later_commit_finishes() {
    let f = fixture_with_cache(64 << 20).await;
    let first_ino = f.meta.create_file(1, "first".into()).await.unwrap();
    let second_ino = f.meta.create_file(1, "second".into()).await.unwrap();
    let write = |ino, slice_id| {
        let chunk_id = crate::vfs::chunk_id_for(ino, 0).unwrap();
        f.meta.write(
            ino,
            chunk_id,
            SliceDesc {
                slice_id,
                chunk_id,
                offset: 0,
                length: 64,
            },
            64,
        )
    };
    let control = &f.control.group_commit;
    control.track.store(true, Ordering::SeqCst);
    control.pause_commit.store(true, Ordering::SeqCst);
    let mut first = write(first_ino, 50_000);
    let mut second = write(second_ino, 50_001);

    tokio::time::timeout(Duration::from_secs(10), async {
        tokio::select! {
            result = &mut first => panic!("first commit must pause: {result:?}"),
            permit = control.reached.acquire() => permit.unwrap().forget(),
        }
        // Enqueue B while A owns the first backend commit.
        assert!(futures::poll!(&mut second).is_pending());
        assert_eq!(f.meta.write_queue.lock().unwrap().pending.len(), 1);
        control.pause_commit.store(true, Ordering::SeqCst);
        control.resume.add_permits(1);

        // B's backend commit stays blocked. A must return independently of
        // it; the timeout bounds failure, while barriers enforce the schedule.
        let (first_result, ()) = tokio::join!(
            tokio::time::timeout(Duration::from_secs(2), &mut first),
            async {
                tokio::select! {
                    result = &mut second => panic!("second commit must pause: {result:?}"),
                    permit = control.reached.acquire() => permit.unwrap().forget(),
                }
            }
        );
        let returned_before_second = first_result.is_ok();
        control.resume.add_permits(1);
        match first_result {
            Ok(result) => result.unwrap(),
            Err(_) => first.await.unwrap(),
        }
        second.await.unwrap();
        assert!(
            returned_before_second,
            "committed leader must return while the later backend commit is blocked"
        );
    })
    .await
    .expect("group commit handoff must finish");

    assert_eq!(control.commits.load(Ordering::SeqCst), 2);
    for ino in [first_ino, second_ino] {
        assert_eq!(f.meta.stat(ino).await.unwrap().unwrap().size, 64);
    }
    shutdown(&f).await;
}

#[tokio::test]
async fn group_commit_handoff_preserves_batch_limit_and_order_with_cancelled_waiters() {
    use crate::workspace_overlay::catalog::ExtentQuery;
    use crate::workspace_overlay::model::ExtentKind;
    use crate::workspace_overlay::resolver::{ResolvedExtent, resolve_extents};

    let f = fixture_with_cache(64 << 20).await;
    let ino = f.meta.create_file(1, "overlapping".into()).await.unwrap();
    let chunk_id = crate::vfs::chunk_id_for(ino, 0).unwrap();
    let control = &f.control.group_commit;
    control.track.store(true, Ordering::SeqCst);
    let guard = f.meta.mutation_gate.lock().await;
    let count = 2 * MAX_GROUP_WRITES + 1;
    let mut writes = (0..count)
        .map(|index| {
            f.meta.write(
                ino,
                chunk_id,
                SliceDesc {
                    slice_id: 50_000 + index as u64,
                    chunk_id,
                    offset: 0,
                    length: 64,
                },
                64,
            )
        })
        .collect::<Vec<_>>();
    for write in &mut writes {
        assert!(futures::poll!(write).is_pending());
    }
    // Cancel the first waiter for each of the first two batches. Their queued
    // writes still commit, and live waiters must inherit the mutation gate.
    drop(writes.remove(MAX_GROUP_WRITES));
    drop(writes.remove(0));
    drop(guard);
    tokio::time::timeout(
        Duration::from_secs(10),
        futures::future::try_join_all(writes),
    )
    .await
    .expect("cancelled gate waiters must not strand later writes")
    .unwrap();

    assert_eq!(control.commits.load(Ordering::SeqCst), 3);
    assert!(f.meta.write_queue.lock().unwrap().pending.is_empty());
    let inode = f.meta.resolve_inode_delta(ino).await.unwrap().unwrap();
    assert_eq!((inode.size, inode.data_version), (64, count as u64 + 1));
    let chain = f.meta.chain().await.unwrap();
    let extents = f
        .meta
        .store
        .get_extent_deltas(ExtentQuery {
            layer_ids: chain.iter().map(|layer| layer.layer_id).collect(),
            ino,
            chunk_index: 0,
            range_start: 0,
            range_end: 64,
        })
        .await
        .unwrap();
    assert_eq!(extents.len(), 1, "only the final overwrite remains active");
    let owned = f
        .meta
        .store
        .gc_snapshot(0, 0)
        .await
        .unwrap()
        .slice_references
        .into_iter()
        .filter(|reference| reference.layer_id == chain[0].layer_id)
        .map(|reference| (reference.slice_id, reference.slice_end))
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(
        owned,
        (0..count)
            .map(|index| (50_000 + index as u64, 64))
            .collect(),
        "every queued write, including cancelled waiters, must retain GC ownership"
    );
    assert_eq!(
        resolve_extents(&chain, &extents, ino, 0, 0..64).unwrap(),
        vec![ResolvedExtent {
            logical_offset: 0,
            length: 64,
            kind: ExtentKind::Data {
                slice_id: 50_000 + count as u64 - 1,
                slice_offset: 0,
            },
        }]
    );
    shutdown(&f).await;
}

#[tokio::test]
async fn group_commit_prefetches_cold_inodes_with_bounded_concurrency() {
    for cached in [true, false] {
        let mut f = fixture_with_cache(64 << 20).await;
        if cached {
            f.meta = f
                .meta
                .with_upper_row_cache(Duration::from_secs(60))
                .with_lease_bound_row_cache(Arc::new(LeaseValidity::new(
                    std::time::Instant::now(),
                    Duration::from_secs(30),
                )));
        }
        let mut inodes = Vec::new();
        for index in 0..MAX_GROUP_WRITES {
            let ino = f
                .meta
                .create_file(1, format!("cold-{index}"))
                .await
                .unwrap();
            // Warm immutable-lower memos, then drop upper permission rows.
            f.meta.cached_permission_snapshot(ino).await.unwrap();
            inodes.push(ino);
        }
        f.meta.invalidate_upper_rows();
        let control = &f.control.group_commit;
        control.track.store(true, Ordering::SeqCst);
        // A successful data commit invalidates permission rows. The second
        // batch must be just as concurrent as the initially cold batch.
        for round in 1..=2 {
            let replies = enqueue(&f, &inodes);
            f.meta
                .commit_next_write_batch(f.meta.mutation_gate.lock().await)
                .await;
            for reply in replies {
                reply.await.unwrap().unwrap();
            }
            assert_eq!(control.reads.load(Ordering::SeqCst), round * inodes.len());
            assert_eq!(control.commits.load(Ordering::SeqCst), round);
            let peak = control.peak.swap(0, Ordering::SeqCst);
            assert!(
                (2..=8).contains(&peak),
                "permission read concurrency: {peak}"
            );
        }
        control.track.store(false, Ordering::SeqCst);
        for ino in inodes {
            assert_eq!(f.meta.stat(ino).await.unwrap().unwrap().size, 64);
            let inode = f.meta.resolve_inode_delta(ino).await.unwrap().unwrap();
            assert_eq!(inode.data_version, 3);
        }
        shutdown(&f).await;
    }
}

#[tokio::test]
async fn group_commit_deduplicates_inodes_and_preserves_member_errors() {
    let f = fixture_with_cache(64 << 20).await;
    let ino = f.meta.create_file(1, "valid".into()).await.unwrap();
    let missing = 9_999_999;
    let control = &f.control.group_commit;
    control.track.store(true, Ordering::SeqCst);
    let replies = enqueue(&f, &[ino, missing, 1, ino, missing]);
    f.meta
        .commit_next_write_batch(f.meta.mutation_gate.lock().await)
        .await;
    for (index, reply) in replies.into_iter().enumerate() {
        let result = reply.await.unwrap();
        match index {
            0 | 3 => result.unwrap(),
            1 | 4 => {
                assert!(matches!(result, Err(MetaError::NotFound(value)) if value == missing));
            }
            2 => assert!(matches!(result, Err(MetaError::NotSupported(_)))),
            _ => unreachable!(),
        }
    }
    assert_eq!(control.reads.load(Ordering::SeqCst), 3);
    assert_eq!(control.commits.load(Ordering::SeqCst), 1);
    control.track.store(false, Ordering::SeqCst);
    let inode = f.meta.resolve_inode_delta(ino).await.unwrap().unwrap();
    assert_eq!((inode.size, inode.data_version), (64, 3));
    shutdown(&f).await;
}

#[tokio::test]
async fn group_commit_refreshes_prefetched_inodes_after_head_conflict() {
    let f = fixture_with_cache(64 << 20).await;
    let ino = f.meta.create_file(1, "conflict".into()).await.unwrap();
    let control = &f.control.group_commit;
    control.track.store(true, Ordering::SeqCst);
    control.pause_commit.store(true, Ordering::SeqCst);
    let replies = enqueue(&f, &[ino]);
    let conflict = async {
        control.reached.acquire().await.unwrap().forget();
        // Another writer advances the head after our snapshot and before
        // our CAS. The retry must preserve its larger size and new mode.
        let snapshot = f.meta.permission_snapshot(vec![ino], None).await.unwrap();
        let mut inode = crate::workspace_overlay::resolver::resolve_inode(
            &snapshot.layers,
            &snapshot.inodes,
            ino,
        )
        .unwrap()
        .unwrap()
        .inode;
        inode.size = 1024;
        inode.mode = 0o100600;
        inode.data_version = 7;
        let mut mutation = VersionedMutation::empty(f.meta.guard().await, snapshot.layers, 4096);
        mutation.inodes.push(inode);
        f.meta.commit_versioned_mutation(mutation).await.unwrap();
        control.resume.add_permits(1);
    };
    tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(
            async {
                f.meta
                    .commit_next_write_batch(f.meta.mutation_gate.lock().await)
                    .await;
            },
            conflict
        );
    })
    .await
    .expect("conflicting group commit must finish");
    for reply in replies {
        reply.await.unwrap().unwrap();
    }
    assert!(control.commits.load(Ordering::SeqCst) >= 2);
    control.track.store(false, Ordering::SeqCst);
    let inode = f.meta.resolve_inode_delta(ino).await.unwrap().unwrap();
    assert_eq!(
        (inode.size, inode.mode, inode.data_version),
        (1024, 0o100600, 8)
    );
    shutdown(&f).await;
}
