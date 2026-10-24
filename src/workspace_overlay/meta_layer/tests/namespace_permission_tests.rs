//! Count actual KV permission snapshots through actor-authenticated mutations.

use super::*;
use crate::meta::layer::{NamespaceActor, scope_namespace_actor};
use crate::meta::store::{FileType, MetaError, SetAttrFlags, SetAttrRequest};
use crate::workspace_overlay::lifecycle::LeaseValidity;

fn actor() -> NamespaceActor {
    NamespaceActor {
        uid: 1234,
        gid: 1234,
        groups: vec![1234],
    }
}

async fn directory<B: WorkspaceKvBackend>(f: &Fixture<B>, parent: i64, name: &str) -> i64 {
    f.meta
        .create_node_with_umask(parent, name.into(), FileType::Dir, 0o777, 0, 1000, 1000, 0)
        .await
        .unwrap()
        .ino
}

async fn directories<B: WorkspaceKvBackend>(f: &Fixture<B>) -> Vec<i64> {
    let mut parent = f.meta.root_ino();
    let mut inodes = Vec::new();
    for depth in 1..=8 {
        parent = directory(f, parent, &format!("dir-{depth}")).await;
        inodes.push(parent);
    }
    inodes
}

async fn chmod<B: WorkspaceKvBackend>(f: &Fixture<B>, ino: i64, mode: u32) {
    f.meta
        .set_attr(
            ino,
            &SetAttrRequest {
                mode: Some(mode),
                ..Default::default()
            },
            SetAttrFlags::empty(),
        )
        .await
        .unwrap();
}

fn denied(error: MetaError) {
    assert!(matches!(error, MetaError::Io(e) if e.raw_os_error() == Some(libc::EACCES)));
}

async fn cold_create_counts<B: WorkspaceKvBackend>(f: &Fixture<B>) -> Vec<(usize, usize)> {
    let mut parent = f.meta.root_ino();
    let mut counts = Vec::new();
    for depth in 1..=8 {
        parent = f
            .meta
            .create_node_with_umask(
                parent,
                format!("depth-{depth}"),
                FileType::Dir,
                0o777,
                0,
                1000,
                1000,
                0,
            )
            .await
            .unwrap()
            .ino;
        if ![1, 4, 8].contains(&depth) {
            continue;
        }
        // The directories have been observed, but every permission row is cold.
        f.meta.invalidate_upper_rows();
        let control = &f.control.group_commit;
        control.reads.store(0, Ordering::SeqCst);
        control.track.store(true, Ordering::SeqCst);
        scope_namespace_actor(Some(actor()), f.meta.create_file(parent, "probe".into()))
            .await
            .unwrap();
        control.track.store(false, Ordering::SeqCst);
        let reads = control.reads.load(Ordering::SeqCst);
        println!("depth={depth}, cold create permission snapshots={reads}");
        counts.push((depth, reads));
    }
    counts
}

#[tokio::test]
async fn cold_namespace_permissions_batch_observed_ancestors() {
    for cached in [false, true] {
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
        let counts = cold_create_counts(&f).await;
        shutdown(&f).await;
        // The writeback template needs one fresh parent snapshot. Each remaining
        // snapshot can hold two ancestors, without a separate discovery read.
        assert_eq!(
            counts,
            vec![(1, 2), (4, 3), (8, 5)],
            "row cache enabled: {cached}"
        );
    }
}

#[tokio::test]
#[ignore = "requires BREWFS_TEST_REDIS_URL"]
async fn real_redis_cold_namespace_permissions_batch_observed_ancestors() {
    let backend = crate::workspace_overlay::stores::redis::RedisWorkspaceBackend::connect(
        &std::env::var("BREWFS_TEST_REDIS_URL").unwrap(),
        &format!("issue-201-{}", uuid::Uuid::new_v4().simple()),
    )
    .await
    .unwrap();
    let f = fixture_with_backend(64 << 20, Arc::new(backend)).await;
    let counts = cold_create_counts(&f).await;
    shutdown(&f).await;
    assert_eq!(counts, vec![(1, 2), (4, 3), (8, 5)]);
}

#[tokio::test]
async fn cold_namespace_permissions_without_topology_batch_the_known_root() {
    let f = fixture_with_cache(64 << 20).await;
    let inodes = directories(&f).await;
    f.meta.invalidate_upper_rows();
    f.meta.directory_hints.invalidate_all();
    let control = &f.control.group_commit;
    control.track.store(true, Ordering::SeqCst);
    scope_namespace_actor(Some(actor()), f.meta.create_file(inodes[7], "cold".into()))
        .await
        .unwrap();
    control.track.store(false, Ordering::SeqCst);
    let reads = control.reads.load(Ordering::SeqCst);
    println!("depth=8, cold topology permission snapshots={reads}");
    assert_eq!(
        reads, 8,
        "discover unknown links without a separate prewalk"
    );
    shutdown(&f).await;
}

#[tokio::test]
async fn namespace_permission_batches_keep_warm_hits_and_recheck_changed_members() {
    let mut f = fixture_with_cache(64 << 20).await;
    f.meta = f
        .meta
        .with_upper_row_cache(Duration::from_secs(60))
        .with_lease_bound_row_cache(Arc::new(LeaseValidity::new(
            std::time::Instant::now(),
            Duration::from_secs(30),
        )));
    let inodes = directories(&f).await;
    scope_namespace_actor(Some(actor()), f.meta.create_file(inodes[7], "first".into()))
        .await
        .unwrap();
    let control = &f.control.group_commit;
    control.reads.store(0, Ordering::SeqCst);
    control.track.store(true, Ordering::SeqCst);
    scope_namespace_actor(Some(actor()), f.meta.create_file(inodes[7], "warm".into()))
        .await
        .unwrap();
    control.track.store(false, Ordering::SeqCst);
    assert_eq!(control.reads.load(Ordering::SeqCst), 1);

    // Invalidate one member of a cached pair. The sibling snapshot must not
    // supply that member's old permissions after its own cache key is removed.
    chmod(&f, inodes[3], 0o700).await;
    denied(
        scope_namespace_actor(
            Some(actor()),
            f.meta.create_file(inodes[7], "denied".into()),
        )
        .await
        .unwrap_err(),
    );
    assert_eq!(f.meta.lookup(inodes[7], "denied").await.unwrap(), None);
    shutdown(&f).await;
}

#[tokio::test]
async fn namespace_permission_batches_follow_renames_past_deleted_hints() {
    for allow in [true, false] {
        let f = fixture_with_cache(64 << 20).await;
        let root = f.meta.root_ino();
        let old = directory(&f, root, "old").await;
        let new = directory(&f, root, "new").await;
        let moved = directory(&f, old, "moved").await;
        let parent = directory(&f, moved, "parent").await;
        scope_namespace_actor(Some(actor()), f.meta.create_file(parent, "warm".into()))
            .await
            .unwrap();
        f.meta
            .rename(old, "moved", new, "moved".into())
            .await
            .unwrap();
        f.meta.rmdir(root, "old").await.unwrap();
        if !allow {
            chmod(&f, new, 0o700).await;
        }
        f.meta.invalidate_upper_rows();
        assert_eq!(f.meta.directory_hints.get(&moved), Some(old));
        let result = scope_namespace_actor(
            Some(actor()),
            f.meta.create_file(parent, "after-rename".into()),
        )
        .await;
        if allow {
            result.unwrap();
        } else {
            denied(result.unwrap_err());
            assert_eq!(f.meta.lookup(parent, "after-rename").await.unwrap(), None);
        }
        shutdown(&f).await;
    }
}

#[tokio::test]
async fn namespace_permission_batches_retry_required_rows_after_speculative_failure() {
    let f = fixture_with_cache(64 << 20).await;
    let inodes = directories(&f).await;
    let control = &f.control.group_commit;
    control.fail_pair_read_once.store(true, Ordering::SeqCst);
    scope_namespace_actor(
        Some(actor()),
        f.meta.create_file(inodes[7], "fallback".into()),
    )
    .await
    .unwrap();
    assert!(!control.fail_pair_read_once.load(Ordering::SeqCst));
    shutdown(&f).await;
}

#[tokio::test]
async fn namespace_permission_batches_reauthorize_after_commit_conflict() {
    let f = fixture_with_cache(64 << 20).await;
    let inodes = directories(&f).await;
    let control = &f.control.group_commit;
    control.pause_namespace_commit.store(true, Ordering::SeqCst);
    let create =
        scope_namespace_actor(Some(actor()), f.meta.create_file(inodes[7], "raced".into()));
    let revoke = async {
        control.reached.acquire().await.unwrap().forget();
        // Bypass this wrapper's mutation gate, as an independent writer would.
        // Its real versioned commit makes the prepared create's CAS conflict.
        let snapshot = f
            .meta
            .permission_snapshot(vec![inodes[3]], None)
            .await
            .unwrap();
        let mut inode = crate::workspace_overlay::resolver::resolve_inode(
            &snapshot.layers,
            &snapshot.inodes,
            inodes[3],
        )
        .unwrap()
        .unwrap()
        .inode;
        inode.mode = 0o700;
        f.meta
            .mutate_inode(&snapshot.layers, inode, Vec::new())
            .await
            .unwrap();
        control.resume.add_permits(1);
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(create, revoke)
    })
    .await
    .unwrap();
    denied(result.unwrap_err());
    assert_eq!(f.meta.lookup(inodes[7], "raced").await.unwrap(), None);
    shutdown(&f).await;
}

#[tokio::test]
async fn namespace_permission_batches_deny_each_ancestor() {
    for cached in [false, true] {
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
        let inodes = directories(&f).await;
        let parent = inodes[7];
        let source =
            scope_namespace_actor(Some(actor()), f.meta.create_file(parent, "source".into()))
                .await
                .unwrap();
        // Revoke each position, including both members of every batch and root.
        // With the cache enabled, leave unaffected permission rows warm.
        for ino in std::iter::once(f.meta.root_ino()).chain(inodes.iter().copied()) {
            chmod(&f, ino, 0o700).await;
            denied(
                scope_namespace_actor(Some(actor()), f.meta.create_file(parent, "denied".into()))
                    .await
                    .unwrap_err(),
            );
            denied(
                scope_namespace_actor(Some(actor()), f.meta.mkdir(parent, "denied-dir".into()))
                    .await
                    .unwrap_err(),
            );
            denied(
                scope_namespace_actor(Some(actor()), f.meta.unlink(parent, "source"))
                    .await
                    .unwrap_err(),
            );
            denied(
                scope_namespace_actor(
                    Some(actor()),
                    f.meta.rename(parent, "source", parent, "renamed".into()),
                )
                .await
                .unwrap_err(),
            );
            assert_eq!(f.meta.lookup(parent, "source").await.unwrap(), Some(source));
            for name in ["denied", "denied-dir", "renamed"] {
                assert_eq!(f.meta.lookup(parent, name).await.unwrap(), None);
            }
            chmod(&f, ino, 0o777).await;
        }
        shutdown(&f).await;
    }
}

#[tokio::test]
async fn namespace_permission_batches_handle_root_short_and_missing_paths() {
    let f = fixture_with_cache(64 << 20).await;
    let root = f.meta.root_ino();
    chmod(&f, root, 0o777).await;
    let shallow = directory(&f, root, "shallow").await;
    let parent = directory(&f, shallow, "parent").await;
    let control = &f.control.group_commit;
    for (ino, expected_reads) in [(root, 1), (shallow, 2), (parent, 2)] {
        f.meta.invalidate_upper_rows();
        control.reads.store(0, Ordering::SeqCst);
        control.track.store(true, Ordering::SeqCst);
        scope_namespace_actor(Some(actor()), f.meta.create_file(ino, "probe".into()))
            .await
            .unwrap();
        control.track.store(false, Ordering::SeqCst);
        assert_eq!(control.reads.load(Ordering::SeqCst), expected_reads);
    }

    let missing = directory(&f, root, "removed").await;
    f.meta.rmdir(root, "removed").await.unwrap();
    let error = scope_namespace_actor(
        Some(actor()),
        f.meta.create_file(missing, "missing-parent".into()),
    )
    .await
    .unwrap_err();
    assert!(matches!(error, MetaError::NotFound(ino) if ino == missing));

    // A missing authoritative ancestor must fail even if root was read ahead.
    let snapshot = f
        .meta
        .permission_snapshot(vec![shallow], None)
        .await
        .unwrap();
    let mut inode = crate::workspace_overlay::resolver::resolve_inode(
        &snapshot.layers,
        &snapshot.inodes,
        shallow,
    )
    .unwrap()
    .unwrap()
    .inode;
    inode.parent_hint = Some(missing);
    f.meta
        .mutate_inode(&snapshot.layers, inode, Vec::new())
        .await
        .unwrap();
    f.meta.directory_hints.invalidate_all();
    let error = scope_namespace_actor(
        Some(actor()),
        f.meta.create_file(parent, "missing-ancestor".into()),
    )
    .await
    .unwrap_err();
    assert!(matches!(error, MetaError::NotFound(ino) if ino == missing));
    assert_eq!(
        f.meta.lookup(parent, "missing-ancestor").await.unwrap(),
        None
    );
    shutdown(&f).await;
}
