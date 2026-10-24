//! Count actual KV permission snapshots through actor-authenticated mutations.

use super::*;
use crate::meta::layer::{NamespaceActor, scope_namespace_actor};
use crate::meta::store::FileType;
use crate::workspace_overlay::lifecycle::LeaseValidity;

fn actor() -> NamespaceActor {
    NamespaceActor {
        uid: 1234,
        gid: 1234,
        groups: vec![1234],
    }
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
