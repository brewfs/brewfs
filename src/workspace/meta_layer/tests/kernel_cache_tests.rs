use super::*;
use crate::chunk::{ChunkLayout, store::InMemoryBlockStore};
use crate::vfs::fs::VFS;
use crate::workspace::catalog::MarkDeleting;
use asyncfuse::raw::{Filesystem, Request};
use std::ffi::OsStr;
use std::time::{Duration, Instant};

// Moved from tests/fuse_cache_ttl.rs so the clock can remain private and
// test-only while exercising the real metadata and FUSE entry points.
#[tokio::test]
async fn cache_fill_detects_other_writer_and_forced_discard() {
    let meta = Arc::new(test_meta().await);
    let other = WorkspaceMetaLayer::new(meta.store().clone(), meta.view_context().await);
    let root = meta.root_ino();
    let ino = other.create_file(root, "old".into()).await.unwrap();
    let ttl = Duration::from_millis(20);
    meta.kernel_cache.lock().await.now = Some(Instant::now());
    let deadline = meta
        .kernel_metadata_cache_deadline(ttl)
        .await
        .unwrap()
        .unwrap();
    other.rename(root, "old", root, "new".into()).await.unwrap();

    // A hit just before expiry must preserve the original checked deadline,
    // regardless of how long the external rename actually took.
    meta.kernel_cache.lock().await.now = Some(deadline - Duration::from_nanos(1));
    assert_eq!(
        meta.kernel_metadata_cache_deadline(ttl).await.unwrap(),
        Some(deadline),
        "cache hits must not slide the checked deadline"
    );
    meta.kernel_cache.lock().await.now = Some(deadline);
    let refreshed = meta
        .kernel_metadata_cache_deadline(ttl)
        .await
        .unwrap()
        .unwrap();
    assert!(refreshed > deadline);
    assert_eq!(refreshed, deadline + ttl);

    let fs = VFS::new_workspace(
        ChunkLayout::default(),
        InMemoryBlockStore::new(),
        meta.clone(),
    )
    .unwrap();
    fs.set_fuse_cache_ttl(ttl);
    let req = Request::default();
    assert!(
        Filesystem::lookup(&fs, req, root as u64, OsStr::new("old"))
            .await
            .is_err()
    );
    assert_eq!(
        Filesystem::lookup(&fs, req, root as u64, OsStr::new("new"))
            .await
            .unwrap()
            .attr
            .ino,
        ino as u64
    );
    meta.store()
        .mark_workspace_deleting(MarkDeleting {
            workspace_id: meta.view_context().await.workspace_id,
            force_fence_lease: true,
        })
        .await
        .unwrap();
    meta.kernel_cache.lock().await.now = Some(refreshed);
    assert!(meta.kernel_metadata_cache_deadline(ttl).await.is_err());
    assert!(
        Filesystem::getattr(&fs, req, ino as u64, None, 0)
            .await
            .is_err(),
        "a cached inode cannot refill after forced discard"
    );
}
