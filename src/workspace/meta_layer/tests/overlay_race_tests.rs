//! Deterministic lookup -> replacement -> OPEN, using the real KV Workspace.
use super::*;
use asyncfuse::raw::{Filesystem, Request};
use std::ffi::OsStr;

#[tokio::test]
#[cfg(target_os = "linux")]
async fn replaced_lookup_open_requests_path_revalidation_without_resurrecting_inode() {
    let f = fixture_with_cache(64 << 20).await;
    let meta = Arc::new(f.meta);
    let layout = ChunkLayout {
        chunk_size: 4096,
        block_size: 4096,
    };
    let vfs = crate::vfs::fs::VFS::from_workspace_components(
        crate::vfs::config::VFSConfig::new(layout),
        Arc::new(crate::chunk::store::InMemoryBlockStore::new()),
        meta.clone(),
    )
    .unwrap();
    let req = || Request {
        unique: 1,
        uid: 0,
        gid: 0,
        pid: 0,
    };
    let root = meta.root_ino();
    let old = meta.create_file(root, "target".into()).await.unwrap();
    let next = meta.create_file(root, "next".into()).await.unwrap();
    let looked_up = Filesystem::lookup(&vfs, req(), root as u64, OsStr::new("target"))
        .await
        .unwrap();
    assert_eq!(looked_up.attr.ino, old as u64);
    meta.rename(root, "next", root, "target".into())
        .await
        .unwrap();
    assert!(meta.stat(old).await.unwrap().is_none());
    let error = Filesystem::open(&vfs, req(), old as u64, libc::O_RDONLY as u32)
        .await
        .unwrap_err();
    assert_eq!(
        error,
        asyncfuse::Errno::from(libc::ESTALE),
        "OPEN carries an obsolete nodeid, not evidence that the pathname is absent"
    );
    assert!(!meta.open_counts.contains_key(&old));
    let looked_up = Filesystem::lookup(&vfs, req(), root as u64, OsStr::new("target"))
        .await
        .unwrap();
    assert_eq!(looked_up.attr.ino, next as u64);
    let opened = Filesystem::open(&vfs, req(), next as u64, libc::O_RDONLY as u32)
        .await
        .unwrap();
    vfs.close(opened.fh).await.unwrap();
    meta.set_attr(
        next,
        &crate::meta::store::SetAttrRequest {
            mode: Some(0),
            ..Default::default()
        },
        crate::meta::store::SetAttrFlags::empty(),
    )
    .await
    .unwrap();
    let denied = Filesystem::open(
        &vfs,
        Request {
            unique: 2,
            uid: 1234,
            gid: 1234,
            pid: 0,
        },
        next as u64,
        libc::O_RDONLY as u32,
    )
    .await
    .unwrap_err();
    assert_eq!(
        denied,
        asyncfuse::Errno::from(libc::EACCES),
        "revalidation must not reuse the replaced inode's permissions"
    );
    // A real missing name remains ENOENT; the FUSE adapter must not mask it.
    assert_eq!(
        Filesystem::lookup(&vfs, req(), root as u64, OsStr::new("absent"))
            .await
            .unwrap_err(),
        asyncfuse::Errno::from(libc::ENOENT)
    );
    f.authority.close_renewals_and_drain().await.unwrap();
    meta.shutdown_session().await.unwrap();
}
