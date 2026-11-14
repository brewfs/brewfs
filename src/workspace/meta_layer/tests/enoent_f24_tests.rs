//! Deterministic rename races against actual KV metadata and FUSE callbacks.
//! The backend wrapper controls scheduling and transport failures only.
use super::*;
use asyncfuse::Errno;
use asyncfuse::raw::{Filesystem, Request};
use std::ffi::OsStr;
use std::sync::atomic::{AtomicBool, AtomicI32};

#[path = "enoent_fixture.rs"]
mod fixture_no_sqlite;
use fixture_no_sqlite::{fixture_with_backend, fixture_with_cache};

#[cfg(target_os = "linux")]
#[path = "enoent_f24_kernel_tests.rs"]
mod kernel;

pub(super) struct Control {
    pause_dentry: AtomicBool,
    present_dentry_only: AtomicBool,
    reached: Semaphore,
    resume: Semaphore,
    inode_error: AtomicI32,
    hide_inode: AtomicBool,
    inode_reads: AtomicUsize,
}

impl Default for Control {
    fn default() -> Self {
        Self {
            pause_dentry: AtomicBool::new(false),
            present_dentry_only: AtomicBool::new(false),
            reached: Semaphore::new(0),
            resume: Semaphore::new(0),
            inode_error: AtomicI32::new(0),
            hide_inode: AtomicBool::new(false),
            inode_reads: AtomicUsize::new(0),
        }
    }
}

impl Control {
    pub(super) fn read_override(
        &self,
        key: &[u8],
    ) -> Option<Result<Option<Vec<u8>>, WorkspaceError>> {
        if key.starts_with(b"delta/inode/") {
            self.inode_reads.fetch_add(1, Ordering::SeqCst);
            let error = self.inode_error.load(Ordering::SeqCst);
            if error != 0 {
                return Some(Err(WorkspaceError::Io(std::io::Error::from_raw_os_error(
                    error,
                ))));
            }
            if self.hide_inode.load(Ordering::SeqCst) {
                return Some(Ok(None));
            }
        }
        None
    }

    pub(super) async fn after_read(&self, key: &[u8], present: bool) {
        if key.starts_with(b"delta/dentry/")
            && (!self.present_dentry_only.load(Ordering::SeqCst) || present)
            && self.pause_dentry.swap(false, Ordering::SeqCst)
        {
            self.reached.add_permits(1);
            self.resume.acquire().await.unwrap().forget();
        }
    }
}

type Meta = WorkspaceMetaLayer<
    KvWorkspaceStore<crate::workspace::stores::local_metadata::LocalMetadataBackend>,
>;

fn cached_meta(meta: Meta, bytes: u64) -> Arc<Meta> {
    // Cache authority is held valid for this short fixture, as in neighboring
    // cache tests. Remote mount authority and real KV mutations remain intact.
    Arc::new(
        meta.with_upper_row_cache_bytes(Duration::from_secs(60), bytes)
            .with_lease_bound_row_cache(Arc::new(crate::workspace::lifecycle::LeaseValidity::new(
                std::time::Instant::now(),
                Duration::from_secs(300),
            )))
            .with_lease_row_retention(true),
    )
}

fn vfs(meta: Arc<Meta>) -> crate::vfs::fs::VFS<crate::chunk::store::InMemoryBlockStore, Meta> {
    crate::vfs::fs::VFS::from_workspace_components(
        crate::vfs::config::VFSConfig::new(ChunkLayout {
            chunk_size: 4096,
            block_size: 4096,
        }),
        Arc::new(crate::chunk::store::InMemoryBlockStore::new()),
        meta,
    )
    .unwrap()
}

fn actor(uid: u32) -> Request {
    Request {
        uid,
        gid: uid,
        pid: 0,
        ..Request::default()
    }
}

async fn create(meta: &Meta, name: &str) -> i64 {
    meta.create_node_with_umask(
        1,
        name.into(),
        crate::meta::store::FileType::File,
        0o600,
        0,
        1000,
        1000,
        0,
    )
    .await
    .unwrap()
    .ino
}

#[tokio::test]
async fn enoent_f24_lookup_to_stat_interleaving() {
    let mut failures = Vec::new();
    for cache_bytes in [0, 64 << 20] {
        for replace in [true, false] {
            let f = fixture_with_cache(cache_bytes).await;
            let meta = cached_meta(f.meta, cache_bytes);
            let old = create(&meta, "target").await;
            let new = create(&meta, "next").await;
            meta.invalidate_upper_rows();
            f.control
                .enoent_f24
                .pause_dentry
                .store(true, Ordering::SeqCst);
            let reader_meta = meta.clone();
            let reader =
                tokio::spawn(async move { reader_meta.lookup_with_attr_bytes(1, b"target").await });
            tokio::time::timeout(
                Duration::from_secs(5),
                f.control.enoent_f24.reached.acquire(),
            )
            .await
            .unwrap()
            .unwrap()
            .forget();
            // The real backend has copied the old dentry; stat has not begun.
            if replace {
                meta.rename(1, "next", 1, "target".into()).await.unwrap();
            } else {
                meta.unlink(1, "target").await.unwrap();
            }
            assert!(meta.stat(old).await.unwrap().is_none());
            f.control.enoent_f24.resume.add_permits(1);
            let observed = tokio::time::timeout(Duration::from_secs(5), reader)
                .await
                .unwrap()
                .unwrap()
                .map(|entry| entry.map(|(ino, _)| ino));
            let expected = replace.then_some(new);
            assert_eq!(meta.lookup(1, "target").await.unwrap(), expected);
            eprintln!(
                "LOOKUP_STAT cache={cache_bytes} replace={replace} old={old} new={new} expected={expected:?} observed={observed:?}"
            );
            if !matches!(&observed, Ok(value) if *value == expected) {
                failures.push(format!(
                    "cache={cache_bytes} replace={replace}: {observed:?}"
                ));
            }
            f.authority.close_renewals_and_drain().await.unwrap();
            meta.shutdown_session().await.unwrap();
        }
    }
    assert!(
        failures.is_empty(),
        "LOOKUP_STAT_INTERLEAVING: {failures:?}"
    );
}

#[tokio::test]
#[cfg(target_os = "linux")]
async fn enoent_f24_retired_inode_getattr_interleaving() {
    let mut failures = Vec::new();
    for cache_bytes in [0, 64 << 20] {
        for lower in [false, true] {
            for replace in [true, false] {
                let f = fixture_with_cache(cache_bytes).await;
                let meta = cached_meta(f.meta, cache_bytes);
                let fs = vfs(meta.clone());
                let name = if lower { "nonzero" } else { "target" };
                if !lower {
                    create(&meta, name).await;
                }
                let new = create(&meta, "next").await;
                let old = Filesystem::lookup(&fs, actor(1000), 1, OsStr::new(name))
                    .await
                    .unwrap()
                    .attr
                    .ino;
                assert_eq!(
                    Filesystem::getattr(&fs, actor(1000), old, None, 0)
                        .await
                        .unwrap()
                        .attr
                        .ino,
                    old
                );
                if replace {
                    meta.rename(1, "next", 1, name.into()).await.unwrap();
                } else {
                    meta.unlink(1, name).await.unwrap();
                }
                assert!(meta.stat(old as i64).await.unwrap().is_none());
                for uid in [0, 1000] {
                    let error = Filesystem::getattr(&fs, actor(uid), old, None, 0)
                        .await
                        .unwrap_err();
                    eprintln!(
                        "RETIRED_GETATTR cache={cache_bytes} lower={lower} replace={replace} uid={uid} old={old} observed={error:?}"
                    );
                    if error != Errno::from(libc::ESTALE) {
                        failures.push(format!("cache={cache_bytes} lower={lower} replace={replace} uid={uid}: {error:?}"));
                    }
                    let current = Filesystem::lookup(&fs, actor(uid), 1, OsStr::new(name)).await;
                    if replace {
                        assert_eq!(current.unwrap().attr.ino, new as u64);
                        assert_eq!(
                            Filesystem::getattr(&fs, actor(uid), new as u64, None, 0)
                                .await
                                .unwrap()
                                .attr
                                .ino,
                            new as u64
                        );
                    } else {
                        assert_eq!(current.unwrap_err(), Errno::from(libc::ENOENT));
                    }
                }
                assert!(meta.open_counts.is_empty());
                f.authority.close_renewals_and_drain().await.unwrap();
                meta.shutdown_session().await.unwrap();
            }
        }
    }
    assert!(
        failures.is_empty(),
        "RETIRED_GETATTR_INTERLEAVING: {failures:?}"
    );
}

#[tokio::test]
async fn enoent_f24_retained_handle_and_permission_controls() {
    let f = fixture_with_cache(64 << 20).await;
    let meta = cached_meta(f.meta, 64 << 20);
    let fs = vfs(meta.clone());
    let old = create(&meta, "target").await;
    let new = create(&meta, "next").await;
    let opened = Filesystem::open(&fs, actor(1000), old as u64, libc::O_RDONLY as u32)
        .await
        .unwrap();
    meta.rename(1, "next", 1, "target".into()).await.unwrap();
    assert_eq!(meta.lookup(1, "target").await.unwrap(), Some(new));
    for fh in [None, Some(opened.fh)] {
        let attr = Filesystem::getattr(&fs, actor(1000), old as u64, fh, 0)
            .await
            .unwrap()
            .attr;
        assert_eq!(attr.ino, old as u64);
        assert_eq!(attr.nlink, 0);
    }
    assert_eq!(
        Filesystem::open(&fs, actor(2000), new as u64, libc::O_RDONLY as u32)
            .await
            .unwrap_err(),
        Errno::from(libc::EACCES)
    );
    assert_eq!(
        Filesystem::lookup(&fs, actor(1000), 1, OsStr::new("absent"))
            .await
            .unwrap_err(),
        Errno::from(libc::ENOENT)
    );
    fs.close(opened.fh).await.unwrap();
    assert!(meta.stat(old).await.unwrap().is_none());
    assert!(meta.open_counts.is_empty());
    f.authority.close_renewals_and_drain().await.unwrap();
    meta.shutdown_session().await.unwrap();
}

#[tokio::test]
async fn lookup_and_getattr_preserve_backend_errors_with_live_handle() {
    for cache_bytes in [0, 64 << 20] {
        let f = fixture_with_cache(cache_bytes).await;
        let meta = cached_meta(f.meta, cache_bytes);
        let fs = vfs(meta.clone());
        let old = create(&meta, "target").await;
        let opened = Filesystem::open(&fs, actor(1000), old as u64, libc::O_RDONLY as u32)
            .await
            .unwrap();
        meta.invalidate_upper_rows();
        let before = f.control.enoent_f24.inode_reads.load(Ordering::SeqCst);
        assert_eq!(
            meta.lookup_with_attr_bytes(1, b"target")
                .await
                .unwrap()
                .unwrap()
                .0,
            old
        );
        assert_eq!(
            f.control.enoent_f24.inode_reads.load(Ordering::SeqCst),
            before + 2,
            "successful lookup reads the two layers once each"
        );
        for error in [libc::EACCES, libc::EIO, libc::ENOENT] {
            meta.invalidate_upper_rows();
            f.control
                .enoent_f24
                .inode_error
                .store(error, Ordering::SeqCst);
            let before = f.control.enoent_f24.inode_reads.load(Ordering::SeqCst);
            let lookup = meta.lookup_with_attr_bytes(1, b"target").await.unwrap_err();
            assert!(
                matches!(lookup, crate::meta::store::MetaError::Io(ref e)
                if e.raw_os_error() == Some(error)),
                "{lookup:?}"
            );
            assert_eq!(
                f.control.enoent_f24.inode_reads.load(Ordering::SeqCst),
                before + 1,
                "backend errors must not be retried as missing attributes"
            );
            for fh in [None, Some(opened.fh)] {
                assert_eq!(
                    Filesystem::getattr(&fs, actor(1000), old as u64, fh, 0)
                        .await
                        .unwrap_err(),
                    Errno::from(error)
                );
            }
        }
        f.control.enoent_f24.inode_error.store(0, Ordering::SeqCst);
        fs.close(opened.fh).await.unwrap();
        f.authority.close_renewals_and_drain().await.unwrap();
        meta.shutdown_session().await.unwrap();
    }
}

#[tokio::test]
async fn lookup_dangling_dentry_is_not_hidden_by_revalidation() {
    let f = fixture_with_cache(0).await;
    let meta = cached_meta(f.meta, 0);
    let ino = create(&meta, "target").await;
    f.control
        .enoent_f24
        .hide_inode
        .store(true, Ordering::SeqCst);
    let error = tokio::time::timeout(
        Duration::from_secs(5),
        meta.lookup_with_attr_bytes(1, b"target"),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(matches!(error, crate::meta::store::MetaError::NotFound(found) if found == ino));
    f.control
        .enoent_f24
        .hide_inode
        .store(false, Ordering::SeqCst);
    f.authority.close_renewals_and_drain().await.unwrap();
    meta.shutdown_session().await.unwrap();
}

#[tokio::test]
async fn lookup_revalidates_lower_and_bounds_repeated_replacements() {
    for cache_bytes in [0, 64 << 20] {
        for changes in [1, 3, 8] {
            let f = fixture_with_cache(cache_bytes).await;
            let meta = cached_meta(f.meta, cache_bytes);
            let name = if changes == 1 { "nonzero" } else { "target" };
            if changes != 1 {
                create(&meta, name).await;
            }
            // Each replacement interrupts a returned upper dentry. The
            // subsequent empty base-layer read belongs to the same lookup.
            f.control
                .enoent_f24
                .present_dentry_only
                .store(changes != 1, Ordering::SeqCst);
            meta.invalidate_upper_rows();
            f.control
                .enoent_f24
                .pause_dentry
                .store(true, Ordering::SeqCst);
            let reader_meta = meta.clone();
            let reader = tokio::spawn(async move {
                reader_meta.lookup_with_attr_bytes(1, name.as_bytes()).await
            });
            let mut newest = 0;
            for change in 0..changes {
                tokio::time::timeout(
                    Duration::from_secs(5),
                    f.control.enoent_f24.reached.acquire(),
                )
                .await
                .unwrap()
                .unwrap()
                .forget();
                newest = create(&meta, "next").await;
                meta.rename(1, "next", 1, name.into()).await.unwrap();
                if change + 1 < changes {
                    f.control
                        .enoent_f24
                        .pause_dentry
                        .store(true, Ordering::SeqCst);
                }
                f.control.enoent_f24.resume.add_permits(1);
            }
            let result = tokio::time::timeout(Duration::from_secs(5), reader)
                .await
                .unwrap()
                .unwrap();
            eprintln!("LOOKUP_REPEATED cache={cache_bytes} changes={changes} result={result:?}");
            if changes == 8 {
                assert!(
                    matches!(result, Err(crate::meta::store::MetaError::Io(ref e))
                    if e.raw_os_error() == Some(libc::ESTALE))
                );
            } else {
                assert_eq!(result.unwrap().unwrap().0, newest);
            }
            assert_eq!(meta.lookup(1, name).await.unwrap(), Some(newest));
            f.authority.close_renewals_and_drain().await.unwrap();
            meta.shutdown_session().await.unwrap();
        }
    }
}
