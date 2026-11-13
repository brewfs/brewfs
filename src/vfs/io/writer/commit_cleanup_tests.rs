//! Real writer/stage regression: metadata finishes after the upload cleanup check.
use super::*;
use crate::chunk::ChunkLayout;
use crate::chunk::store::InMemoryBlockStore;
use crate::meta::client::session::SessionInfo;
use crate::meta::factory::create_meta_store_from_url;
use crate::meta::file_lock::{FileLockInfo, FileLockQuery, FileLockRange, FileLockType};
use crate::meta::store::*;
use crate::vfs::cache::write_back::FsWriteBackCache;
use crate::vfs::config::ReadConfig;
use crate::vfs::handles::DirHandle;
use async_trait::async_trait;
use futures::FutureExt;
use std::sync::atomic::AtomicUsize;

/// Gates the real promotion I/O, without replacing stage or upload behavior.
pub(super) struct CleanupProbe {
    pause_first: AtomicBool,
    calls: AtomicUsize,
    entered: Notify,
    release: Notify,
}

impl CleanupProbe {
    pub(super) async fn enter(&self) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.pause_first.swap(false, Ordering::AcqRel) {
            self.entered.notify_one();
            self.release.notified().await;
        }
    }
}

struct GatedMeta<M> {
    inner: Arc<M>,
    entered: Notify,
    release: Notify,
    first_write: AtomicBool,
}

#[async_trait]
impl<M: MetaLayer + 'static> MetaLayer for GatedMeta<M> {
    async fn write(
        &self,
        ino: i64,
        chunk_id: u64,
        slice: SliceDesc,
        new_size: u64,
    ) -> Result<(), MetaError> {
        if self.first_write.swap(false, Ordering::AcqRel) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        self.inner.write(ino, chunk_id, slice, new_size).await
    }

    fn root_ino(&self) -> i64 {
        self.inner.root_ino()
    }

    fn chroot(&self, inode: i64) {
        self.inner.chroot(inode)
    }

    async fn initialize(&self) -> Result<(), MetaError> {
        self.inner.initialize().await
    }

    async fn stat_fs(&self) -> Result<StatFsSnapshot, MetaError> {
        self.inner.stat_fs().await
    }

    async fn stat(&self, ino: i64) -> Result<Option<FileAttr>, MetaError> {
        self.inner.stat(ino).await
    }

    async fn stat_fresh(&self, ino: i64) -> Result<Option<FileAttr>, MetaError> {
        self.inner.stat_fresh(ino).await
    }

    async fn lookup(&self, parent: i64, name: &str) -> Result<Option<i64>, MetaError> {
        self.inner.lookup(parent, name).await
    }

    async fn lookup_path(&self, path: &str) -> Result<Option<(i64, FileType)>, MetaError> {
        self.inner.lookup_path(path).await
    }

    async fn readdir(&self, ino: i64) -> Result<Vec<DirEntry>, MetaError> {
        self.inner.readdir(ino).await
    }

    async fn opendir(&self, ino: i64) -> Result<DirHandle, MetaError> {
        self.inner.opendir(ino).await
    }

    async fn mkdir(&self, parent: i64, name: String) -> Result<i64, MetaError> {
        self.inner.mkdir(parent, name).await
    }

    async fn rmdir(&self, parent: i64, name: &str) -> Result<(), MetaError> {
        self.inner.rmdir(parent, name).await
    }

    async fn create_file(&self, parent: i64, name: String) -> Result<i64, MetaError> {
        self.inner.create_file(parent, name).await
    }

    async fn create_node(
        &self,
        parent: i64,
        name: String,
        kind: FileType,
        mode: u32,
        uid: u32,
        gid: u32,
        rdev: u32,
    ) -> Result<i64, MetaError> {
        self.inner
            .create_node(parent, name, kind, mode, uid, gid, rdev)
            .await
    }

    async fn link(&self, ino: i64, parent: i64, name: &str) -> Result<FileAttr, MetaError> {
        self.inner.link(ino, parent, name).await
    }

    async fn symlink(
        &self,
        parent: i64,
        name: &str,
        target: &str,
    ) -> Result<(i64, FileAttr), MetaError> {
        self.inner.symlink(parent, name, target).await
    }

    async fn unlink(&self, parent: i64, name: &str) -> Result<(), MetaError> {
        self.inner.unlink(parent, name).await
    }

    async fn rename(
        &self,
        old_parent: i64,
        old_name: &str,
        new_parent: i64,
        new_name: String,
    ) -> Result<(), MetaError> {
        self.inner
            .rename(old_parent, old_name, new_parent, new_name)
            .await
    }

    async fn rename_noreplace(
        &self,
        old_parent: i64,
        old_name: &str,
        new_parent: i64,
        new_name: String,
    ) -> Result<(), MetaError> {
        self.inner
            .rename_noreplace(old_parent, old_name, new_parent, new_name)
            .await
    }

    async fn rename_exchange(
        &self,
        old_parent: i64,
        old_name: &str,
        new_parent: i64,
        new_name: &str,
    ) -> Result<(), MetaError> {
        self.inner
            .rename_exchange(old_parent, old_name, new_parent, new_name)
            .await
    }

    async fn set_file_size(&self, ino: i64, size: u64) -> Result<(), MetaError> {
        self.inner.set_file_size(ino, size).await
    }

    async fn extend_file_size(&self, ino: i64, size: u64) -> Result<(), MetaError> {
        self.inner.extend_file_size(ino, size).await
    }

    async fn truncate(&self, ino: i64, size: u64, chunk_size: u64) -> Result<(), MetaError> {
        self.inner.truncate(ino, size, chunk_size).await
    }

    async fn get_names(&self, ino: i64) -> Result<Vec<(Option<i64>, String)>, MetaError> {
        self.inner.get_names(ino).await
    }

    async fn get_dentries(&self, ino: i64) -> Result<Vec<(i64, String)>, MetaError> {
        self.inner.get_dentries(ino).await
    }

    async fn get_dir_parent(&self, dir_ino: i64) -> Result<Option<i64>, MetaError> {
        self.inner.get_dir_parent(dir_ino).await
    }

    async fn get_paths(&self, ino: i64) -> Result<Vec<String>, MetaError> {
        self.inner.get_paths(ino).await
    }

    async fn read_symlink(&self, ino: i64) -> Result<String, MetaError> {
        self.inner.read_symlink(ino).await
    }

    async fn set_attr(
        &self,
        ino: i64,
        req: &SetAttrRequest,
        flags: SetAttrFlags,
    ) -> Result<FileAttr, MetaError> {
        self.inner.set_attr(ino, req, flags).await
    }

    async fn open(&self, ino: i64, flags: OpenFlags) -> Result<FileAttr, MetaError> {
        self.inner.open(ino, flags).await
    }

    async fn close(&self, ino: i64) -> Result<(), MetaError> {
        self.inner.close(ino).await
    }

    async fn get_deleted_files(&self) -> Result<Vec<i64>, MetaError> {
        self.inner.get_deleted_files().await
    }

    async fn remove_file_metadata(&self, ino: i64) -> Result<(), MetaError> {
        self.inner.remove_file_metadata(ino).await
    }

    async fn get_slices(&self, chunk_id: u64) -> Result<Vec<SliceDesc>, MetaError> {
        self.inner.get_slices(chunk_id).await
    }

    async fn append_slice(&self, chunk_id: u64, slice: SliceDesc) -> Result<(), MetaError> {
        self.inner.append_slice(chunk_id, slice).await
    }

    async fn next_id(&self, key: &str) -> Result<i64, MetaError> {
        self.inner.next_id(key).await
    }

    async fn start_session(&self, session_info: SessionInfo) -> Result<(), MetaError> {
        self.inner.start_session(session_info).await
    }

    async fn shutdown_session(&self) -> Result<(), MetaError> {
        self.inner.shutdown_session().await
    }

    async fn get_plock(
        &self,
        inode: i64,
        query: &FileLockQuery,
    ) -> Result<FileLockInfo, MetaError> {
        self.inner.get_plock(inode, query).await
    }

    async fn set_plock(
        &self,
        inode: i64,
        owner: i64,
        block: bool,
        lock_type: FileLockType,
        range: FileLockRange,
        pid: u32,
    ) -> Result<(), MetaError> {
        self.inner
            .set_plock(inode, owner, block, lock_type, range, pid)
            .await
    }

    async fn get_flock(&self, inode: i64, owner: i64) -> Result<FileLockType, MetaError> {
        self.inner.get_flock(inode, owner).await
    }

    async fn set_flock(
        &self,
        inode: i64,
        owner: i64,
        block: bool,
        lock_type: FileLockType,
    ) -> Result<(), MetaError> {
        self.inner.set_flock(inode, owner, block, lock_type).await
    }

    async fn set_xattr(
        &self,
        inode: i64,
        name: &str,
        value: &[u8],
        flags: u32,
    ) -> Result<(), MetaError> {
        self.inner.set_xattr(inode, name, value, flags).await
    }

    async fn get_xattr(&self, inode: i64, name: &str) -> Result<Option<Vec<u8>>, MetaError> {
        self.inner.get_xattr(inode, name).await
    }

    async fn list_xattr(&self, inode: i64) -> Result<Vec<String>, MetaError> {
        self.inner.list_xattr(inode).await
    }

    async fn remove_xattr(&self, inode: i64, name: &str) -> Result<(), MetaError> {
        self.inner.remove_xattr(inode, name).await
    }

    async fn set_acl(&self, inode: i64, rule: AclRule) -> Result<(), MetaError> {
        self.inner.set_acl(inode, rule).await
    }

    async fn get_acl(
        &self,
        inode: i64,
        acl_type: u8,
        acl_id: u32,
    ) -> Result<Option<AclRule>, MetaError> {
        self.inner.get_acl(inode, acl_type, acl_id).await
    }
}

#[test]
fn commit_before_upload_cleans_stage_and_preserves_newer_value() {
    metadata_finishes_last(false);
}

#[test]
fn commit_before_upload_waits_for_stage_retirement_before_publication() {
    metadata_finishes_last(true);
}

fn metadata_finishes_last(pause_cleanup: bool) {
    // Holding this runtime's sole blocking worker pauses actual tokio::fs stage
    // I/O without changing SliceState or replacing FsWriteBackCache. SQLite's
    // connection is initialized before this gate and runs on its own worker.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let layout = ChunkLayout {
            chunk_size: 8192,
            block_size: 4096,
        };
        let inner = create_meta_store_from_url("sqlite::memory:")
            .await
            .unwrap()
            .layer();
        let ino = inner
            .create_file(1, "metadata-last-stage".to_string())
            .await
            .unwrap();
        let meta = Arc::new(GatedMeta {
            inner,
            entered: Notify::new(),
            release: Notify::new(),
            first_write: AtomicBool::new(true),
        });
        let store = Arc::new(InMemoryBlockStore::new());
        let backend = Arc::new(Backend::new(store.clone(), meta.clone()));
        let reader = Arc::new(DataReader::new(
            Arc::new(ReadConfig::new(layout)),
            backend.clone(),
        ));
        let temp = tempfile::tempdir().unwrap();
        let read_cache = temp.path().join("read");
        let cache = Arc::new(FsWriteBackCache::new_with_sync_and_read_cache(
            temp.path().join("writeback"),
            false,
            read_cache.clone(),
            0,
        ));
        let observed = Arc::new(CleanupProbe {
            pause_first: AtomicBool::new(pause_cleanup),
            calls: AtomicUsize::new(0),
            entered: Notify::new(),
            release: Notify::new(),
        });
        let config = Arc::new(
            WriteConfig::new(layout)
                .page_size(4096)
                .freeze_min_bytes(4096)
                .auto_flush_max_age(Duration::from_secs(3600))
                .writeback_require_stage_before_commit(false)
                .writeback_mode(WriteBackMode::CommitBeforeUpload),
        );
        let writer = Arc::new(DataWriter::new(
            config,
            backend,
            reader,
            Some(cache.clone()),
        ));
        let file = writer.ensure_file(Inode::new(ino, 0));
        assert!(
            file.shared
                .writeback_cleanup_probe
                .set(observed.clone())
                .is_ok()
        );
        let (release_stage, stage_gate) = std::sync::mpsc::channel::<()>();
        let (blocking_entered, blocking_ready) = tokio::sync::oneshot::channel();
        let blocking = tokio::task::spawn_blocking(move || {
            let _ = blocking_entered.send(());
            // Dropping the sender on assertion failure also releases this gate.
            let _ = stage_gate.recv();
        });
        blocking_ready.await.unwrap();
        file.write_at(4096, &vec![0x5a; 4096]).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), meta.entered.notified())
            .await
            .expect("commit_chunk must claim metadata before stage can finish");
        let cid = chunk_id_for(ino, 0).unwrap();
        let slice = file.shared.inner.lock().await.chunks[&cid]
            .slices
            .front()
            .unwrap()
            .clone();
        assert!(slice.lock().meta_write_started);
        release_stage.send(()).unwrap();
        blocking.await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                {
                    let state = slice.lock();
                    if matches!(state.state, SliceStatus::Uploaded) && !state.upload_task_active {
                        break;
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("upload must finish its cleanup attempt while metadata is blocked");
        assert_eq!(cache.recover().await.unwrap().len(), 1);
        let a_slice_id = slice.lock().slice_id.unwrap();
        meta.release.notify_one();
        let flush = file.flush();
        tokio::pin!(flush);
        if pause_cleanup {
            tokio::time::timeout(Duration::from_secs(5), observed.entered.notified())
                .await
                .expect("metadata owner must enter stage retirement");
            assert!(matches!(slice.lock().state, SliceStatus::Uploaded));
            assert!(
                writer.buffer_usage.load(Ordering::Acquire) > 0,
                "writer payload must remain charged while stage retirement is pending"
            );
            assert!(
                flush.as_mut().now_or_never().is_none(),
                "flush must not publish Committed before stage retirement"
            );
            file.mark_released();
            assert!(
                !file.released_cleanup_ready().await,
                "writer must remain owned during retirement"
            );
            file.mark_active();
            FileWriter::remove_writeback_record_if_uploaded_committed(&file.shared, &slice).await;
            assert_eq!(observed.calls.load(Ordering::SeqCst), 1);
            observed.release.notify_one();
        }
        tokio::time::timeout(Duration::from_secs(5), flush)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(slice.lock().state, SliceStatus::Committed));
        assert!(slice.lock().upload_complete());
        let remaining = cache.recover().await.unwrap();
        // The uploader can arrive after metadata cleaned and published. It must
        // not try to promote the now-removed source or race the same temp link.
        FileWriter::remove_writeback_record_if_uploaded_committed(&file.shared, &slice).await;
        FileWriter::remove_writeback_record_if_uploaded_committed(&file.shared, &slice).await;
        assert_eq!(observed.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            tokio::fs::read(crate::chunk::store::persistent_slice_cache_path(
                &read_cache,
                a_slice_id
            ))
            .await
            .unwrap(),
            vec![0x5a; 4096]
        );

        // Write a known newer value through the real writer, then invoke exactly
        // the SSD overlay used by the production cfg(not(test)) fallback. Tests
        // deliberately do not pretend DataWriter's test-only read path calls it.
        // Hold this second metadata write only to retain its real SliceState
        // until we can observe the upload owner exiting. Pending-byte counters
        // alone become zero before that owner's stage removal finishes.
        meta.first_write.store(true, Ordering::Release);
        file.write_at(4096, &vec![0xa5; 4096]).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), meta.entered.notified())
            .await
            .expect("newer value must reach the metadata gate");
        let b_slice = file.shared.inner.lock().await.chunks[&cid]
            .slices
            .back()
            .unwrap()
            .clone();
        assert!(b_slice.lock().meta_write_started);
        meta.release.notify_one();
        tokio::time::timeout(Duration::from_secs(5), file.flush())
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                {
                    let state = b_slice.lock();
                    if matches!(state.state, SliceStatus::Committed)
                        && state.upload_complete()
                        && !state.upload_task_active
                    {
                        break;
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("newer value's upload and cleanup owner must finish");
        assert!(!file.has_overlay_state().await);
        assert_eq!(writer.recent_pending_upload_bytes(), 0);
        let remote = meta.get_slices(cid).await.unwrap();
        let b_slice_id = remote.iter().map(|desc| desc.slice_id).max().unwrap();
        assert_ne!(a_slice_id, b_slice_id);
        let mut actual = vec![0; 4096];
        store
            .read_range((b_slice_id, 0), 0, &mut actual)
            .await
            .unwrap();
        assert_eq!(actual, vec![0xa5; 4096], "remote newer value");
        cache
            .overlay_dirty_range(ino, cid, 4096, &mut actual)
            .await
            .unwrap();
        assert_eq!(
            actual,
            vec![0xa5; 4096],
            "stale stage must not cover newer remote data"
        );
        assert!(
            remaining.is_empty(),
            "uploaded and metadata-committed slice left stale recoverable stage: {remaining:?}"
        );
        assert!(cache.recover().await.unwrap().is_empty());
        assert!(!cache.has_recoverable_data());
    });
}

#[test]
fn commit_before_upload_cleans_stage_when_metadata_finishes_last() {
    // Holding this runtime's sole blocking worker pauses actual tokio::fs stage
    // I/O without changing SliceState or replacing FsWriteBackCache. SQLite's
    // connection is initialized before this gate and runs on its own worker.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let layout = ChunkLayout {
            chunk_size: 8192,
            block_size: 4096,
        };
        let inner = create_meta_store_from_url("sqlite::memory:")
            .await
            .unwrap()
            .layer();
        let ino = inner
            .create_file(1, "metadata-last-stage".to_string())
            .await
            .unwrap();
        let meta = Arc::new(GatedMeta {
            inner,
            entered: Notify::new(),
            release: Notify::new(),
            first_write: AtomicBool::new(true),
        });
        let backend = Arc::new(Backend::new(
            Arc::new(InMemoryBlockStore::new()),
            meta.clone(),
        ));
        let reader = Arc::new(DataReader::new(
            Arc::new(ReadConfig::new(layout)),
            backend.clone(),
        ));
        let temp = tempfile::tempdir().unwrap();
        let cache = Arc::new(FsWriteBackCache::new_with_sync(temp.path().into(), false));
        let config = Arc::new(
            WriteConfig::new(layout)
                .page_size(4096)
                .freeze_min_bytes(4096)
                .auto_flush_max_age(Duration::from_secs(3600))
                .writeback_require_stage_before_commit(false)
                .writeback_mode(WriteBackMode::CommitBeforeUpload),
        );
        let writer = Arc::new(DataWriter::new(
            config,
            backend,
            reader,
            Some(cache.clone()),
        ));
        let file = writer.ensure_file(Inode::new(ino, 0));
        let (release_stage, stage_gate) = std::sync::mpsc::channel::<()>();
        let (blocking_entered, blocking_ready) = tokio::sync::oneshot::channel();
        let blocking = tokio::task::spawn_blocking(move || {
            let _ = blocking_entered.send(());
            // Dropping the sender on assertion failure also releases this gate.
            let _ = stage_gate.recv();
        });
        blocking_ready.await.unwrap();
        file.write_at(4096, &vec![0x5a; 4096]).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), meta.entered.notified())
            .await
            .expect("commit_chunk must claim metadata before stage can finish");
        let cid = chunk_id_for(ino, 0).unwrap();
        let slice = file.shared.inner.lock().await.chunks[&cid]
            .slices
            .front()
            .unwrap()
            .clone();
        assert!(slice.lock().meta_write_started);
        release_stage.send(()).unwrap();
        blocking.await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                {
                    let state = slice.lock();
                    if matches!(state.state, SliceStatus::Uploaded) && !state.upload_task_active {
                        break;
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("upload must finish its cleanup attempt while metadata is blocked");
        assert_eq!(cache.recover().await.unwrap().len(), 1);
        meta.release.notify_one();
        tokio::time::timeout(Duration::from_secs(5), file.flush())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(slice.lock().state, SliceStatus::Committed));
        assert!(slice.lock().upload_complete());
        let remaining = cache.recover().await.unwrap();
        assert!(
            remaining.is_empty(),
            "uploaded and metadata-committed slice left stale recoverable stage: {remaining:?}"
        );
        assert!(!cache.has_recoverable_data());
    });
}
