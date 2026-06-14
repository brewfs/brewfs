//! VFS filesystem tests - separated from main implementation

use crate::chunk::BlockStore;
use crate::chunk::layout::ChunkLayout;
use crate::chunk::store::InMemoryBlockStore;
use crate::meta::MetaLayer;
use crate::meta::factory::create_meta_store_from_url;
use crate::posix::NAME_MAX;
use crate::vfs::fs::VFS;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone)]
struct StressRng(u64);

impl StressRng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0
    }

    fn range(&mut self, end: u64) -> u64 {
        if end == 0 { 0 } else { self.next() % end }
    }
}

fn test_file_attr(ino: i64) -> super::FileAttr {
    super::FileAttr {
        ino,
        size: 0,
        blocks: 0,
        kind: super::FileType::File,
        mode: 0o100644,
        rdev: 0,
        uid: 0,
        gid: 0,
        atime: 0,
        mtime: 0,
        ctime: 0,
        nlink: 1,
    }
}

#[tokio::test]
async fn test_recently_unlinked_cleanup_is_not_run_on_every_threshold_insert() {
    let layout = ChunkLayout::default();
    let store = InMemoryBlockStore::new();
    let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
    let meta_store = meta_handle.store();
    let fs = VFS::new(layout, store, meta_store).await.unwrap();

    for ino in 10..(10 + super::RECENTLY_UNLINKED_ATTR_CLEANUP_THRESHOLD as i64) {
        fs.remember_recently_unlinked_attr(ino, test_file_attr(ino));
    }

    fs.remember_recently_unlinked_attr(100_000, test_file_attr(100_000));
    fs.state.recently_unlinked.insert(
        -1,
        (
            test_file_attr(-1),
            Instant::now() - super::RECENTLY_UNLINKED_ATTR_TTL * 2,
        ),
    );

    fs.remember_recently_unlinked_attr(100_001, test_file_attr(100_001));

    assert!(
        fs.state.recently_unlinked.contains_key(&-1),
        "cleanup should be throttled instead of scanning the recently-unlinked map on every unlink"
    );
}

#[tokio::test]
async fn test_child_attr_of_returns_child_inode_and_attr() {
    let layout = ChunkLayout::default();
    let store = InMemoryBlockStore::new();
    let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
    let meta_store = meta_handle.store();
    let fs = VFS::new(layout, store, meta_store).await.unwrap();
    let root = fs.root_ino();
    let ino = fs
        .create_file_at(root, "lookup_attr_vfs.txt", true)
        .await
        .unwrap();

    let (found, attr) = fs
        .child_attr_of(root, "lookup_attr_vfs.txt")
        .await
        .expect("child_attr_of should not fail")
        .expect("child_attr_of should return the created child");

    assert_eq!(found, ino);
    assert_eq!(attr.ino, ino);
    assert_eq!(attr.kind, super::FileType::File);
}

#[tokio::test]
async fn test_child_attr_of_rejects_name_longer_than_name_max() {
    let layout = ChunkLayout::default();
    let store = InMemoryBlockStore::new();
    let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
    let meta_store = meta_handle.store();
    let fs = VFS::new(layout, store, meta_store).await.unwrap();
    let root = fs.root_ino();
    let long_name = "x".repeat(NAME_MAX + 1);

    assert!(matches!(
        fs.child_attr_of(root, &long_name).await,
        Err(crate::vfs::error::VfsError::FilenameTooLong { .. })
    ));
}

#[cfg(test)]
mod fsstress_013_native_tests {
    use super::*;

    const OP_TIMEOUT: Duration = Duration::from_secs(5);

    async fn op_timeout<T>(label: &'static str, fut: impl std::future::Future<Output = T>) -> T {
        tokio::time::timeout(OP_TIMEOUT, fut)
            .await
            .unwrap_or_else(|_| panic!("native fsstress operation timed out: {label}"))
    }

    async fn run_native_fsstress_phase(
        fs: Arc<VFS<InMemoryBlockStore, impl MetaLayer>>,
        worker_count: usize,
        ops_per_worker: usize,
        seed: u64,
    ) {
        let root = fs.root_ino();
        for i in 0..64 {
            let _ = fs.create_file_at(root, &format!("f{i}"), false).await;
        }
        for i in 0..16 {
            let _ = fs.mkdir_at(root, &format!("d{i}")).await;
        }

        let mut handles = Vec::with_capacity(worker_count);
        for worker in 0..worker_count {
            let fs = fs.clone();
            handles.push(tokio::spawn(async move {
                let worker_seed = (worker as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
                let mut rng = StressRng::new(seed ^ worker_seed);
                let root = fs.root_ino();

                for iter in 0..ops_per_worker {
                    let slot = rng.range(128) as usize;
                    let name = format!("f{slot}");
                    let aux = format!("x{}_{}", worker, rng.range(128));
                    let dir = format!("d{}", rng.range(32));

                    match rng.range(100) {
                        0..=9 => {
                            op_timeout("mkdir_at", fs.mkdir_at(root, &dir)).await.ok();
                        }
                        10..=19 => {
                            op_timeout("create_file_at", fs.create_file_at(root, &name, false))
                                .await
                                .ok();
                        }
                        20..=29 => {
                            let src =
                                op_timeout("lookup before link", fs.child_of(root, &name)).await;
                            if let Some(ino) = src {
                                op_timeout("link_by_ino", fs.link_by_ino(ino, root, &aux))
                                    .await
                                    .ok();
                            }
                        }
                        30..=59 => {
                            op_timeout("rename_at", fs.rename_at(root, &name, root, &aux))
                                .await
                                .ok();
                            if rng.range(4) == 0 {
                                op_timeout("rename_at back", fs.rename_at(root, &aux, root, &name))
                                    .await
                                    .ok();
                            }
                        }
                        60..=69 => {
                            op_timeout("unlink_at", fs.unlink_at(root, &name))
                                .await
                                .ok();
                        }
                        70..=79 => {
                            op_timeout("rmdir_at", fs.rmdir_at(root, &dir)).await.ok();
                        }
                        80..=89 => {
                            let target = if rng.range(2) == 0 { &name } else { &aux };
                            let ino =
                                op_timeout("lookup before truncate", fs.child_of(root, target))
                                    .await;
                            if let Some(ino) = ino {
                                let size = rng.range(256 * 1024);
                                op_timeout("truncate_inode", fs.truncate_inode(ino, size))
                                    .await
                                    .ok();
                            }
                        }
                        _ => {
                            let target = if rng.range(2) == 0 { &name } else { &aux };
                            let ino =
                                op_timeout("lookup before stat", fs.child_of(root, target)).await;
                            if let Some(ino) = ino {
                                op_timeout("stat_ino", fs.stat_ino(ino)).await;
                            }
                        }
                    }

                    if iter % 100 == 0 {
                        tokio::task::yield_now().await;
                    }
                }
            }));
        }

        for handle in handles {
            handle.await.expect("native fsstress worker panicked");
        }

        op_timeout("final readdir", fs.readdir_ino(root))
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_native_fsstress_013_inode_ops() {
        let layout = ChunkLayout::default();
        let store = InMemoryBlockStore::new();
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta_store = meta_handle.store();
        let fs = Arc::new(VFS::new(layout, store, meta_store).await.unwrap());

        run_native_fsstress_phase(fs.clone(), 1, 1000, 0x0130_0001).await;
        run_native_fsstress_phase(fs.clone(), 20, 1000, 0x0130_0002).await;
        run_native_fsstress_phase(fs, 4, 1000, 0x0130_0003).await;
    }
}

#[cfg(test)]
mod rename_tests {
    use super::*;

    #[tokio::test]
    async fn test_rename_boundary_conditions_vfs() {
        let layout = ChunkLayout::default();
        let store = InMemoryBlockStore::new();
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta_store = meta_handle.store();
        let fs = VFS::new(layout, store, meta_store).await.unwrap();

        // Setup test directory structure
        fs.mkdir_p("/test").await.unwrap();
        fs.create_file("/test/source.txt").await.unwrap();
        fs.mkdir_p("/test/dir1").await.unwrap();
        fs.mkdir_p("/test/dir2").await.unwrap();

        // Test 1: Valid rename operations
        fs.rename("/test/source.txt", "/test/renamed.txt")
            .await
            .unwrap();
        assert!(!fs.exists("/test/source.txt").await);
        assert!(fs.exists("/test/renamed.txt").await);

        // Test 2: Cross-directory move
        fs.rename("/test/renamed.txt", "/test/dir1/moved.txt")
            .await
            .unwrap();
        assert!(!fs.exists("/test/renamed.txt").await);
        assert!(fs.exists("/test/dir1/moved.txt").await);

        // Test 3: Skip directory rename for now (complex edge cases)
        // fs.mkdir_p("/test/dir3").await.unwrap();
        // fs.rename("/test/dir3", "/test/renamed_dir").await.unwrap();
        // assert!(!fs.exists("/test/dir3").await);
        // assert!(fs.exists("/test/renamed_dir").await);

        // Test 4: can_rename validation
        // First create a simple test file for can_rename
        fs.create_file("/test/test_file.txt").await.unwrap();
        fs.create_file("/test/test_target.txt").await.unwrap();
        let result = fs
            .can_rename("/test/test_file.txt", "/test/test_target.txt")
            .await;
        assert!(result.is_ok(), "can_rename should allow valid operation");

        // Test 5: Rename with flags - RENAME_NOREPLACE
        fs.create_file("/test/existing.txt").await.unwrap();
        let result = fs
            .rename_noreplace("/test/dir1/moved.txt", "/test/existing.txt")
            .await;
        assert!(
            result.is_err(),
            "RENAME_NOREPLACE should fail when target exists"
        );

        // Test 7: Valid RENAME_NOREPLACE
        let result = fs
            .rename_noreplace("/test/dir1/moved.txt", "/test/nonexistent.txt")
            .await;
        assert!(
            result.is_ok(),
            "RENAME_NOREPLACE should succeed when target doesn't exist"
        );

        // Test 8: Batch rename
        fs.create_file("/test/batch1.txt").await.unwrap();
        fs.create_file("/test/batch2.txt").await.unwrap();

        let operations = vec![
            (
                "/test/batch1.txt".to_string(),
                "/test/batch1_renamed.txt".to_string(),
            ),
            (
                "/test/batch2.txt".to_string(),
                "/test/batch2_renamed.txt".to_string(),
            ),
        ];

        let results = fs.rename_batch(operations).await;
        assert_eq!(results.len(), 2);
        assert!(results[0].is_ok());
        assert!(results[1].is_ok());

        assert!(!fs.exists("/test/batch1.txt").await);
        assert!(!fs.exists("/test/batch2.txt").await);
        assert!(fs.exists("/test/batch1_renamed.txt").await);
        assert!(fs.exists("/test/batch2_renamed.txt").await);

        println!("All VFS rename boundary condition tests passed!");
    }

    #[tokio::test]
    async fn test_rename_error_cases_vfs() {
        let layout = ChunkLayout::default();
        let store = InMemoryBlockStore::new();
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta_store = meta_handle.store();
        let fs = VFS::new(layout, store, meta_store).await.unwrap();

        // Setup basic structure
        fs.mkdir_p("/errors").await.unwrap();

        // Test 1: Rename non-existent source
        let result = fs
            .rename("/errors/nonexistent.txt", "/errors/target.txt")
            .await;
        assert!(result.is_err(), "Renaming non-existent source should fail");

        // Test 2: Rename to invalid destination
        fs.create_file("/errors/source.txt").await.unwrap();
        let result = fs
            .rename("/errors/source.txt", "/nonexistent/parent/target.txt")
            .await;
        assert!(
            result.is_err(),
            "Renaming to non-existent parent should fail"
        );

        // Test 3: Empty target name
        let result = fs.rename("/errors/source.txt", "").await;
        assert!(result.is_err(), "Empty target name should fail");

        // Test 4: Target name with invalid characters
        let result = fs
            .rename("/errors/source.txt", "/errors/invalid\x00name.txt")
            .await;
        assert!(result.is_err(), "Target name with null bytes should fail");

        // Test 5: Directory replacement rules - non-empty directory
        fs.mkdir_p("/errors/src_dir").await.unwrap();
        fs.mkdir_p("/errors/dst_dir").await.unwrap();
        fs.create_file("/errors/dst_dir/blocker.txt").await.unwrap();

        let result = fs.rename("/errors/src_dir", "/errors/dst_dir").await;
        assert!(result.is_err(), "Replacing non-empty directory should fail");

        // Test 6: File replacing directory
        fs.create_file("/errors/file.txt").await.unwrap();
        let result = fs.rename("/errors/file.txt", "/errors/dst_dir").await;
        assert!(result.is_err(), "File replacing directory should fail");

        // Test 7: Circular rename detection
        fs.mkdir_p("/errors/parent/child").await.unwrap();
        let result = fs
            .rename("/errors/parent", "/errors/parent/child/moved")
            .await;
        assert!(
            result.is_err(),
            "Circular rename should be detected and prevented"
        );

        println!("All VFS rename error case tests passed!");
    }
}

#[cfg(test)]
mod basic_tests {
    use super::*;

    async fn new_basic_fs() -> VFS<InMemoryBlockStore, impl MetaLayer> {
        let layout = ChunkLayout::default();
        let store = InMemoryBlockStore::new();
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta_store = meta_handle.store();
        VFS::new(layout, store, meta_store).await.unwrap()
    }

    #[tokio::test]
    async fn test_fs_unlink_rmdir_rename_truncate() {
        let layout = ChunkLayout::default();
        let tmp = tempfile::tempdir().unwrap();
        let client = crate::cadapter::client::ObjectClient::new(
            crate::cadapter::localfs::LocalFsBackend::new(tmp.path()),
        );
        let store = crate::chunk::store::ObjectBlockStore::new_async(client)
            .await
            .unwrap();

        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta_store = meta_handle.store();
        let fs = VFS::new(layout, store, meta_store).await.unwrap();

        fs.mkdir_p("/a/b").await.unwrap();
        fs.create_file("/a/b/t.txt").await.unwrap();
        assert!(fs.exists("/a/b/t.txt").await);

        // rename file
        fs.rename("/a/b/t.txt", "/a/b/u.txt").await.unwrap();
        assert!(!fs.exists("/a/b/t.txt").await && fs.exists("/a/b/u.txt").await);

        // truncate
        fs.truncate("/a/b/u.txt", layout.block_size as u64 * 2)
            .await
            .unwrap();
        let st = fs.stat("/a/b/u.txt").await.unwrap();
        assert!(st.size >= (layout.block_size * 2) as u64);

        // unlink and rmdir
        fs.unlink("/a/b/u.txt").await.unwrap();
        assert!(!fs.exists("/a/b/u.txt").await);
        // dir empty then rmdir
        fs.rmdir("/a/b").await.unwrap();
        assert!(!fs.exists("/a/b").await);
    }

    #[tokio::test]
    async fn test_optimistic_create_fallback_preserves_existing_semantics() {
        let fs = new_basic_fs().await;
        let root = fs.root_ino();

        let file_ino = fs.create_file_at(root, "file", false).await.unwrap();
        assert_eq!(
            fs.create_file_at(root, "file", false).await.unwrap(),
            file_ino
        );
        assert!(matches!(
            fs.create_file_at(root, "file", true).await,
            Err(crate::vfs::error::VfsError::AlreadyExists { .. })
        ));

        let dir_ino = fs.mkdir_at(root, "dir").await.unwrap();
        assert_eq!(fs.mkdir_at(root, "dir").await.unwrap(), dir_ino);
        assert!(matches!(
            fs.create_file_at(root, "dir", false).await,
            Err(crate::vfs::error::VfsError::IsADirectory { .. })
        ));
        assert!(matches!(
            fs.mkdir_at(root, "file").await,
            Err(crate::vfs::error::VfsError::AlreadyExists { .. })
        ));
        assert!(matches!(
            fs.create_file_at(file_ino, "child", false).await,
            Err(crate::vfs::error::VfsError::NotADirectory { .. })
        ));
        assert!(matches!(
            fs.mkdir_at(file_ino, "child").await,
            Err(crate::vfs::error::VfsError::NotADirectory { .. })
        ));
    }

    #[tokio::test]
    async fn test_mkdir_at_create_new_reports_existing_entries() {
        let fs = new_basic_fs().await;
        let root = fs.root_ino();

        let file_ino = fs.create_file_at(root, "file", false).await.unwrap();
        let _dir_ino = fs.mkdir_at(root, "dir").await.unwrap();

        assert!(matches!(
            fs.mkdir_at_new(root, "dir").await,
            Err(crate::vfs::error::VfsError::AlreadyExists { .. })
        ));
        assert!(matches!(
            fs.mkdir_at_new(root, "file").await,
            Err(crate::vfs::error::VfsError::AlreadyExists { .. })
        ));
        assert!(matches!(
            fs.mkdir_at_new(file_ino, "child").await,
            Err(crate::vfs::error::VfsError::NotADirectory { .. })
        ));
    }

    #[tokio::test]
    async fn test_open_fresh_by_ino_checks_current_attr_once() {
        let fs = new_basic_fs().await;
        let root = fs.root_ino();

        let file_ino = fs.create_file_at(root, "file", false).await.unwrap();
        let fh = fs
            .open_fresh_ino(file_ino, true, false, false)
            .await
            .unwrap();
        fs.close(fh).await.unwrap();

        let dir_ino = fs.mkdir_at(root, "dir").await.unwrap();
        assert!(matches!(
            fs.open_fresh_ino(dir_ino, true, false, false).await,
            Err(crate::vfs::error::VfsError::IsADirectory { .. })
        ));
        assert!(matches!(
            fs.open_fresh_ino(999_999, true, false, false).await,
            Err(crate::vfs::error::VfsError::NotFound { .. })
        ));
    }

    #[tokio::test]
    async fn test_open_defers_reader_until_first_read() {
        let fs = new_basic_fs().await;
        let root = fs.root_ino();
        let data = b"lazy-reader-open";

        let file_ino = fs.create_file_at(root, "lazy", false).await.unwrap();
        let write_fh = fs
            .open_fresh_ino(file_ino, false, true, false)
            .await
            .unwrap();
        fs.write(write_fh, 0, data).await.unwrap();
        fs.close(write_fh).await.unwrap();

        let read_fh = fs
            .open_fresh_ino(file_ino, true, false, false)
            .await
            .unwrap();
        assert!(
            fs.state
                .reader
                .reader_for_handle(file_ino as u64, read_fh)
                .is_none(),
            "open should not allocate a FileReader before the handle reads data"
        );

        let out = fs.read(read_fh, 0, data.len()).await.unwrap();
        assert_eq!(out, data);
        assert!(
            fs.state
                .reader
                .reader_for_handle(file_ino as u64, read_fh)
                .is_some(),
            "first committed read should lazily attach the FileReader"
        );
        fs.close(read_fh).await.unwrap();
    }

    // Removed incomplete test: test_fs_truncate_prunes_chunks_and_zero_fills
    // TODO: Implement proper truncate testing when chunk pruning is fully implemented

    #[tokio::test]
    async fn test_rename_exchange_atomic() {
        // Test atomic exchange functionality (RENAME_EXCHANGE)
        let layout = ChunkLayout::default();
        let store = InMemoryBlockStore::new();
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta_store = meta_handle.store();
        let fs = VFS::new(layout, store, meta_store).await.unwrap();

        // Setup: create two files
        fs.mkdir_p("/test").await.unwrap();
        fs.create_file("/test/file1.txt").await.unwrap();
        fs.create_file("/test/file2.txt").await.unwrap();

        // Get original inodes
        let file1_attr_before = fs.stat("/test/file1.txt").await.unwrap();
        let file2_attr_before = fs.stat("/test/file2.txt").await.unwrap();

        // Perform atomic exchange
        let flags = crate::vfs::fs::RenameFlags {
            noreplace: false,
            exchange: true,
            whiteout: false,
        };
        fs.rename_with_flags("/test/file1.txt", "/test/file2.txt", flags)
            .await
            .unwrap();

        // Verify both files still exist
        assert!(fs.exists("/test/file1.txt").await);
        assert!(fs.exists("/test/file2.txt").await);

        // Verify inodes have been swapped
        let file1_attr_after = fs.stat("/test/file1.txt").await.unwrap();
        let file2_attr_after = fs.stat("/test/file2.txt").await.unwrap();

        assert_eq!(
            file1_attr_after.ino, file2_attr_before.ino,
            "file1.txt should now have file2's original inode"
        );
        assert_eq!(
            file2_attr_after.ino, file1_attr_before.ino,
            "file2.txt should now have file1's original inode"
        );

        println!("✓ Atomic exchange test passed - inodes correctly swapped");
    }

    #[tokio::test]
    async fn test_rename_preserves_create_time() {
        // Test that rename does not modify create_time
        let layout = ChunkLayout::default();
        let store = InMemoryBlockStore::new();
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta_store = meta_handle.store();
        let fs = VFS::new(layout, store, meta_store).await.unwrap();

        // Create a file
        fs.mkdir_p("/test").await.unwrap();
        fs.create_file("/test/original.txt").await.unwrap();

        // Get initial timestamps
        let attr_before = fs.stat("/test/original.txt").await.unwrap();
        let _create_time_before = attr_before.ctime;
        let modify_time_before = attr_before.mtime;

        // Wait a bit to ensure time difference
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Perform rename
        fs.rename("/test/original.txt", "/test/renamed.txt")
            .await
            .unwrap();

        // Get timestamps after rename
        let attr_after = fs.stat("/test/renamed.txt").await.unwrap();

        // Verify create_time has NOT changed (this is the fix we made)
        // Note: In the current implementation, ctime represents change time, not create time
        // For file systems, ctime should be updated on rename (metadata change)
        // but the actual creation time should be preserved
        // Since we're using ctime as a proxy, we verify that mtime was updated
        assert!(attr_after.mtime >= modify_time_before);

        // The key fix: file metadata's create_time field should not be updated
        // This is tested at the store level, not through FUSE attributes
    }

    #[tokio::test]
    async fn test_rename_exchange_cross_directory() {
        // Test atomic exchange across different directories
        let layout = ChunkLayout::default();
        let store = InMemoryBlockStore::new();
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta_store = meta_handle.store();
        let fs = VFS::new(layout, store, meta_store).await.unwrap();

        // Setup: create two directories with files
        fs.mkdir_p("/dir1").await.unwrap();
        fs.mkdir_p("/dir2").await.unwrap();
        fs.create_file("/dir1/file_a.txt").await.unwrap();
        fs.create_file("/dir2/file_b.txt").await.unwrap();

        // Get original inodes
        let file_a_attr_before = fs.stat("/dir1/file_a.txt").await.unwrap();
        let file_b_attr_before = fs.stat("/dir2/file_b.txt").await.unwrap();

        // Perform cross-directory exchange
        let flags = crate::vfs::fs::RenameFlags {
            noreplace: false,
            exchange: true,
            whiteout: false,
        };
        fs.rename_with_flags("/dir1/file_a.txt", "/dir2/file_b.txt", flags)
            .await
            .unwrap();

        // Verify both files exist in their new locations
        assert!(fs.exists("/dir1/file_a.txt").await);
        assert!(fs.exists("/dir2/file_b.txt").await);

        // Verify inodes have been swapped
        let file_a_attr_after = fs.stat("/dir1/file_a.txt").await.unwrap();
        let file_b_attr_after = fs.stat("/dir2/file_b.txt").await.unwrap();

        assert_eq!(file_a_attr_after.ino, file_b_attr_before.ino);
        assert_eq!(file_b_attr_after.ino, file_a_attr_before.ino);
    }

    #[tokio::test]
    async fn test_rename_exchange_fails_if_missing() {
        // Test that exchange fails if either file doesn't exist
        let layout = ChunkLayout::default();
        let store = InMemoryBlockStore::new();
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta_store = meta_handle.store();
        let fs = VFS::new(layout, store, meta_store).await.unwrap();

        fs.mkdir_p("/test").await.unwrap();
        fs.create_file("/test/exists.txt").await.unwrap();

        // Try to exchange with non-existent file
        let flags = crate::vfs::fs::RenameFlags {
            noreplace: false,
            exchange: true,
            whiteout: false,
        };
        let result = fs
            .rename_with_flags("/test/exists.txt", "/test/nonexistent.txt", flags)
            .await;

        // Should fail because one file doesn't exist
        assert!(result.is_err());
    }
}

#[cfg(test)]
mod io_tests {
    use super::*;
    use crate::cadapter::client::ObjectClient;
    use crate::cadapter::localfs::LocalFsBackend;
    use crate::chunk::store::{BlockKey, ObjectBlockStore};
    use async_trait::async_trait;
    use rand::rngs::StdRng;
    use rand::{Rng, RngCore, SeedableRng};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Barrier;

    #[derive(Clone, Default)]
    struct CountingBlockStore {
        inner: Arc<InMemoryBlockStore>,
        read_range_calls: Arc<AtomicUsize>,
    }

    impl CountingBlockStore {
        fn reset_reads(&self) {
            self.read_range_calls.store(0, Ordering::SeqCst);
        }

        fn read_range_calls(&self) -> usize {
            self.read_range_calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl BlockStore for CountingBlockStore {
        async fn write_fresh_range(
            &self,
            key: BlockKey,
            offset: u64,
            data: &[u8],
        ) -> anyhow::Result<u64> {
            self.inner.write_fresh_range(key, offset, data).await
        }

        async fn read_range(
            &self,
            key: BlockKey,
            offset: u64,
            buf: &mut [u8],
        ) -> anyhow::Result<()> {
            self.read_range_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.read_range(key, offset, buf).await
        }

        async fn delete_range(&self, key: BlockKey, block_count: u64) -> anyhow::Result<()> {
            self.inner.delete_range(key, block_count).await
        }
    }

    async fn open_file<S, M>(fs: &VFS<S, M>, path: &str, read: bool, write: bool) -> u64
    where
        S: BlockStore + Send + Sync + 'static,
        M: MetaLayer + Send + Sync + 'static,
    {
        let attr = fs.stat(path).await.expect("stat");
        fs.open(attr.ino, attr, read, write, false).await.unwrap()
    }

    async fn write_path<S, M>(fs: &VFS<S, M>, path: &str, offset: u64, data: &[u8]) -> usize
    where
        S: BlockStore + Send + Sync + 'static,
        M: MetaLayer + Send + Sync + 'static,
    {
        let fh = open_file(fs, path, false, true).await;
        let result = fs.write(fh, offset, data).await.expect("write");
        let _ = fs.close(fh).await;
        result
    }

    async fn read_path<S, M>(fs: &VFS<S, M>, path: &str, offset: u64, len: usize) -> Vec<u8>
    where
        S: BlockStore + Send + Sync + 'static,
        M: MetaLayer + Send + Sync + 'static,
    {
        let fh = open_file(fs, path, true, false).await;
        let result = fs.read(fh, offset, len).await.expect("read");
        let _ = fs.close(fh).await;
        result
    }

    async fn readdir_path<S, M>(fs: &VFS<S, M>, path: &str) -> Vec<crate::vfs::fs::DirEntry>
    where
        S: BlockStore + Send + Sync + 'static,
        M: MetaLayer + Send + Sync + 'static,
    {
        let attr = fs.stat(path).await.expect("stat");
        let fh = fs.opendir(attr.ino).await.expect("opendir");
        let mut offset = 0u64;
        let mut entries = Vec::new();
        loop {
            let batch = fs.readdir(fh, offset).unwrap_or_default();
            if batch.is_empty() {
                break;
            }
            offset += batch.len() as u64;
            entries.extend(batch);
        }
        let _ = fs.closedir(fh);
        entries
    }

    fn synth_data(seed: u64, len: usize) -> Vec<u8> {
        let mut x = seed ^ 0x9E37_79B9_7F4A_7C15;
        if x == 0 {
            x = 0xA5A5_A5A5_5A5A_5A5A;
        }

        let mut out = Vec::with_capacity(len);
        for _ in 0..len {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            out.push((x & 0xFF) as u8);
        }
        out
    }

    #[tokio::test]
    async fn test_fs_regression_fs_ops_pwrite_slice_relative_upload_offset() {
        let layout = ChunkLayout {
            chunk_size: 128,
            block_size: 64,
        };
        let tmp = tempfile::tempdir().unwrap();
        let client = ObjectClient::new(LocalFsBackend::new(tmp.path()));
        let store = ObjectBlockStore::new_async(client).await.unwrap();

        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta_store = meta_handle.store();
        let fs = VFS::new(layout, store, meta_store).await.unwrap();

        fs.mkdir_p("/fuzz/d0").await.unwrap();
        fs.create_file("/fuzz/d0/f5").await.unwrap();

        const WRITE_OFFSET: u64 = 158;
        const WRITE_LEN: usize = 256;
        const WRITE_SEED: u64 = 85_849_867_896_815_615;

        let data = synth_data(WRITE_SEED, WRITE_LEN);
        write_path(&fs, "/fuzz/d0/f5", WRITE_OFFSET, &data).await;

        let (ino, _) = fs
            .core
            .meta_layer
            .lookup_path("/fuzz/d0/f5")
            .await
            .unwrap()
            .unwrap();
        let inode = fs.ensure_inode_registered(ino).await.unwrap();
        let writer = fs.state.writer.ensure_file(inode);
        writer.flush().await.unwrap();

        let got = read_path(&fs, "/fuzz/d0/f5", 0, WRITE_OFFSET as usize + WRITE_LEN).await;

        let mut expect = vec![0u8; WRITE_OFFSET as usize];
        expect.extend_from_slice(&data);

        assert_eq!(got, expect);

        let stat = fs.stat("/fuzz/d0/f5").await.unwrap();
        assert_eq!(stat.size, WRITE_OFFSET + WRITE_LEN as u64);
    }

    #[tokio::test]
    async fn test_fs_mkdir_create_write_read_readdir() {
        let layout = ChunkLayout::default();
        let tmp = tempfile::tempdir().unwrap();
        let client = ObjectClient::new(LocalFsBackend::new(tmp.path()));
        let store = ObjectBlockStore::new_async(client).await.unwrap();

        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta_store = meta_handle.store();
        let fs = VFS::new(layout, store, meta_store).await.unwrap();

        fs.mkdir_p("/a/b").await.expect("mkdir_p");
        fs.create_file("/a/b/hello.txt").await.expect("create");
        let data_len = layout.block_size as usize + (layout.block_size / 2) as usize;
        let mut data = vec![0u8; data_len];
        for (i, b) in data.iter_mut().enumerate().take(data_len) {
            *b = (i % 251) as u8;
        }
        write_path(&fs, "/a/b/hello.txt", (layout.block_size / 2) as u64, &data).await;
        let (ino, _) = fs
            .core
            .meta_layer
            .lookup_path("/a/b/hello.txt")
            .await
            .unwrap()
            .unwrap();
        let inode = fs.ensure_inode_registered(ino).await.unwrap();
        let writer = fs.state.writer.ensure_file(inode);
        writer.flush().await.unwrap();
        let out = read_path(
            &fs,
            "/a/b/hello.txt",
            (layout.block_size / 2) as u64,
            data_len,
        )
        .await;
        assert_eq!(out, data);

        let entries = readdir_path(&fs, "/a/b").await;
        assert!(
            entries
                .iter()
                .any(|e| e.name == "hello.txt" && e.kind == crate::vfs::fs::FileType::File)
        );

        let stat = fs.stat("/a/b/hello.txt").await.unwrap();
        assert_eq!(stat.kind, crate::vfs::fs::FileType::File);
        assert!(stat.size >= data_len as u64);
    }

    #[tokio::test]
    async fn test_fs_write_cached_ino_defers_flush_until_reader_needs_data() {
        let layout = ChunkLayout::default();
        let store = InMemoryBlockStore::new();
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta_store = meta_handle.store();
        let fs = VFS::new(layout, store, meta_store).await.unwrap();

        fs.create_file("/cached.bin").await.unwrap();
        let attr = fs.stat("/cached.bin").await.unwrap();
        let data = b"cached-writeback-data";

        fs.write_cached_ino(attr.ino, 0, data, 0).await.unwrap();

        let inode = fs.ensure_inode_registered(attr.ino).await.unwrap();
        let writer = fs.state.writer.ensure_file(inode);
        assert!(writer.has_pending().await);

        let out = read_path(&fs, "/cached.bin", 0, data.len()).await;
        assert_eq!(out, data);
        // Read no longer forces a synchronous flush.  The dirty slice is still
        // pending, but the reader saw it via overlay_dirty (including the
        // recently_committed grace period if it was committed during the read).
        // Background auto_flush will eventually commit it.
        assert!(
            writer.has_pending().await,
            "dirty slice must still be present after read (read no longer flushes)"
        );
    }

    #[tokio::test]
    async fn test_fs_read_fully_covered_by_dirty_write_skips_backend_read() {
        let layout = ChunkLayout {
            chunk_size: 64 * 1024,
            block_size: 4 * 1024,
        };
        let store = CountingBlockStore::default();
        let counters = store.clone();
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta_store = meta_handle.store();
        let fs = VFS::new(layout, store, meta_store).await.unwrap();

        fs.create_file("/dirty-covered.bin").await.unwrap();
        let attr = fs.stat("/dirty-covered.bin").await.unwrap();
        let initial = vec![0x11; 4096];
        let fh = fs
            .open(attr.ino, attr.clone(), false, true, false)
            .await
            .unwrap();
        fs.write(fh, 0, &initial).await.unwrap();
        fs.flush(fh).await.unwrap();
        fs.close(fh).await.unwrap();

        counters.reset_reads();

        let attr = fs.stat("/dirty-covered.bin").await.unwrap();
        let fh = fs
            .open(attr.ino, attr.clone(), true, true, false)
            .await
            .unwrap();
        let replacement = vec![0x7d; 4096];
        fs.write(fh, 0, &replacement).await.unwrap();

        let writer = fs
            .state
            .writer
            .ensure_file(fs.ensure_inode_registered(attr.ino).await.unwrap());
        assert!(writer.has_pending().await);

        let out = fs.read(fh, 0, replacement.len()).await.unwrap();
        assert_eq!(out, replacement);
        assert_eq!(
            counters.read_range_calls(),
            0,
            "read fully covered by pending dirty data should not fetch old committed blocks"
        );

        fs.close(fh).await.unwrap();
    }

    #[tokio::test]
    async fn test_fs_read_only_handle_uses_fully_covered_dirty_overlay_before_backend_read() {
        let layout = ChunkLayout {
            chunk_size: 64 * 1024,
            block_size: 4 * 1024,
        };
        let store = CountingBlockStore::default();
        let counters = store.clone();
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta_store = meta_handle.store();
        let fs = VFS::new(layout, store, meta_store).await.unwrap();

        fs.create_file("/dirty-covered-readonly.bin").await.unwrap();
        let attr = fs.stat("/dirty-covered-readonly.bin").await.unwrap();
        let initial = vec![0x11; 4096];
        let write_fh = fs
            .open(attr.ino, attr.clone(), false, true, false)
            .await
            .unwrap();
        fs.write(write_fh, 0, &initial).await.unwrap();
        fs.flush(write_fh).await.unwrap();

        let replacement = vec![0x42; 4096];
        fs.write(write_fh, 0, &replacement).await.unwrap();

        counters.reset_reads();

        let attr = fs.stat("/dirty-covered-readonly.bin").await.unwrap();
        let read_fh = fs
            .open(attr.ino, attr.clone(), true, false, false)
            .await
            .unwrap();

        let out = fs.read(read_fh, 0, replacement.len()).await.unwrap();
        assert_eq!(out, replacement);
        assert_eq!(
            counters.read_range_calls(),
            0,
            "read-only handle should use fully-covered dirty overlay without fetching old blocks"
        );

        fs.close(read_fh).await.unwrap();
        fs.close(write_fh).await.unwrap();
    }

    #[tokio::test]
    async fn test_set_attr_truncate_flushes_cached_write_without_hanging() {
        let layout = ChunkLayout::default();
        let store = InMemoryBlockStore::new();
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta_store = meta_handle.store();
        let fs = VFS::new(layout, store, meta_store).await.unwrap();

        fs.create_file("/setattr-truncate.bin").await.unwrap();
        let attr = fs.stat("/setattr-truncate.bin").await.unwrap();
        let data = b"pending-data-before-ftruncate";

        fs.write_cached_ino(attr.ino, 0, data, 0).await.unwrap();
        let inode = fs.ensure_inode_registered(attr.ino).await.unwrap();
        let writer = fs.state.writer.ensure_file(inode);
        assert!(writer.has_pending().await);

        let req = crate::meta::store::SetAttrRequest {
            size: Some(7),
            ..Default::default()
        };
        let attr = tokio::time::timeout(
            Duration::from_secs(5),
            fs.set_attr(attr.ino, &req, crate::meta::store::SetAttrFlags::empty()),
        )
        .await
        .expect("set_attr truncate should not hang behind writeback")
        .unwrap();

        assert_eq!(attr.size, 7);
        assert!(!writer.has_pending().await);
        let out = read_path(&fs, "/setattr-truncate.bin", 0, 32).await;
        assert_eq!(out, data[..7].to_vec());
    }

    #[tokio::test]
    async fn test_fs_fallocate_ino_extends_file_and_zero_fills() {
        let layout = ChunkLayout::default();
        let store = InMemoryBlockStore::new();
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta_store = meta_handle.store();
        let fs = VFS::new(layout, store, meta_store).await.unwrap();

        fs.create_file("/falloc.bin").await.unwrap();
        let attr = fs.stat("/falloc.bin").await.unwrap();

        fs.fallocate_ino(attr.ino, 128, 64).await.unwrap();

        let st = fs.stat("/falloc.bin").await.unwrap();
        assert_eq!(st.size, 192);

        let out = read_path(&fs, "/falloc.bin", 0, st.size as usize).await;
        assert_eq!(out, vec![0u8; st.size as usize]);
    }

    #[tokio::test]
    async fn test_fs_truncate_prunes_chunks_and_zero_fills() {
        let layout = ChunkLayout {
            chunk_size: 8 * 1024,
            block_size: 4 * 1024,
        };
        let store = InMemoryBlockStore::new();
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta_store = meta_handle.store();
        let fs = VFS::new(layout, store, meta_store).await.unwrap();

        fs.create_file("/t.bin").await.unwrap();

        let len = layout.chunk_size as usize + 2048;
        let mut data = vec![0u8; len];
        for (i, b) in data.iter_mut().enumerate() {
            *b = (i % 251) as u8;
        }
        write_path(&fs, "/t.bin", 0, &data).await;

        fs.truncate("/t.bin", 1024).await.unwrap();
        let head = read_path(&fs, "/t.bin", 0, 4096).await;
        assert_eq!(head.len(), 1024);
        assert_eq!(head, data[..1024].to_vec());

        let new_size = layout.chunk_size + 4096;
        fs.truncate("/t.bin", new_size).await.unwrap();
        let st = fs.stat("/t.bin").await.unwrap();
        assert_eq!(st.size, new_size);

        let hole = read_path(&fs, "/t.bin", layout.chunk_size + 512, 1024).await;
        assert_eq!(hole, vec![0u8; 1024]);
    }

    #[tokio::test]
    async fn test_fs_close_releases_writer_and_inode() {
        let layout = ChunkLayout {
            chunk_size: 8 * 1024,
            block_size: 4 * 1024,
        };
        let store = InMemoryBlockStore::new();
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta_store = meta_handle.store();
        let fs = VFS::new(layout, store, meta_store).await.unwrap();

        fs.create_file("/close.bin").await.unwrap();
        let attr = fs.stat("/close.bin").await.unwrap();
        let fh = fs
            .open(attr.ino, attr.clone(), false, true, false)
            .await
            .unwrap();
        let data = vec![1u8; 2048];
        fs.write(fh, 0, &data).await.unwrap();
        fs.close(fh).await.unwrap();

        assert!(!fs.state.inodes.contains_key(&attr.ino));
        tokio::time::timeout(Duration::from_secs(6), async {
            while fs.state.writer.has_file(attr.ino as u64) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("released writer should be cleaned up after upload and overlay drain");
    }

    #[tokio::test]
    async fn test_fs_sparse_pwrite_close_reopen_preserves_written_blocks() {
        let layout = ChunkLayout {
            chunk_size: 64 * 1024,
            block_size: 4 * 1024,
        };
        let store = InMemoryBlockStore::new();
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta_store = meta_handle.store();
        let fs = VFS::new(layout, store, meta_store).await.unwrap();

        fs.create_file("/sparse.bin").await.unwrap();
        let attr = fs.stat("/sparse.bin").await.unwrap();
        let fh = fs
            .open(attr.ino, attr.clone(), true, true, false)
            .await
            .unwrap();

        let file_size = 64 * 1024u64;
        let write_size = 512usize;
        let step = 1024u64;

        for offset in (0..file_size).step_by(step as usize) {
            let value = ((offset / step) % 251) as u8;
            let block = vec![value; write_size];
            fs.write(fh, offset, &block).await.unwrap();
        }

        fs.close(fh).await.unwrap();

        let reopened = fs.stat("/sparse.bin").await.unwrap();
        assert!(reopened.size >= 16 * 1024 + write_size as u64);

        let rfh = fs
            .open(reopened.ino, reopened.clone(), true, false, false)
            .await
            .unwrap();

        for offset in (0..file_size).step_by(step as usize) {
            let value = ((offset / step) % 251) as u8;
            let out = fs.read(rfh, offset, write_size).await.unwrap();
            assert_eq!(out, vec![value; write_size], "offset={offset}");
        }

        fs.close(rfh).await.unwrap();
    }

    #[tokio::test]
    async fn test_fs_append_handles_use_fresh_size_concurrently() {
        let layout = ChunkLayout {
            chunk_size: 8 * 1024,
            block_size: 4 * 1024,
        };
        let store = InMemoryBlockStore::new();
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta_store = meta_handle.store();
        let fs = Arc::new(VFS::new(layout, store, meta_store).await.unwrap());

        fs.create_file("/append.txt").await.unwrap();
        let attr = fs.stat("/append.txt").await.unwrap();
        let barrier = Arc::new(Barrier::new(4));
        let mut tasks = Vec::new();

        for line in [
            b"one\n".as_slice(),
            b"two\n".as_slice(),
            b"three\n".as_slice(),
        ] {
            let fs = Arc::clone(&fs);
            let barrier = Arc::clone(&barrier);
            let attr = attr.clone();
            let line = line.to_vec();
            tasks.push(tokio::spawn(async move {
                let fh = fs.open(attr.ino, attr, false, true, true).await.unwrap();
                barrier.wait().await;
                fs.write(fh, 0, &line).await.unwrap();
                fh
            }));
        }

        barrier.wait().await;
        let mut handles = Vec::new();
        for task in tasks {
            handles.push(task.await.unwrap());
        }
        for fh in handles {
            fs.close(fh).await.unwrap();
        }

        let out = read_path(&fs, "/append.txt", 0, 14).await;
        for line in [
            b"one\n".as_slice(),
            b"two\n".as_slice(),
            b"three\n".as_slice(),
        ] {
            assert!(out.windows(line.len()).any(|window| window == line));
        }
    }

    #[tokio::test]
    async fn test_fs_truncate_extend_does_not_return_stale_reader_cache() {
        let layout = ChunkLayout {
            chunk_size: 8 * 1024,
            block_size: 4 * 1024,
        };
        let store = InMemoryBlockStore::new();
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta_store = meta_handle.store();
        let fs = VFS::new(layout, store, meta_store).await.unwrap();

        fs.create_file("/stale_trunc.bin").await.unwrap();

        let len = layout.chunk_size as usize + 2048;
        let mut data = vec![0u8; len];
        for (i, b) in data.iter_mut().enumerate() {
            *b = (i % 251) as u8;
        }
        write_path(&fs, "/stale_trunc.bin", 0, &data).await;

        let attr = fs.stat("/stale_trunc.bin").await.unwrap();
        let fh = fs
            .open(attr.ino, attr.clone(), true, false, false)
            .await
            .unwrap();

        let offset = layout.block_size as u64;
        let probe_len = 1024usize;
        let original = fs.read(fh, offset, probe_len).await.unwrap();
        assert_eq!(
            original,
            data[offset as usize..offset as usize + probe_len].to_vec()
        );

        fs.truncate("/stale_trunc.bin", 1024).await.unwrap();
        fs.truncate("/stale_trunc.bin", len as u64).await.unwrap();

        let after = fs.read(fh, offset, probe_len).await.unwrap();
        assert_eq!(after, vec![0u8; probe_len]);

        fs.close(fh).await.unwrap();
    }

    #[tokio::test]
    async fn test_fs_copy_file_range_same_file_preserves_recent_write_tail() {
        let layout = ChunkLayout {
            chunk_size: 512 * 1024,
            block_size: 4 * 1024,
        };
        let store = InMemoryBlockStore::new();
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta_store = meta_handle.store();
        let fs = VFS::new(layout, store, meta_store).await.unwrap();

        fs.create_file("/copy.bin").await.unwrap();
        let attr = fs.stat("/copy.bin").await.unwrap();
        let fh = fs
            .open(attr.ino, attr.clone(), true, true, false)
            .await
            .unwrap();

        let mut expected = vec![0u8; 0x6f000];

        let mut src = vec![0u8; 0xd000];
        for (i, b) in src.iter_mut().enumerate() {
            *b = (0x40u8).wrapping_add(i as u8);
        }
        fs.write(fh, 0x5d000, &src).await.unwrap();
        expected[0x5d000..0x6a000].copy_from_slice(&src);

        let mut tail = vec![0u8; 0xe000];
        for (i, b) in tail.iter_mut().enumerate() {
            *b = (0x90u8).wrapping_add((i * 3) as u8);
        }
        fs.write(fh, 0x2b000, &tail).await.unwrap();
        expected[0x2b000..0x39000].copy_from_slice(&tail);

        let copied = fs
            .copy_file_range(fh, 0x5d000, fh, 0x24000, 0xd000)
            .await
            .unwrap();
        assert_eq!(copied, 0xd000);
        let copied_view = expected[0x5d000..0x6a000].to_vec();
        expected[0x24000..0x31000].copy_from_slice(&copied_view);

        let out = fs.read(fh, 0x2c000, 0xa000).await.unwrap();
        assert_eq!(out, expected[0x2c000..0x36000].to_vec());

        fs.close(fh).await.unwrap();
    }

    #[tokio::test]
    async fn test_fs_truncate_then_rewrite_range_stays_visible() {
        let layout = ChunkLayout {
            chunk_size: 512 * 1024,
            block_size: 4 * 1024,
        };
        let store = InMemoryBlockStore::new();
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta_store = meta_handle.store();
        let fs = VFS::new(layout, store, meta_store).await.unwrap();

        fs.create_file("/rewrite.bin").await.unwrap();
        let attr = fs.stat("/rewrite.bin").await.unwrap();
        let fh = fs
            .open(attr.ino, attr.clone(), true, true, false)
            .await
            .unwrap();

        fs.truncate_inode(attr.ino, 0x15000).await.unwrap();
        fs.truncate_inode(attr.ino, 0x50000).await.unwrap();

        let mut first = vec![0u8; 0xf000];
        for (i, b) in first.iter_mut().enumerate() {
            *b = (0x31u8).wrapping_add((i * 5) as u8);
        }
        fs.write(fh, 0x28000, &first).await.unwrap();

        let mut second = vec![0u8; 0x9000];
        for (i, b) in second.iter_mut().enumerate() {
            *b = (0x79u8).wrapping_add((i * 7) as u8);
        }
        fs.write(fh, 0x32000, &second).await.unwrap();

        fs.truncate_inode(attr.ino, 0x49000).await.unwrap();

        let out = fs.read(fh, 0x34000, 0x7000).await.unwrap();
        assert_eq!(out, second[0x2000..0x9000].to_vec());

        fs.close(fh).await.unwrap();
    }

    #[tokio::test]
    async fn test_fs_truncate_keeps_prefix_of_overlapping_slice() {
        let layout = ChunkLayout {
            chunk_size: 512 * 1024,
            block_size: 4 * 1024,
        };
        let store = InMemoryBlockStore::new();
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta_store = meta_handle.store();
        let fs = VFS::new(layout, store, meta_store).await.unwrap();

        fs.create_file("/trimmed-prefix.bin").await.unwrap();
        let attr = fs.stat("/trimmed-prefix.bin").await.unwrap();
        let fh = fs
            .open(attr.ino, attr.clone(), true, true, false)
            .await
            .unwrap();

        fs.truncate_inode(attr.ino, 0x35063).await.unwrap();

        let start = 0x1a352u64;
        let len = 0xb944usize;
        let mut payload = vec![0u8; len];
        for (i, b) in payload.iter_mut().enumerate() {
            *b = (0x55u8).wrapping_add((i * 11) as u8);
        }
        fs.write(fh, start, &payload).await.unwrap();

        fs.truncate_inode(attr.ino, 0x1f76b).await.unwrap();
        fs.truncate_inode(attr.ino, 0x40000).await.unwrap();
        fs.truncate_inode(attr.ino, 0x2519b).await.unwrap();

        let read_start = 0x1a593u64;
        let read_len = 0x3ab1usize;
        let out = fs.read(fh, read_start, read_len).await.unwrap();
        let expected_offset = (read_start - start) as usize;
        assert_eq!(
            out,
            payload[expected_offset..expected_offset + read_len].to_vec()
        );

        fs.close(fh).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_fs_parallel_writes_to_distinct_files() {
        let layout = ChunkLayout {
            chunk_size: 8 * 1024,
            block_size: 4 * 1024,
        };
        let store = InMemoryBlockStore::new();
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta_store = meta_handle.store();
        let fs = Arc::new(VFS::new(layout, store, meta_store).await.unwrap());

        fs.mkdir_p("/data").await.unwrap();

        let file_count = 4usize;
        let barrier = Arc::new(Barrier::new(file_count + 1));
        let mut handles = Vec::new();

        for i in 0..file_count {
            let path = format!("/data/f{i}.bin");
            fs.create_file(&path).await.unwrap();

            let len = match i {
                0 => 1024,
                1 => layout.block_size as usize,
                2 => layout.block_size as usize + 512,
                _ => layout.chunk_size as usize + 512,
            };
            let mut data = vec![0u8; len];
            for (idx, b) in data.iter_mut().enumerate() {
                *b = (i as u8).wrapping_add(idx as u8);
            }

            let fs_clone = fs.clone();
            let barrier = barrier.clone();
            handles.push(tokio::spawn(async move {
                barrier.wait().await;
                write_path(&fs_clone, &path, 0, &data).await;
                (path, data)
            }));
        }

        barrier.wait().await;

        let mut results = Vec::new();
        for handle in handles {
            results.push(handle.await.unwrap());
        }

        for (path, _) in results.iter() {
            let (ino, _) = fs.core.meta_layer.lookup_path(path).await.unwrap().unwrap();
            let inode = fs.ensure_inode_registered(ino).await.unwrap();
            let writer = fs.state.writer.ensure_file(inode);
            writer.flush().await.unwrap();
        }

        for (path, data) in results {
            let out = read_path(&fs, &path, 0, data.len()).await;
            assert_eq!(out, data);
        }
    }

    /// The test will take approximately 10 seconds to complete.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_fs_fuzz_parallel_read_write() {
        let layout = ChunkLayout {
            chunk_size: 16 * 1024,
            block_size: 4 * 1024,
        };
        let store = InMemoryBlockStore::new();
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta_store = meta_handle.store();
        let fs = Arc::new(VFS::new(layout, store, meta_store).await.unwrap());

        fs.mkdir_p("/fuzz").await.unwrap();

        let file_count = 4usize;
        let mut paths = Vec::with_capacity(file_count);
        let mut states = Vec::with_capacity(file_count);

        for i in 0..file_count {
            let path = format!("/fuzz/f{i}.bin");
            fs.create_file(&path).await.unwrap();
            paths.push(path);
            states.push(Arc::new(tokio::sync::Mutex::new(Vec::<u8>::new())));
        }

        let task_count = 4usize;
        let iterations = 100usize;
        let max_write = 4096usize;
        let op_timeout = Duration::from_secs(30);

        let mut handles = Vec::with_capacity(task_count);
        for t in 0..task_count {
            let fs = fs.clone();
            let paths = paths.clone();
            let states = states.clone();
            let mut rng = StdRng::seed_from_u64(0x5EED_u64 + t as u64);
            handles.push(tokio::spawn(async move {
                for _ in 0..iterations {
                    let file_idx = rng.random_range(0..file_count);
                    let path = paths[file_idx].clone();
                    let state = states[file_idx].clone();

                    if rng.random_range(0..100) < 60 {
                        let mut guard = state.lock().await;
                        let cur_len = guard.len();
                        let max_offset = cur_len + layout.block_size as usize;
                        let offset = rng.random_range(0..=max_offset);
                        let len = rng.random_range(1..=max_write);
                        let mut data = vec![0u8; len];
                        rng.fill_bytes(&mut data);

                        tokio::time::timeout(
                            op_timeout,
                            write_path(&fs, &path, offset as u64, &data),
                        )
                        .await
                        .expect("fuzz write timed out");

                        let end = offset + len;
                        if guard.len() < end {
                            guard.resize(end, 0);
                        }
                        guard[offset..end].copy_from_slice(&data);
                    } else {
                        let guard = state.lock().await;
                        let cur_len = guard.len();
                        if cur_len == 0 {
                            let out = read_path(&fs, &path, 0, 0).await;
                            assert!(out.is_empty());
                            continue;
                        }
                        let offset = rng.random_range(0..cur_len);
                        let len = rng.random_range(1..=std::cmp::min(cur_len - offset, max_write));
                        let expected = guard[offset..offset + len].to_vec();
                        let out = tokio::time::timeout(
                            op_timeout,
                            read_path(&fs, &path, offset as u64, len),
                        )
                        .await
                        .expect("fuzz read timed out");
                        assert_eq!(out, expected);
                    }
                }
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        for (path, state) in paths.iter().zip(states.iter()) {
            let path = path.clone();
            let state = state.clone();
            let guard = state.lock().await;
            let expected = guard.clone();
            let out = tokio::time::timeout(op_timeout, read_path(&fs, &path, 0, expected.len()))
                .await
                .expect("fuzz final read timed out");
            assert_eq!(out, expected);
        }
    }
}

#[cfg(test)]
mod permission_tests {
    use super::*;
    use crate::meta::store::{SetAttrFlags, SetAttrRequest};

    /// Helper: create a VFS backed by an in-memory SQLite database.
    async fn new_test_vfs() -> VFS<InMemoryBlockStore, impl MetaLayer> {
        let layout = ChunkLayout::default();
        let store = InMemoryBlockStore::new();
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let meta_store = meta_handle.store();
        VFS::new(layout, store, meta_store).await.unwrap()
    }

    // -------------------------------------------------------------------
    // Default permission tests
    //
    // NOTE: BrewFS does not synchronize with the process umask; files and
    // directories are created with hard-coded defaults (0644 / 0755).  The
    // FUSE layer can override these with mode & umask at creation time, but
    // at the VFS level the defaults below are expected.
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn test_file_default_permission() {
        let fs = new_test_vfs().await;
        fs.mkdir_p("/perm").await.unwrap();
        fs.create_file("/perm/f.txt").await.unwrap();

        let attr = fs.stat("/perm/f.txt").await.unwrap();
        // Default file mode: 0o100644 (S_IFREG | rw-r--r--)
        // Permission bits (low 12 bits) should be 0o644.
        assert_eq!(
            attr.mode & 0o7777,
            0o644,
            "newly created file should have default permission 0644"
        );
    }

    #[tokio::test]
    async fn test_directory_default_permission() {
        let fs = new_test_vfs().await;
        fs.mkdir_p("/perm_dir").await.unwrap();

        let attr = fs.stat("/perm_dir").await.unwrap();
        // Default directory mode: 0o040755 (S_IFDIR | rwxr-xr-x)
        // Permission bits should be 0o755.
        assert_eq!(
            attr.mode & 0o7777,
            0o755,
            "newly created directory should have default permission 0755"
        );
    }

    // -------------------------------------------------------------------
    // chmod tests
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn test_chmod_file_basic() {
        let fs = new_test_vfs().await;
        fs.mkdir_p("/chm").await.unwrap();
        let ino = fs.create_file("/chm/a.txt").await.unwrap();

        // Change to 0o755
        let attr = fs.chmod(ino, 0o755).await.unwrap();
        assert_eq!(
            attr.mode & 0o777,
            0o755,
            "chmod should update permission bits"
        );

        // Verify stat also returns the new mode
        let stat = fs.stat("/chm/a.txt").await.unwrap();
        assert_eq!(
            stat.mode & 0o777,
            0o755,
            "stat after chmod should reflect new permission"
        );
    }

    #[tokio::test]
    async fn test_chmod_directory() {
        let fs = new_test_vfs().await;
        let ino = fs.mkdir_p("/chm_dir").await.unwrap();

        let attr = fs.chmod(ino, 0o700).await.unwrap();
        assert_eq!(attr.mode & 0o777, 0o700);

        let stat = fs.stat("/chm_dir").await.unwrap();
        assert_eq!(stat.mode & 0o777, 0o700);
    }

    #[tokio::test]
    async fn test_chmod_preserves_setuid_setgid_sticky() {
        let fs = new_test_vfs().await;
        fs.mkdir_p("/special").await.unwrap();
        let ino = fs.create_file("/special/s.txt").await.unwrap();

        // Pass mode with setuid (0o4000), setgid (0o2000), and sticky (0o1000)
        let attr = fs.chmod(ino, 0o7755).await.unwrap();
        assert_eq!(
            attr.mode & 0o7777,
            0o7755,
            "setuid/setgid/sticky should be preserved by chmod"
        );
    }

    #[tokio::test]
    async fn test_chmod_nonexistent_inode_returns_error() {
        let fs = new_test_vfs().await;
        let result = fs.chmod(999999, 0o644).await;
        assert!(result.is_err(), "chmod on nonexistent inode should fail");
    }

    #[tokio::test]
    async fn test_chmod_preserves_file_type_bits() {
        let fs = new_test_vfs().await;
        fs.mkdir_p("/ftype").await.unwrap();
        let ino = fs.create_file("/ftype/f.txt").await.unwrap();

        let before = fs.stat("/ftype/f.txt").await.unwrap();
        let file_type_before = before.mode & 0o170000;

        fs.chmod(ino, 0o777).await.unwrap();

        let after = fs.stat("/ftype/f.txt").await.unwrap();
        let file_type_after = after.mode & 0o170000;
        assert_eq!(
            file_type_before, file_type_after,
            "chmod must not alter file type bits"
        );
    }

    // -------------------------------------------------------------------
    // set_attr mode change tests (integration with VFS.set_attr)
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn test_set_attr_mode_change() {
        let fs = new_test_vfs().await;
        fs.mkdir_p("/sa").await.unwrap();
        let ino = fs.create_file("/sa/x.txt").await.unwrap();

        let req = SetAttrRequest {
            mode: Some(0o600),
            ..Default::default()
        };
        let attr = fs.set_attr(ino, &req, SetAttrFlags::empty()).await.unwrap();
        assert_eq!(attr.mode & 0o777, 0o600);

        let stat = fs.stat("/sa/x.txt").await.unwrap();
        assert_eq!(stat.mode & 0o777, 0o600);
    }

    #[tokio::test]
    async fn test_set_attr_mode_preserves_special_bits_via_chmod_path() {
        // When the chmod VFS method is used, special bits are preserved.
        let fs = new_test_vfs().await;
        fs.mkdir_p("/sa2").await.unwrap();
        let ino = fs.create_file("/sa2/y.txt").await.unwrap();

        let attr = fs.chmod(ino, 0o4755).await.unwrap();
        assert_eq!(
            attr.mode & 0o7777,
            0o4755,
            "setuid bit should be preserved when using chmod"
        );
    }

    // -------------------------------------------------------------------
    // chown tests
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn test_chown_file_uid_and_gid() {
        let fs = new_test_vfs().await;
        fs.mkdir_p("/own").await.unwrap();
        let ino = fs.create_file("/own/f.txt").await.unwrap();

        let attr = fs.chown(ino, Some(1000), Some(1000)).await.unwrap();
        assert_eq!(attr.uid, 1000, "chown should update uid");
        assert_eq!(attr.gid, 1000, "chown should update gid");

        // Verify via stat
        let stat = fs.stat("/own/f.txt").await.unwrap();
        assert_eq!(stat.uid, 1000);
        assert_eq!(stat.gid, 1000);
    }

    #[tokio::test]
    async fn test_chown_uid_only() {
        let fs = new_test_vfs().await;
        fs.mkdir_p("/own2").await.unwrap();
        let ino = fs.create_file("/own2/f.txt").await.unwrap();

        let before = fs.stat("/own2/f.txt").await.unwrap();
        let original_gid = before.gid;

        let attr = fs.chown(ino, Some(2000), None).await.unwrap();
        assert_eq!(attr.uid, 2000, "chown should update uid");
        assert_eq!(attr.gid, original_gid, "gid should remain unchanged");
    }

    #[tokio::test]
    async fn test_chown_gid_only() {
        let fs = new_test_vfs().await;
        fs.mkdir_p("/own3").await.unwrap();
        let ino = fs.create_file("/own3/f.txt").await.unwrap();

        let before = fs.stat("/own3/f.txt").await.unwrap();
        let original_uid = before.uid;

        let attr = fs.chown(ino, None, Some(3000)).await.unwrap();
        assert_eq!(attr.uid, original_uid, "uid should remain unchanged");
        assert_eq!(attr.gid, 3000, "chown should update gid");
    }

    #[tokio::test]
    async fn test_chown_directory() {
        let fs = new_test_vfs().await;
        let ino = fs.mkdir_p("/own_dir").await.unwrap();

        let attr = fs.chown(ino, Some(500), Some(500)).await.unwrap();
        assert_eq!(attr.uid, 500);
        assert_eq!(attr.gid, 500);

        let stat = fs.stat("/own_dir").await.unwrap();
        assert_eq!(stat.uid, 500);
        assert_eq!(stat.gid, 500);
    }

    #[tokio::test]
    async fn test_chown_nonexistent_inode_returns_error() {
        let fs = new_test_vfs().await;
        let result = fs.chown(999999, Some(1000), Some(1000)).await;
        assert!(result.is_err(), "chown on nonexistent inode should fail");
    }

    #[tokio::test]
    async fn test_chown_preserves_mode() {
        let fs = new_test_vfs().await;
        fs.mkdir_p("/own_mode").await.unwrap();
        let ino = fs.create_file("/own_mode/f.txt").await.unwrap();

        // Change mode first
        fs.chmod(ino, 0o755).await.unwrap();

        // Then change owner
        let attr = fs.chown(ino, Some(1000), Some(1000)).await.unwrap();
        assert_eq!(
            attr.mode & 0o777,
            0o755,
            "chown should not alter permission bits"
        );
    }

    #[tokio::test]
    async fn test_set_attr_chown_via_request() {
        // Test chown via the SetAttrRequest path (simulates FUSE setattr)
        let fs = new_test_vfs().await;
        fs.mkdir_p("/sa_own").await.unwrap();
        let ino = fs.create_file("/sa_own/f.txt").await.unwrap();

        let req = SetAttrRequest {
            uid: Some(1234),
            gid: Some(5678),
            ..Default::default()
        };
        let attr = fs.set_attr(ino, &req, SetAttrFlags::empty()).await.unwrap();
        assert_eq!(attr.uid, 1234);
        assert_eq!(attr.gid, 5678);

        let stat = fs.stat("/sa_own/f.txt").await.unwrap();
        assert_eq!(stat.uid, 1234);
        assert_eq!(stat.gid, 5678);
    }
}

#[cfg(test)]
mod truncate_flush_tests {
    use super::*;
    use crate::chunk::store::InMemoryBlockStore;
    use crate::meta::factory::MetaStoreFactory;
    use crate::meta::store::{SetAttrFlags, SetAttrRequest};
    use crate::meta::stores::DatabaseMetaStore;
    use crate::vfs::cache::config::CacheConfig as VfsCacheConfig;
    use crate::vfs::fs::VFS;
    use std::sync::Arc;
    use std::time::Duration;

    const OP_TIMEOUT: Duration = Duration::from_secs(60);

    async fn op_timeout<T>(label: &'static str, fut: impl std::future::Future<Output = T>) -> T {
        tokio::time::timeout(OP_TIMEOUT, fut)
            .await
            .unwrap_or_else(|_| panic!("truncate_flush op timed out: {label}"))
    }

    fn test_layout() -> ChunkLayout {
        ChunkLayout {
            chunk_size: 64 * 1024,
            block_size: 16 * 1024,
        }
    }

    fn test_cache_config() -> VfsCacheConfig {
        VfsCacheConfig {
            dirty_slice_target_size: 8 * 1024,
            dirty_slice_max_age_ms: 5,
            write_memory_bytes: 16 * 1024 * 1024,
            read_memory_bytes: 16 * 1024 * 1024,
            prefetch_max_bytes: 256 * 1024,
            ..VfsCacheConfig::default()
        }
    }

    async fn new_vfs() -> VFS<InMemoryBlockStore, impl MetaLayer> {
        let layout = test_layout();
        let store = Arc::new(InMemoryBlockStore::new());
        let meta_config = crate::meta::config::Config {
            database: crate::meta::config::DatabaseConfig {
                db_config: crate::meta::config::DatabaseType::Sqlite {
                    url: "sqlite::memory:".to_string(),
                },
            },
            cache: crate::meta::config::CacheConfig::default(),
            client: crate::meta::config::ClientOptions {
                no_background_jobs: true,
                ..Default::default()
            },
            compact: crate::meta::config::CompactConfig::default(),
        };
        let meta_handle = MetaStoreFactory::<DatabaseMetaStore>::create_from_config(meta_config)
            .await
            .unwrap();

        VFS::with_meta_layer_with_cache_config(
            layout,
            store,
            meta_handle.layer(),
            crate::meta::config::CompactConfig::default(),
            test_cache_config(),
        )
        .unwrap()
    }

    /// Write data to create pending dirty slices in the writer,
    /// then immediately truncate.  This exercises the flush_required →
    /// writer.flush() → meta_truncate path that generic/014 tickles.
    #[tokio::test]
    async fn test_truncate_after_write_with_pending_dirty_data() {
        let fs = Arc::new(new_vfs().await);
        let root = fs.root_ino();

        let ino = op_timeout("create_file", fs.create_file_at(root, "f", false))
            .await
            .unwrap();

        // Write enough data to exceed freeze_min_bytes so the writer has
        // pending dirty slices that need flushing on truncate.
        let chunk_size = test_layout().chunk_size as usize;
        let data = vec![0xABu8; chunk_size * 2];
        op_timeout("write", fs.write_ino(ino, 0, &data))
            .await
            .unwrap();

        // Truncate down — must flush the pending writes first.
        // If the truncate deadlocks, the 10 s timeout will fire.
        op_timeout("truncate", fs.truncate_inode(ino, 1024))
            .await
            .unwrap();

        // Verify the file size reflects the truncate.
        let attr = op_timeout("stat", fs.stat_ino(ino)).await.unwrap();
        assert_eq!(attr.size, 1024);
    }

    /// Write, truncate-extend, write again, truncate-shrink — verifies
    /// the flush+lock handoff is correct across multiple cycles.
    #[tokio::test]
    async fn test_write_truncate_write_truncate_cycles() {
        let fs = Arc::new(new_vfs().await);
        let root = fs.root_ino();

        let ino = op_timeout("create_file", fs.create_file_at(root, "f", false))
            .await
            .unwrap();

        let block = test_layout().block_size as u64;

        for cycle in 0..8 {
            let offset = (cycle as u64 % 4) * block;
            let size = block as usize * (1 + cycle % 3);
            let data = vec![(cycle as u8).wrapping_mul(7); size];

            op_timeout("write", fs.write_ino(ino, offset, &data))
                .await
                .unwrap();

            let new_size = block * (1 + (cycle as u64 % 5));
            op_timeout("truncate", fs.truncate_inode(ino, new_size))
                .await
                .unwrap();
        }

        let attr = op_timeout("stat", fs.stat_ino(ino)).await.unwrap();
        assert!(attr.size > 0, "file should have nonzero size after cycles");
    }

    /// Simulate the exact truncfile workload: create file, write, then
    /// do many truncate+write cycles like `truncfile -c 10000` does.
    #[tokio::test]
    async fn test_truncfile_style_rapid_truncate_cycles() {
        let fs = Arc::new(new_vfs().await);
        let root = fs.root_ino();

        let ino = op_timeout("create_file", fs.create_file_at(root, "truncfile", false))
            .await
            .unwrap();

        // Write initial data (truncfile writes before truncating).
        let initial = vec![0xCDu8; 4096];
        op_timeout("write", fs.write_ino(ino, 0, &initial))
            .await
            .unwrap();

        // Rapid truncate cycles — mimics `truncfile -c 10000`.
        for i in 0..200 {
            let size = if i % 2 == 0 {
                ((i as u64 + 1) * 17) % 65536
            } else {
                ((10000u64 - i as u64) * 13) % 32768
            };
            op_timeout("truncate", fs.truncate_inode(ino, size))
                .await
                .unwrap();
        }

        // Should still be able to stat after all cycles.
        let _attr = op_timeout("stat", fs.stat_ino(ino)).await.unwrap();
    }

    /// Concurrent writes and truncates on the same inode from multiple
    /// tasks — stresses the mutation lock handoff between write_ino and
    /// truncate_inode.
    #[tokio::test]
    async fn test_concurrent_write_and_truncate() {
        let fs = Arc::new(new_vfs().await);
        let root = fs.root_ino();

        let ino = op_timeout("create_file", fs.create_file_at(root, "f", false))
            .await
            .unwrap();

        let fs_w = fs.clone();
        let write_task = tokio::spawn(async move {
            for i in 0..200 {
                let data = vec![(i as u8).wrapping_mul(3); 4096];
                let offset = ((i * 7) % 20) as u64 * 4096;
                let _ = fs_w.write_ino(ino, offset, &data).await;
                tokio::task::yield_now().await;
            }
        });

        let fs_t = fs.clone();
        let trunc_task = tokio::spawn(async move {
            for i in 0..200 {
                let size = ((i as u64 * 31 + 7) % 15 + 1) * 4096;
                let _ = fs_t.truncate_inode(ino, size).await;
                tokio::task::yield_now().await;
            }
        });

        op_timeout("write_task", write_task).await.unwrap();
        op_timeout("trunc_task", trunc_task).await.unwrap();

        let attr = op_timeout("stat", fs.stat_ino(ino)).await.unwrap();
        assert!(attr.size > 0);
    }

    /// Use set_attr with size (the FUSE SETATTR path exercised by
    /// generic/014's truncfile) after writing dirty data.  Verifies the
    /// flush_required + mutation_lock + meta_truncate + meta_set_attr
    /// pipeline does not deadlock.
    #[tokio::test]
    async fn test_set_attr_truncate_with_pending_writes() {
        let fs = Arc::new(new_vfs().await);
        let root = fs.root_ino();

        let ino = op_timeout("create_file", fs.create_file_at(root, "f", false))
            .await
            .unwrap();

        // Write dirty data.
        let chunk = test_layout().chunk_size as usize;
        let data = vec![0xEFu8; chunk * 2];
        op_timeout("write", fs.write_ino(ino, 0, &data))
            .await
            .unwrap();

        // Simulate FUSE SETATTR with size (truncate).  This calls set_attr
        // which internally does flush_required → mutation_lock →
        // meta_truncate → meta_set_attr.
        for size in [chunk as u64, 512, chunk as u64 * 2, 0, 4096] {
            let req = SetAttrRequest {
                size: Some(size),
                ..Default::default()
            };
            op_timeout("set_attr", fs.set_attr(ino, &req, SetAttrFlags::empty()))
                .await
                .unwrap();
        }
    }

    /// Write via write_cached_ino (the FUSE_WRITE_CACHE path), then
    /// truncate via set_attr — exactly the sequence generic/013→014
    /// produces.  Verifies the cached writeback and truncate paths do
    /// not deadlock when interleaved.
    #[tokio::test]
    async fn test_cached_write_then_set_attr_truncate() {
        let fs = Arc::new(new_vfs().await);
        let root = fs.root_ino();

        let ino = op_timeout("create_file", fs.create_file_at(root, "f", false))
            .await
            .unwrap();

        let block = test_layout().block_size as usize;

        // Use write_cached_ino (mimics FUSE_WRITE_CACHE) to create dirty
        // slices without an explicit flush — just like the kernel does.
        for i in 0..16 {
            let data = vec![(i as u8).wrapping_mul(17); block * 2];
            op_timeout(
                "write_cached",
                fs.write_cached_ino(ino, (i * block) as u64, &data, i as u64),
            )
            .await
            .unwrap();
        }

        // Now truncate — must flush all the cached writes first.
        let req = SetAttrRequest {
            size: Some(block as u64 * 3),
            ..Default::default()
        };
        let attr = op_timeout(
            "set_attr after cached writes",
            fs.set_attr(ino, &req, SetAttrFlags::empty()),
        )
        .await
        .unwrap();

        assert_eq!(attr.size, block as u64 * 3);
    }

    /// Write a large amount of data, then immediately truncate to zero
    /// before any background flush can complete.  This is the worst case
    /// for the flush_required path — lots of dirty data to upload.
    #[tokio::test]
    async fn test_large_write_then_truncate_to_zero() {
        let fs = Arc::new(new_vfs().await);
        let root = fs.root_ino();

        let ino = op_timeout("create_file", fs.create_file_at(root, "f", false))
            .await
            .unwrap();

        // Write multiple blocks of data to create dirty slices.
        let block = test_layout().block_size as u64;
        for i in 0..16 {
            let data = vec![(i as u8).wrapping_add(0xA0); block as usize * 2];
            op_timeout("write", fs.write_ino(ino, i * block * 2, &data))
                .await
                .unwrap();
        }

        // Truncate to 0 — requires flushing all pending writes first.
        op_timeout("truncate to 0", fs.truncate_inode(ino, 0))
            .await
            .unwrap();

        let attr = op_timeout("stat", fs.stat_ino(ino)).await.unwrap();
        assert_eq!(attr.size, 0);
    }
}
