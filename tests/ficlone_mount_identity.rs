#![cfg(target_os = "linux")]

//! Exercise the real FUSE ioctl callback with kernel-owned source descriptors.
//! A private FUSE mount provides the positive control; no object transport is
//! under test (SQLite metadata and the existing in-memory block store).
use asyncfuse::raw::{Filesystem, Request};
use brewfs::chunk::store::{BlockKey, BlockStore};
use brewfs::fuse::mount::{FuseConcurrencyConfig, mount_vfs_unprivileged};
use brewfs::{
    ChunkLayout, Config, DatabaseConfig, DatabaseMetaStore, DatabaseType, InMemoryBlockStore, VFS,
};
use std::os::fd::AsRawFd;
use std::os::unix::fs::OpenOptionsExt;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

const SOURCE: &[u8] = b"BREWFS-SOURCE";
const ORIGINAL: &[u8] = b"ORIGINAL";

async fn metadata(path: &std::path::Path) -> anyhow::Result<DatabaseMetaStore> {
    Ok(DatabaseMetaStore::from_config(Config {
        database: DatabaseConfig {
            db_config: DatabaseType::Sqlite {
                url: format!("sqlite://{}?mode=rwc", path.join("meta.db").display()),
            },
        },
        cache: Default::default(),
        client: Default::default(),
        compact: Default::default(),
    })
    .await?)
}

#[derive(Default)]
struct ReadGate {
    armed: AtomicBool,
    entered: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}

#[derive(Clone)]
struct PausedReadStore {
    data: Arc<InMemoryBlockStore>,
    gate: Arc<ReadGate>,
}

#[async_trait::async_trait]
impl BlockStore for PausedReadStore {
    async fn write_fresh_range(
        &self,
        key: BlockKey,
        offset: u64,
        data: &[u8],
    ) -> anyhow::Result<u64> {
        self.data.write_fresh_range(key, offset, data).await
    }
    async fn read_range(&self, key: BlockKey, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
        if self.gate.armed.swap(false, Ordering::AcqRel) {
            self.gate.entered.notify_one();
            self.gate.resume.notified().await;
        }
        self.data.read_range(key, offset, buf).await
    }
    async fn delete_range(&self, key: BlockKey, count: u64) -> anyhow::Result<()> {
        self.data.delete_range(key, count).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires /dev/fuse and fusermount3; mounts only a private temporary directory"]
async fn ficlone_mount_identity_and_destination_access() -> anyhow::Result<()> {
    let temp = tempfile::tempdir()?;
    let mount_path = temp.path().join("mount with spaces");
    std::fs::create_dir(&mount_path)?;
    let store = DatabaseMetaStore::from_config(Config {
        database: DatabaseConfig {
            db_config: DatabaseType::Sqlite {
                url: format!(
                    "sqlite://{}?mode=rwc",
                    temp.path().join("meta.db").display()
                ),
            },
        },
        cache: Default::default(),
        client: Default::default(),
        compact: Default::default(),
    })
    .await?;
    let fs = VFS::new(ChunkLayout::default(), InMemoryBlockStore::new(), store).await?;
    let src = fs.create_file("/source").await?;
    // The extra write-only FD exercises descriptor access, not pathname mode
    // checks; direct VFS creation may use root ownership in the test backend.
    fs.chmod(src, 0o666).await?;
    let src_handle = fs
        .open(src, fs.stat("/source").await?, true, true, false)
        .await?;
    fs.write(src_handle, 0, SOURCE).await?;
    fs.fsync(src_handle, false).await?;
    fs.close(src_handle).await?;
    let dst = fs.create_file("/destination").await?;
    let dst_handle = fs
        .open(dst, fs.stat("/destination").await?, true, true, false)
        .await?;
    let readonly_handle = fs
        .open(dst, fs.stat("/destination").await?, true, false, false)
        .await?;
    let mount = mount_vfs_unprivileged(
        fs.clone(),
        &mount_path,
        FuseConcurrencyConfig {
            worker_count: 2,
            max_background: 16,
        },
    )
    .await?;

    // Finish all observations and unmount before asserting, including on the
    // vulnerable baseline where a rejected clone actually mutates the target.
    let observations: anyhow::Result<Vec<_>> = async {
        let source_path = mount_path.join("source");
        let (legitimate, write_only, path_only) =
            tokio::task::spawn_blocking(move || -> std::io::Result<_> {
                Ok((
                    std::fs::File::open(&source_path)?,
                    std::fs::OpenOptions::new().write(true).open(&source_path)?,
                    std::fs::OpenOptions::new()
                        .read(true)
                        .custom_flags(libc::O_PATH)
                        .open(&source_path)?,
                ))
            })
            .await??;
        let outside_path = temp.path().join("source");
        std::fs::write(&outside_path, b"EXTERNAL-SOURCE")?;
        let outside = std::fs::File::open(&outside_path)?;
        let deleted_dir = temp.path().join("deleted");
        std::fs::create_dir(&deleted_dir)?;
        let deleted_path = deleted_dir.join("source");
        std::fs::write(&deleted_path, b"DELETED-EXTERNAL")?;
        let deleted = std::fs::File::open(&deleted_path)?;
        std::fs::remove_file(&deleted_path)?;

        let mut observations = Vec::new();
        for (label, fd, destination, expected_errno) in [
            (
                "external suffix collision",
                i64::from(outside.as_raw_fd()),
                dst_handle,
                Some(libc::EXDEV),
            ),
            (
                "deleted external suffix collision",
                i64::from(deleted.as_raw_fd()),
                dst_handle,
                Some(libc::EXDEV),
            ),
            (
                "same mount",
                i64::from(legitimate.as_raw_fd()),
                dst_handle,
                None,
            ),
            (
                "write-only source",
                i64::from(write_only.as_raw_fd()),
                dst_handle,
                Some(libc::EBADF),
            ),
            (
                "O_PATH source",
                i64::from(path_only.as_raw_fd()),
                dst_handle,
                Some(libc::EBADF),
            ),
            (
                "read-only destination",
                i64::from(legitimate.as_raw_fd()),
                readonly_handle,
                Some(libc::EBADF),
            ),
            (
                "invalid destination handle",
                i64::from(legitimate.as_raw_fd()),
                u64::MAX,
                Some(libc::EBADF),
            ),
            ("negative source fd", -1, dst_handle, Some(libc::EBADF)),
            (
                "out-of-range source fd",
                i64::MAX,
                dst_handle,
                Some(libc::EBADF),
            ),
            (
                "unopened source fd",
                i64::from(i32::MAX),
                dst_handle,
                Some(libc::EBADF),
            ),
        ] {
            for cmd in [libc::FICLONE as u32, libc::FICLONERANGE as u32] {
                fs.truncate_inode(dst, 0).await?;
                fs.write(dst_handle, 0, ORIGINAL).await?;
                fs.fsync(dst_handle, false).await?;
                let mut range = Vec::new();
                range.extend_from_slice(&fd.to_ne_bytes());
                range.extend_from_slice(&0u64.to_ne_bytes());
                range.extend_from_slice(&(SOURCE.len() as u64).to_ne_bytes());
                range.extend_from_slice(&0u64.to_ne_bytes());
                let result = Filesystem::ioctl(
                    &fs,
                    Request {
                        unique: 1,
                        uid: 0,
                        gid: 0,
                        pid: std::process::id(),
                    },
                    dst as u64,
                    destination,
                    0,
                    cmd,
                    fd as u64,
                    &range,
                    0,
                )
                .await;
                let errno = result
                    .err()
                    .map(|error| std::io::Error::from(error).raw_os_error().unwrap());
                let bytes = fs.read(dst_handle, 0, 64).await?;
                println!(
                    "{label}, ioctl={cmd:#x}: errno={errno:?}, destination={:?}",
                    String::from_utf8_lossy(&bytes)
                );
                observations.push((label, cmd, errno, expected_errno, bytes));
            }
        }
        Ok(observations)
    }
    .await;
    let write_close = fs.close(dst_handle).await;
    let readonly_close = fs.close(readonly_handle).await;
    mount.unmount().await?;
    println!("private FUSE mount unmounted");
    write_close?;
    readonly_close?;
    let observations = observations?;
    let mut failures = Vec::new();
    for (label, cmd, errno, expected_errno, bytes) in observations {
        let expected = if expected_errno.is_some() {
            ORIGINAL
        } else {
            SOURCE
        };
        if errno != expected_errno || bytes != expected {
            failures.push(format!("{label}, ioctl={cmd:#x}: errno={errno:?}, expected={expected_errno:?}; destination={bytes:?}, expected={expected:?}"));
        }
    }
    anyhow::ensure!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

#[tokio::test]
async fn copy_file_range_retains_same_handle_for_overlap() -> anyhow::Result<()> {
    let temp = tempfile::tempdir()?;
    let fs = VFS::new(
        ChunkLayout::default(),
        InMemoryBlockStore::new(),
        metadata(temp.path()).await?,
    )
    .await?;
    let ino = fs.create_file("/overlap").await?;
    let fh = fs
        .open(ino, fs.stat("/overlap").await?, true, true, false)
        .await?;
    fs.write(fh, 0, b"0123456789abcdef").await?;
    fs.fsync(fh, false).await?;
    assert_eq!(fs.copy_file_range(fh, 0, fh, 4, 8).await?, 8);
    assert_eq!(fs.read(fh, 0, 16).await?, b"012301234567cdef");
    assert_eq!(fs.copy_file_range(fh, u64::MAX, fh, 0, 1).await?, 0);
    assert!(
        fs.copy_file_range(fh, 0, fh, i64::MAX as u64, 1)
            .await
            .is_err()
    );
    fs.close(fh).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires /dev/fuse and fusermount3; mounts only a private temporary directory"]
async fn clone_uses_retained_handles_across_close_and_fd_reuse() -> anyhow::Result<()> {
    let mut observations = Vec::new();
    for cmd in [libc::FICLONE as u32, libc::FICLONERANGE as u32] {
        let temp = tempfile::tempdir()?;
        let mount_path = temp.path().join("mount");
        std::fs::create_dir(&mount_path)?;
        let gate = Arc::new(ReadGate::default());
        let store = PausedReadStore {
            data: Arc::new(InMemoryBlockStore::new()),
            gate: gate.clone(),
        };
        // A new VFS after durable setup guarantees a cold source read. The
        // pause is entirely in this test's block adapter, with no product hooks.
        let setup = VFS::new(
            ChunkLayout::default(),
            store.clone(),
            metadata(temp.path()).await?,
        )
        .await?;
        let src = setup.create_file("/source").await?;
        let dst = setup.create_file("/destination").await?;
        for (ino, name, bytes) in [(src, "/source", SOURCE), (dst, "/destination", ORIGINAL)] {
            let fh = setup
                .open(ino, setup.stat(name).await?, true, true, false)
                .await?;
            setup.write(fh, 0, bytes).await?;
            setup.fsync(fh, false).await?;
            setup.close(fh).await?;
        }
        drop(setup);
        let fs = VFS::new(ChunkLayout::default(), store, metadata(temp.path()).await?).await?;
        let destination = fs
            .open(dst, fs.stat("/destination").await?, true, true, false)
            .await?;
        let mount = mount_vfs_unprivileged(
            fs.clone(),
            &mount_path,
            FuseConcurrencyConfig {
                worker_count: 2,
                max_background: 16,
            },
        )
        .await?;

        let result: anyhow::Result<_> = async {
            let path = mount_path.join("source");
            let caller = tokio::task::spawn_blocking(move || std::fs::File::open(path)).await??;
            let fd = caller.as_raw_fd();
            let mut range = Vec::new();
            range.extend_from_slice(&i64::from(fd).to_ne_bytes());
            range.extend_from_slice(&0u64.to_ne_bytes());
            range.extend_from_slice(&(SOURCE.len() as u64).to_ne_bytes());
            range.extend_from_slice(&0u64.to_ne_bytes());
            gate.armed.store(true, Ordering::Release);
            let mut clone = Box::pin(Filesystem::ioctl(&fs, Request {
                unique: 2, uid: 0, gid: 0, pid: std::process::id(),
            }, dst as u64, destination, 0, cmd, fd as u64, &range, 0));
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                tokio::select! {
                    outcome = clone.as_mut() => anyhow::bail!("clone completed before source read gate: success={}", outcome.is_ok()),
                    _ = gate.entered.notified() => Ok(()),
                }
            }).await??;

            // Validation has completed and the copy is now suspended reading
            // its source. A close must not retire the validated destination.
            let mut close = Box::pin(fs.close(destination));
            let early_close = match futures_util::poll!(close.as_mut()) {
                std::task::Poll::Ready(result) => Some(result),
                std::task::Poll::Pending => None,
            };
            let close_waited = early_close.is_none();
            let original_still_open = fs.read(destination, 0, 1).await.is_ok();
            let reopened = fs.open(dst, fs.stat("/destination").await?, true, false, false).await?;

            let outside_path = temp.path().join("outside");
            std::fs::write(&outside_path, b"REUSED-EXTERNAL-FD")?;
            let outside = std::fs::File::open(&outside_path)?;
            let replace = tokio::task::spawn_blocking(move || -> std::io::Result<_> {
                // SAFETY: caller owns fd exclusively; dup2 replaces that owned
                // slot, and caller later closes the replacement exactly once.
                if unsafe { libc::dup2(outside.as_raw_fd(), fd) } != fd {
                    return Err(std::io::Error::last_os_error());
                }
                Ok((caller, outside))
            });
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                while std::fs::read_link(format!("/proc/self/fd/{fd}"))? != outside_path {
                    tokio::task::yield_now().await;
                }
                Ok::<_, std::io::Error>(())
            }).await??;

            gate.resume.notify_one();
            tokio::time::timeout(std::time::Duration::from_secs(10), clone).await??;
            match early_close {
                Some(result) => result?,
                None => tokio::time::timeout(std::time::Duration::from_secs(10), close).await??,
            }
            drop(tokio::time::timeout(std::time::Duration::from_secs(10), replace).await???);
            let bytes = fs.read(reopened, 0, 64).await?;
            fs.close(reopened).await?;
            println!("ioctl={cmd:#x}: close_waited={close_waited}, validated_handle_still_open={original_still_open}, source_fd_reused=true, destination={:?}", String::from_utf8_lossy(&bytes));
            Ok((cmd, close_waited, original_still_open, bytes))
        }.await;
        gate.resume.notify_one();
        let _ = fs.close(destination).await;
        mount.unmount().await?;
        observations.push(result?);
    }
    for (cmd, close_waited, still_open, bytes) in observations {
        anyhow::ensure!(
            close_waited && still_open,
            "ioctl={cmd:#x}: close retired the validated handle before clone completed (waited={close_waited}, still_open={still_open})"
        );
        anyhow::ensure!(bytes == SOURCE, "ioctl={cmd:#x}: FD reuse redirected clone");
    }
    Ok(())
}
