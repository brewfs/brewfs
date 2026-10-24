#![cfg(target_os = "linux")]

//! Exercise the real FUSE ioctl callback with kernel-owned source descriptors.
//! A private FUSE mount provides the positive control; no object transport is
//! under test (SQLite metadata and the existing in-memory block store).
use asyncfuse::raw::{Filesystem, Request};
use brewfs::fuse::mount::{FuseConcurrencyConfig, mount_vfs_unprivileged};
use brewfs::{
    ChunkLayout, Config, DatabaseConfig, DatabaseMetaStore, DatabaseType, InMemoryBlockStore, VFS,
};
use std::os::fd::AsRawFd;
use std::os::unix::fs::OpenOptionsExt;

const SOURCE: &[u8] = b"BREWFS-SOURCE";
const ORIGINAL: &[u8] = b"ORIGINAL";

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
        for (label, source, destination, expected_errno) in [
            (
                "external suffix collision",
                &outside,
                dst_handle,
                Some(libc::EXDEV),
            ),
            (
                "deleted external suffix collision",
                &deleted,
                dst_handle,
                Some(libc::EXDEV),
            ),
            ("same mount", &legitimate, dst_handle, None),
            (
                "write-only source",
                &write_only,
                dst_handle,
                Some(libc::EBADF),
            ),
            ("O_PATH source", &path_only, dst_handle, Some(libc::EBADF)),
            (
                "read-only destination",
                &legitimate,
                readonly_handle,
                Some(libc::EBADF),
            ),
            (
                "invalid destination handle",
                &legitimate,
                u64::MAX,
                Some(libc::EBADF),
            ),
        ] {
            for cmd in [libc::FICLONE as u32, libc::FICLONERANGE as u32] {
                fs.truncate_inode(dst, 0).await?;
                fs.write(dst_handle, 0, ORIGINAL).await?;
                fs.fsync(dst_handle, false).await?;
                let fd = i64::from(source.as_raw_fd());
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
