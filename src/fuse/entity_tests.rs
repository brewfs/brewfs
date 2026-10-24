use super::*;
use crate::cadapter::localfs::LocalFsBackend;
use crate::workspace_overlay::entity::{
    keys::EntityKeys,
    lifecycle::Lifecycle,
    memory::MemoryEntityBackend,
    resolver::{ImmutableLower, test_support::MemoryLower},
};
use futures_util::TryStreamExt;
use uuid::Uuid;

fn owner() -> Credentials {
    Credentials {
        uid: 1000,
        gid: 1000,
        groups: vec![1000],
        umask: 0,
    }
}

fn request() -> Request {
    Request {
        uid: 1000,
        gid: 1000,
        ..Default::default()
    }
}

async fn fixture() -> (
    tempfile::TempDir,
    EntityFuse<MemoryEntityBackend, MemoryLower, LocalFsBackend>,
    u64,
    Arc<MemoryEntityBackend>,
) {
    let temp = tempfile::tempdir().unwrap();
    let backend = Arc::new(MemoryEntityBackend::new(1000));
    let keys = EntityKeys::new(Uuid::new_v4(), Uuid::new_v4());
    let mut lower = MemoryLower::empty();
    lower.inodes.get_mut(&1).unwrap().mode = 0o777;
    let lower = Arc::new(lower);
    let lifecycle = Lifecycle::new(backend.clone(), keys.clone());
    lifecycle.provision(lower.binding()).await.unwrap();
    let session = lifecycle.acquire(Uuid::new_v4(), 1_000_000).await.unwrap();
    let namespace = EntityNamespace::new(backend.clone(), lower, keys, session);
    let data = EntityData::new(
        namespace.clone(),
        Arc::new(LocalFsBackend::new(temp.path())),
        4096,
    )
    .await
    .unwrap();
    let directory = namespace
        .create(
            CreateRequest {
                parent: 1,
                name: b"directory".to_vec(),
                kind: EntityKind::Directory,
                mode: 0o700,
                rdev: 0,
                symlink_target: None,
            },
            owner(),
        )
        .await
        .unwrap();
    namespace
        .create(
            CreateRequest {
                parent: directory.ino,
                name: b"child".to_vec(),
                kind: EntityKind::File,
                mode: 0o600,
                rdev: 0,
                symlink_target: None,
            },
            owner(),
        )
        .await
        .unwrap();
    let fuse = EntityFuse::new(namespace, data, 1).unwrap();
    let node = fuse.node(directory.ino);
    (temp, fuse, node, backend)
}

async fn check_both_directory_streams(
    fuse: &EntityFuse<MemoryEntityBackend, MemoryLower, LocalFsBackend>,
    node: u64,
    fh: u64,
) {
    let names: Vec<_> = fuse
        .readdir(request(), node, fh, 0)
        .await
        .unwrap()
        .entries
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .into_iter()
        .map(|entry| entry.name)
        .collect();
    assert_eq!(names, [".", "..", "child"].map(OsString::from));
    let names: Vec<_> = fuse
        .readdirplus(request(), node, fh, 0, 0)
        .await
        .unwrap()
        .entries
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .into_iter()
        .map(|entry| entry.name)
        .collect();
    assert_eq!(names, [".", "..", "child"].map(OsString::from));
}

#[tokio::test]
async fn readable_directory_without_search_permission_can_be_enumerated() {
    let (_temp, fuse, node, _backend) = fixture().await;
    let ino = fuse.ino(node).unwrap();
    fuse.state
        .namespace
        .chmod(ino, 0o400, owner())
        .await
        .unwrap();
    let opened = fuse
        .opendir(request(), node, libc::O_RDONLY as u32)
        .await
        .unwrap();
    check_both_directory_streams(&fuse, node, opened.fh).await;
    assert!(matches!(
        fuse.state.namespace.lookup(ino, b"child", &owner()).await,
        Err(EntityError::Permission)
    ));
}

#[tokio::test]
async fn directory_handle_retains_read_grant_after_chmod() {
    let (_temp, fuse, node, _backend) = fixture().await;
    let opened = fuse
        .opendir(request(), node, libc::O_RDONLY as u32)
        .await
        .unwrap();
    fuse.state
        .namespace
        .chmod(fuse.ino(node).unwrap(), 0, owner())
        .await
        .unwrap();
    check_both_directory_streams(&fuse, node, opened.fh).await;
    assert!(
        fuse.opendir(request(), node, libc::O_RDONLY as u32)
            .await
            .is_err()
    );
    assert!(
        fuse.readdir(request(), node, opened.fh + 1, 0)
            .await
            .is_err()
    );
    fuse.releasedir(request(), node, opened.fh, 0)
        .await
        .unwrap();
    assert!(fuse.readdir(request(), node, opened.fh, 0).await.is_err());
    assert!(
        fuse.readdirplus(request(), node, opened.fh, 0, 0)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn directory_handle_still_requires_live_writer_authority() {
    let (_temp, fuse, node, backend) = fixture().await;
    let opened = fuse
        .opendir(request(), node, libc::O_RDONLY as u32)
        .await
        .unwrap();
    backend.set_time(2_000_000).unwrap();
    assert_eq!(
        fuse.readdir(request(), node, opened.fh, 0).await.err(),
        Some(libc::ESTALE.into())
    );
    assert_eq!(
        fuse.readdirplus(request(), node, opened.fh, 0, 0)
            .await
            .err(),
        Some(libc::ESTALE.into())
    );
}

#[tokio::test]
async fn opening_directory_still_requires_ancestor_search_permission() {
    let (_temp, fuse, node, _backend) = fixture().await;
    let root = Credentials {
        uid: 0,
        gid: 0,
        groups: vec![0],
        umask: 0,
    };
    fuse.state.namespace.chmod(1, 0o744, root).await.unwrap();
    assert_eq!(
        fuse.opendir(request(), node, libc::O_RDONLY as u32)
            .await
            .err(),
        Some(libc::EACCES.into())
    );
}

async fn directory_entries(
    fuse: &EntityFuse<MemoryEntityBackend, MemoryLower, LocalFsBackend>,
    node: u64,
    fh: u64,
    offset: u64,
    plus: bool,
) -> Vec<(OsString, i64)> {
    if plus {
        fuse.readdirplus(request(), node, fh, offset, 0)
            .await
            .unwrap()
            .entries
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .map(|entry| {
                assert_eq!(entry.attr.ino, entry.inode);
                assert_eq!(entry.attr.kind, entry.kind);
                (entry.name, entry.offset)
            })
            .collect()
    } else {
        fuse.readdir(request(), node, fh, offset as i64)
            .await
            .unwrap()
            .entries
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .map(|entry| (entry.name, entry.offset))
            .collect()
    }
}

async fn assert_rewind_refreshes(plus: bool) {
    let (_temp, fuse, node, _backend) = fixture().await;
    let fh = fuse.opendir(request(), node, 0).await.unwrap().fh;
    let original = directory_entries(&fuse, node, fh, 0, plus).await;
    assert_eq!(
        original,
        vec![(".".into(), 1), ("..".into(), 2), ("child".into(), 3)]
    );
    assert!(directory_entries(&fuse, node, fh, 3, plus).await.is_empty());

    let created = fuse
        .create(request(), node, OsStr::new("before"), 0o644, 0)
        .await
        .unwrap();
    fuse.release(request(), created.attr.ino, created.fh, 0, 0, false)
        .await
        .unwrap();
    // A new name sorts before child, but continuation cookies must still refer
    // to the original snapshot, including when switching enumeration modes.
    assert_eq!(
        directory_entries(&fuse, node, fh, 2, plus).await,
        original[2..]
    );
    assert_eq!(
        directory_entries(&fuse, node, fh, 2, !plus).await,
        original[2..]
    );
    fuse.unlink(request(), node, OsStr::new("child"))
        .await
        .unwrap();
    assert_eq!(
        directory_entries(&fuse, node, fh, 2, !plus).await,
        original[2..]
    );

    let refreshed = directory_entries(&fuse, node, fh, 0, plus).await;
    assert_eq!(
        refreshed,
        vec![(".".into(), 1), ("..".into(), 2), ("before".into(), 3)],
        "rewinding the same directory handle must refresh created and removed names"
    );
    assert_eq!(
        directory_entries(&fuse, node, fh, 2, !plus).await,
        refreshed[2..]
    );
    assert!(directory_entries(&fuse, node, fh, 3, plus).await.is_empty());
    fuse.releasedir(request(), node, fh, 0).await.unwrap();
}

#[tokio::test]
async fn readdir_rewind_refreshes_created_and_removed_names() {
    assert_rewind_refreshes(false).await;
}

#[tokio::test]
async fn readdirplus_rewind_refreshes_created_and_removed_names() {
    assert_rewind_refreshes(true).await;
}

#[tokio::test]
async fn rewind_refresh_failure_keeps_snapshot() {
    for plus in [false, true] {
        let (_temp, fuse, node, _backend) = fixture().await;
        let fh = fuse.opendir(request(), node, 0).await.unwrap().fh;
        let original = directory_entries(&fuse, node, fh, 0, plus).await;
        fuse.unlink(request(), node, OsStr::new("child"))
            .await
            .unwrap();
        fuse.rmdir(request(), 1, OsStr::new("directory"))
            .await
            .unwrap();

        assert_eq!(
            fuse.readdir(request(), node, fh, 0).await.err(),
            Some(libc::ENOENT.into())
        );
        assert_eq!(
            fuse.readdirplus(request(), node, fh, 0, 0).await.err(),
            Some(libc::ENOENT.into())
        );
        // Failed refreshes preserve both the old cookies and inode attributes.
        assert_eq!(
            directory_entries(&fuse, node, fh, 2, plus).await,
            original[2..]
        );
        assert_eq!(
            directory_entries(&fuse, node, fh, 2, !plus).await,
            original[2..]
        );
        fuse.releasedir(request(), node, fh, 0).await.unwrap();
    }
}

#[tokio::test]
async fn large_directory_fuse_enumerates_and_removes_4097_children() {
    let (_temp, fuse, node, _backend) = fixture().await;
    for index in 0..4096 {
        fuse.state
            .namespace
            .create(
                CreateRequest {
                    parent: fuse.ino(node).unwrap(),
                    name: format!("file-{index:04}").into_bytes(),
                    kind: EntityKind::File,
                    mode: 0o600,
                    rdev: 0,
                    symlink_target: None,
                },
                owner(),
            )
            .await
            .unwrap();
    }
    let opened = fuse
        .opendir(request(), node, libc::O_RDONLY as u32)
        .await
        .unwrap();
    let entries = fuse
        .readdir(request(), node, opened.fh, 0)
        .await
        .unwrap()
        .entries
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(entries.len(), 4099);
    // Resume at a returned cookie, including on the far side of the old cap.
    let offset = entries[4095].offset;
    let tail = fuse
        .readdirplus(request(), node, opened.fh, offset as u64, 0)
        .await
        .unwrap()
        .entries
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(tail.len(), 3);
    assert_eq!(tail[0].name, entries[4096].name);
    assert_eq!(tail[2].name, entries[4098].name);
    fuse.releasedir(request(), node, opened.fh, 0)
        .await
        .unwrap();
    for entry in entries.into_iter().skip(2) {
        fuse.unlink(request(), node, &entry.name).await.unwrap();
    }
    fuse.rmdir(request(), 1, OsStr::new("directory"))
        .await
        .unwrap();
}
