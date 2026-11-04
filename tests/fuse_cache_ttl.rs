#![cfg(feature = "workspace-overlay")]

use asyncfuse::SetAttr;
use asyncfuse::raw::{Filesystem, Request};
use brewfs::chunk::{ChunkLayout, store::InMemoryBlockStore};
use brewfs::config::{Cli, Command, MountConfig};
use brewfs::meta::MetaLayer;
use brewfs::vfs::fs::VFS;
use brewfs::workspace_overlay::catalog::{AcquireLease, CreateVolumeRoot, WorkspaceStore};
use brewfs::workspace_overlay::ids::{LayerId, LeaseId, WorkspaceId};
use brewfs::workspace_overlay::meta_layer::WorkspaceMetaLayer;
use brewfs::workspace_overlay::model::{ViewContext, WORKSPACE_SCHEMA_VERSION};
use brewfs::workspace_overlay::stores::database::SqliteWorkspaceStore;
use clap::Parser;
use std::ffi::OsStr;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

// Set the environment before starting a fresh test process, so parallel tests
// cannot race on process-global environment changes or the legacy TTL OnceLock.
fn cache_ttl_test_process(test_name: &str, env_ttl: Option<&str>) -> bool {
    const CHILD_TEST: &str = "BREWFS_CACHE_TTL_TEST_CHILD";
    if std::env::var(CHILD_TEST).as_deref() == Ok(test_name) {
        return true;
    }
    let mut child = std::process::Command::new(std::env::current_exe().unwrap());
    child
        .args(["--exact", test_name, "--nocapture"])
        .env(CHILD_TEST, test_name)
        .env_remove("BREWFS_CACHE_TTL_MS");
    if let Some(ttl) = env_ttl {
        child.env("BREWFS_CACHE_TTL_MS", ttl);
    }
    assert!(child.status().unwrap().success(), "{test_name}");
    false
}

fn ttl_mount_config(volume_format: &str, extra_args: &[&str]) -> MountConfig {
    let mut args = vec![
        "brewfs",
        "mount",
        "/mnt/ttl-test",
        "--volume-format",
        volume_format,
    ];
    if volume_format != "flat-v1" {
        args.extend(["--workspace", "00000000-0000-0000-0000-000000000100"]);
    }
    args.extend_from_slice(extra_args);
    let Command::Mount(args) = Cli::parse_from(args).cmd else {
        panic!("expected mount command");
    };
    MountConfig::from_sources(*args).unwrap()
}

#[test]
fn workspace_cache_ttl_environment_zero() {
    if !cache_ttl_test_process("workspace_cache_ttl_environment_zero", Some("0")) {
        return;
    }
    assert_eq!(ttl_mount_config("workspace-v1", &[]).fuse_cache_ttl_ms, 0);
}

#[test]
fn workspace_cache_ttl_environment_positive() {
    if !cache_ttl_test_process("workspace_cache_ttl_environment_positive", Some("500")) {
        return;
    }
    assert_eq!(ttl_mount_config("workspace-v1", &[]).fuse_cache_ttl_ms, 500);
}

#[test]
fn workspace_cache_ttl_cli_overrides_environment() {
    if !cache_ttl_test_process("workspace_cache_ttl_cli_overrides_environment", Some("500")) {
        return;
    }
    assert_eq!(
        ttl_mount_config("workspace-v1", &["--fuse-cache-ttl-ms", "300"]).fuse_cache_ttl_ms,
        300
    );
}

#[test]
fn cache_ttl_unconfigured_defaults_are_unchanged() {
    if !cache_ttl_test_process("cache_ttl_unconfigured_defaults_are_unchanged", None) {
        return;
    }
    assert_eq!(
        ttl_mount_config("workspace-v1", &[]).fuse_cache_ttl_ms,
        1_000
    );
    assert_eq!(ttl_mount_config("flat-v1", &[]).fuse_cache_ttl_ms, 0);
    assert_eq!(
        ttl_mount_config("workspace-entity-v1", &[]).fuse_cache_ttl_ms,
        0
    );
}

#[test]
fn cache_ttl_explicit_yaml_overrides_environment() {
    if !cache_ttl_test_process("cache_ttl_explicit_yaml_overrides_environment", Some("500")) {
        return;
    }
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(file.path(), "fuse:\n  cache_ttl_ms: 200\n").unwrap();
    let path = file.path().to_str().unwrap();
    for format in ["workspace-v1", "flat-v1"] {
        assert_eq!(
            ttl_mount_config(format, &["--config", path]).fuse_cache_ttl_ms,
            200
        );
        assert_eq!(
            ttl_mount_config(format, &["--config", path, "--fuse-cache-ttl-ms", "300"])
                .fuse_cache_ttl_ms,
            300
        );
    }
}

#[test]
fn cache_ttl_invalid_environment_preserves_defaults() {
    if !cache_ttl_test_process(
        "cache_ttl_invalid_environment_preserves_defaults",
        Some("invalid"),
    ) {
        return;
    }
    assert_eq!(
        ttl_mount_config("workspace-v1", &[]).fuse_cache_ttl_ms,
        1_000
    );
    assert_eq!(ttl_mount_config("flat-v1", &[]).fuse_cache_ttl_ms, 0);
}

#[tokio::test]
async fn strict_environment_zero_returns_zero_lookup_and_getattr_ttl() {
    if !cache_ttl_test_process(
        "strict_environment_zero_returns_zero_lookup_and_getattr_ttl",
        Some("0"),
    ) {
        return;
    }
    let config = ttl_mount_config("workspace-v1", &[]);
    let meta = Arc::new(test_meta().await);
    let root = meta.root_ino() as u64;
    meta.create_file(root as i64, "strict".into())
        .await
        .unwrap();
    let fs = new_ttl_test_vfs(meta);
    fs.set_fuse_cache_ttl(Duration::from_millis(config.fuse_cache_ttl_ms));
    let req = Request::default();
    let lookup = Filesystem::lookup(&fs, req, root, OsStr::new("strict"))
        .await
        .unwrap();
    assert_eq!(lookup.ttl, Duration::ZERO);
    let getattr = Filesystem::getattr(&fs, req, lookup.attr.ino, None, 0)
        .await
        .unwrap();
    assert_eq!(getattr.ttl, Duration::ZERO);
}

async fn test_meta() -> WorkspaceMetaLayer<SqliteWorkspaceStore> {
    let store = Arc::new(
        SqliteWorkspaceStore::connect("sqlite::memory:")
            .await
            .unwrap(),
    );
    store.initialize_workspace_schema().await.unwrap();
    let workspace_id = WorkspaceId::from_uuid(Uuid::from_u128(100));
    let workspace = store
        .create_volume_root(CreateVolumeRoot {
            upper_bucket: None,
            volume_format: "workspace-v1".into(),
            schema_version: WORKSPACE_SCHEMA_VERSION,
            volume_id: Uuid::from_u128(101),
            workspace_id,
            root_layer_id: LayerId::from_uuid(Uuid::from_u128(102)),
            writable_layer_id: LayerId::from_uuid(Uuid::from_u128(103)),
            owner_id: Some("meta-test".into()),
        })
        .await
        .unwrap();
    let lease = store
        .acquire_lease(AcquireLease {
            workspace_id,
            lease_id: LeaseId::from_uuid(Uuid::from_u128(104)),
            holder_generation: 1,
            ttl_ns: 60_000_000_000,
        })
        .await
        .unwrap();
    WorkspaceMetaLayer::new(
        store,
        ViewContext {
            workspace_id,
            head_layer_id: workspace.head_layer_id,
            head_epoch: workspace.head_epoch,
            lease_id: lease.lease_id,
            holder_generation: lease.holder_generation,
        },
    )
}

fn new_ttl_test_vfs(
    meta: Arc<WorkspaceMetaLayer<SqliteWorkspaceStore>>,
) -> VFS<InMemoryBlockStore, impl MetaLayer> {
    VFS::new_workspace(ChunkLayout::default(), InMemoryBlockStore::new(), meta).unwrap()
}

#[tokio::test]
async fn cache_fill_rejects_released_lease_but_default_reply_is_unchanged() {
    use brewfs::workspace_overlay::catalog::ReleaseLease;
    let meta = Arc::new(test_meta().await);
    let ino = meta
        .create_file(meta.root_ino(), "file".into())
        .await
        .unwrap();
    let cached = new_ttl_test_vfs(meta.clone());
    cached.set_fuse_cache_ttl(Duration::from_secs(1));
    let legacy = new_ttl_test_vfs(meta.clone());
    let view = meta.view_context().await;
    meta.store()
        .release_lease(ReleaseLease {
            lease_id: view.lease_id,
            holder_generation: view.holder_generation,
        })
        .await
        .unwrap();
    let req = Request::default();
    assert!(
        Filesystem::getattr(&cached, req, ino as u64, None, 0)
            .await
            .is_err()
    );
    assert_eq!(
        Filesystem::getattr(&legacy, req, ino as u64, None, 0)
            .await
            .unwrap()
            .ttl,
        Duration::ZERO
    );
}

/// Two independent FUSE connections and metadata resolvers, sharing one test
/// lease. The CLI correctly forbids two independent live writer leases; sharing
/// this credential deliberately exercises an external same-generation writer.
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires /dev/fuse and mount permission; run explicitly"]
async fn two_mounts_observe_external_metadata_after_cache_expiry() {
    use brewfs::fuse::mount::{FuseConcurrencyConfig, mount_vfs_unprivileged};
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    use std::sync::atomic::Ordering;
    let meta = Arc::new(test_meta().await);
    let other = Arc::new(WorkspaceMetaLayer::new(
        meta.store().clone(),
        meta.view_context().await,
    ));
    let fs = new_ttl_test_vfs(meta);
    fs.set_fuse_cache_ttl(Duration::from_secs(1));
    let stats = fs.stats().clone();
    let first = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();
    let a = mount_vfs_unprivileged(fs.clone(), first.path(), FuseConcurrencyConfig::default())
        .await
        .unwrap();
    let b = mount_vfs_unprivileged(
        new_ttl_test_vfs(other),
        second.path(),
        FuseConcurrencyConfig::default(),
    )
    .await
    .unwrap();
    let first_path = first.path().to_path_buf();
    let second_path = second.path().to_path_buf();
    let result = tokio::task::spawn_blocking(move || {
        for path in [&first_path, &second_path] {
            assert!(
                std::process::Command::new("mountpoint")
                    .arg("-q")
                    .arg(path)
                    .status()
                    .unwrap()
                    .success()
            );
        }
        let original = first_path.join("file");
        let writer = second_path.join("file");
        std::fs::File::create(&writer).unwrap();
        std::fs::set_permissions(&writer, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert_eq!(std::fs::metadata(&original).unwrap().mode() & 0o777, 0o644);
        let before = stats.fuse_lookup_ops.load(Ordering::Relaxed);
        for _ in 0..1000 {
            std::fs::metadata(&original).unwrap();
        }
        assert_eq!(
            stats.fuse_lookup_ops.load(Ordering::Relaxed),
            before,
            "warm stats must hit the kernel cache"
        );
        std::fs::set_permissions(&writer, std::fs::Permissions::from_mode(0o600)).unwrap();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&writer)
            .unwrap()
            .set_len(123)
            .unwrap();
        let started = std::time::Instant::now();
        loop {
            let attr = std::fs::metadata(&original).unwrap();
            if attr.mode() & 0o777 == 0o600 && attr.len() == 123 {
                break;
            }
            assert!(
                started.elapsed() < Duration::from_millis(1500),
                "metadata stayed stale past TTL plus scheduling allowance"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        eprintln!(
            "external chmod/truncate visible after {:?}",
            started.elapsed()
        );
        // Revoke a named user without changing mode bits: expiry must also
        // flush Linux's cached POSIX ACL, not just observe a mode change.
        let set_named_acl = |permission: u16| {
            let mut acl = 2u32.to_le_bytes().to_vec();
            for (tag, perm, id) in [
                (1u16, 6u16, u32::MAX),
                (2, permission, 65534),
                (4, 0, u32::MAX),
                (16, 4, u32::MAX),
                (32, 0, u32::MAX),
            ] {
                acl.extend(tag.to_le_bytes());
                acl.extend(perm.to_le_bytes());
                acl.extend(id.to_le_bytes());
            }
            let path = std::ffi::CString::new(writer.as_os_str().as_encoded_bytes()).unwrap();
            let result = unsafe { libc::setxattr(path.as_ptr(), c"system.posix_acl_access".as_ptr(), acl.as_ptr().cast(), acl.len(), 0) };
            assert_eq!(result, 0, "{}", std::io::Error::last_os_error());
        };
        let named_user_can_read = || {
            std::process::Command::new("python3").args(["-c", "import os,sys; os.setgroups([]); os.setgid(65534); os.setuid(65534); sys.exit(not os.access(sys.argv[1], os.R_OK))"])
                .arg(&original).status().unwrap().success()
        };
        set_named_acl(4);
        std::thread::sleep(Duration::from_millis(1100));
        assert!(named_user_can_read());
        set_named_acl(0);
        let started = std::time::Instant::now();
        while named_user_can_read() {
            assert!(started.elapsed() < Duration::from_millis(1500), "named ACL revocation outlived the metadata TTL");
            std::thread::sleep(Duration::from_millis(10));
        }
        eprintln!("external same-mode ACL revocation visible after {:?}", started.elapsed());
        std::fs::rename(&writer, second_path.join("renamed")).unwrap();
        std::thread::sleep(Duration::from_millis(1100));
        assert_eq!(
            std::fs::metadata(&original).unwrap_err().kind(),
            std::io::ErrorKind::NotFound
        );
        let renamed = first_path.join("renamed");
        assert_eq!(std::fs::metadata(&renamed).unwrap().len(), 123);
        std::fs::remove_file(second_path.join("renamed")).unwrap();
        std::thread::sleep(Duration::from_millis(1100));
        assert_eq!(
            std::fs::metadata(&renamed).unwrap_err().kind(),
            std::io::ErrorKind::NotFound
        );
    })
    .await;
    b.unmount().await.unwrap();
    a.unmount().await.unwrap();
    result.unwrap();
}

#[tokio::test]
async fn writable_acl_fuse_replies_use_only_explicit_cache_ttl() {
    for configured in [None, Some(0), Some(7), Some(1000)] {
        let meta = Arc::new(test_meta().await);
        assert_eq!(
            meta.posix_acl_capability(),
            brewfs::meta::layer::PosixAclCapability::ReadWrite
        );
        let root = meta.root_ino() as u64;
        let fs = new_ttl_test_vfs(meta.clone());
        let cloned = fs.clone();
        if let Some(ms) = configured {
            fs.set_fuse_cache_ttl(Duration::from_millis(ms));
        }
        let ttl = Duration::from_millis(configured.unwrap_or(0));
        let req = Request {
            pid: std::process::id(),
            ..Request::default()
        };

        // Integration tests link the library without cfg(test); virtual stats
        // are only exposed when the observability feature is enabled.
        let virtual_stats = Filesystem::lookup(&cloned, req, root, OsStr::new(".stats")).await;
        if cfg!(feature = "observability") {
            assert_eq!(virtual_stats.unwrap().ttl, Duration::ZERO);
        } else {
            assert_eq!(virtual_stats.unwrap_err(), libc::ENOENT.into());
        }
        for name in ["file", ".stats"] {
            let real_ino = meta.create_file(root as i64, name.into()).await.unwrap();
            let lookup = Filesystem::lookup(&cloned, req, root, OsStr::new(name))
                .await
                .unwrap();
            assert_eq!(lookup.attr.ino, real_ino as u64);
            assert!(lookup.ttl <= ttl, "LOOKUP {name}, {configured:?}");
            if ttl >= Duration::from_secs(1) {
                assert!(!lookup.ttl.is_zero());
            }
            let ino = lookup.attr.ino;
            let getattr = Filesystem::getattr(&cloned, req, ino, None, 0)
                .await
                .unwrap();
            assert!(getattr.ttl <= ttl);
            if ttl >= Duration::from_secs(1) {
                assert!(!getattr.ttl.is_zero());
            }
            assert_eq!(
                Filesystem::setattr(&cloned, req, ino, None, SetAttr::default())
                    .await
                    .unwrap()
                    .ttl,
                Duration::ZERO
            );
            assert_eq!(
                Filesystem::setattr(
                    &cloned,
                    req,
                    ino,
                    None,
                    SetAttr {
                        mode: Some(0o600),
                        ..SetAttr::default()
                    }
                )
                .await
                .unwrap()
                .ttl,
                Duration::ZERO
            );
            assert_eq!(
                Filesystem::setattr(
                    &cloned,
                    req,
                    ino,
                    None,
                    SetAttr {
                        size: Some(0),
                        ..SetAttr::default()
                    }
                )
                .await
                .unwrap()
                .ttl,
                Duration::ZERO
            );
        }
        let created = Filesystem::create(
            &cloned,
            req,
            root,
            OsStr::new("created"),
            0o644,
            libc::O_RDWR as u32,
        )
        .await
        .unwrap();
        assert_eq!(created.ttl, Duration::ZERO);
        Filesystem::release(
            &cloned,
            req,
            created.attr.ino,
            created.fh,
            libc::O_RDWR as u32,
            0,
            false,
        )
        .await
        .unwrap();
    }
}
