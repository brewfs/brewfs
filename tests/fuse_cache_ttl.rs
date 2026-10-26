#![cfg(feature = "workspace-overlay")]

use asyncfuse::SetAttr;
use asyncfuse::raw::{Filesystem, Request};
use brewfs::chunk::{ChunkLayout, store::InMemoryBlockStore};
use brewfs::meta::MetaLayer;
use brewfs::vfs::fs::VFS;
use brewfs::workspace_overlay::catalog::{AcquireLease, CreateVolumeRoot, WorkspaceStore};
use brewfs::workspace_overlay::ids::{LayerId, LeaseId, WorkspaceId};
use brewfs::workspace_overlay::meta_layer::WorkspaceMetaLayer;
use brewfs::workspace_overlay::model::{ViewContext, WORKSPACE_SCHEMA_VERSION};
use brewfs::workspace_overlay::stores::database::SqliteWorkspaceStore;
use std::ffi::OsStr;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

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

        let virtual_stats = Filesystem::lookup(&cloned, req, root, OsStr::new(".stats"))
            .await
            .unwrap();
        assert_eq!(virtual_stats.ttl, Duration::ZERO);
        for name in ["file", ".stats"] {
            meta.create_file(root as i64, name.into()).await.unwrap();
            let lookup = Filesystem::lookup(&cloned, req, root, OsStr::new(name))
                .await
                .unwrap();
            assert_eq!(lookup.ttl, ttl, "LOOKUP {name}, {configured:?}");
            let ino = lookup.attr.ino;
            assert_eq!(
                Filesystem::getattr(&cloned, req, ino, None, 0)
                    .await
                    .unwrap()
                    .ttl,
                ttl
            );
            assert_eq!(
                Filesystem::setattr(&cloned, req, ino, None, SetAttr::default())
                    .await
                    .unwrap()
                    .ttl,
                ttl
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
                ttl
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
