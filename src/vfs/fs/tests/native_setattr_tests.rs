use super::*;
use crate::meta::layer::PosixAclCapability;
use crate::meta::store::{SetAttrFlags, SetAttrRequest};
use crate::native_base::runtime::{
    BaseDataSource, NativeDataRuntime, NativeIoError, ZeroBaseDataSource,
};
use crate::native_base::write::memory::MemoryControlStore;
use crate::native_base::write::overlay::{OverlayParams, WriteOverlay};
use crate::native_base::write::receipts::MemorySink;
use crate::vfs::config::VFSConfig;
use crate::workspace_overlay::catalog::{AcquireLease, CreateVolumeRoot, WorkspaceStore};
use crate::workspace_overlay::ids::{LayerId, LeaseId, WorkspaceId};
use crate::workspace_overlay::meta_layer::WorkspaceMetaLayer;
use crate::workspace_overlay::model::ViewContext;
use crate::workspace_overlay::stores::database::SqliteWorkspaceStore;
use asyncfuse::SetAttr;
use asyncfuse::raw::{Filesystem, Request};
use uuid::Uuid;

async fn attach_runtime<M: MetaLayer + 'static>(
    fs: &VFS<InMemoryBlockStore, M>,
) -> Arc<NativeDataRuntime> {
    attach_runtime_with_base(fs, Arc::new(ZeroBaseDataSource)).await
}

async fn attach_runtime_with_base<M: MetaLayer + 'static>(
    fs: &VFS<InMemoryBlockStore, M>,
    base: Arc<dyn BaseDataSource>,
) -> Arc<NativeDataRuntime> {
    let overlay = Arc::new(WriteOverlay::new(
        Arc::new(MemoryControlStore::default()),
        Arc::new(MemorySink::default()),
        OverlayParams {
            volume_id: [1; 16],
            workspace_id: [2; 16],
            domain_id: [3; 16],
            writer_generation: 1,
            block_size: 64,
        },
    ));
    let runtime = Arc::new(NativeDataRuntime::new(overlay, base));
    runtime.initialize([4; 16], 1).await.unwrap();
    fs.attach_native_runtime(runtime.clone()).unwrap();
    runtime
}

async fn workspace_vfs() -> VFS<InMemoryBlockStore, WorkspaceMetaLayer<SqliteWorkspaceStore>> {
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
            schema_version: crate::workspace_overlay::model::WORKSPACE_SCHEMA_VERSION,
            volume_id: Uuid::from_u128(101),
            workspace_id,
            root_layer_id: LayerId::from_uuid(Uuid::from_u128(102)),
            writable_layer_id: LayerId::from_uuid(Uuid::from_u128(103)),
            owner_id: Some("native-setattr-test".into()),
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
    let meta = Arc::new(WorkspaceMetaLayer::new(
        store,
        ViewContext {
            workspace_id,
            head_layer_id: workspace.head_layer_id,
            head_epoch: workspace.head_epoch,
            lease_id: lease.lease_id,
            holder_generation: lease.holder_generation,
        },
    ));
    meta.initialize().await.unwrap();
    assert_eq!(meta.posix_acl_capability(), PosixAclCapability::ReadWrite);
    VFS::from_workspace_components(
        VFSConfig::new(ChunkLayout::default()),
        Arc::new(InMemoryBlockStore::new()),
        meta,
    )
    .unwrap()
}

async fn resize<M: MetaLayer + 'static>(
    fs: &VFS<InMemoryBlockStore, M>,
    ino: i64,
    size: u64,
    actor: bool,
) {
    let req = SetAttrRequest {
        size: Some(size),
        ..Default::default()
    };
    let attr = if actor {
        fs.set_attr_from_fuse_as(ino, &req, SetAttrFlags::empty(), 1000, &[1000])
            .await
    } else {
        fs.set_attr_from_fuse(ino, &req, SetAttrFlags::empty())
            .await
    }
    .unwrap();
    assert_eq!(attr.size, size);
}

async fn shrink_extend<M: MetaLayer + 'static>(fs: VFS<InMemoryBlockStore, M>, actor: bool) {
    let runtime = attach_runtime(&fs).await;
    fs.create_file("/file").await.unwrap();
    let attr = fs.stat("/file").await.unwrap();
    let ino = attr.ino;
    let mut fh = fs.open(ino, attr, true, true, false).await.unwrap();
    if actor {
        fs.set_attr(
            ino,
            &SetAttrRequest {
                uid: Some(1000),
                gid: Some(1000),
                ..Default::default()
            },
            SetAttrFlags::empty(),
        )
        .await
        .unwrap();
    }
    let mut original = vec![0xa5; 128];
    original[..4].copy_from_slice(b"keep");
    let mut expected = vec![0; 128];
    expected[..4].copy_from_slice(b"keep");

    // Exercise both committed extents and writes still pending in the runtime.
    for committed in [true, false] {
        if committed {
            fs.write(fh, 0, &original).await.unwrap();
            fs.fsync(fh, false).await.unwrap();
        } else {
            fs.write_ino(ino, 0, &original).await.unwrap();
        }
        resize(&fs, ino, 4, actor).await;
        assert_eq!(runtime.size(ino as u64).await.unwrap(), 4);
        assert_eq!(fs.read(fh, 0, 128).await.unwrap(), b"keep");
        resize(&fs, ino, 128, actor).await;
        assert_eq!(runtime.size(ino as u64).await.unwrap(), 128);
        assert_eq!(fs.read(fh, 0, 128).await.unwrap(), expected);
        fs.fsync(fh, false).await.unwrap();
        fs.close(fh).await.unwrap();
        fh = fs.open_fresh_ino(ino, true, true, false).await.unwrap();
        assert_eq!(fs.read(fh, 0, 128).await.unwrap(), expected);
    }
    fs.truncate_inode(ino, 2).await.unwrap();
    resize(&fs, ino, 4, actor).await;
    assert_eq!(fs.read(fh, 0, 128).await.unwrap(), b"ke\0\0");
    fs.close(fh).await.unwrap();
}

#[tokio::test]
async fn native_setattr_non_atomic_shrink_extend() {
    let meta = create_meta_store_from_url("sqlite::memory:").await.unwrap();
    let fs = VFS::new(
        ChunkLayout::default(),
        InMemoryBlockStore::new(),
        meta.store(),
    )
    .await
    .unwrap();
    assert_ne!(
        fs.meta_layer().posix_acl_capability(),
        PosixAclCapability::ReadWrite
    );
    shrink_extend(fs, false).await;
}

#[tokio::test]
async fn native_setattr_workspace_shrink_extend() {
    shrink_extend(workspace_vfs().await, false).await;
}

#[tokio::test]
async fn native_setattr_actor_shrink_extend() {
    shrink_extend(workspace_vfs().await, true).await;
}

#[tokio::test]
async fn native_setattr_open_trunc_then_short_write() {
    let fs = workspace_vfs().await;
    let runtime = attach_runtime(&fs).await;
    fs.create_file("/file").await.unwrap();
    let ino = fs.stat("/file").await.unwrap().ino;
    fs.write_ino(ino, 0, b"old contents").await.unwrap();
    let old_fh = fs.open_fresh_ino(ino, true, true, false).await.unwrap();
    fs.fsync(old_fh, false).await.unwrap();
    let opened = Filesystem::open(
        &fs,
        Request::default(),
        ino as u64,
        (libc::O_RDWR | libc::O_TRUNC) as u32,
    )
    .await
    .unwrap();
    assert_eq!(runtime.size(ino as u64).await.unwrap(), 0);
    assert!(fs.read(old_fh, 0, 32).await.unwrap().is_empty());
    fs.write(opened.fh, 0, b"new").await.unwrap();
    fs.fsync(opened.fh, false).await.unwrap();
    assert_eq!(fs.read(old_fh, 0, 32).await.unwrap(), b"new");
    fs.close(opened.fh).await.unwrap();
    fs.close(old_fh).await.unwrap();
    let fh = fs.open_fresh_ino(ino, true, false, false).await.unwrap();
    assert_eq!(fs.read(fh, 0, 32).await.unwrap(), b"new");
    fs.close(fh).await.unwrap();
}

#[tokio::test]
async fn native_setattr_ftruncate_keeps_write_handle_authority() {
    let fs = workspace_vfs().await;
    let runtime = attach_runtime(&fs).await;
    fs.create_file("/file").await.unwrap();
    let ino = fs.stat("/file").await.unwrap().ino;
    let fh = fs.open_fresh_ino(ino, true, true, false).await.unwrap();
    // FUSE_WRITE_CACHE can supply dirty data without a file handle.
    fs.write_cached_ino(ino, 0, b"keep old tail", 1)
        .await
        .unwrap();
    fs.chmod(ino, 0).await.unwrap();
    let actor = Request {
        uid: 1000,
        gid: 1000,
        ..Default::default()
    };
    let shrink = SetAttr {
        size: Some(4),
        ..Default::default()
    };
    assert!(
        Filesystem::setattr(&fs, actor, ino as u64, None, shrink.clone())
            .await
            .is_err()
    );
    assert_eq!(
        runtime.read(ino as u64, 0, 32).await.unwrap(),
        b"keep old tail"
    );

    // An existing writable descriptor still authorizes ftruncate after chmod.
    let reply = Filesystem::setattr(&fs, actor, ino as u64, Some(fh), shrink)
        .await
        .unwrap();
    assert_eq!(reply.attr.size, 4);
    assert_eq!(runtime.size(ino as u64).await.unwrap(), 4);
    assert_eq!(fs.read(fh, 0, 32).await.unwrap(), b"keep");
    let reply = Filesystem::setattr(
        &fs,
        actor,
        ino as u64,
        Some(fh),
        SetAttr {
            size: Some(8),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(reply.attr.size, 8);
    fs.fsync(fh, false).await.unwrap();
    assert_eq!(fs.read(fh, 0, 32).await.unwrap(), b"keep\0\0\0\0");
    fs.close(fh).await.unwrap();
}

#[tokio::test]
async fn native_setattr_denied_preserves_data() {
    let fs = workspace_vfs().await;
    let runtime = attach_runtime(&fs).await;
    fs.create_file("/file").await.unwrap();
    let ino = fs.stat("/file").await.unwrap().ino;
    fs.write_ino(ino, 0, b"private").await.unwrap();
    fs.chmod(ino, 0o600).await.unwrap();
    let error = fs
        .set_attr_from_fuse_as(
            ino,
            &SetAttrRequest {
                size: Some(0),
                ..Default::default()
            },
            SetAttrFlags::empty(),
            1234,
            &[1234],
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        crate::vfs::error::VfsError::PermissionDenied { .. }
    ));
    assert_eq!(fs.stat("/file").await.unwrap().size, 7);
    assert_eq!(runtime.read(ino as u64, 0, 32).await.unwrap(), b"private");
}

#[tokio::test]
async fn native_setattr_serializes_native_writes() {
    let fs = workspace_vfs().await;
    attach_runtime(&fs).await;
    fs.create_file("/file").await.unwrap();
    let ino = fs.stat("/file").await.unwrap().ino;
    let fh = fs.open_fresh_ino(ino, true, true, false).await.unwrap();
    for route in 0..3 {
        let mutation_guard = fs.state.append_lock(ino).lock_owned().await;
        let write = async {
            match route {
                0 => fs.write(fh, 0, b"data").await,
                1 => fs.write_ino(ino, 0, b"data").await,
                _ => fs.write_cached_ino(ino, 0, b"data", 1).await,
            }
        };
        tokio::pin!(write);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut write)
                .await
                .is_err(),
            "native write route {route} must wait for the setattr mutation lock"
        );
        drop(mutation_guard);
        assert_eq!(write.await.unwrap(), 4);
    }
    fs.close(fh).await.unwrap();
}

struct FailingBase;

#[async_trait::async_trait]
impl BaseDataSource for FailingBase {
    async fn size(&self, _inode: u64) -> Result<u64, NativeIoError> {
        Err(NativeIoError::Base("injected baseline failure".into()))
    }

    async fn read_exact(
        &self,
        _inode: u64,
        _offset: u64,
        _output: &mut [u8],
    ) -> Result<(), NativeIoError> {
        unreachable!("truncate only needs the baseline size")
    }
}

#[tokio::test]
async fn native_setattr_runtime_error_preserves_metadata() {
    let fs = workspace_vfs().await;
    attach_runtime_with_base(&fs, Arc::new(FailingBase)).await;
    fs.create_file("/file").await.unwrap();
    let ino = fs.stat("/file").await.unwrap().ino;
    let error = fs
        .set_attr_from_fuse(
            ino,
            &SetAttrRequest {
                size: Some(128),
                ..Default::default()
            },
            SetAttrFlags::empty(),
        )
        .await
        .expect_err("native runtime failure must reach the FUSE caller");
    assert!(error.to_string().contains("injected baseline failure"));
    assert_eq!(fs.stat("/file").await.unwrap().size, 0);
}
