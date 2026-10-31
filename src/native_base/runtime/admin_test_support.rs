//! Shared nonempty P1 qualification fixture. Publication alone is test-only;
//! namespace writes, extents, leases, immutable lower reads and native writes
//! all use their production implementations.

use std::sync::Arc;

use uuid::Uuid;

use crate::chunk::{BlockStore, ChunkLayout, InMemoryBlockStore, SliceDesc};
use crate::meta::MetaLayer;
use crate::native_base::write::{
    overlay::{OverlayParams, WriteOverlay},
    receipts::MemorySink,
    store::ControlStore,
};
use crate::workspace_overlay::{
    admin::verify_revision,
    catalog::{AcquireLease, CreateVolumeRoot, ReleaseLease, WorkspaceStore},
    ids::{LayerId, LeaseId, WorkspaceId},
    meta_layer::WorkspaceMetaLayer,
    model::{BaseRevision, SnapshotLease, ViewContext},
};

use super::{NativeDataRuntime, NativeVolumeHeader, WorkspaceBaseDataSource, inspect_p1_control};

pub(crate) const LOWER_BYTES: &[u8] = b"immutable lower content";
pub(crate) const LOWER_SLICE: u64 = 77;
pub(crate) const LAYOUT: ChunkLayout = ChunkLayout {
    chunk_size: 4096,
    block_size: 64,
};

pub(crate) struct Seed {
    pub root_workspace: WorkspaceId,
    pub revision: BaseRevision,
    pub inode: i64,
    pub blocks: Arc<InMemoryBlockStore>,
}

pub(crate) async fn seed_catalog_lower<W: WorkspaceStore + 'static>(
    catalog: Arc<W>,
    volume_id: Uuid,
) -> Seed {
    catalog.initialize_workspace_schema().await.unwrap();
    let root = catalog
        .create_volume_root(CreateVolumeRoot {
            volume_format: "workspace-v1".into(),
            schema_version: 1,
            volume_id,
            workspace_id: WorkspaceId::new(),
            root_layer_id: LayerId::new(),
            writable_layer_id: LayerId::new(),
            owner_id: None,
            upper_bucket: None,
        })
        .await
        .unwrap();
    let lease = catalog
        .acquire_lease(AcquireLease {
            workspace_id: root.workspace_id,
            lease_id: LeaseId::new(),
            holder_generation: 1,
            ttl_ns: 60_000_000_000,
        })
        .await
        .unwrap();
    let source = WorkspaceMetaLayer::with_chunk_size(
        catalog.clone(),
        ViewContext {
            workspace_id: root.workspace_id,
            head_layer_id: root.head_layer_id,
            head_epoch: root.head_epoch,
            lease_id: lease.lease_id,
            holder_generation: 1,
        },
        LAYOUT.chunk_size,
    );
    source.initialize().await.unwrap();
    let inode = source
        .create_file(source.root_ino(), "nonempty-source".into())
        .await
        .unwrap();
    let blocks = Arc::new(InMemoryBlockStore::new());
    blocks
        .write_fresh_range((LOWER_SLICE, 0), 0, LOWER_BYTES)
        .await
        .unwrap();
    let chunk_id = crate::vfs::chunk_id_for(inode, 0).unwrap();
    source
        .write(
            inode,
            chunk_id,
            SliceDesc {
                slice_id: LOWER_SLICE,
                chunk_id,
                offset: 0,
                length: LOWER_BYTES.len() as u64,
            },
            LOWER_BYTES.len() as u64,
        )
        .await
        .unwrap();
    catalog
        .release_lease(ReleaseLease {
            lease_id: lease.lease_id,
            holder_generation: 1,
        })
        .await
        .unwrap();
    let revision =
        crate::workspace_overlay::test_base_fixture::install_test_base(&catalog, root.workspace_id)
            .await
            .revision;
    verify_revision(catalog.as_ref(), &revision).await.unwrap();
    Seed {
        root_workspace: root.workspace_id,
        revision,
        inode,
        blocks,
    }
}

pub(crate) struct NativeP1RuntimeFixture {
    pub control: Arc<dyn ControlStore>,
    pub header: NativeVolumeHeader,
    pub namespace: String,
    pub blocks: Arc<InMemoryBlockStore>,
    pub sink: Arc<MemorySink>,
}

impl NativeP1RuntimeFixture {
    pub(crate) async fn open<W: WorkspaceStore + 'static>(
        &self,
        catalog: Arc<W>,
        workspace_id: WorkspaceId,
        generation: u64,
    ) -> (NativeDataRuntime, SnapshotLease) {
        let workspace = catalog.load_workspace(workspace_id).await.unwrap();
        assert!(
            workspace.upper_bucket.is_none(),
            "P1 catalog profile uses the shared object pool"
        );
        let actual = inspect_p1_control(
            self.control.as_ref(),
            &self.header,
            &self.namespace,
            workspace_id,
        )
        .await
        .unwrap();
        assert!(!actual.deleting);
        let domain = actual
            .domain
            .expect("Manager child has an actual native domain");
        assert_eq!(domain.state, "active");
        let lease = catalog
            .acquire_lease(AcquireLease {
                workspace_id,
                lease_id: LeaseId::new(),
                holder_generation: generation,
                ttl_ns: 60_000_000_000,
            })
            .await
            .unwrap();
        let meta = Arc::new(WorkspaceMetaLayer::with_chunk_size(
            catalog,
            ViewContext {
                workspace_id,
                head_layer_id: workspace.head_layer_id,
                head_epoch: workspace.head_epoch,
                lease_id: lease.lease_id,
                holder_generation: generation,
            },
            LAYOUT.chunk_size,
        ));
        meta.initialize().await.unwrap();
        let inode = meta
            .lookup(meta.root_ino(), "nonempty-source")
            .await
            .unwrap()
            .expect("nonempty namespace is retained");
        assert!(meta.stat(inode).await.unwrap().unwrap().size > 0);
        let runtime = NativeDataRuntime::new(
            Arc::new(WriteOverlay::new(
                self.control.clone(),
                self.sink.clone(),
                OverlayParams {
                    volume_id: self.header.volume_id,
                    workspace_id: *workspace_id.as_bytes(),
                    domain_id: *domain.domain_id.as_bytes(),
                    writer_generation: generation,
                    block_size: LAYOUT.block_size as u64,
                },
            )),
            Arc::new(WorkspaceBaseDataSource::new(
                self.blocks.clone(),
                meta,
                LAYOUT,
            )),
        );
        runtime
            .initialize(*domain.owner_id.as_bytes(), workspace.head_epoch)
            .await
            .unwrap();
        (runtime, lease)
    }
}
