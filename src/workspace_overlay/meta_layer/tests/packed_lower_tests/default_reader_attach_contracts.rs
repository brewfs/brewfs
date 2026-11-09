use super::*;
use crate::workspace_overlay::catalog::*;
use crate::workspace_overlay::digest::CanonicalLayerDelta;
use crate::workspace_overlay::ids::*;
use crate::workspace_overlay::model::*;

struct BindingWithoutPins<W> {
    inner: Arc<W>,
    binding: Option<PackedLowerBinding>,
    probes: AtomicU64,
}

#[tokio::test]
async fn packed_default_reader_missing_binding_fences_before_unsupported_lifecycle() {
    let f = fixture().await;
    let store = Arc::new(BindingWithoutPins {
        inner: f.meta.store().clone(),
        binding: None,
        probes: AtomicU64::new(0),
    });
    let error = store
        .clone()
        .open_packed_reader_session(
            f.meta.guard().await,
            f.lower.mount_budget(),
            crate::workspace_overlay::packed_reader_lifecycle::PackedReaderLeaseOptions::default(),
        )
        .await
        .err()
        .unwrap();
    assert!(matches!(error, WorkspaceError::Fenced));
    assert_eq!(store.probes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn packed_default_reader_present_binding_still_cannot_attach_without_pins() {
    let f = fixture().await;
    let store = Arc::new(BindingWithoutPins {
        inner: f.meta.store().clone(),
        binding: Some(f.authority.binding.clone()),
        probes: AtomicU64::new(0),
    });
    let before = f.gets.load(Ordering::SeqCst);
    let candidate =
        WorkspaceMetaLayer::with_chunk_size(store.clone(), f.meta.view_context().await, 4096)
            .with_packed_v3_lower_from_store(
                f.lower.clone(),
                f.upper.clone(),
                ChunkLayout {
                    chunk_size: 4096,
                    block_size: 4096,
                },
            )
            .await;
    assert!(matches!(candidate, Err(MetaError::NotSupported(_))));
    assert_eq!(store.probes.load(Ordering::SeqCst), 1);
    assert_eq!(
        f.gets.load(Ordering::SeqCst),
        before,
        "unsupported lifecycle must not perform lower I/O"
    );
}

#[async_trait]
impl<W: WorkspaceStore + 'static> WorkspaceStore for BindingWithoutPins<W> {
    async fn load_packed_lower_binding(
        &self,
        _: HeadGuard,
    ) -> Result<Option<PackedLowerBinding>, WorkspaceError> {
        self.probes.fetch_add(1, Ordering::SeqCst);
        Ok(self.binding.clone())
    }
    fn name(&self) -> &'static str {
        self.inner.name()
    }
    fn capabilities(&self) -> WorkspaceStoreCapabilities {
        self.inner.capabilities()
    }
    async fn initialize_workspace_schema(&self) -> Result<(), WorkspaceError> {
        self.inner.initialize_workspace_schema().await
    }
    async fn load_volume_header(&self) -> Result<Option<VolumeHeader>, WorkspaceError> {
        self.inner.load_volume_header().await
    }
    async fn load_workspace(&self, id: WorkspaceId) -> Result<WorkspaceRecord, WorkspaceError> {
        self.inner.load_workspace(id).await
    }
    async fn load_layer(&self, id: LayerId) -> Result<LayerRecord, WorkspaceError> {
        self.inner.load_layer(id).await
    }
    async fn load_layer_chain(&self, head: LayerId) -> Result<Vec<LayerRecord>, WorkspaceError> {
        self.inner.load_layer_chain(head).await
    }
    async fn allocate_id(&self, name: &str) -> Result<i64, WorkspaceError> {
        self.inner.allocate_id(name).await
    }
    async fn create_volume_root(
        &self,
        request: CreateVolumeRoot,
    ) -> Result<WorkspaceRecord, WorkspaceError> {
        self.inner.create_volume_root(request).await
    }
    async fn create_workspace(
        &self,
        request: CreateWorkspace,
    ) -> Result<WorkspaceRecord, WorkspaceError> {
        self.inner.create_workspace(request).await
    }
    async fn list_workspaces(&self) -> Result<Vec<WorkspaceRecord>, WorkspaceError> {
        self.inner.list_workspaces().await
    }
    async fn create_snapshot(
        &self,
        request: CreateSnapshot,
    ) -> Result<SnapshotRecord, WorkspaceError> {
        self.inner.create_snapshot(request).await
    }
    async fn load_snapshot(&self, id: SnapshotId) -> Result<SnapshotRecord, WorkspaceError> {
        self.inner.load_snapshot(id).await
    }
    async fn list_snapshots(&self) -> Result<Vec<SnapshotRecord>, WorkspaceError> {
        self.inner.list_snapshots().await
    }
    async fn delete_snapshot(&self, id: SnapshotId) -> Result<(), WorkspaceError> {
        self.inner.delete_snapshot(id).await
    }
    async fn acquire_lease(&self, request: AcquireLease) -> Result<SnapshotLease, WorkspaceError> {
        self.inner.acquire_lease(request).await
    }
    async fn renew_lease(&self, request: RenewLease) -> Result<SnapshotLease, WorkspaceError> {
        self.inner.renew_lease(request).await
    }
    async fn release_lease(&self, request: ReleaseLease) -> Result<(), WorkspaceError> {
        self.inner.release_lease(request).await
    }
    async fn reap_expired_leases(&self) -> Result<u64, WorkspaceError> {
        self.inner.reap_expired_leases().await
    }
    async fn list_leases(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Vec<SnapshotLease>, WorkspaceError> {
        self.inner.list_leases(workspace_id).await
    }
    async fn get_dentry_deltas(
        &self,
        request: DentryQuery,
    ) -> Result<Vec<DentryDelta>, WorkspaceError> {
        self.inner.get_dentry_deltas(request).await
    }
    async fn get_inode_deltas(
        &self,
        request: InodeQuery,
    ) -> Result<Vec<InodeDelta>, WorkspaceError> {
        self.inner.get_inode_deltas(request).await
    }
    async fn get_extent_deltas(
        &self,
        request: ExtentQuery,
    ) -> Result<Vec<DataExtentDelta>, WorkspaceError> {
        self.inner.get_extent_deltas(request).await
    }
    async fn get_xattr_deltas(
        &self,
        request: XattrQuery,
    ) -> Result<Vec<XattrDelta>, WorkspaceError> {
        self.inner.get_xattr_deltas(request).await
    }
    async fn get_acl_deltas(&self, request: AclQuery) -> Result<Vec<AclDelta>, WorkspaceError> {
        self.inner.get_acl_deltas(request).await
    }
    async fn apply_namespace_mutation(
        &self,
        request: NamespaceMutation,
    ) -> Result<MutationResult, WorkspaceError> {
        self.inner.apply_namespace_mutation(request).await
    }
    async fn apply_inode_mutation(
        &self,
        request: InodeMutation,
    ) -> Result<InodeDelta, WorkspaceError> {
        self.inner.apply_inode_mutation(request).await
    }
    async fn append_data_extent(
        &self,
        request: AppendDataExtent,
    ) -> Result<DataExtentDelta, WorkspaceError> {
        self.inner.append_data_extent(request).await
    }
    async fn apply_data_mutation(
        &self,
        request: DataMutation,
    ) -> Result<DataMutationResult, WorkspaceError> {
        self.inner.apply_data_mutation(request).await
    }
    async fn apply_xattr_mutation(&self, request: XattrMutation) -> Result<(), WorkspaceError> {
        self.inner.apply_xattr_mutation(request).await
    }
    async fn apply_acl_mutation(&self, request: AclMutation) -> Result<(), WorkspaceError> {
        self.inner.apply_acl_mutation(request).await
    }
    async fn load_layer_delta(
        &self,
        layer_id: LayerId,
    ) -> Result<CanonicalLayerDelta, WorkspaceError> {
        self.inner.load_layer_delta(layer_id).await
    }
    async fn begin_workspace_seal(
        &self,
        request: BeginWorkspaceSeal,
    ) -> Result<SealJournal, WorkspaceError> {
        self.inner.begin_workspace_seal(request).await
    }
    async fn commit_workspace_seal(
        &self,
        request: CommitWorkspaceSeal,
    ) -> Result<SealResult, WorkspaceError> {
        self.inner.commit_workspace_seal(request).await
    }
    async fn abort_workspace_seal(
        &self,
        request: AbortWorkspaceSeal,
    ) -> Result<(), WorkspaceError> {
        self.inner.abort_workspace_seal(request).await
    }
    async fn load_seal_journal(
        &self,
        journal_id: JournalId,
    ) -> Result<SealJournal, WorkspaceError> {
        self.inner.load_seal_journal(journal_id).await
    }
    async fn list_seal_journals(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Vec<SealJournal>, WorkspaceError> {
        self.inner.list_seal_journals(workspace_id).await
    }
    async fn mark_workspace_deleting(&self, request: MarkDeleting) -> Result<(), WorkspaceError> {
        self.inner.mark_workspace_deleting(request).await
    }
    async fn record_orphan_slice(&self, request: RecordOrphanSlice) -> Result<(), WorkspaceError> {
        self.inner.record_orphan_slice(request).await
    }
    async fn gc_snapshot(
        &self,
        now_ns: i64,
        lease_grace_ns: u64,
    ) -> Result<GcSnapshot, WorkspaceError> {
        self.inner.gc_snapshot(now_ns, lease_grace_ns).await
    }
    async fn delete_layer_metadata(
        &self,
        request: DeleteLayerMetadata,
    ) -> Result<(), WorkspaceError> {
        self.inner.delete_layer_metadata(request).await
    }
    async fn finalize_layer_metadata_deletion(
        &self,
        layer_ids: Vec<LayerId>,
    ) -> Result<(), WorkspaceError> {
        self.inner.finalize_layer_metadata_deletion(layer_ids).await
    }
    async fn prune_terminal_records(
        &self,
        now_ns: i64,
        grace_ns: u64,
    ) -> Result<(), WorkspaceError> {
        self.inner.prune_terminal_records(now_ns, grace_ns).await
    }
}
