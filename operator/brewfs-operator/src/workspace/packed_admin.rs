//! Actual Redis/TiKV packed-v3 consumer used by the operator.
use super::super::crd::WorkspaceCatalogBackend;
use super::*;
use crate::crd::BrewFSCluster;
use brewfs::cadapter::client::ObjectClient;
use brewfs::cadapter::s3::{S3Backend, S3Config};
use brewfs::chunk::cache::ChunksCacheConfig;
use brewfs::chunk::store::BlockStoreConfig;
use brewfs::chunk::{ChunkLayout, Compression, ObjectBlockStore};
use brewfs::workspace_overlay::ids::{JournalId, LeaseId};
use brewfs::workspace_overlay::packed_admin::{
    PackedCleanAdmission, PackedHeadlessSnapshotDescription, PackedHeadlessSnapshotRequest,
    PackedPublishedViewReport, PackedRecoveredMountReport, PackedReleasedMountReference,
};
use brewfs::workspace_overlay::packed_v3::wire005::V3MountBudget;
use brewfs::workspace_overlay::publish::binding::PackedLowerBindingRecord;
use brewfs::workspace_overlay::stores::kv_backend::WorkspaceKvBackend;
use brewfs::workspace_overlay::stores::redis::RedisWorkspaceBackend;
use brewfs::workspace_overlay::stores::tikv::TiKvWorkspaceBackend;
use kube::ResourceExt;

type PackedStoreFactory<B> = Arc<
    dyn Fn(
            Arc<V3MountBudget>,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = anyhow::Result<KvWorkspaceStore<B>>> + Send>,
        > + Send
        + Sync,
>;

struct PackedStoreWorkspaceAdmin<B: WorkspaceKvBackend> {
    native: StoreWorkspaceAdmin<KvWorkspaceStore<B>>,
    budget: Arc<V3MountBudget>,
    publisher_store_factory: PackedStoreFactory<B>,
    object_config: S3Config,
    object_access_key: String,
    object_secret_key: String,
    layout: ChunkLayout,
    lease_ttl_seconds: u32,
}
impl<B: WorkspaceKvBackend> PackedStoreWorkspaceAdmin<B> {
    fn new(
        store: KvWorkspaceStore<B>,
        budget: Arc<V3MountBudget>,
        cluster: &BrewFSCluster,
        namespace: &str,
        lease_ttl_seconds: u32,
        publisher_store_factory: PackedStoreFactory<B>,
        object_credentials: super::super::object_credentials::AdminObjectCredentials,
    ) -> Self {
        Self {
            native: StoreWorkspaceAdmin::new(store),
            budget,
            publisher_store_factory,
            object_config: S3Config {
                bucket: cluster.spec.rustfs.bucket.clone(),
                region: Some(cluster.spec.rustfs.region.clone()),
                endpoint: Some(format!(
                    "http://{}-rustfs.{namespace}.svc.cluster.local:{}",
                    cluster.name_any(),
                    cluster.spec.rustfs.port
                )),
                part_size: cluster.spec.mount_config.part_size,
                max_concurrency: cluster.spec.mount_config.max_concurrency,
                force_path_style: cluster.spec.mount_config.force_path_style,
                ..Default::default()
            },
            object_access_key: object_credentials.access_key,
            object_secret_key: object_credentials.secret_key,
            layout: ChunkLayout {
                chunk_size: cluster.spec.mount_config.chunk_size,
                block_size: cluster.spec.mount_config.block_size,
            },
            lease_ttl_seconds,
        }
    }
    async fn fresh_publication_scope(
        &self,
    ) -> anyhow::Result<(Arc<KvWorkspaceStore<B>>, Arc<V3MountBudget>)> {
        let budget = V3MountBudget::from_env()?;
        let store = (self.publisher_store_factory)(budget.clone()).await?;
        Ok((Arc::new(store), budget))
    }

    async fn object_client(
        &self,
        budget: &Arc<V3MountBudget>,
    ) -> anyhow::Result<ObjectClient<S3Backend>> {
        let client = ObjectClient::new(
            S3Backend::with_static_credentials(
                self.object_config.clone(),
                self.object_access_key.clone(),
                self.object_secret_key.clone(),
            )
            .await?,
        );
        let observer = budget
            .read_observer(client.read_observer())
            .map_err(|error| anyhow!(error.to_string()))?;
        Ok(client.with_read_observer(
            observer,
            brewfs::cadapter::read_observer::Engine::PackedV3,
            brewfs::cadapter::read_observer::Phase::Startup,
            brewfs::cadapter::read_observer::Origin::Demand,
        ))
    }

    async fn run_packed_snapshot(
        &self,
        workspace: WorkspaceId,
        reference: Option<PackedReleasedMountReference>,
        request: EnsureSnapshotRequest,
        ttl_seconds: u32,
    ) -> anyhow::Result<SnapshotView> {
        match self
            .native
            .store
            .inspect_packed_snapshot(request.snapshot_id)
            .await
        {
            Ok(existing) => {
                if existing.owner_id != request.owner_id
                    || existing.name.as_deref() != Some(request.name.as_str())
                {
                    bail!("snapshot deterministic ID has a different owner or name");
                }
                return Ok(SnapshotView {
                    snapshot_id: existing.snapshot_id,
                    revision: existing.revision,
                });
            }
            Err(WorkspaceError::SnapshotNotFound(_)) => {}
            Err(error) => return Err(error.into()),
        }
        let (store, budget) = self.fresh_publication_scope().await?;
        let ticket = if let Some(reference) = reference {
            match store
                .admit_clean_packed_source(reference, budget.clone())
                .await?
            {
                PackedCleanAdmission::Ready(ticket) => Some(ticket),
                PackedCleanAdmission::RequiresRecovery => None,
            }
        } else {
            None
        };
        // This is a report from authenticated Redis/TiKV state, not a PCR
        // or a recovery capability minted from Kubernetes observations.
        let recovered = if ticket.is_none() {
            store.inspect_recovered_packed_mount(workspace).await?
        } else {
            None
        };
        let client = self.object_client(&budget).await?;
        let scratch =
            Arc::new(tempfile::tempdir().context("create bounded packed-v3 operator scratch")?);
        let upper = Arc::new(
            ObjectBlockStore::new_with_configs_async(
                client.clone(),
                ChunksCacheConfig::with_budgets(
                    1 << 20,
                    1 << 20,
                    scratch.path().join("chunk-cache"),
                ),
                BlockStoreConfig {
                    block_size: self.layout.block_size as usize,
                    compression: Compression::None,
                    populate_write_cache_after_upload: false,
                    range_background_prefetch: false,
                    page_cache_capacity: 0,
                    ..Default::default()
                },
            )
            .await?,
        );
        let scope = *request.snapshot_id.as_uuid();
        let publication = PackedHeadlessSnapshotRequest::bounded_operator(
            PackedHeadlessSnapshotDescription {
                snapshot_id: request.snapshot_id,
                snapshot_name: request.name,
                owner_id: request.owner_id,
            },
            LeaseId::new(),
            JournalId::from_uuid(deterministic_uuid(scope, "packed-native-journal")),
            LayerId::from_uuid(deterministic_uuid(scope, "packed-next-head")),
            u64::from(ttl_seconds) * 1_000_000_000,
            scratch.path().to_path_buf(),
        )
        .with_temporary_owner(scratch)?;
        // NQB routes an existing actual operation to its PPJ. Neither Kubernetes
        // status nor an operator-generated journal ID grants recovery authority.
        let result = match (ticket, recovered) {
            (Some(ticket), _) => {
                store
                    .publish_clean_packed_snapshot(ticket, client, upper, self.layout, publication)
                    .await
            }
            (None, Some(report)) => {
                store
                    .publish_recovered_packed_snapshot(
                        report.released,
                        client,
                        upper,
                        self.layout,
                        publication,
                    )
                    .await
            }
            (None, None) => {
                store
                    .recover_clean_packed_snapshot(
                        workspace,
                        None,
                        client,
                        upper,
                        self.layout,
                        publication,
                    )
                    .await
            }
        };
        match result {
            Ok(result) => Ok(SnapshotView {
                snapshot_id: result.snapshot_id,
                revision: result.packed_carrier_revision,
            }),
            Err(failure) => Err(anyhow!("packed-v3 publication failed: {}", failure.error())),
        }
    }
}

pub async fn connect_workspace_admin_for_cluster(
    client: &kube::Client,
    cluster: &BrewFSCluster,
    kubernetes_namespace: &str,
    spec: &WorkspaceClusterSpec,
) -> anyhow::Result<Arc<dyn WorkspaceAdmin>> {
    spec.validate().map_err(|error| anyhow!(error))?;
    let object_credentials = super::super::object_credentials::load_admin_object_credentials(
        client,
        kubernetes_namespace,
        cluster,
        spec,
    )
    .await?;
    let metadata_credentials = Arc::new(
        super::super::metadata_credentials::load_metadata_credentials(
            client,
            kubernetes_namespace,
            spec,
        )
        .await?,
    );
    let namespace = catalog_namespace(&cluster.name_any(), kubernetes_namespace, spec);
    let budget = V3MountBudget::from_env()?;
    match spec.catalog_backend {
        WorkspaceCatalogBackend::Redis => {
            let port = u16::try_from(cluster.spec.redis.port)
                .ok()
                .filter(|port| *port > 0)
                .ok_or_else(|| anyhow!("Redis metadata port must be between 1 and 65535"))?;
            let host = format!(
                "{}-workspace-redis.{kubernetes_namespace}.svc.cluster.local",
                cluster.name_any()
            );
            let backend = metadata_credentials
                .connect_redis(&host, port, &namespace)
                .await?;
            let publisher_store_factory: PackedStoreFactory<RedisWorkspaceBackend> =
                Arc::new(move |budget| {
                    let credentials = metadata_credentials.clone();
                    let host = host.clone();
                    let namespace = namespace.clone();
                    Box::pin(async move {
                        let backend = credentials
                            .connect_redis(&host, port, &namespace)
                            .await
                            .context("connect Redis packed-v3 publication scope")?;
                        Ok(KvWorkspaceStore::new(backend).with_packed_reader_pin_budget(budget))
                    })
                });
            Ok(Arc::new(PackedStoreWorkspaceAdmin::new(
                KvWorkspaceStore::new(backend).with_packed_reader_pin_budget(budget.clone()),
                budget,
                cluster,
                kubernetes_namespace,
                spec.lease_ttl_seconds,
                publisher_store_factory,
                object_credentials,
            )))
        }
        WorkspaceCatalogBackend::TiKv => {
            let backend = metadata_credentials
                .connect_tikv(spec.tikv_pd_endpoints.clone(), &namespace, budget.clone())
                .await
                .context("connect TiKV workspace catalog")?;
            let endpoints = spec.tikv_pd_endpoints.clone();
            let publisher_store_factory: PackedStoreFactory<TiKvWorkspaceBackend> =
                Arc::new(move |budget| {
                    let credentials = metadata_credentials.clone();
                    let endpoints = endpoints.clone();
                    let namespace = namespace.clone();
                    Box::pin(async move {
                        let backend = credentials
                            .connect_tikv(endpoints, &namespace, budget.clone())
                            .await
                            .context("connect TiKV packed-v3 publication scope")?;
                        Ok(KvWorkspaceStore::new(backend).with_packed_reader_pin_budget(budget))
                    })
                });
            Ok(Arc::new(PackedStoreWorkspaceAdmin::new(
                KvWorkspaceStore::new(backend).with_packed_reader_pin_budget(budget.clone()),
                budget,
                cluster,
                kubernetes_namespace,
                spec.lease_ttl_seconds,
                publisher_store_factory,
                object_credentials,
            )))
        }
    }
}

#[async_trait]
impl<B: WorkspaceKvBackend> WorkspaceAdmin for PackedStoreWorkspaceAdmin<B> {
    async fn packed_gc_admin(
        self: Arc<Self>,
    ) -> anyhow::Result<Arc<dyn brewfs::workspace_overlay::packed_admin::PackedGcAdmin>> {
        // Consume the already authenticated facade scope. A second connection
        // would leave its original SDK workers outside the returned GC owner.
        let store = self.native.store.clone();
        let budget = self.budget.clone();
        let result = async {
            let client = ObjectClient::new(
                S3Backend::with_gc_static_credentials(
                    self.object_config.clone(),
                    self.object_access_key.clone(),
                    self.object_secret_key.clone(),
                )
                .await?,
            );
            store
                .open_packed_gc_admin(client, budget.clone(), self.layout)
                .await
                .map_err(anyhow::Error::from)
        }
        .await;
        if result.is_err() {
            // Authentication/configuration errors retain ownership until every
            // metadata worker has joined, before the budget is closed.
            let drained = store.shutdown_metadata_backend().await;
            budget.close();
            drained?;
        }
        result
    }
    async fn ensure_volume(&self, identity: VolumeIdentity) -> anyhow::Result<VolumeView> {
        let (store, budget) = self.fresh_publication_scope().await?;
        let volume = CreateVolumeRoot {
            upper_bucket: None,
            volume_format: OPERATOR_VOLUME_FORMAT.into(),
            schema_version: WORKSPACE_SCHEMA_VERSION,
            volume_id: identity.volume_id,
            workspace_id: identity.root_workspace_id,
            root_layer_id: identity.root_layer_id,
            writable_layer_id: identity.writable_layer_id,
            owner_id: Some(identity.owner_id.clone()),
        };
        let client = self.object_client(&budget).await?;
        let scratch =
            Arc::new(tempfile::tempdir().context("create bounded packed-v3 root scratch")?);
        let upper = Arc::new(
            ObjectBlockStore::new_with_configs_async(
                client.clone(),
                ChunksCacheConfig::with_budgets(
                    1 << 20,
                    1 << 20,
                    scratch.path().join("chunk-cache"),
                ),
                BlockStoreConfig {
                    block_size: self.layout.block_size as usize,
                    compression: Compression::None,
                    populate_write_cache_after_upload: false,
                    range_background_prefetch: false,
                    page_cache_capacity: 0,
                    ..Default::default()
                },
            )
            .await?,
        );
        let scope = *identity.root_snapshot_id.as_uuid();
        let request = PackedHeadlessSnapshotRequest::bounded_operator(
            PackedHeadlessSnapshotDescription {
                snapshot_id: identity.root_snapshot_id,
                snapshot_name: format!("{}-root", identity.owner_id),
                owner_id: Some(identity.owner_id),
            },
            LeaseId::new(),
            JournalId::from_uuid(deterministic_uuid(scope, "packed-bootstrap-native-journal")),
            LayerId::from_uuid(deterministic_uuid(scope, "packed-bootstrap-next-head")),
            u64::from(self.lease_ttl_seconds) * 1_000_000_000,
            scratch.path().to_path_buf(),
        )
        .with_temporary_owner(scratch)?;
        let result = store
            .bootstrap_initial_packed_snapshot(volume, client, upper, self.layout, request)
            .await
            .map_err(|failure| anyhow!("packed-v3 root bootstrap failed: {}", failure.error()))?;
        // The actual publisher has drained and closed its canonical scope.
        // This result is a report; future operations connect a fresh scope and
        // still acquire native/persisted-reader authority from Redis/TiKV.
        Ok(VolumeView {
            volume_id: identity.volume_id,
            root_snapshot_id: result.snapshot_id,
            root_revision: result.packed_carrier_revision,
            packed_binding: Some(result.binding),
        })
    }
    async fn ensure_workspace(
        &self,
        request: EnsureWorkspaceRequest,
    ) -> anyhow::Result<WorkspaceView> {
        self.native.ensure_workspace(request).await
    }
    async fn ensure_packed_workspace(
        &self,
        request: EnsureWorkspaceRequest,
    ) -> anyhow::Result<WorkspaceView> {
        match self.native.store.load_workspace(request.workspace_id).await {
            Ok(record) if record.owner_id == request.owner_id => {
                self.native
                    .store
                    .inspect_packed_workspace_binding(request.workspace_id)
                    .await?
                    .ok_or_else(|| anyhow!("packed workspace binding is missing"))?;
                if record.fork_base.as_ref() != Some(&request.base_revision) {
                    bail!("packed workspace deterministic ID has a different original fork source");
                }
            }
            Ok(_) => bail!("deterministic packed workspace ID has another owner"),
            Err(WorkspaceError::WorkspaceNotFound(_)) => {
                self.native
                    .store
                    .create_workspace_from_packed_carrier(CreateWorkspace {
                        upper_bucket: None,
                        workspace_id: request.workspace_id,
                        head_layer_id: request.head_layer_id,
                        base_revision: request.base_revision,
                        owner_id: request.owner_id,
                    })
                    .await?;
            }
            Err(error) => return Err(error.into()),
        }
        self.inspect_workspace(request.workspace_id).await
    }
    async fn packed_binding(
        &self,
        id: WorkspaceId,
    ) -> anyhow::Result<Option<PackedLowerBindingRecord>> {
        Ok(self
            .native
            .store
            .inspect_packed_workspace_binding(id)
            .await?)
    }
    async fn verify_clean_packed_mount(
        &self,
        reference: PackedReleasedMountReference,
    ) -> anyhow::Result<bool> {
        Ok(matches!(
            self.native
                .store
                .admit_clean_packed_source(reference, self.budget.clone())
                .await?,
            PackedCleanAdmission::Ready(_)
        ))
    }
    async fn clean_released_packed_mount(
        &self,
        workspace: WorkspaceId,
    ) -> anyhow::Result<Option<PackedReleasedMountReference>> {
        Ok(self
            .native
            .store
            .inspect_original_clean_packed_mount(workspace)
            .await?)
    }
    async fn recovered_packed_mount(
        &self,
        workspace: WorkspaceId,
    ) -> anyhow::Result<Option<PackedRecoveredMountReport>> {
        Ok(self
            .native
            .store
            .inspect_recovered_packed_mount(workspace)
            .await?)
    }
    async fn expired_packed_mount_recovery(
        &self,
        original: PackedReleasedMountReference,
    ) -> anyhow::Result<Option<(PackedReleasedMountReference, u64)>> {
        Ok(self
            .native
            .store
            .inspect_expired_packed_mount_recovery(original)
            .await?)
    }
    async fn unstarted_packed_mount_recovery(
        &self,
        original: PackedReleasedMountReference,
        failed_lease: LeaseId,
        next_lease: LeaseId,
    ) -> anyhow::Result<Option<u64>> {
        Ok(self
            .native
            .store
            .inspect_unstarted_packed_mount_recovery(original, failed_lease, next_lease)
            .await?)
    }
    async fn original_packed_mount_for_cleanup(
        &self,
        original: PackedReleasedMountReference,
    ) -> anyhow::Result<bool> {
        Ok(self
            .native
            .store
            .verify_original_packed_mount_for_cleanup(original)
            .await?)
    }
    async fn packed_mount_recovery_for_cleanup(
        &self,
        original: PackedReleasedMountReference,
    ) -> anyhow::Result<Option<PackedRecoveredMountReport>> {
        Ok(self
            .native
            .store
            .inspect_packed_mount_recovery_for_cleanup(original)
            .await?)
    }
    async fn clean_published_view(
        &self,
        workspace: WorkspaceId,
    ) -> anyhow::Result<Option<PackedPublishedViewReport>> {
        Ok(self
            .native
            .store
            .inspect_clean_published_view(workspace)
            .await?)
    }
    async fn clean_unmounted_packed_epoch(
        &self,
        workspace: WorkspaceId,
    ) -> anyhow::Result<Option<u64>> {
        Ok(self
            .native
            .store
            .inspect_unmounted_packed_source(workspace)
            .await?
            .map(|binding| binding.head_epoch))
    }
    async fn pin_clean_packed_snapshot(
        &self,
        workspace: WorkspaceId,
        request: EnsureSnapshotRequest,
    ) -> anyhow::Result<SnapshotView> {
        let snapshot = self
            .native
            .store
            .pin_clean_packed_snapshot(
                workspace,
                CreateSnapshot {
                    snapshot_id: request.snapshot_id,
                    name: Some(request.name),
                    revision: request.revision,
                    owner_id: request.owner_id,
                },
            )
            .await?;
        Ok(SnapshotView {
            snapshot_id: snapshot.snapshot_id,
            revision: snapshot.revision,
        })
    }
    async fn verify_packed_revision(&self, revision: &BaseRevision) -> anyhow::Result<()> {
        self.native
            .store
            .inspect_packed_carrier_revision(revision)
            .await?;
        Ok(())
    }
    async fn publish_packed_snapshot(
        &self,
        reference: PackedReleasedMountReference,
        request: EnsureSnapshotRequest,
        ttl_seconds: u32,
    ) -> anyhow::Result<SnapshotView> {
        let workspace = reference.guard.workspace_id;
        self.run_packed_snapshot(workspace, Some(reference), request, ttl_seconds)
            .await
    }
    async fn recover_packed_snapshot(
        &self,
        workspace: WorkspaceId,
        request: EnsureSnapshotRequest,
        ttl_seconds: u32,
    ) -> anyhow::Result<SnapshotView> {
        self.run_packed_snapshot(workspace, None, request, ttl_seconds)
            .await
    }
    async fn inspect_workspace(&self, id: WorkspaceId) -> anyhow::Result<WorkspaceView> {
        let view = self
            .native
            .store
            .inspect_packed_workspace_view(id)
            .await?
            .ok_or_else(|| anyhow!("packed-v3 workspace binding is missing"))?;
        Ok(WorkspaceView {
            record: view.workspace,
            base_revision: view.base_revision,
            layer_depth: view.layer_depth,
            leases: view.leases,
        })
    }
    async fn verify_revision(&self, revision: &BaseRevision) -> anyhow::Result<()> {
        self.native.verify_revision(revision).await
    }
    async fn ensure_snapshot(
        &self,
        request: EnsureSnapshotRequest,
    ) -> anyhow::Result<SnapshotView> {
        self.native.ensure_snapshot(request).await
    }
    async fn load_snapshot(&self, id: SnapshotId) -> anyhow::Result<SnapshotView> {
        let snapshot = self.native.store.inspect_packed_snapshot(id).await?;
        Ok(SnapshotView {
            snapshot_id: snapshot.snapshot_id,
            revision: snapshot.revision,
        })
    }
    async fn delete_snapshot(&self, id: SnapshotId) -> anyhow::Result<()> {
        self.native.delete_snapshot(id).await
    }
    async fn mark_workspace_deleting(&self, id: WorkspaceId, force: bool) -> anyhow::Result<()> {
        self.native.mark_workspace_deleting(id, force).await
    }
    async fn reap_expired_leases(&self) -> anyhow::Result<u64> {
        self.native.reap_expired_leases().await
    }
    async fn list_workspaces(&self) -> anyhow::Result<Vec<WorkspaceRecord>> {
        self.native.list_workspaces().await
    }
    async fn list_snapshots(&self) -> anyhow::Result<Vec<SnapshotView>> {
        self.native.list_snapshots().await
    }
}
