//! Minimal fresh fixture bootstrap for the opt-in entity mount benchmark.
//!
//! Callers provide dedicated Redis metadata scopes and an object bucket. This
//! wiring is not production admission, ownership transfer or crash recovery.

use std::sync::Arc;

use uuid::Uuid;

use super::data::EntityData;
use super::keys::EntityKeys;
use super::lifecycle::{Lifecycle, WriterSession};
use super::model::LowerBinding;
use super::namespace::EntityNamespace;
use super::native_lower::NativeLower;
use super::packed_lower::PackedLower;
use super::redis::RedisEntityBackend;
use super::resolver::{ImmutableLower, ImmutableLowerData};
use super::writeback::WritebackConfig;
use super::{EntityError, Result};
use crate::cadapter::client::{ObjectBackend, ObjectClient};
use crate::cadapter::read_observer::ReadClass;
use crate::workspace_overlay::catalog::{
    AcquireLease, CreateVolumeRoot, HeadGuard, ReleaseLease, WorkspaceStore,
};
use crate::workspace_overlay::error::WorkspaceError;
use crate::workspace_overlay::ids::{LayerId, LeaseId, WorkspaceId};
use crate::workspace_overlay::model::{BaseRevision, LayerRecord, WORKSPACE_SCHEMA_VERSION};
use crate::workspace_overlay::packed_v3::wire005::{
    AuthenticatedV3Snapshot, V3IndexReader, V3MountBudget, V3ObjectKind, V3ObjectRef,
};
use crate::workspace_overlay::publish::binding::{InstallPackedLowerBinding, VerifiedPackedLower};
use crate::workspace_overlay::stores::kv_store::KvWorkspaceStore;
use crate::workspace_overlay::stores::kv_store::packed_reader_pins::AcquirePackedReaderPin;
use crate::workspace_overlay::stores::redis::RedisWorkspaceBackend;

const SETUP_TTL_NS: u64 = 300_000_000_000;

pub(crate) enum ExperimentalLower<O: ObjectBackend + Clone> {
    Packed(Box<PackedLower<O, RedisWorkspaceBackend>>),
    Native(NativeLower<RedisEntityBackend, O>),
}
impl<O: ObjectBackend + Clone + 'static> ExperimentalLower<O> {
    fn view(&self) -> &dyn ImmutableLowerData {
        match self {
            Self::Packed(lower) => lower.as_ref(),
            Self::Native(lower) => lower,
        }
    }
}
#[async_trait::async_trait]
impl<O: ObjectBackend + Clone + 'static> ImmutableLower for ExperimentalLower<O> {
    fn binding(&self) -> &LowerBinding {
        self.view().binding()
    }
    async fn inode(&self, ino: i64) -> Result<Option<super::model::EntityInode>> {
        self.view().inode(ino).await
    }
    async fn dentry(&self, ino: i64, name: &[u8]) -> Result<Option<super::model::EntityDentry>> {
        self.view().dentry(ino, name).await
    }
    async fn xattr(&self, ino: i64, name: &[u8]) -> Result<Option<Vec<u8>>> {
        self.view().xattr(ino, name).await
    }
    async fn chunk(
        &self,
        ino: i64,
        index: u64,
        size: u64,
    ) -> Result<Option<super::model::ChunkMap>> {
        self.view().chunk(ino, index, size).await
    }
    async fn readdir(&self, ino: i64) -> Result<Vec<(Vec<u8>, super::model::EntityDentry)>> {
        self.view().readdir(ino).await
    }
    async fn readdir_page(
        &self,
        ino: i64,
        offset: u64,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, super::model::EntityDentry)>> {
        self.view().readdir_page(ino, offset, limit).await
    }
}
#[async_trait::async_trait]
impl<O: ObjectBackend + Clone + 'static> ImmutableLowerData for ExperimentalLower<O> {
    async fn read_range(&self, ino: i64, offset: u64, length: usize) -> Result<Vec<u8>> {
        self.view().read_range(ino, offset, length).await
    }
}
pub(crate) type ExperimentalNamespace<O> =
    EntityNamespace<RedisEntityBackend, ExperimentalLower<O>>;
pub(crate) type ExperimentalData<O> = EntityData<RedisEntityBackend, ExperimentalLower<O>, O>;

pub(crate) struct FreshMountOptions {
    pub namespace_id: Option<Uuid>,
    pub lease_ttl_ns: i64,
    pub chunk_size: u64,
    pub writeback: Option<WritebackConfig>,
}

pub(crate) struct RedisEntityExperiment<O: ObjectBackend + Clone> {
    packed: Arc<KvWorkspaceStore<RedisWorkspaceBackend>>,
    entity: Arc<RedisEntityBackend>,
    objects: O,
    volume_id: Uuid,
}

pub(crate) struct ExperimentalEntityMount<O: ObjectBackend + Clone> {
    namespace: Arc<ExperimentalNamespace<O>>,
    data: Arc<ExperimentalData<O>>,
    lifecycle: Arc<Lifecycle<RedisEntityBackend>>,
    session: WriterSession,
}

fn backend_error(error: impl std::fmt::Display) -> EntityError {
    EntityError::Backend(error.to_string())
}

fn catalog_error(error: WorkspaceError) -> EntityError {
    match error {
        WorkspaceError::Fenced => EntityError::Fenced,
        WorkspaceError::Busy => EntityError::Busy,
        WorkspaceError::CorruptMetadata(message) => EntityError::Corrupt(message),
        other => backend_error(other),
    }
}

impl<O: ObjectBackend + Clone + 'static> RedisEntityExperiment<O> {
    pub async fn create_fresh_from_native(
        &self,
        source: Option<&std::path::Path>,
        manifest: Option<&str>,
        options: FreshMountOptions,
    ) -> Result<Arc<ExperimentalEntityMount<O>>> {
        let lower = match (source, manifest) {
            (Some(source), None) => {
                NativeLower::capture(
                    self.entity.clone(),
                    Arc::new(self.objects.clone()),
                    self.volume_id,
                    source,
                    options.chunk_size,
                )
                .await?
            }
            (None, Some(key)) => {
                NativeLower::open(
                    self.entity.clone(),
                    Arc::new(self.objects.clone()),
                    self.volume_id,
                    key,
                )
                .await?
            }
            _ => {
                return Err(EntityError::Invalid(
                    "select one native source or native manifest".into(),
                ));
            }
        };
        let namespace_id = options.namespace_id.unwrap_or_else(Uuid::new_v4);
        if namespace_id.is_nil() {
            return Err(EntityError::Invalid("nil entity namespace".into()));
        }
        Ok(Arc::new(
            self.finish_entity(ExperimentalLower::Native(lower), namespace_id, options)
                .await?,
        ))
    }

    /// Restart recovery is deliberately upload-only. It neither takes over an
    /// expired writer nor reconstructs metadata publications from a local log.
    pub async fn recover_uploads(
        &self,
        namespace_id: Uuid,
        config: WritebackConfig,
    ) -> Result<super::writeback::WritebackStatus> {
        let keys = EntityKeys::new(self.volume_id, namespace_id);
        let descriptor = Lifecycle::new(self.entity.clone(), keys.clone())
            .descriptor()
            .await?;
        if descriptor.format != super::model::ENTITY_WRITEBACK_FORMAT
            || descriptor.state != super::lifecycle::NamespaceState::Active
        {
            return Err(EntityError::Fenced);
        }
        let queue = super::writeback::EntityWriteback::open(
            self.entity.clone(),
            Arc::new(self.objects.clone()),
            keys,
            config,
        )?;
        queue.upload_pass().await?;
        while queue.status().await?.pending_objects > 0 {
            queue.upload_pass().await?;
        }
        queue.status().await
    }
    pub async fn connect(
        redis_url: &str,
        catalog_namespace: &str,
        volume_id: Uuid,
        objects: O,
    ) -> Result<Self> {
        if volume_id.is_nil() {
            return Err(EntityError::Invalid(
                "entity experiment requires a volume UUID".into(),
            ));
        }
        let packed = RedisWorkspaceBackend::connect(redis_url, catalog_namespace)
            .await
            .map_err(catalog_error)?;
        let entity = RedisEntityBackend::connect(redis_url, volume_id).await?;
        Ok(Self {
            packed: Arc::new(
                KvWorkspaceStore::new(packed)
                    .with_packed_reader_pin_budget(V3MountBudget::defaults()),
            ),
            entity: Arc::new(entity),
            objects,
            volume_id,
        })
    }

    /// Attach a standalone immutable graph to a never-initialized private
    /// packed catalog and never-used entity namespace. This does not transfer
    /// ownership from an existing catalog or import a mutable workspace.
    pub async fn create_fresh_from_manifest(
        &self,
        manifest_key: &str,
        options: FreshMountOptions,
    ) -> Result<Arc<ExperimentalEntityMount<O>>> {
        let namespace_id = options.namespace_id.unwrap_or_else(Uuid::new_v4);
        if namespace_id.is_nil()
            || options.lease_ttl_ns <= 0
            || options.chunk_size == 0
            || options.chunk_size > super::data::MAX_IO_BYTES as u64
        {
            return Err(EntityError::Invalid(
                "invalid fresh entity experiment options".into(),
            ));
        }
        let component = manifest_key.rsplit('/').next().unwrap_or_default();
        let digest: [u8; 32] = hex::decode(component)
            .ok()
            .and_then(|bytes| bytes.try_into().ok())
            .ok_or_else(|| {
                EntityError::Invalid("manifest key must end in its SHA-256 address".into())
            })?;
        let client = ObjectClient::new(self.objects.clone());
        let object_len = client
            .typed_object_size(ReadClass::ManifestProbe, manifest_key)
            .await
            .map_err(backend_error)?
            .ok_or(EntityError::NotFound)?;
        let reference = V3ObjectRef {
            key: manifest_key.to_owned(),
            kind: V3ObjectKind::Manifest,
            object_len,
            digest,
        };
        let snapshot = AuthenticatedV3Snapshot::open(&client, &reference)
            .await
            .map_err(|error| EntityError::Corrupt(error.to_string()))?;
        let verified = VerifiedPackedLower::from_authenticated_snapshot(
            &snapshot,
            &V3IndexReader::new(client.clone(), 0),
        )
        .await
        .map_err(catalog_error)?;
        let binding = LowerBinding {
            manifest_key: manifest_key.as_bytes().to_vec(),
            manifest_digest: digest,
            root_ino: snapshot.manifest().root_inode as i64,
            max_ino: verified.highest_inode(),
        };
        self.packed
            .initialize_workspace_schema()
            .await
            .map_err(catalog_error)?;
        let workspace = self
            .packed
            .create_volume_root(CreateVolumeRoot {
                volume_format: "workspace-v1".into(),
                schema_version: WORKSPACE_SCHEMA_VERSION,
                volume_id: self.volume_id,
                workspace_id: WorkspaceId::new(),
                root_layer_id: LayerId::new(),
                writable_layer_id: LayerId::new(),
                owner_id: Some("experimental-entity-bootstrap".into()),
            })
            .await
            .map_err(catalog_error)?;
        let lease = self
            .packed
            .acquire_lease(AcquireLease {
                workspace_id: workspace.workspace_id,
                lease_id: LeaseId::new(),
                holder_generation: 1,
                ttl_ns: SETUP_TTL_NS,
            })
            .await
            .map_err(catalog_error)?;
        let guard = HeadGuard {
            workspace_id: workspace.workspace_id,
            expected_head_layer_id: workspace.head_layer_id,
            expected_head_epoch: workspace.head_epoch,
            lease_id: lease.lease_id,
            holder_generation: lease.holder_generation,
        };
        let result = self
            .finish_fresh(guard, verified, binding, namespace_id, options)
            .await;
        let released = self
            .packed
            .release_lease(ReleaseLease {
                lease_id: lease.lease_id,
                holder_generation: lease.holder_generation,
            })
            .await
            .map_err(catalog_error);
        let mount = result?;
        if let Err(error) = released {
            let _ = mount.lifecycle.enter_maintenance(&mount.session).await;
            return Err(error);
        }
        Ok(Arc::new(mount))
    }

    async fn finish_fresh(
        &self,
        guard: HeadGuard,
        verified: VerifiedPackedLower,
        binding: LowerBinding,
        namespace_id: Uuid,
        options: FreshMountOptions,
    ) -> Result<ExperimentalEntityMount<O>> {
        let layers: [LayerRecord; 2] = self
            .packed
            .load_layer_chain(guard.expected_head_layer_id)
            .await
            .map_err(catalog_error)?
            .try_into()
            .map_err(|_| EntityError::Corrupt("fresh source layer pair".into()))?;
        let base = &layers[1];
        let expected_base = BaseRevision {
            layer_id: base.layer_id,
            sealed_version: base.sealed_version.ok_or(EntityError::Fenced)?,
            root_hash: base.root_hash.ok_or(EntityError::Fenced)?,
        };
        let installed = self
            .packed
            .install_packed_lower_binding(InstallPackedLowerBinding {
                guard: guard.clone(),
                expected_layers: layers,
                expected_base,
                expected_binding: None,
                lower: verified,
            })
            .await
            .map_err(catalog_error)?;
        let source_guard = HeadGuard {
            expected_head_epoch: installed.head_epoch,
            ..guard.clone()
        };
        let layers: [LayerRecord; 2] = self
            .packed
            .load_layer_chain(guard.expected_head_layer_id)
            .await
            .map_err(catalog_error)?
            .try_into()
            .map_err(|_| EntityError::Corrupt("installed source layer pair".into()))?;
        let source = self
            .packed
            .acquire_packed_reader_pin(&AcquirePackedReaderPin {
                slot: 0,
                expected_slot_generation: 0,
                pin_id: Uuid::new_v4(),
                owner_id: "experimental-entity-bootstrap".into(),
                holder_generation: 1,
                ttl_ns: SETUP_TTL_NS,
                gc_grace_ns: 0,
                guard: source_guard,
                expected_layers: layers,
                expected_binding: installed.clone(),
            })
            .await
            .map_err(catalog_error)?;
        let result = PackedLower::open(
            self.packed.clone(),
            ObjectClient::new(self.objects.clone()),
            self.volume_id,
            namespace_id,
            &source,
            binding.clone(),
        )
        .await;
        let released = self
            .packed
            .release_packed_reader_pin(&source, Uuid::new_v4())
            .await
            .map_err(catalog_error);
        let lower = result?;
        released?;
        self.finish_entity(
            ExperimentalLower::Packed(Box::new(lower)),
            namespace_id,
            options,
        )
        .await
    }

    async fn finish_entity(
        &self,
        lower: ExperimentalLower<O>,
        namespace_id: Uuid,
        options: FreshMountOptions,
    ) -> Result<ExperimentalEntityMount<O>> {
        let binding = lower.binding().clone();
        let keys = EntityKeys::new(self.volume_id, namespace_id);
        let lifecycle = Arc::new(Lifecycle::new(self.entity.clone(), keys.clone()));
        lifecycle.provision(&binding).await?;
        let session = lifecycle
            .acquire(Uuid::new_v4(), options.lease_ttl_ns)
            .await?;
        if let ExperimentalLower::Packed(packed) = &lower
            && let Err(error) = packed
                .bind_session(&session, &keys, options.chunk_size)
                .await
        {
            let _ = lifecycle.enter_maintenance(&session).await;
            return Err(error);
        }
        let lower = Arc::new(lower);
        let namespace = EntityNamespace::new(
            self.entity.clone(),
            lower.clone(),
            keys.clone(),
            session.clone(),
        );
        let data = match EntityData::new_with_writeback(
            namespace.clone(),
            Arc::new(self.objects.clone()),
            options.chunk_size,
            options.writeback,
        )
        .await
        {
            Ok(data) => data,
            Err(error) => {
                let _ = lifecycle.enter_maintenance(&session).await;
                return Err(error);
            }
        };
        Ok(ExperimentalEntityMount {
            namespace,
            data,
            lifecycle,
            session,
        })
    }
}

impl<O: ObjectBackend + Clone + 'static> ExperimentalEntityMount<O> {
    pub fn lower_binding(&self) -> &LowerBinding {
        self.namespace.lower.binding()
    }
    pub fn namespace(&self) -> &Arc<ExperimentalNamespace<O>> {
        &self.namespace
    }
    pub fn data(&self) -> &Arc<ExperimentalData<O>> {
        &self.data
    }
    pub fn keys(&self) -> &EntityKeys {
        &self.namespace.keys
    }
    pub fn root_ino(&self) -> i64 {
        self.namespace.lower.binding().root_ino
    }
    pub async fn renew(&self, ttl_ns: i64) -> Result<()> {
        self.lifecycle.renew(&self.session, ttl_ns).await
    }

    /// The caller first stops external request admission and drops its file
    /// registry. This waits for every retained entity handle/read/upload.
    pub async fn orderly_close(&self) -> Result<()> {
        self.data
            .drain_remote(std::time::Duration::from_secs(60))
            .await?;
        self.lifecycle.enter_maintenance(&self.session).await?;
        Ok(())
    }
}
