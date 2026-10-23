use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::crd::{ConsumerContainerSpec, ConsumerVolumeSpec};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceClusterSpec {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub catalog_backend: WorkspaceCatalogBackend,
    #[serde(default)]
    pub namespace: Option<String>,
    #[serde(default)]
    pub tikv_pd_endpoints: Vec<String>,
    #[serde(default)]
    pub metadata_runtime_secret_ref: Option<WorkspaceMetadataSecretRef>,
    #[serde(default)]
    pub metadata_admin_secret_ref: Option<WorkspaceMetadataSecretRef>,
    #[serde(default)]
    pub object_runtime_secret_ref: Option<WorkspaceObjectSecretRef>,
    #[serde(default)]
    pub object_admin_secret_ref: Option<WorkspaceObjectSecretRef>,
    #[serde(default = "default_lease_ttl_seconds")]
    pub lease_ttl_seconds: u32,
    #[serde(default = "default_heartbeat_seconds")]
    pub heartbeat_seconds: u32,
    #[serde(default = "default_gc_grace_seconds")]
    pub gc_grace_seconds: u32,
    #[serde(default = "default_catalog_storage_size")]
    pub catalog_storage_size: String,
}

impl Default for WorkspaceClusterSpec {
    fn default() -> Self {
        Self {
            enabled: false,
            catalog_backend: WorkspaceCatalogBackend::Redis,
            namespace: None,
            tikv_pd_endpoints: Vec::new(),
            metadata_runtime_secret_ref: None,
            metadata_admin_secret_ref: None,
            object_runtime_secret_ref: None,
            object_admin_secret_ref: None,
            lease_ttl_seconds: default_lease_ttl_seconds(),
            heartbeat_seconds: default_heartbeat_seconds(),
            gc_grace_seconds: default_gc_grace_seconds(),
            catalog_storage_size: default_catalog_storage_size(),
        }
    }
}

impl WorkspaceClusterSpec {
    pub fn validate(&self) -> Result<(), String> {
        if self.enabled {
            self.object_secret_names()?;
            self.metadata_secret_names()?;
        }
        if !(10..=300).contains(&self.lease_ttl_seconds) {
            return Err("leaseTtlSeconds must be between 10 and 300".into());
        }
        if self.heartbeat_seconds == 0
            || self.heartbeat_seconds.saturating_mul(2) >= self.lease_ttl_seconds
        {
            return Err(
                "heartbeatSeconds must be greater than zero and less than half the lease TTL"
                    .into(),
            );
        }
        if self.gc_grace_seconds < self.lease_ttl_seconds {
            return Err("gcGraceSeconds must be at least leaseTtlSeconds".into());
        }
        if self.catalog_storage_size.trim().is_empty() {
            return Err("catalogStorageSize must not be empty".into());
        }
        if self.catalog_backend == WorkspaceCatalogBackend::TiKv
            && self.tikv_pd_endpoints.is_empty()
        {
            return Err("tikvPdEndpoints is required for the TiKV catalog backend".into());
        }
        if self
            .namespace
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
        {
            return Err("workspace namespace must not be empty".into());
        }
        Ok(())
    }

    pub fn object_secret_names(&self) -> Result<(&str, &str), String> {
        let runtime = self.object_runtime_secret_ref.as_ref().ok_or_else(|| {
            "objectRuntimeSecretRef is required for workspace runtime".to_string()
        })?;
        let admin = self.object_admin_secret_ref.as_ref().ok_or_else(|| {
            "objectAdminSecretRef is required for workspace administration".to_string()
        })?;
        validate_object_secret_name(&runtime.name)?;
        validate_object_secret_name(&admin.name)?;
        if runtime.name == admin.name {
            return Err("runtime and admin object Secret references must differ".into());
        }
        Ok((&runtime.name, &admin.name))
    }

    pub fn metadata_secret_names(&self) -> Result<(&str, &str), String> {
        let runtime = self.metadata_runtime_secret_ref.as_ref().ok_or_else(|| {
            "metadataRuntimeSecretRef is required for workspace runtime".to_string()
        })?;
        let admin = self.metadata_admin_secret_ref.as_ref().ok_or_else(|| {
            "metadataAdminSecretRef is required for workspace administration".to_string()
        })?;
        validate_object_secret_name(&runtime.name)?;
        validate_object_secret_name(&admin.name)?;
        if runtime.name == admin.name {
            return Err("runtime and admin metadata Secret references must differ".into());
        }
        // Same-role object/metadata values may share a Secret, but a runtime
        // Secret must never carry either administrative role's credentials.
        if self
            .object_admin_secret_ref
            .as_ref()
            .is_some_and(|object| object.name == runtime.name)
            || self
                .object_runtime_secret_ref
                .as_ref()
                .is_some_and(|object| object.name == admin.name)
        {
            return Err(
                "runtime and admin Secret references cannot cross object/metadata roles".into(),
            );
        }
        Ok((&runtime.name, &admin.name))
    }
}

/// Namespace-local transport credentials; their presence alone grants no admin capability.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
pub struct WorkspaceMetadataSecretRef {
    pub name: String,
}

/// Namespace-local object credentials. Keys are accessKey and secretKey.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
pub struct WorkspaceObjectSecretRef {
    pub name: String,
}

fn validate_object_secret_name(name: &str) -> Result<(), String> {
    if name.is_empty()
        || name.len() > 253
        || name.split('.').any(|part| {
            part.is_empty()
                || part.len() > 63
                || !part
                    .as_bytes()
                    .first()
                    .is_some_and(u8::is_ascii_alphanumeric)
                || !part
                    .as_bytes()
                    .last()
                    .is_some_and(u8::is_ascii_alphanumeric)
                || !part
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        })
    {
        return Err("credential Secret name must be a DNS subdomain".into());
    }
    Ok(())
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, Default, PartialEq, Eq, JsonSchema)]
pub enum WorkspaceCatalogBackend {
    #[default]
    Redis,
    TiKv,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceClusterStatus {
    pub volume_id: String,
    pub schema_version: u32,
    pub catalog_backend: WorkspaceCatalogBackend,
    pub catalog_namespace: String,
    pub root_snapshot_id: String,
    pub root_revision: WorkspaceRevision,
    #[serde(default)]
    pub capabilities: WorkspaceCapabilityStatus,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<WorkspaceCondition>,
}

/// Explicit capability/binding state for lower metadata. The operator
/// currently bootstraps the native catalog only; a missing capability is
/// represented in status instead of being inferred from rootRevision.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceCapabilityStatus {
    #[serde(default = "default_workspace_volume_format")]
    pub volume_format: String,
    #[serde(default)]
    pub packed_v3: PackedV3Capability,
    #[serde(default)]
    pub packed_binding: PackedBindingStatus,
}

impl WorkspaceCapabilityStatus {
    pub fn native_only() -> Self {
        Self {
            volume_format: default_workspace_volume_format(),
            packed_v3: PackedV3Capability::Unsupported,
            packed_binding: PackedBindingStatus::Unavailable,
        }
    }

    pub fn packed_v3_ready(&self) -> bool {
        self.packed_v3 == PackedV3Capability::Readonly
            && matches!(self.packed_binding, PackedBindingStatus::Present { .. })
    }

    /// Return the condition used by controllers before advertising a packed
    /// lower. Any unsupported, missing, or corrupt binding is fail-closed.
    pub fn packed_lower_condition(&self, observed_generation: Option<i64>) -> WorkspaceCondition {
        if self.packed_v3_ready() {
            WorkspaceCondition {
                condition_type: "PackedLowerReady".into(),
                status: "True".into(),
                reason: "CapabilityAndBindingVerified".into(),
                message: "readonly packed-v3 capability and PWB3 binding are available".into(),
                observed_generation,
                last_transition_time: Utc::now(),
            }
        } else {
            let (reason, message) = match &self.packed_binding {
                PackedBindingStatus::Missing => (
                    "BindingMissing",
                    "packed-v3 capability is not mountable because the PWB3 binding is missing",
                ),
                PackedBindingStatus::Corrupt => (
                    "BindingCorrupt",
                    "packed-v3 capability is not mountable because the PWB3 binding is corrupt",
                ),
                PackedBindingStatus::Unavailable => (
                    "UnsupportedCapability",
                    "packed-v3 workspace mounting is not supported by this catalog",
                ),
                PackedBindingStatus::Present { .. } => (
                    "UnsupportedCapability",
                    "PWB3 binding exists but the packed-v3 capability is not enabled",
                ),
            };
            WorkspaceCondition {
                condition_type: "PackedLowerReady".into(),
                status: "False".into(),
                reason: reason.into(),
                message: message.into(),
                observed_generation,
                last_transition_time: Utc::now(),
            }
        }
    }
}

impl Default for WorkspaceCapabilityStatus {
    fn default() -> Self {
        Self::native_only()
    }
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, Default, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum PackedV3Capability {
    #[default]
    Unsupported,
    Readonly,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase", tag = "state")]
pub enum PackedBindingStatus {
    #[default]
    Unavailable,
    Missing,
    Corrupt,
    Present {
        version: u64,
        manifest_digest: String,
    },
}

impl JsonSchema for PackedBindingStatus {
    fn schema_name() -> String {
        "PackedBindingStatus".into()
    }

    fn json_schema(_: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        // Keep the tagged serde representation, but define shared properties
        // once: kube cannot hoist conflicting state enums from oneOf branches.
        // CEL retains the union's field-presence contract in a structural schema.
        serde_json::from_value(serde_json::json!({
            "type": "object",
            "required": ["state"],
            "properties": {
                "state": {
                    "type": "string",
                    "enum": ["unavailable", "missing", "corrupt", "present"],
                },
                "version": { "type": "integer", "format": "uint64", "minimum": 1.0 },
                "manifest_digest": {
                    "type": "string",
                    "minLength": 64,
                    "maxLength": 64,
                    "pattern": "^[0-9a-f]{64}$",
                },
            },
            "x-kubernetes-validations": [{
                "rule": "self.state == 'present' ? has(self.version) && has(self.manifest_digest) : !has(self.version) && !has(self.manifest_digest)",
                "message": "present bindings require version and manifest_digest; other states must omit both",
            }],
        }))
        .expect("static packed binding structural schema")
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceRevision {
    pub layer_id: String,
    pub sealed_version: u64,
    pub root_hash: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NamespacedNameRef {
    pub name: String,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, JsonSchema)]
pub enum WorkspaceSourceKind {
    ClusterRoot,
    Snapshot,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceSourceSpec {
    pub kind: WorkspaceSourceKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

impl WorkspaceSourceSpec {
    pub fn validate(&self) -> Result<(), String> {
        match (self.kind, self.name.as_deref()) {
            (WorkspaceSourceKind::ClusterRoot, None) => Ok(()),
            (WorkspaceSourceKind::Snapshot, Some(name)) if !name.trim().is_empty() => Ok(()),
            (WorkspaceSourceKind::ClusterRoot, Some(_)) => {
                Err("ClusterRoot source must not set name".into())
            }
            (WorkspaceSourceKind::Snapshot, _) => {
                Err("Snapshot source requires a non-empty name".into())
            }
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, Default, PartialEq, Eq, JsonSchema)]
pub enum WorkspaceDesiredState {
    #[default]
    Active,
    Suspended,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, Default, PartialEq, Eq, JsonSchema)]
pub enum WorkspaceDeletionPolicy {
    #[default]
    Delete,
    Retain,
    SnapshotAndDelete,
}

#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "storage.brewfs.io",
    version = "v1alpha1",
    kind = "BrewFSWorkspace",
    plural = "brewfsworkspaces",
    namespaced,
    status = "BrewFSWorkspaceStatus",
    shortname = "bfws"
)]
#[serde(rename_all = "camelCase")]
pub struct BrewFSWorkspaceSpec {
    pub cluster_ref: NamespacedNameRef,
    pub source: WorkspaceSourceSpec,
    #[serde(default)]
    pub desired_state: WorkspaceDesiredState,
    #[serde(default)]
    pub owner_id: Option<String>,
    #[serde(default)]
    pub deletion_policy: WorkspaceDeletionPolicy,
}

impl BrewFSWorkspaceSpec {
    pub fn validate(&self) -> Result<(), String> {
        if self.cluster_ref.name.trim().is_empty() {
            return Err("clusterRef.name must not be empty".into());
        }
        if self
            .owner_id
            .as_deref()
            .is_some_and(|owner| owner.trim().is_empty())
        {
            return Err("ownerId must not be empty when set".into());
        }
        self.source.validate()
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BrewFSWorkspaceStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    #[serde(default)]
    pub capabilities: WorkspaceCapabilityStatus,
    pub phase: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub origin_revision: Option<WorkspaceRevision>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_base_revision: Option<WorkspaceRevision>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub head_layer_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub head_epoch: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_mount_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_clean_release_epoch: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<WorkspaceCondition>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceCondition {
    #[serde(rename = "type")]
    pub condition_type: String,
    pub status: String,
    pub reason: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    pub last_transition_time: DateTime<Utc>,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, Default, PartialEq, Eq, JsonSchema)]
pub enum WorkspaceCacheMode {
    WriteThrough,
    #[default]
    WriteBack,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, Default, PartialEq, Eq, JsonSchema)]
pub enum WorkspaceCacheReclaimPolicy {
    #[default]
    Delete,
    Retain,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceCacheSpec {
    #[serde(default)]
    pub mode: WorkspaceCacheMode,
    #[serde(default)]
    pub storage_class_name: Option<String>,
    #[serde(default = "default_cache_size")]
    pub size: String,
    #[serde(default)]
    pub reclaim_policy: WorkspaceCacheReclaimPolicy,
}

impl Default for WorkspaceCacheSpec {
    fn default() -> Self {
        Self {
            mode: WorkspaceCacheMode::WriteBack,
            storage_class_name: None,
            size: default_cache_size(),
            reclaim_policy: WorkspaceCacheReclaimPolicy::Delete,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceAgentSpec {
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub annotations: BTreeMap<String, String>,
    #[serde(default)]
    pub containers: Vec<ConsumerContainerSpec>,
    #[serde(default)]
    pub init_containers: Vec<ConsumerContainerSpec>,
    #[serde(default)]
    pub volumes: Vec<ConsumerVolumeSpec>,
}

#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "storage.brewfs.io",
    version = "v1alpha1",
    kind = "BrewFSWorkspaceMount",
    plural = "brewfsworkspacemounts",
    namespaced,
    status = "BrewFSWorkspaceMountStatus",
    shortname = "bfwsm"
)]
#[serde(rename_all = "camelCase")]
pub struct BrewFSWorkspaceMountSpec {
    /// Cluster that owns the referenced workspace and its catalog.
    ///
    /// Keeping this reference on the mount makes the catalog boundary
    /// explicit. The controller verifies it against the workspace before
    /// creating any workload or acquiring a lease.
    pub cluster_ref: NamespacedNameRef,
    pub workspace_ref: NamespacedNameRef,
    #[serde(default = "default_workspace_mount_path")]
    pub mount_path: String,
    #[serde(default = "default_workspace_image")]
    pub image: String,
    #[serde(default = "default_image_pull_policy")]
    pub image_pull_policy: String,
    #[serde(default)]
    pub cache: WorkspaceCacheSpec,
    #[serde(default)]
    pub agent: Option<WorkspaceAgentSpec>,
    #[serde(default = "default_termination_grace_period_seconds")]
    pub termination_grace_period_seconds: u32,
    #[serde(default)]
    pub service_account_name: Option<String>,
    #[serde(default)]
    pub node_selector: BTreeMap<String, String>,
}

impl BrewFSWorkspaceMountSpec {
    pub fn validate(&self, lease_ttl_seconds: u32) -> Result<(), String> {
        if self.cluster_ref.name.trim().is_empty() {
            return Err("clusterRef.name must not be empty".into());
        }
        if self.workspace_ref.name.trim().is_empty() {
            return Err("workspaceRef.name must not be empty".into());
        }
        validate_mount_path(&self.mount_path)?;
        if self.cache.mode == WorkspaceCacheMode::WriteBack && self.cache.size.trim().is_empty() {
            return Err("WriteBack cache requires a non-empty persistent volume size".into());
        }
        if self.termination_grace_period_seconds < lease_ttl_seconds.saturating_add(30) {
            return Err("terminationGracePeriodSeconds must cover lease TTL plus the 30 second drain timeout".into());
        }
        if self
            .agent
            .as_ref()
            .is_some_and(|agent| agent.containers.is_empty())
        {
            return Err("agent.containers must not be empty when agent is configured".into());
        }
        if let Some(agent) = &self.agent {
            for volume in &agent.volumes {
                if matches!(
                    volume.name.as_str(),
                    "workspace-mount" | "workspace-cache" | "workspace-config" | "fuse-device"
                ) {
                    return Err(format!(
                        "agent volume name {:?} is reserved by the workspace runtime",
                        volume.name
                    ));
                }
                if volume.host_path.is_some() || volume.secret_name.is_some() {
                    return Err("workspace agents cannot request hostPath or Secret volumes".into());
                }
            }
            for container in agent.containers.iter().chain(&agent.init_containers) {
                if container.volume_mounts.iter().any(|mount| {
                    matches!(
                        mount.name.as_str(),
                        "workspace-mount" | "workspace-cache" | "workspace-config" | "fuse-device"
                    )
                }) {
                    return Err(
                        "agent containers cannot mount workspace runtime internal volumes".into(),
                    );
                }
                if container
                    .security_context
                    .as_ref()
                    .and_then(|security| security.privileged)
                    == Some(true)
                    || container
                        .security_context
                        .as_ref()
                        .is_some_and(|security| !security.capabilities_add.is_empty())
                {
                    return Err(
                        "workspace agents cannot be privileged or add Linux capabilities".into(),
                    );
                }
                if container
                    .env_from
                    .iter()
                    .any(|source| source.secret_name.is_some())
                {
                    return Err("workspace agents cannot import Secret envFrom sources".into());
                }
            }
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BrewFSWorkspaceMountStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    pub phase: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pod_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pod_uid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lease_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub holder_generation: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mounted_head_layer_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mounted_head_epoch: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mounted_base_revision: Option<WorkspaceRevision>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<WorkspaceCondition>,
}

#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "storage.brewfs.io",
    version = "v1alpha1",
    kind = "BrewFSWorkspaceSnapshot",
    plural = "brewfsworkspacesnapshots",
    namespaced,
    status = "BrewFSWorkspaceSnapshotStatus",
    shortname = "bfwss"
)]
#[serde(rename_all = "camelCase")]
pub struct BrewFSWorkspaceSnapshotSpec {
    pub cluster_ref: NamespacedNameRef,
    pub workspace_ref: NamespacedNameRef,
}

impl BrewFSWorkspaceSnapshotSpec {
    pub fn validate(&self) -> Result<(), String> {
        if self.cluster_ref.name.trim().is_empty() {
            return Err("clusterRef.name must not be empty".into());
        }
        if self.workspace_ref.name.trim().is_empty() {
            return Err("workspaceRef.name must not be empty".into());
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BrewFSWorkspaceSnapshotStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    pub phase: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_workspace_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_head_epoch: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revision: Option<WorkspaceRevision>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<WorkspaceCondition>,
}

fn validate_mount_path(path: &str) -> Result<(), String> {
    if !path.starts_with('/') {
        return Err("mountPath must be absolute".into());
    }
    let normalized = path.trim_end_matches('/');
    if normalized.is_empty()
        || ["/proc", "/sys", "/dev", "/var/run/secrets"]
            .iter()
            .any(|reserved| {
                normalized == *reserved
                    || normalized
                        .strip_prefix(reserved)
                        .is_some_and(|suffix| suffix.starts_with('/'))
            })
    {
        return Err(format!("mountPath {path:?} is reserved"));
    }
    if !path
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'-' | b'_' | b'.'))
    {
        return Err("mountPath contains unsupported characters".into());
    }
    if normalized
        .split('/')
        .skip(1)
        .any(|component| component.is_empty() || matches!(component, "." | ".."))
    {
        return Err("mountPath must not contain empty, '.' or '..' components".into());
    }
    Ok(())
}

fn default_workspace_volume_format() -> String {
    "workspace-v1".into()
}

fn default_lease_ttl_seconds() -> u32 {
    30
}

fn default_heartbeat_seconds() -> u32 {
    10
}

fn default_gc_grace_seconds() -> u32 {
    60
}

fn default_catalog_storage_size() -> String {
    "5Gi".into()
}

fn default_cache_size() -> String {
    "20Gi".into()
}

fn default_workspace_mount_path() -> String {
    "/workspace".into()
}

fn default_workspace_image() -> String {
    "brewfs:local".into()
}

fn default_image_pull_policy() -> String {
    "IfNotPresent".into()
}

fn default_termination_grace_period_seconds() -> u32 {
    90
}

#[cfg(test)]
mod tests {
    use super::*;
    use kube::CustomResourceExt;

    #[test]
    fn packed_binding_status_keeps_tagged_wire_states() {
        let cases = [
            (
                PackedBindingStatus::Unavailable,
                serde_json::json!({ "state": "unavailable" }),
            ),
            (
                PackedBindingStatus::Missing,
                serde_json::json!({ "state": "missing" }),
            ),
            (
                PackedBindingStatus::Corrupt,
                serde_json::json!({ "state": "corrupt" }),
            ),
            (
                PackedBindingStatus::Present {
                    version: 7,
                    manifest_digest: "ab".repeat(32),
                },
                serde_json::json!({
                    "state": "present",
                    "version": 7,
                    "manifest_digest": "ab".repeat(32),
                }),
            ),
        ];
        for (status, wire) in cases {
            assert_eq!(serde_json::to_value(&status).unwrap(), wire);
            assert_eq!(
                serde_json::from_value::<PackedBindingStatus>(wire).unwrap(),
                status
            );
        }
    }

    #[test]
    fn packed_binding_crd_schema_is_structural_and_keeps_present_fields_guarded() {
        // Exercise the CustomResource structural rewrite that the crdgen CLI
        // runs, rather than only schemars' unmodified standalone schema.
        let crd = serde_json::to_value(BrewFSWorkspace::crd()).unwrap();
        let schema = crd
            .pointer("/spec/versions/0/schema/openAPIV3Schema/properties/status/properties/capabilities/properties/packedBinding")
            .expect("packed binding must survive real CRD generation");
        assert_eq!(schema["type"], "object");
        assert_eq!(schema["required"], serde_json::json!(["state"]));
        assert_eq!(schema["properties"]["state"]["type"], "string");
        assert_eq!(
            schema["properties"]["state"]["enum"],
            serde_json::json!(["unavailable", "missing", "corrupt", "present"])
        );
        assert_eq!(schema["properties"]["version"]["type"], "integer");
        assert_eq!(schema["properties"]["version"]["minimum"], 1.0);
        let digest = &schema["properties"]["manifest_digest"];
        assert_eq!(digest["type"], "string");
        assert_eq!(digest["minLength"], 64);
        assert_eq!(digest["maxLength"], 64);
        assert_eq!(digest["pattern"], "^[0-9a-f]{64}$");
        assert!(schema.get("oneOf").is_none());
        assert!(schema.get("x-kubernetes-preserve-unknown-fields").is_none());
        assert_eq!(
            schema["x-kubernetes-validations"][0]["rule"],
            "self.state == 'present' ? has(self.version) && has(self.manifest_digest) : !has(self.version) && !has(self.manifest_digest)"
        );
    }

    #[test]
    fn source_union_is_strict() {
        assert!(WorkspaceSourceSpec {
            kind: WorkspaceSourceKind::ClusterRoot,
            name: None,
        }
        .validate()
        .is_ok());
        assert!(WorkspaceSourceSpec {
            kind: WorkspaceSourceKind::ClusterRoot,
            name: Some("unexpected".into()),
        }
        .validate()
        .is_err());
        assert!(WorkspaceSourceSpec {
            kind: WorkspaceSourceKind::Snapshot,
            name: None,
        }
        .validate()
        .is_err());
    }

    #[test]
    fn packed_capability_status_is_fail_closed_without_binding() {
        let capability = WorkspaceCapabilityStatus::native_only();
        assert_eq!(WorkspaceCapabilityStatus::default(), capability);
        assert!(!capability.packed_v3_ready());
        let condition = capability.packed_lower_condition(Some(3));
        assert_eq!(condition.condition_type, "PackedLowerReady");
        assert_eq!(condition.status, "False");
        assert_eq!(condition.reason, "UnsupportedCapability");
        assert_eq!(condition.observed_generation, Some(3));

        let missing = WorkspaceCapabilityStatus {
            packed_v3: PackedV3Capability::Readonly,
            packed_binding: PackedBindingStatus::Missing,
            ..capability.clone()
        };
        assert!(!missing.packed_v3_ready());
        assert_eq!(
            missing.packed_lower_condition(None).reason,
            "BindingMissing"
        );

        let present = WorkspaceCapabilityStatus {
            packed_v3: PackedV3Capability::Readonly,
            packed_binding: PackedBindingStatus::Present {
                version: 7,
                manifest_digest: "00".repeat(32),
            },
            ..capability
        };
        assert!(present.packed_v3_ready());
        let condition = present.packed_lower_condition(Some(4));
        assert_eq!(condition.status, "True");
        assert_eq!(condition.reason, "CapabilityAndBindingVerified");
    }

    #[test]
    fn cluster_timing_constraints_are_fail_closed() {
        let mut spec = WorkspaceClusterSpec {
            enabled: true,
            object_runtime_secret_ref: Some(WorkspaceObjectSecretRef {
                name: "runtime-objects".into(),
            }),
            object_admin_secret_ref: Some(WorkspaceObjectSecretRef {
                name: "admin-objects".into(),
            }),
            metadata_runtime_secret_ref: Some(WorkspaceMetadataSecretRef {
                name: "runtime-metadata".into(),
            }),
            metadata_admin_secret_ref: Some(WorkspaceMetadataSecretRef {
                name: "admin-metadata".into(),
            }),
            ..WorkspaceClusterSpec::default()
        };
        assert!(spec.validate().is_ok());
        spec.heartbeat_seconds = 15;
        assert!(spec.validate().is_err());
        spec.heartbeat_seconds = 10;
        spec.gc_grace_seconds = 20;
        assert!(spec.validate().is_err());
    }

    #[test]
    fn object_secret_references_are_required_distinct_and_namespace_local() {
        let mut spec = WorkspaceClusterSpec {
            enabled: true,
            metadata_runtime_secret_ref: Some(WorkspaceMetadataSecretRef {
                name: "runtime-metadata".into(),
            }),
            metadata_admin_secret_ref: Some(WorkspaceMetadataSecretRef {
                name: "admin-metadata".into(),
            }),
            ..Default::default()
        };
        assert!(spec.validate().is_err());
        spec.object_runtime_secret_ref = Some(WorkspaceObjectSecretRef {
            name: "runtime-objects".into(),
        });
        assert!(spec.validate().is_err());
        spec.object_admin_secret_ref = spec.object_runtime_secret_ref.clone();
        assert!(spec.validate().is_err());
        spec.object_admin_secret_ref = Some(WorkspaceObjectSecretRef {
            name: "admin-objects".into(),
        });
        assert!(spec.validate().is_ok());
        for invalid in ["other/runtime", "Runtime", "", "..", "bad-", "$(secret)"] {
            spec.object_runtime_secret_ref = Some(WorkspaceObjectSecretRef {
                name: invalid.into(),
            });
            assert!(spec.validate().is_err());
        }
    }

    #[test]
    fn metadata_secret_references_are_required_and_cannot_cross_roles() {
        let mut spec = WorkspaceClusterSpec {
            enabled: true,
            object_runtime_secret_ref: Some(WorkspaceObjectSecretRef {
                name: "runtime-objects".into(),
            }),
            object_admin_secret_ref: Some(WorkspaceObjectSecretRef {
                name: "admin-objects".into(),
            }),
            ..Default::default()
        };
        assert!(spec.validate().is_err());
        spec.metadata_runtime_secret_ref = Some(WorkspaceMetadataSecretRef {
            name: "runtime-metadata".into(),
        });
        assert!(spec.validate().is_err());
        spec.metadata_admin_secret_ref = spec.metadata_runtime_secret_ref.clone();
        assert!(spec.validate().is_err());
        spec.metadata_admin_secret_ref = Some(WorkspaceMetadataSecretRef {
            name: "admin-metadata".into(),
        });
        assert!(spec.validate().is_ok());
        for invalid in ["other/runtime", "Runtime", "", "..", "bad-", "$(secret)"] {
            spec.metadata_runtime_secret_ref = Some(WorkspaceMetadataSecretRef {
                name: invalid.into(),
            });
            assert!(spec.validate().is_err());
        }
        spec.metadata_runtime_secret_ref = Some(WorkspaceMetadataSecretRef {
            name: "admin-objects".into(),
        });
        assert!(spec.validate().is_err());
        spec.metadata_runtime_secret_ref = Some(WorkspaceMetadataSecretRef {
            name: "runtime-metadata".into(),
        });
        spec.metadata_admin_secret_ref = Some(WorkspaceMetadataSecretRef {
            name: "runtime-objects".into(),
        });
        assert!(spec.validate().is_err());
        spec.metadata_runtime_secret_ref = Some(WorkspaceMetadataSecretRef {
            name: "runtime-objects".into(),
        });
        spec.metadata_admin_secret_ref = Some(WorkspaceMetadataSecretRef {
            name: "admin-objects".into(),
        });
        assert!(
            spec.validate().is_ok(),
            "combined Secrets may contain only one role"
        );
    }

    #[test]
    fn workspace_mount_rejects_reserved_paths_and_short_shutdown() {
        let mut spec = BrewFSWorkspaceMountSpec {
            cluster_ref: NamespacedNameRef {
                name: "demo".into(),
            },
            workspace_ref: NamespacedNameRef {
                name: "agent".into(),
            },
            mount_path: "/proc".into(),
            image: default_workspace_image(),
            image_pull_policy: default_image_pull_policy(),
            cache: WorkspaceCacheSpec::default(),
            agent: None,
            termination_grace_period_seconds: 90,
            service_account_name: None,
            node_selector: BTreeMap::new(),
        };
        assert!(spec.validate(30).is_err());
        spec.mount_path = "/proc/self/fd".into();
        assert!(spec.validate(30).is_err());
        spec.mount_path = "/workspace;touch /tmp/pwned".into();
        assert!(spec.validate(30).is_err());
        spec.mount_path = "/workspace/../dev".into();
        assert!(spec.validate(30).is_err());
        spec.mount_path = "/workspace".into();
        spec.termination_grace_period_seconds = 59;
        assert!(spec.validate(30).is_err());
        spec.termination_grace_period_seconds = 60;
        assert!(spec.validate(30).is_ok());
    }

    #[test]
    fn workspace_agent_cannot_mount_runtime_internal_volumes() {
        let spec: BrewFSWorkspaceMountSpec = serde_json::from_value(serde_json::json!({
            "clusterRef": { "name": "demo" },
            "workspaceRef": { "name": "agent" },
            "mountPath": "/workspace",
            "terminationGracePeriodSeconds": 90,
            "agent": {
                "containers": [{
                    "name": "agent",
                    "image": "example/agent:test",
                    "volumeMounts": [{
                        "name": "fuse-device",
                        "mountPath": "/dev/fuse"
                    }]
                }]
            }
        }))
        .unwrap();

        assert!(spec.validate(30).is_err());
    }
}
