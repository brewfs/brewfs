use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context as _};
use brewfs::workspace_overlay::error::WorkspaceError;
use brewfs::workspace_overlay::ids::{LayerId, LeaseId, SnapshotId, WorkspaceId};
use brewfs::workspace_overlay::model::{LeaseState, SnapshotLease};
use chrono::Utc;
use kube::api::{Api, DeleteParams, ListParams, Patch, PatchParams, PostParams};
use kube::runtime::controller::Action;
use kube::{Client, Resource, ResourceExt};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::json;
use thiserror::Error;
use uuid::Uuid;

use crate::crd::BrewFSCluster;
use crate::reconciler::OperatorContext;

use super::admin::{
    catalog_namespace, connect_workspace_admin_for_cluster, deterministic_uuid,
    revision_from_status, revision_to_status, EnsureSnapshotRequest, EnsureWorkspaceRequest,
    VolumeIdentity, WorkspaceAdmin,
};
use super::crd::{
    BrewFSWorkspace, BrewFSWorkspaceMount, BrewFSWorkspaceSnapshot, BrewFSWorkspaceSnapshotStatus,
    BrewFSWorkspaceStatus, WorkspaceCapabilityStatus, WorkspaceCondition, WorkspaceDeletionPolicy,
    WorkspaceDesiredState, WorkspaceRevision, WorkspaceSourceKind,
};
use super::workload::{
    patch_mount_backend_status, reconcile_workspace_catalog, reconcile_workspace_mount_workload,
};

const WORKSPACE_FINALIZER: &str = "storage.brewfs.io/workspace-protection";
const MOUNT_FINALIZER: &str = "storage.brewfs.io/workspace-mount-protection";
const SNAPSHOT_FINALIZER: &str = "storage.brewfs.io/workspace-snapshot-protection";
const CLUSTER_FINALIZER: &str = "storage.brewfs.io/workspace-cluster-protection";
const FORCE_DELETE_ANNOTATION: &str = "storage.brewfs.io/force-delete";
const FORCE_DELETE_REASON_ANNOTATION: &str = "storage.brewfs.io/force-delete-reason";
const ID_NAMESPACE: Uuid = Uuid::from_u128(0xb7e6_f500_9a2d_4d17_8b4d_7f24_4c45_a011);

#[derive(Debug, Error)]
pub enum WorkspaceReconcileError {
    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
}

pub async fn guard_cluster_workspace_lifecycle(
    cluster: &BrewFSCluster,
    client: &Client,
    namespace: &str,
) -> anyhow::Result<bool> {
    let enabled = cluster
        .spec
        .workspace
        .as_ref()
        .is_some_and(|spec| spec.enabled);
    let protected = cluster
        .meta()
        .finalizers
        .as_ref()
        .is_some_and(|finalizers| finalizers.iter().any(|value| value == CLUSTER_FINALIZER));
    if !enabled && !protected {
        return Ok(false);
    }
    let api: Api<BrewFSCluster> = Api::namespaced(client.clone(), namespace);
    if cluster.meta().deletion_timestamp.is_none() {
        if !enabled {
            bail!("workspace support cannot be disabled after the catalog was initialized");
        }
        return ensure_finalizer(&api, &cluster.name_any(), cluster, CLUSTER_FINALIZER).await;
    }

    let workspace_list = Api::<BrewFSWorkspace>::namespaced(client.clone(), namespace)
        .list(&ListParams::default().limit(256))
        .await?;
    if workspace_list
        .metadata
        .continue_
        .as_deref()
        .is_some_and(|value| !value.is_empty())
    {
        bail!("cluster deletion workspace inventory is incomplete");
    }
    let workspaces = workspace_list
        .items
        .into_iter()
        .filter(|workspace| workspace.spec.cluster_ref.name == cluster.name_any())
        .collect::<Vec<_>>();
    let mount_list = Api::<BrewFSWorkspaceMount>::namespaced(client.clone(), namespace)
        .list(&ListParams::default().limit(256))
        .await?;
    if mount_list
        .metadata
        .continue_
        .as_deref()
        .is_some_and(|value| !value.is_empty())
    {
        bail!("cluster deletion mount inventory is incomplete");
    }
    let mount_count = mount_list
        .items
        .into_iter()
        .filter(|mount| mount.spec.cluster_ref.name == cluster.name_any())
        .count();
    let snapshot_list = Api::<BrewFSWorkspaceSnapshot>::namespaced(client.clone(), namespace)
        .list(&ListParams::default().limit(256))
        .await?;
    if snapshot_list
        .metadata
        .continue_
        .as_deref()
        .is_some_and(|value| !value.is_empty())
    {
        bail!("cluster deletion snapshot inventory is incomplete");
    }
    let snapshot_count = snapshot_list
        .items
        .into_iter()
        .filter(|snapshot| snapshot.spec.cluster_ref.name == cluster.name_any())
        .count();
    if !workspaces.is_empty() || mount_count != 0 || snapshot_count != 0 {
        bail!(
            "cluster deletion is blocked by {} workspaces, {} mounts, and {} snapshots",
            workspaces.len(),
            mount_count,
            snapshot_count
        );
    }
    if let Some(workspace_status) = cluster
        .status
        .as_ref()
        .and_then(|status| status.workspace.as_ref())
    {
        let spec = cluster
            .spec
            .workspace
            .as_ref()
            .ok_or_else(|| anyhow!("restore spec.workspace to complete protected deletion"))?;
        let admin = connect_workspace_admin_for_cluster(client, cluster, namespace, spec).await?;
        let root_workspace_id = WorkspaceId::from_uuid(deterministic_uuid(
            resource_uuid(&resource_uid(cluster)?),
            "root-workspace",
        ));
        let retained = admin
            .list_workspaces()
            .await?
            .into_iter()
            .filter(|workspace| {
                workspace.workspace_id != root_workspace_id
                    && workspace.state != brewfs::workspace_overlay::model::WorkspaceState::Deleting
            })
            .count();
        if retained != 0 {
            bail!("cluster deletion is blocked by {retained} retained backend workspaces");
        }
        let root_snapshot_id: SnapshotId = workspace_status
            .root_snapshot_id
            .parse()
            .context("parse cluster root snapshot ID")?;
        let retained_snapshots = admin
            .list_snapshots()
            .await?
            .into_iter()
            .filter(|snapshot| snapshot.snapshot_id != root_snapshot_id)
            .count();
        if retained_snapshots != 0 {
            bail!("cluster deletion is blocked by {retained_snapshots} retained backend snapshots");
        }
    }
    remove_finalizer(&api, &cluster.name_any(), cluster, CLUSTER_FINALIZER).await?;
    Ok(true)
}

pub async fn reconcile_cluster_workspace(
    cluster: &BrewFSCluster,
    client: &Client,
    kubernetes_namespace: &str,
) -> anyhow::Result<()> {
    let Some(spec) = cluster.spec.workspace.as_ref() else {
        patch_cluster_workspace_status(client, kubernetes_namespace, cluster, None).await?;
        return Ok(());
    };
    if !spec.enabled {
        patch_cluster_workspace_status(client, kubernetes_namespace, cluster, None).await?;
        return Ok(());
    }
    spec.validate().map_err(|error| anyhow!(error))?;
    let uid = resource_uid(cluster)?;
    let scope = resource_uuid(&uid);
    let volume_id = deterministic_uuid(scope, "volume");
    reconcile_workspace_catalog(client, kubernetes_namespace, cluster, spec).await?;
    let admin =
        connect_workspace_admin_for_cluster(client, cluster, kubernetes_namespace, spec).await?;
    let volume = admin
        .ensure_volume(VolumeIdentity {
            volume_id,
            root_workspace_id: WorkspaceId::from_uuid(deterministic_uuid(scope, "root-workspace")),
            root_layer_id: LayerId::from_uuid(deterministic_uuid(scope, "root-layer")),
            writable_layer_id: LayerId::from_uuid(deterministic_uuid(scope, "root-head")),
            root_snapshot_id: SnapshotId::from_uuid(deterministic_uuid(scope, "root-snapshot")),
            owner_id: format!("k8s-cluster/{kubernetes_namespace}/{}", cluster.name_any()),
        })
        .await?;
    let capabilities = match volume.packed_binding.as_ref() {
        Some(binding) => super::crd::WorkspaceCapabilityStatus {
            volume_format: "workspace-v1".into(),
            packed_v3: super::crd::PackedV3Capability::Readonly,
            packed_binding: super::crd::PackedBindingStatus::Present {
                version: binding.binding.binding_version,
                manifest_digest: hex::encode(binding.binding.manifest.digest),
            },
        },
        None => bail!("packed-v3 root carrier binding is not initialized"),
    };
    let mut conditions = vec![
        condition(
            "WorkspaceCatalogReady",
            "True",
            "PackedCatalogReady",
            "packed-v3 workspace catalog and carrier root snapshot are ready",
            cluster.metadata.generation,
        ),
        capabilities.packed_lower_condition(cluster.metadata.generation),
    ];
    if let Some(previous) = cluster
        .status
        .as_ref()
        .and_then(|status| status.workspace.as_ref())
    {
        preserve_condition_transition_times(&previous.conditions, &mut conditions);
    }
    let workspace_status = super::crd::WorkspaceClusterStatus {
        volume_id: volume.volume_id.to_string(),
        schema_version: brewfs::workspace_overlay::model::WORKSPACE_SCHEMA_VERSION,
        catalog_backend: spec.catalog_backend,
        catalog_namespace: catalog_namespace(&cluster.name_any(), kubernetes_namespace, spec),
        root_snapshot_id: volume.root_snapshot_id.to_string(),
        root_revision: revision_to_status(&volume.root_revision),
        capabilities: capabilities.clone(),
        conditions,
    };
    patch_cluster_workspace_status(
        client,
        kubernetes_namespace,
        cluster,
        Some(workspace_status),
    )
    .await
}

pub async fn reconcile_workspace(
    workspace: Arc<BrewFSWorkspace>,
    ctx: Arc<OperatorContext>,
) -> Result<Action, WorkspaceReconcileError> {
    let namespace = workspace
        .namespace()
        .ok_or_else(|| anyhow!("BrewFSWorkspace must be namespaced"))?;
    let api: Api<BrewFSWorkspace> = Api::namespaced(ctx.client.clone(), &namespace);
    let name = workspace.name_any();

    if workspace.meta().deletion_timestamp.is_some() {
        cleanup_workspace(&ctx.client, &namespace, &workspace).await?;
        remove_finalizer(&api, &name, &workspace, WORKSPACE_FINALIZER).await?;
        return Ok(Action::await_change());
    }
    if ensure_finalizer(&api, &name, &workspace, WORKSPACE_FINALIZER).await? {
        return Ok(Action::await_change());
    }
    if let Err(error) = workspace.spec.validate() {
        patch_workspace_status(
            &api,
            &workspace,
            workspace_status(&workspace, "Failed", &error, None, None),
        )
        .await?;
        return Ok(Action::await_change());
    }

    let cluster = load_cluster(&ctx.client, &namespace, &workspace.spec.cluster_ref.name).await?;
    let Some(cluster_workspace) = cluster
        .status
        .as_ref()
        .and_then(|status| status.workspace.as_ref())
    else {
        patch_workspace_status(
            &api,
            &workspace,
            workspace_status(
                &workspace,
                "Pending",
                "waiting for WorkspaceCatalogReady",
                None,
                None,
            ),
        )
        .await?;
        return Ok(Action::requeue(Duration::from_secs(10)));
    };
    let cluster_spec = enabled_cluster_workspace_spec(&cluster)?;
    let admin =
        connect_workspace_admin_for_cluster(&ctx.client, &cluster, &namespace, cluster_spec)
            .await?;
    // Packed-v3 recovery is performed by its recorded bootstrap/snapshot operation.
    // Bounded source checks below keep unrelated incomplete workspaces RecoveryRequired.

    let source = resolve_workspace_source(
        &ctx.client,
        &namespace,
        &workspace,
        &cluster,
        admin.as_ref(),
    )
    .await?;
    admin.verify_revision(&source).await?;
    if workspace
        .status
        .as_ref()
        .and_then(|status| status.origin_revision.as_ref())
        .is_some_and(|origin| *origin != revision_to_status(&source))
    {
        return Err(anyhow!("workspace source is immutable after creation").into());
    }

    let volume_id = Uuid::parse_str(&cluster_workspace.volume_id)
        .context("parse cluster workspace volume ID")?;
    let workspace_uid = resource_uid(&*workspace)?;
    let workspace_uuid = deterministic_uuid(volume_id, &format!("workspace/{workspace_uid}"));
    let workspace_id = WorkspaceId::from_uuid(workspace_uuid);
    if workspace
        .status
        .as_ref()
        .and_then(|status| status.workspace_id.as_deref())
        .is_some_and(|existing| existing != workspace_id.to_string())
    {
        return Err(anyhow!("clusterRef is immutable after workspace creation").into());
    }
    let workspace_request = EnsureWorkspaceRequest {
        workspace_id,
        head_layer_id: LayerId::from_uuid(deterministic_uuid(workspace_uuid, "head/0")),
        base_revision: source.clone(),
        owner_id: workspace
            .spec
            .owner_id
            .clone()
            .or_else(|| Some(format!("k8s-workspace/{namespace}/{name}"))),
    };
    let view = if cluster_workspace.capabilities.packed_v3_ready() {
        admin.ensure_packed_workspace(workspace_request).await?
    } else {
        admin.ensure_workspace(workspace_request).await?
    };

    if view.layer_depth != 2 {
        return Err(anyhow!("workspace backend returned a non-canonical layer depth").into());
    }

    // A Releasing lease still owns the writable mount slot until the backend
    // records the durable Released transition. Treat it as busy for both
    // clean-source admission and the workspace phase; checking only Active
    // would let a suspended workspace advertise a clean view while a release
    // transaction is still in flight.
    let active_lease = view
        .leases
        .iter()
        .any(|lease| matches!(lease.state, LeaseState::Active | LeaseState::Releasing));
    let packed_binding = admin.packed_binding(workspace_id).await?;
    if cluster_workspace.capabilities.packed_v3_ready() && packed_binding.is_none() {
        return Err(anyhow!("packed-v3 workspace is missing its mandatory binding").into());
    }
    let clean_epoch = if packed_binding.is_some() && !active_lease {
        match admin.clean_published_view(workspace_id).await? {
            Some(report) => Some(report.head_epoch),
            None => match admin.recovered_packed_mount(workspace_id).await? {
                Some(report) => {
                    if report.released.guard.expected_head_layer_id != view.record.head_layer_id
                        || report.released.guard.expected_head_epoch != view.record.head_epoch
                    {
                        return Err(anyhow!("recovered packed-v3 workspace view changed").into());
                    }
                    Some(report.released.guard.expected_head_epoch)
                }
                None => match admin.clean_unmounted_packed_epoch(workspace_id).await? {
                    Some(epoch) => Some(epoch),
                    None => clean_reference::verified_clean_mount_reference(
                        &ctx.client,
                        &namespace,
                        &workspace,
                        &view,
                        admin.as_ref(),
                    )
                    .await?
                    .map(|reference| reference.guard.expected_head_epoch),
                },
            },
        }
    } else {
        None
    };
    let (phase, message) = match workspace.spec.desired_state {
        WorkspaceDesiredState::Active => ("Active", "workspace is available for mounting"),
        WorkspaceDesiredState::Suspended if active_lease => (
            "Quiescing",
            "waiting for the writable mount lease to be released",
        ),
        WorkspaceDesiredState::Suspended if packed_binding.is_some() && clean_epoch.is_none() => (
            "RecoveryRequired",
            "packed-v3 clean source proof is unavailable",
        ),
        WorkspaceDesiredState::Suspended => ("Suspended", "workspace clean source was verified"),
    };
    let mut status = workspace_status(
        &workspace,
        phase,
        message,
        Some(workspace_id.to_string()),
        Some(revision_to_status(&view.base_revision)),
    );
    status.capabilities = match &packed_binding {
        Some(binding) => super::crd::WorkspaceCapabilityStatus {
            volume_format: "workspace-v1".into(),
            packed_v3: super::crd::PackedV3Capability::Readonly,
            packed_binding: super::crd::PackedBindingStatus::Present {
                version: binding.binding.binding_version,
                manifest_digest: hex::encode(binding.binding.manifest.digest),
            },
        },
        None => super::crd::WorkspaceCapabilityStatus::native_only(),
    };
    status.origin_revision = workspace
        .status
        .as_ref()
        .and_then(|status| status.origin_revision.clone())
        .or_else(|| Some(revision_to_status(&source)));
    status.head_layer_id = Some(view.record.head_layer_id.to_string());
    status.head_epoch = Some(view.record.head_epoch);
    status.active_mount_ref = if active_lease {
        active_mount_name(&ctx.client, &namespace, &workspace).await?
    } else {
        None
    };
    status.last_clean_release_epoch = if packed_binding.is_some() {
        clean_epoch
    } else if !active_lease {
        Some(view.record.head_epoch)
    } else {
        None
    };
    let mut base_verified = condition(
        "BaseVerified",
        "True",
        "ExactRevisionMatched",
        "the sealed lower revision tuple and fixed two-layer view were verified",
        workspace.metadata.generation,
    );
    if let Some(existing) = workspace.status.as_ref().and_then(|status| {
        status.conditions.iter().find(|condition| {
            condition.condition_type == base_verified.condition_type
                && condition.status == base_verified.status
                && condition.reason == base_verified.reason
                && condition.message == base_verified.message
        })
    }) {
        base_verified.last_transition_time = existing.last_transition_time;
    }
    let mut packed_lower = status
        .capabilities
        .packed_lower_condition(workspace.metadata.generation);
    if let Some(existing) = workspace.status.as_ref().and_then(|status| {
        status.conditions.iter().find(|condition| {
            condition.condition_type == packed_lower.condition_type
                && condition.status == packed_lower.status
                && condition.reason == packed_lower.reason
                && condition.message == packed_lower.message
        })
    }) {
        packed_lower.last_transition_time = existing.last_transition_time;
    }
    status.conditions = vec![base_verified, packed_lower];
    patch_workspace_status(&api, &workspace, status).await?;
    Ok(Action::requeue(Duration::from_secs(30)))
}

pub async fn reconcile_workspace_mount(
    mount: Arc<BrewFSWorkspaceMount>,
    ctx: Arc<OperatorContext>,
) -> Result<Action, WorkspaceReconcileError> {
    let namespace = mount
        .namespace()
        .ok_or_else(|| anyhow!("BrewFSWorkspaceMount must be namespaced"))?;
    let api: Api<BrewFSWorkspaceMount> = Api::namespaced(ctx.client.clone(), &namespace);
    let name = mount.name_any();
    if mount.meta().deletion_timestamp.is_some() {
        super::packed_recovery::finish_mount_workload(&ctx.client, &namespace, &mount).await?;
        verify_mount_lease_released(&ctx.client, &namespace, &mount).await?;
        remove_finalizer(&api, &name, &mount, MOUNT_FINALIZER).await?;
        return Ok(Action::await_change());
    }
    if ensure_finalizer(&api, &name, &mount, MOUNT_FINALIZER).await? {
        return Ok(Action::await_change());
    }

    let workspace_api: Api<BrewFSWorkspace> = Api::namespaced(ctx.client.clone(), &namespace);
    let Some(workspace) = workspace_api
        .get_opt(&mount.spec.workspace_ref.name)
        .await
        .context("load referenced BrewFSWorkspace")?
    else {
        super::workload::patch_mount_status(
            &api,
            &mount,
            "WaitingForWorkspace",
            "referenced workspace does not exist",
            None,
        )
        .await?;
        return Ok(Action::requeue(Duration::from_secs(10)));
    };
    if let Err(error) = validate_mount_cluster_ref(
        &mount.spec.cluster_ref.name,
        &workspace.spec.cluster_ref.name,
    ) {
        super::workload::patch_mount_status(
            &api,
            &mount,
            "Failed",
            error,
            workspace.status.as_ref(),
        )
        .await?;
        return Ok(Action::await_change());
    }
    if workspace.meta().deletion_timestamp.is_some() {
        super::packed_recovery::finish_mount_workload(&ctx.client, &namespace, &mount).await?;
        super::workload::patch_mount_status(
            &api,
            &mount,
            "Releasing",
            "workspace deletion is draining this mount",
            workspace.status.as_ref(),
        )
        .await?;
        return Ok(Action::requeue(Duration::from_secs(5)));
    }
    let cluster = load_cluster(&ctx.client, &namespace, &workspace.spec.cluster_ref.name).await?;
    let cluster_spec = enabled_cluster_workspace_spec(&cluster)?;
    if let Err(error) = mount.spec.validate(cluster_spec.lease_ttl_seconds) {
        super::workload::patch_mount_status(&api, &mount, "Failed", &error, None).await?;
        return Ok(Action::await_change());
    }
    if workspace.spec.desired_state == WorkspaceDesiredState::Suspended {
        super::packed_recovery::finish_mount_workload(&ctx.client, &namespace, &mount).await?;
        super::workload::patch_mount_status(
            &api,
            &mount,
            "Released",
            "workspace is suspended; mount workload is stopped",
            workspace.status.as_ref(),
        )
        .await?;
        return Ok(Action::requeue(Duration::from_secs(10)));
    }
    if super::packed_recovery::recovery_is_held(&mount) {
        if mount
            .status
            .as_ref()
            .is_some_and(|status| status.phase == "RecoveryCompleted")
        {
            return Ok(Action::await_change());
        }
        super::packed_recovery::finish_mount_workload(&ctx.client, &namespace, &mount).await?;
        super::workload::patch_mount_status(
            &api,
            &mount,
            "RecoveryCompleted",
            "original same-PVC recovery completed; suspend the workspace before publication",
            workspace.status.as_ref(),
        )
        .await?;
        return Ok(Action::await_change());
    }
    if workspace
        .status
        .as_ref()
        .is_none_or(|status| status.phase != "Active" || status.workspace_id.is_none())
    {
        super::workload::patch_mount_status(
            &api,
            &mount,
            "WaitingForWorkspace",
            "workspace is not Active",
            workspace.status.as_ref(),
        )
        .await?;
        return Ok(Action::requeue(Duration::from_secs(10)));
    }

    ensure_single_mount(&ctx.client, &namespace, &mount).await?;
    reconcile_workspace_mount_workload(
        &ctx.client,
        &namespace,
        &mount,
        &workspace,
        &cluster,
        cluster_spec,
    )
    .await?;
    let admin =
        connect_workspace_admin_for_cluster(&ctx.client, &cluster, &namespace, cluster_spec)
            .await?;
    admin.reap_expired_leases().await?;
    let workspace_id: WorkspaceId = workspace
        .status
        .as_ref()
        .and_then(|status| status.workspace_id.as_deref())
        .ok_or_else(|| anyhow!("active workspace has no backend identity"))?
        .parse()
        .context("parse active workspace backend ID")?;
    let backend = admin.inspect_workspace(workspace_id).await?;
    patch_mount_backend_status(&ctx.client, &api, &mount, &backend).await?;
    Ok(Action::requeue(Duration::from_secs(15)))
}

async fn active_mount_name(
    client: &Client,
    namespace: &str,
    workspace: &BrewFSWorkspace,
) -> anyhow::Result<Option<String>> {
    let mounts = Api::<BrewFSWorkspaceMount>::namespaced(client.clone(), namespace)
        .list(&ListParams::default().limit(256))
        .await?;
    if mounts
        .metadata
        .continue_
        .as_deref()
        .is_some_and(|value| !value.is_empty())
    {
        bail!("workspace mount inventory is incomplete");
    }
    Ok(mounts
        .items
        .into_iter()
        .filter(|mount| {
            mount.spec.workspace_ref.name == workspace.name_any()
                && mount.spec.cluster_ref.name == workspace.spec.cluster_ref.name
                && mount.meta().deletion_timestamp.is_none()
        })
        .map(|mount| mount.name_any())
        .min())
}

pub async fn reconcile_workspace_snapshot(
    snapshot: Arc<BrewFSWorkspaceSnapshot>,
    ctx: Arc<OperatorContext>,
) -> Result<Action, WorkspaceReconcileError> {
    let namespace = snapshot
        .namespace()
        .ok_or_else(|| anyhow!("BrewFSWorkspaceSnapshot must be namespaced"))?;
    let api: Api<BrewFSWorkspaceSnapshot> = Api::namespaced(ctx.client.clone(), &namespace);
    let name = snapshot.name_any();
    if snapshot.meta().deletion_timestamp.is_some() {
        cleanup_snapshot(&ctx.client, &namespace, &snapshot).await?;
        remove_finalizer(&api, &name, &snapshot, SNAPSHOT_FINALIZER).await?;
        return Ok(Action::await_change());
    }
    if ensure_finalizer(&api, &name, &snapshot, SNAPSHOT_FINALIZER).await? {
        return Ok(Action::await_change());
    }
    if let Err(error) = snapshot.spec.validate() {
        patch_snapshot_status(&api, &snapshot, "Failed", &error, None, None, None).await?;
        return Ok(Action::await_change());
    }

    let workspace_api: Api<BrewFSWorkspace> = Api::namespaced(ctx.client.clone(), &namespace);
    let workspace = workspace_api
        .get(&snapshot.spec.workspace_ref.name)
        .await
        .context("load snapshot source workspace")?;
    if workspace.spec.cluster_ref.name != snapshot.spec.cluster_ref.name {
        return Err(
            anyhow!("snapshot and source workspace must reference the same cluster").into(),
        );
    }
    let cluster = load_cluster(&ctx.client, &namespace, &snapshot.spec.cluster_ref.name).await?;
    let cluster_spec = enabled_cluster_workspace_spec(&cluster)?;
    let cluster_workspace = cluster
        .status
        .as_ref()
        .and_then(|status| status.workspace.as_ref())
        .ok_or_else(|| anyhow!("cluster workspace catalog is not ready"))?;
    let workspace_id: WorkspaceId = workspace
        .status
        .as_ref()
        .and_then(|status| status.workspace_id.as_deref())
        .ok_or_else(|| anyhow!("source workspace has no backend identity"))?
        .parse()
        .context("parse source workspace ID")?;
    let admin =
        connect_workspace_admin_for_cluster(&ctx.client, &cluster, &namespace, cluster_spec)
            .await?;
    // Packed-v3 recovery is performed by its recorded bootstrap/snapshot operation.
    // Bounded source checks below keep unrelated incomplete workspaces RecoveryRequired.
    let volume_id = Uuid::parse_str(&cluster_workspace.volume_id)
        .context("parse cluster workspace volume ID")?;
    let snapshot_uid = resource_uid(&*snapshot)?;
    let snapshot_id = SnapshotId::from_uuid(deterministic_uuid(
        volume_id,
        &format!("snapshot/{snapshot_uid}"),
    ));
    let current = admin.inspect_workspace(workspace_id).await?;
    let recorded_source = snapshot.status.as_ref().and_then(|status| {
        status
            .source_workspace_id
            .clone()
            .zip(status.source_head_epoch)
    });
    if let Some((recorded_workspace_id, _)) = recorded_source.as_ref() {
        if recorded_workspace_id != &workspace_id.to_string() {
            patch_snapshot_status(
                &api,
                &snapshot,
                "Failed",
                "snapshot intent belongs to a different backend workspace",
                None,
                None,
                recorded_source,
            )
            .await?;
            return Ok(Action::await_change());
        }
    }
    let existing = match admin.load_snapshot(snapshot_id).await {
        Ok(existing) => Some(existing),
        Err(error)
            if error
                .downcast_ref::<WorkspaceError>()
                .is_some_and(|error| matches!(error, WorkspaceError::SnapshotNotFound(_))) =>
        {
            None
        }
        Err(error) => return Err(error.into()),
    };
    // An immutable backend snapshot survives parent reactivation. Check the
    // backend before the suspension gate; a Ready status alone is not proof.
    if snapshot_requires_suspension(&workspace, &snapshot, existing.is_some()) {
        patch_snapshot_status(
            &api,
            &snapshot,
            "Pending",
            "source workspace must be fully Suspended before snapshot",
            None,
            None,
            recorded_source,
        )
        .await?;
        return Ok(Action::requeue(Duration::from_secs(10)));
    }
    if let Some(existing) = existing {
        if admin.packed_binding(workspace_id).await?.is_some() {
            admin.verify_packed_revision(&existing.revision).await?;
        }
        let source =
            recorded_source.or_else(|| Some((workspace_id.to_string(), current.record.head_epoch)));
        patch_snapshot_status(
            &api,
            &snapshot,
            "Ready",
            "immutable revision is pinned",
            Some(existing.snapshot_id.to_string()),
            Some(revision_to_status(&existing.revision)),
            source,
        )
        .await?;
        return Ok(Action::requeue(Duration::from_secs(300)));
    }
    let Some((_, source_head_epoch)) = recorded_source else {
        patch_snapshot_status(
            &api,
            &snapshot,
            "Pending",
            "snapshot intent recorded; waiting to pin the source revision",
            None,
            None,
            Some((workspace_id.to_string(), current.record.head_epoch)),
        )
        .await?;
        return Ok(Action::await_change());
    };
    let request = EnsureSnapshotRequest {
        snapshot_id,
        name: format!("{namespace}/{name}"),
        revision: current.base_revision.clone(),
        owner_id: workspace.spec.owner_id.clone(),
    };
    let pinned = if admin.packed_binding(workspace_id).await?.is_some() {
        if admin
            .clean_unmounted_packed_epoch(workspace_id)
            .await?
            .is_some()
            || admin.clean_published_view(workspace_id).await?.is_some()
        {
            admin
                .pin_clean_packed_snapshot(workspace_id, request)
                .await?
        } else {
            match clean_reference::verified_clean_mount_reference(
                &ctx.client,
                &namespace,
                &workspace,
                &current,
                admin.as_ref(),
            )
            .await?
            {
                Some(reference) => {
                    admin
                        .publish_packed_snapshot(reference, request, cluster_spec.lease_ttl_seconds)
                        .await?
                }
                None => {
                    admin
                        .recover_packed_snapshot(
                            workspace_id,
                            request,
                            cluster_spec.lease_ttl_seconds,
                        )
                        .await?
                }
            }
        }
    } else {
        // No seal transaction exists: a native workspace head is never
        // turned into an immutable revision. Snapshots come only from a
        // packed publication.
        patch_snapshot_status(
            &api,
            &snapshot,
            "Failed",
            "snapshots require a packed-v3 workspace; native workspaces are never sealed",
            None,
            None,
            Some((workspace_id.to_string(), source_head_epoch)),
        )
        .await?;
        return Ok(Action::await_change());
    };
    patch_snapshot_status(
        &api,
        &snapshot,
        "Ready",
        "the packed workspace revision was pinned",
        Some(pinned.snapshot_id.to_string()),
        Some(revision_to_status(&pinned.revision)),
        Some((workspace_id.to_string(), source_head_epoch)),
    )
    .await?;
    Ok(Action::requeue(Duration::from_secs(300)))
}

pub fn error_policy_workspace(
    _object: Arc<BrewFSWorkspace>,
    _error: &WorkspaceReconcileError,
    _ctx: Arc<OperatorContext>,
) -> Action {
    Action::requeue(Duration::from_secs(15))
}

pub fn error_policy_mount(
    _object: Arc<BrewFSWorkspaceMount>,
    _error: &WorkspaceReconcileError,
    _ctx: Arc<OperatorContext>,
) -> Action {
    Action::requeue(Duration::from_secs(15))
}

fn snapshot_requires_suspension(
    workspace: &BrewFSWorkspace,
    snapshot: &BrewFSWorkspaceSnapshot,
    snapshot_exists: bool,
) -> bool {
    !snapshot_exists
        && workspace.meta().deletion_timestamp.is_none()
        && (workspace.spec.desired_state != WorkspaceDesiredState::Suspended
            || workspace.status.as_ref().is_none_or(|status| {
                status.phase != "Suspended"
                    && !(status.phase == "RecoveryRequired"
                        && snapshot.status.as_ref().is_some_and(|intent| {
                            intent.source_workspace_id.is_some()
                                && intent.source_head_epoch.is_some()
                        }))
            }))
}

pub fn error_policy_snapshot(
    _object: Arc<BrewFSWorkspaceSnapshot>,
    _error: &WorkspaceReconcileError,
    _ctx: Arc<OperatorContext>,
) -> Action {
    Action::requeue(Duration::from_secs(15))
}

async fn resolve_workspace_source(
    client: &Client,
    namespace: &str,
    workspace: &BrewFSWorkspace,
    cluster: &BrewFSCluster,
    admin: &dyn WorkspaceAdmin,
) -> anyhow::Result<brewfs::workspace_overlay::model::BaseRevision> {
    match workspace.spec.source.kind {
        WorkspaceSourceKind::ClusterRoot => {
            let revision = cluster
                .status
                .as_ref()
                .and_then(|status| status.workspace.as_ref())
                .map(|status| &status.root_revision)
                .ok_or_else(|| anyhow!("cluster root revision is not ready"))?;
            revision_from_status(revision)
        }
        WorkspaceSourceKind::Snapshot => {
            let name = workspace
                .spec
                .source
                .name
                .as_deref()
                .ok_or_else(|| anyhow!("snapshot source name is missing"))?;
            let api: Api<BrewFSWorkspaceSnapshot> = Api::namespaced(client.clone(), namespace);
            let snapshot = api
                .get(name)
                .await
                .with_context(|| format!("load BrewFSWorkspaceSnapshot {name}"))?;
            if snapshot.spec.cluster_ref.name != workspace.spec.cluster_ref.name {
                bail!("snapshot source belongs to another BrewFSCluster");
            }
            let status = snapshot
                .status
                .as_ref()
                .filter(|status| status.phase == "Ready")
                .ok_or_else(|| anyhow!("snapshot source is not Ready"))?;
            let snapshot_id: SnapshotId = status
                .snapshot_id
                .as_deref()
                .ok_or_else(|| anyhow!("snapshot status has no backend ID"))?
                .parse()
                .context("parse snapshot backend ID")?;
            let backend = admin.load_snapshot(snapshot_id).await?;
            let status_revision = status
                .revision
                .as_ref()
                .ok_or_else(|| anyhow!("snapshot status has no revision"))?;
            if revision_to_status(&backend.revision) != *status_revision {
                bail!("snapshot status does not match the backend revision");
            }
            Ok(backend.revision)
        }
    }
}

async fn cleanup_workspace(
    client: &Client,
    namespace: &str,
    workspace: &BrewFSWorkspace,
) -> anyhow::Result<()> {
    let mount_api: Api<BrewFSWorkspaceMount> = Api::namespaced(client.clone(), namespace);
    let mounts = mount_api.list(&ListParams::default().limit(256)).await?;
    if mounts
        .metadata
        .continue_
        .as_deref()
        .is_some_and(|value| !value.is_empty())
    {
        bail!("workspace mount deletion inventory is incomplete");
    }
    let mut remaining = false;
    for mount in mounts.items {
        if mount.spec.workspace_ref.name == workspace.name_any()
            && mount.spec.cluster_ref.name == workspace.spec.cluster_ref.name
        {
            remaining = true;
            if mount.meta().deletion_timestamp.is_none() {
                mount_api
                    .delete(&mount.name_any(), &DeleteParams::default())
                    .await?;
            }
        }
    }
    if remaining {
        bail!("waiting for BrewFSWorkspaceMount finalizers");
    }
    let cluster = load_cluster(client, namespace, &workspace.spec.cluster_ref.name).await?;
    let cluster_spec = enabled_cluster_workspace_spec(&cluster)?;
    let admin =
        connect_workspace_admin_for_cluster(client, &cluster, namespace, cluster_spec).await?;
    let workspace_id: WorkspaceId = workspace
        .status
        .as_ref()
        .and_then(|status| status.workspace_id.as_deref())
        .ok_or_else(|| anyhow!("cannot safely delete a workspace without its backend ID"))?
        .parse()?;
    admin.reap_expired_leases().await?;
    let view = admin.inspect_workspace(workspace_id).await?;
    if view
        .leases
        .iter()
        .any(|lease| matches!(lease.state, LeaseState::Active | LeaseState::Releasing))
    {
        bail!("waiting for the workspace writable lease to be released");
    }
    if workspace.spec.deletion_policy == WorkspaceDeletionPolicy::Retain {
        return Ok(());
    }
    let force = force_delete_requested(workspace)?;
    if workspace.spec.deletion_policy == WorkspaceDeletionPolicy::SnapshotAndDelete {
        ensure_final_snapshot(client, namespace, workspace).await?;
    }
    admin.mark_workspace_deleting(workspace_id, force).await
}

async fn ensure_final_snapshot(
    client: &Client,
    namespace: &str,
    workspace: &BrewFSWorkspace,
) -> anyhow::Result<()> {
    let uid = resource_uid(workspace)?;
    let suffix = uid.replace('-', "").chars().take(12).collect::<String>();
    let prefix = workspace.name_any().chars().take(40).collect::<String>();
    let name = format!("{prefix}-final-{suffix}");
    let api: Api<BrewFSWorkspaceSnapshot> = Api::namespaced(client.clone(), namespace);
    let snapshot = match api.get_opt(&name).await? {
        Some(snapshot) => snapshot,
        None => {
            api.create(
                &PostParams::default(),
                &BrewFSWorkspaceSnapshot::new(
                    &name,
                    super::crd::BrewFSWorkspaceSnapshotSpec {
                        cluster_ref: workspace.spec.cluster_ref.clone(),
                        workspace_ref: super::crd::NamespacedNameRef {
                            name: workspace.name_any(),
                        },
                    },
                ),
            )
            .await?
        }
    };
    if snapshot
        .status
        .as_ref()
        .is_none_or(|status| status.phase != "Ready")
    {
        bail!("waiting for final snapshot {name} to become Ready");
    }
    Ok(())
}

async fn verify_mount_lease_released(
    client: &Client,
    namespace: &str,
    mount: &BrewFSWorkspaceMount,
) -> anyhow::Result<()> {
    let workspace_api: Api<BrewFSWorkspace> = Api::namespaced(client.clone(), namespace);
    let workspace = workspace_api
        .get(&mount.spec.workspace_ref.name)
        .await
        .context("load workspace while releasing mount")?;
    let cluster = load_cluster(client, namespace, &workspace.spec.cluster_ref.name).await?;
    let cluster_spec = enabled_cluster_workspace_spec(&cluster)?;
    let admin =
        connect_workspace_admin_for_cluster(client, &cluster, namespace, cluster_spec).await?;
    admin.reap_expired_leases().await?;
    let workspace_id: WorkspaceId = workspace
        .status
        .as_ref()
        .and_then(|status| status.workspace_id.as_deref())
        .ok_or_else(|| anyhow!("workspace has no backend identity"))?
        .parse()?;
    let view = admin.inspect_workspace(workspace_id).await?;
    require_mount_lease_released(
        &view.leases,
        mount
            .status
            .as_ref()
            .and_then(|status| status.lease_id.as_deref()),
    )?;
    Ok(())
}

/// Validate the exact lease recorded by the mount before removing its
/// Kubernetes finalizer. Historical expired leases are retained in the
/// catalog, so checking that *all* leases are Released would deadlock cleanup;
/// the mount's own lease must instead reach Released, while an unknown lease
/// ID remains fail-closed.
fn require_mount_lease_released(
    leases: &[SnapshotLease],
    lease_id: Option<&str>,
) -> anyhow::Result<()> {
    let Some(lease_id) = lease_id else {
        if let Some(lease) = leases
            .iter()
            .find(|lease| lease.state != LeaseState::Released)
        {
            match lease.state {
                LeaseState::Active | LeaseState::Releasing => {
                    bail!("waiting for backend confirmation that the writable lease is released")
                }
                LeaseState::Expired => {
                    bail!("mount lease identity is missing and an expired lease needs recovery")
                }
                LeaseState::Released => unreachable!("filtered above"),
            }
        }
        return Ok(());
    };
    let lease_id: LeaseId = lease_id.parse().context("parse mount lease ID")?;
    let lease = leases
        .iter()
        .find(|lease| lease.lease_id == lease_id)
        .ok_or_else(|| anyhow!("mount lease is missing from the authoritative backend"))?;
    match lease.state {
        LeaseState::Released => Ok(()),
        LeaseState::Active | LeaseState::Releasing => {
            bail!("waiting for backend confirmation that the writable lease is released")
        }
        LeaseState::Expired => {
            bail!("mount lease expired without a clean release; recovery is required")
        }
    }
}

async fn cleanup_snapshot(
    client: &Client,
    namespace: &str,
    snapshot: &BrewFSWorkspaceSnapshot,
) -> anyhow::Result<()> {
    let workspace_api: Api<BrewFSWorkspace> = Api::namespaced(client.clone(), namespace);
    let workspaces = workspace_api
        .list(&ListParams::default().limit(256))
        .await?;
    if workspaces
        .metadata
        .continue_
        .as_deref()
        .is_some_and(|value| !value.is_empty())
    {
        bail!("snapshot reference inventory is incomplete");
    }
    if workspaces.items.iter().any(|workspace| {
        workspace.spec.source.kind == WorkspaceSourceKind::Snapshot
            && workspace.spec.source.name.as_deref() == Some(&snapshot.name_any())
    }) {
        bail!("snapshot is still referenced by a BrewFSWorkspace");
    }
    let Some(snapshot_id) = snapshot
        .status
        .as_ref()
        .and_then(|status| status.snapshot_id.as_deref())
    else {
        return Ok(());
    };
    let cluster = load_cluster(client, namespace, &snapshot.spec.cluster_ref.name).await?;
    let cluster_spec = enabled_cluster_workspace_spec(&cluster)?;
    let admin =
        connect_workspace_admin_for_cluster(client, &cluster, namespace, cluster_spec).await?;
    admin.delete_snapshot(snapshot_id.parse()?).await
}

async fn ensure_single_mount(
    client: &Client,
    namespace: &str,
    mount: &BrewFSWorkspaceMount,
) -> anyhow::Result<()> {
    let api: Api<BrewFSWorkspaceMount> = Api::namespaced(client.clone(), namespace);
    let current_uid = resource_uid(mount)?;
    let candidates = api.list(&ListParams::default().limit(256)).await?;
    if candidates
        .metadata
        .continue_
        .as_deref()
        .is_some_and(|value| !value.is_empty())
    {
        bail!("workspace mount ownership inventory is incomplete");
    }
    let conflict = candidates.items.into_iter().any(|candidate| {
        candidate.spec.workspace_ref.name == mount.spec.workspace_ref.name
            && candidate.spec.cluster_ref.name == mount.spec.cluster_ref.name
            && candidate.meta().deletion_timestamp.is_none()
            && resource_uid(&candidate).is_ok_and(|uid| uid < current_uid)
    });
    if conflict {
        bail!("another BrewFSWorkspaceMount owns the writable mount slot");
    }
    Ok(())
}

async fn load_cluster(
    client: &Client,
    namespace: &str,
    name: &str,
) -> anyhow::Result<BrewFSCluster> {
    Api::<BrewFSCluster>::namespaced(client.clone(), namespace)
        .get(name)
        .await
        .with_context(|| format!("load BrewFSCluster {name}"))
}

fn enabled_cluster_workspace_spec(
    cluster: &BrewFSCluster,
) -> anyhow::Result<&super::crd::WorkspaceClusterSpec> {
    cluster
        .spec
        .workspace
        .as_ref()
        .filter(|spec| spec.enabled)
        .ok_or_else(|| anyhow!("BrewFSCluster does not have workspace support enabled"))
}

fn workspace_status(
    workspace: &BrewFSWorkspace,
    phase: &str,
    message: &str,
    workspace_id: Option<String>,
    base_revision: Option<WorkspaceRevision>,
) -> BrewFSWorkspaceStatus {
    BrewFSWorkspaceStatus {
        observed_generation: workspace.metadata.generation,
        capabilities: WorkspaceCapabilityStatus::native_only(),
        phase: phase.into(),
        message: message.into(),
        workspace_id,
        origin_revision: None,
        current_base_revision: base_revision,
        head_layer_id: None,
        head_epoch: None,
        active_mount_ref: None,
        last_clean_release_epoch: None,
        conditions: Vec::new(),
    }
}

fn condition(
    condition_type: &str,
    status: &str,
    reason: &str,
    message: &str,
    observed_generation: Option<i64>,
) -> WorkspaceCondition {
    WorkspaceCondition {
        condition_type: condition_type.into(),
        status: status.into(),
        reason: reason.into(),
        message: message.into(),
        observed_generation,
        last_transition_time: Utc::now(),
    }
}

fn preserve_condition_transition_times(
    previous: &[WorkspaceCondition],
    next: &mut [WorkspaceCondition],
) {
    for candidate in next {
        if let Some(existing) = previous.iter().find(|existing| {
            existing.condition_type == candidate.condition_type
                && existing.status == candidate.status
                && existing.reason == candidate.reason
                && existing.message == candidate.message
        }) {
            candidate.last_transition_time = existing.last_transition_time;
        }
    }
}

async fn patch_workspace_status(
    api: &Api<BrewFSWorkspace>,
    workspace: &BrewFSWorkspace,
    status: BrewFSWorkspaceStatus,
) -> anyhow::Result<()> {
    if workspace.status.as_ref() == Some(&status) {
        return Ok(());
    }
    patch_status(api, &workspace.name_any(), "BrewFSWorkspace", status).await
}

async fn patch_snapshot_status(
    api: &Api<BrewFSWorkspaceSnapshot>,
    snapshot: &BrewFSWorkspaceSnapshot,
    phase: &str,
    message: &str,
    snapshot_id: Option<String>,
    revision: Option<WorkspaceRevision>,
    source: Option<(String, u64)>,
) -> anyhow::Result<()> {
    let (source_workspace_id, source_head_epoch) = source
        .map(|(workspace_id, head_epoch)| (Some(workspace_id), Some(head_epoch)))
        .unwrap_or((None, None));
    let status = BrewFSWorkspaceSnapshotStatus {
        observed_generation: snapshot.metadata.generation,
        phase: phase.into(),
        message: message.into(),
        snapshot_id,
        source_workspace_id,
        source_head_epoch,
        revision,
        conditions: Vec::new(),
    };
    if snapshot.status.as_ref() == Some(&status) {
        return Ok(());
    }
    patch_status(api, &snapshot.name_any(), "BrewFSWorkspaceSnapshot", status).await
}

async fn patch_cluster_workspace_status(
    client: &Client,
    namespace: &str,
    cluster: &BrewFSCluster,
    workspace: Option<super::crd::WorkspaceClusterStatus>,
) -> anyhow::Result<()> {
    if cluster
        .status
        .as_ref()
        .and_then(|status| status.workspace.as_ref())
        == workspace.as_ref()
    {
        return Ok(());
    }
    let api: Api<BrewFSCluster> = Api::namespaced(client.clone(), namespace);
    api.patch_status(
        &cluster.name_any(),
        &PatchParams::default(),
        &Patch::Merge(json!({ "status": { "workspace": workspace } })),
    )
    .await?;
    Ok(())
}

async fn patch_status<K, S>(api: &Api<K>, name: &str, kind: &str, status: S) -> anyhow::Result<()>
where
    K: Clone + std::fmt::Debug + DeserializeOwned + Serialize + kube::Resource<DynamicType = ()>,
    S: Serialize,
{
    api.patch_status(
        name,
        &PatchParams::apply("brewfs-workspace-operator").force(),
        &Patch::Apply(json!({
            "apiVersion": "storage.brewfs.io/v1alpha1",
            "kind": kind,
            "status": status,
        })),
    )
    .await?;
    Ok(())
}

async fn ensure_finalizer<K>(
    api: &Api<K>,
    name: &str,
    object: &K,
    finalizer: &str,
) -> anyhow::Result<bool>
where
    K: Clone + std::fmt::Debug + DeserializeOwned + Serialize + kube::Resource<DynamicType = ()>,
{
    let mut finalizers = object.meta().finalizers.clone().unwrap_or_default();
    if finalizers.iter().any(|value| value == finalizer) {
        return Ok(false);
    }
    finalizers.push(finalizer.into());
    api.patch(
        name,
        &PatchParams::default(),
        &Patch::Merge(json!({ "metadata": { "finalizers": finalizers } })),
    )
    .await?;
    Ok(true)
}

async fn remove_finalizer<K>(
    api: &Api<K>,
    name: &str,
    object: &K,
    finalizer: &str,
) -> anyhow::Result<()>
where
    K: Clone + std::fmt::Debug + DeserializeOwned + Serialize + kube::Resource<DynamicType = ()>,
{
    let finalizers = object
        .meta()
        .finalizers
        .clone()
        .unwrap_or_default()
        .into_iter()
        .filter(|value| value != finalizer)
        .collect::<Vec<_>>();
    api.patch(
        name,
        &PatchParams::default(),
        &Patch::Merge(json!({ "metadata": { "finalizers": finalizers } })),
    )
    .await?;
    Ok(())
}

fn force_delete_requested(workspace: &BrewFSWorkspace) -> anyhow::Result<bool> {
    let annotations = workspace.annotations();
    if annotations
        .get(FORCE_DELETE_ANNOTATION)
        .is_none_or(|value| value != "true")
    {
        return Ok(false);
    }
    if annotations
        .get(FORCE_DELETE_REASON_ANNOTATION)
        .is_none_or(|reason| reason.trim().is_empty())
    {
        bail!("force-delete requires a non-empty force-delete-reason annotation");
    }
    Ok(true)
}

fn resource_uid<K: kube::ResourceExt>(resource: &K) -> anyhow::Result<String> {
    resource
        .uid()
        .ok_or_else(|| anyhow!("{} has no Kubernetes UID", resource.name_any()))
}

fn resource_uuid(uid: &str) -> Uuid {
    Uuid::parse_str(uid).unwrap_or_else(|_| deterministic_uuid(ID_NAMESPACE, uid))
}

fn validate_mount_cluster_ref(
    mount_cluster: &str,
    workspace_cluster: &str,
) -> Result<(), &'static str> {
    if mount_cluster.trim().is_empty() {
        return Err("clusterRef.name must not be empty");
    }
    if workspace_cluster.trim().is_empty() {
        return Err("referenced workspace has an empty clusterRef.name");
    }
    if mount_cluster != workspace_cluster {
        return Err("clusterRef must match the referenced workspace cluster");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use kube::api::ObjectMeta;

    #[test]
    fn recovered_snapshot_reuse_survives_parent_reactivation() {
        let mut workspace = BrewFSWorkspace::new(
            "parent",
            super::super::crd::BrewFSWorkspaceSpec {
                cluster_ref: super::super::crd::NamespacedNameRef { name: "c".into() },
                source: super::super::crd::WorkspaceSourceSpec {
                    kind: WorkspaceSourceKind::ClusterRoot,
                    name: None,
                },
                desired_state: WorkspaceDesiredState::Active,
                owner_id: None,
                deletion_policy: WorkspaceDeletionPolicy::Delete,
            },
        );
        let mut snapshot = BrewFSWorkspaceSnapshot::new(
            "snapshot",
            super::super::crd::BrewFSWorkspaceSnapshotSpec {
                cluster_ref: super::super::crd::NamespacedNameRef { name: "c".into() },
                workspace_ref: super::super::crd::NamespacedNameRef {
                    name: "parent".into(),
                },
            },
        );
        // Backend publication is authoritative, even if the Ready status write
        // was lost. Resuming the parent cannot invalidate an immutable snapshot.
        assert!(!snapshot_requires_suspension(&workspace, &snapshot, true));
        assert!(snapshot_requires_suspension(&workspace, &snapshot, false));
        snapshot.status = Some(BrewFSWorkspaceSnapshotStatus {
            phase: "Ready".into(),
            ..Default::default()
        });
        // A stale/forged Ready status must not permit a new snapshot of an
        // active parent when the backend snapshot does not exist.
        assert!(snapshot_requires_suspension(&workspace, &snapshot, false));
        workspace.spec.desired_state = WorkspaceDesiredState::Suspended;
        workspace.status = Some(BrewFSWorkspaceStatus {
            phase: "Suspended".into(),
            ..Default::default()
        });
        assert!(!snapshot_requires_suspension(&workspace, &snapshot, false));
        workspace.status.as_mut().unwrap().phase = "RecoveryRequired".into();
        assert!(snapshot_requires_suspension(&workspace, &snapshot, false));
        let status = snapshot.status.as_mut().unwrap();
        status.source_workspace_id = Some("recorded-source".into());
        status.source_head_epoch = Some(1);
        assert!(!snapshot_requires_suspension(&workspace, &snapshot, false));
    }

    #[test]
    fn fallback_resource_uuid_is_stable() {
        assert_eq!(resource_uuid("not-a-uuid"), resource_uuid("not-a-uuid"));
        assert_ne!(resource_uuid("not-a-uuid"), resource_uuid("another-uid"));
    }

    #[test]
    fn mount_cluster_reference_is_fail_closed() {
        assert!(validate_mount_cluster_ref("demo", "demo").is_ok());
        assert_eq!(
            validate_mount_cluster_ref("other", "demo"),
            Err("clusterRef must match the referenced workspace cluster")
        );
        assert_eq!(
            validate_mount_cluster_ref("", "demo"),
            Err("clusterRef.name must not be empty")
        );
    }

    #[test]
    fn force_delete_requires_an_audit_reason() {
        let mut workspace = BrewFSWorkspace::new(
            "test",
            super::super::crd::BrewFSWorkspaceSpec {
                cluster_ref: super::super::crd::NamespacedNameRef { name: "c".into() },
                source: super::super::crd::WorkspaceSourceSpec {
                    kind: WorkspaceSourceKind::ClusterRoot,
                    name: None,
                },
                desired_state: WorkspaceDesiredState::Active,
                owner_id: None,
                deletion_policy: WorkspaceDeletionPolicy::Delete,
            },
        );
        workspace.metadata = ObjectMeta {
            annotations: Some(std::collections::BTreeMap::from([(
                FORCE_DELETE_ANNOTATION.into(),
                "true".into(),
            )])),
            ..ObjectMeta::default()
        };
        assert!(force_delete_requested(&workspace).is_err());
    }

    fn synthetic_lease(id: u128, state: LeaseState) -> SnapshotLease {
        SnapshotLease {
            lease_id: LeaseId::from_uuid(Uuid::from_u128(id)),
            workspace_id: WorkspaceId::from_uuid(Uuid::from_u128(10)),
            base_revision: brewfs::workspace_overlay::model::BaseRevision {
                layer_id: LayerId::from_uuid(Uuid::from_u128(11)),
                sealed_version: 1,
                root_hash: [0; 32],
            },
            holder_generation: 1,
            writable: true,
            state,
            expires_at_ns: 0,
            created_at_ns: id as i64,
            updated_at_ns: id as i64,
        }
    }

    #[test]
    fn mount_finalizer_requires_its_exact_lease_to_be_released() {
        let released = synthetic_lease(20, LeaseState::Released);
        let expired = synthetic_lease(21, LeaseState::Expired);
        let active = synthetic_lease(22, LeaseState::Active);
        let leases = vec![released.clone(), expired, active.clone()];
        let released_id = released.lease_id.to_string();
        require_mount_lease_released(&leases, Some(&released_id)).unwrap();

        let expired_id = leases[1].lease_id.to_string();
        let error = require_mount_lease_released(&leases, Some(&expired_id)).unwrap_err();
        assert!(error.to_string().contains("recovery is required"));

        let missing_id = LeaseId::from_uuid(Uuid::from_u128(99)).to_string();
        let error = require_mount_lease_released(&[released], Some(&missing_id)).unwrap_err();
        assert!(error
            .to_string()
            .contains("missing from the authoritative backend"));

        let error = require_mount_lease_released(&[active], None).unwrap_err();
        assert!(error.to_string().contains("writable lease is released"));
    }
}

#[path = "clean_reference.rs"]
mod clean_reference;
