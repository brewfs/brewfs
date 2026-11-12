//! Kubernetes observations route a bounded proof check; status grants no proof.
use super::*;
use brewfs::workspace::catalog::HeadGuard;
use brewfs::workspace::packed_admin::PackedReleasedMountReference;

pub(super) async fn verified_clean_mount_reference(
    client: &Client,
    namespace: &str,
    workspace: &BrewFSWorkspace,
    view: &super::super::admin::WorkspaceView,
    admin: &dyn WorkspaceAdmin,
) -> anyhow::Result<Option<PackedReleasedMountReference>> {
    let mounts = Api::<BrewFSWorkspaceMount>::namespaced(client.clone(), namespace)
        .list(&ListParams::default().limit(256))
        .await?;
    if mounts
        .metadata
        .continue_
        .as_deref()
        .is_some_and(|value| !value.is_empty())
    {
        bail!("clean mount reference inventory is incomplete");
    }
    let mut candidates = Vec::new();
    for mount in mounts.items {
        if mount.spec.workspace_ref.name != workspace.name_any()
            || mount.spec.cluster_ref.name != workspace.spec.cluster_ref.name
        {
            continue;
        }
        let Some(status) = &mount.status else {
            continue;
        };
        let (Some(lease), Some(generation), Some(head), Some(epoch), Some(pod_uid)) = (
            status.lease_id.as_deref(),
            status.holder_generation,
            status.mounted_head_layer_id.as_deref(),
            status.mounted_head_epoch,
            status.pod_uid.as_deref(),
        ) else {
            continue;
        };
        if epoch != view.record.head_epoch
            || head != view.record.head_layer_id.to_string()
            || status.workspace_id.as_deref() != Some(view.record.workspace_id.to_string().as_str())
        {
            continue;
        }
        let reference = PackedReleasedMountReference {
            guard: HeadGuard {
                workspace_id: view.record.workspace_id,
                expected_head_layer_id: head.parse()?,
                expected_head_epoch: epoch,
                lease_id: lease.parse()?,
                holder_generation: generation,
            },
            mount_uid: Uuid::parse_str(&resource_uid(&mount)?)?,
            pod_uid: Uuid::parse_str(pod_uid)?,
        };
        if admin.verify_clean_packed_mount(reference.clone()).await? {
            candidates.push(reference);
        }
    }
    match candidates.len() {
        0 => {
            admin
                .clean_released_packed_mount(view.record.workspace_id)
                .await
        }
        1 => Ok(candidates.pop()),
        _ => bail!("multiple original clean packed mount references matched"),
    }
}
