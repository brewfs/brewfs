//! Real same-PVC recovery CLI Job; Kubernetes facts never grant writer authority.
use std::collections::{BTreeMap, BTreeSet};

use anyhow::{anyhow, bail, Context as _};
use brewfs::workspace::catalog::HeadGuard;
use brewfs::workspace::ids::{LeaseId, WorkspaceId};
use brewfs::workspace::model::LeaseState;
use brewfs::workspace::packed_admin::PackedReleasedMountReference;
use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::batch::v1::{Job, JobSpec};
use k8s_openapi::api::core::v1::{
    ConfigMap, ConfigMapVolumeSource, Container, EnvVar, EnvVarSource, ObjectFieldSelector,
    PersistentVolumeClaim, PersistentVolumeClaimVolumeSource, Pod, PodSpec, PodTemplateSpec,
    SecretKeySelector, Volume, VolumeMount,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::api::{Api, DeleteParams, ListParams, Patch, PatchParams, PostParams, Preconditions};
use kube::{Client, Resource, ResourceExt};
use serde_json::json;
use uuid::Uuid;

use super::admin::{connect_workspace_admin_for_cluster, deterministic_uuid, WorkspaceAdmin};
use super::crd::{
    BrewFSWorkspace, BrewFSWorkspaceMount, WorkspaceCacheMode, WorkspaceCatalogBackend,
};
use super::workload::delete_workspace_mount_workload;
use crate::crd::BrewFSCluster;

const PVC_UID: &str = "storage.brewfs.io/packed-writeback-pvc-uid";

pub async fn pin_writeback_pvc_uid(
    client: &Client,
    namespace: &str,
    mount: &BrewFSWorkspaceMount,
) -> anyhow::Result<()> {
    let pvc = Api::<PersistentVolumeClaim>::namespaced(client.clone(), namespace)
        .get(&format!("{}-workspace-cache", mount.name_any()))
        .await?;
    let uid = pvc
        .uid()
        .ok_or_else(|| anyhow!("writeback PVC has no UID"))?;
    let mount_uid = mount.uid().ok_or_else(|| anyhow!("mount has no UID"))?;
    if !pvc
        .owner_references()
        .iter()
        .any(|owner| owner.kind == "BrewFSWorkspaceMount" && owner.uid == mount_uid)
    {
        bail!("writeback PVC does not belong to the original mount");
    }
    let api = Api::<BrewFSWorkspaceMount>::namespaced(client.clone(), namespace);
    let current = api.get(&mount.name_any()).await?;
    if current.uid() != Some(mount_uid) {
        bail!("mount identity changed before writeback PVC pin");
    }
    if let Some(previous) = current.annotations().get(PVC_UID) {
        if previous != &uid {
            bail!("original writeback PVC identity changed");
        }
        return Ok(());
    }
    // Kubernetes resourceVersion makes the first pin an exact metadata update.
    let rv = current
        .resource_version()
        .ok_or_else(|| anyhow!("mount has no resourceVersion"))?;
    let mut annotations = current.annotations().clone();
    annotations.insert(PVC_UID.into(), uid);
    api.patch(
        &mount.name_any(),
        &PatchParams::default(),
        &Patch::Merge(json!({"metadata":{"resourceVersion":rv,"annotations":annotations}})),
    )
    .await?;
    Ok(())
}

pub fn recovery_is_held(mount: &BrewFSWorkspaceMount) -> bool {
    mount.status.as_ref().is_some_and(|status| {
        matches!(
            status.phase.as_str(),
            "RecoveryRequired" | "RecoveryCompleted" | "RecoveryFailed"
        )
    })
}

async fn stop_original_workload(
    client: &Client,
    namespace: &str,
    mount: &BrewFSWorkspaceMount,
) -> anyhow::Result<()> {
    let name = format!("{}-workspace", mount.name_any());
    let sets = Api::<StatefulSet>::namespaced(client.clone(), namespace);
    if let Some(set) = sets.get_opt(&name).await? {
        let mount_uid = mount.uid().ok_or_else(|| anyhow!("mount has no UID"))?;
        if !set
            .owner_references()
            .iter()
            .any(|owner| owner.kind == "BrewFSWorkspaceMount" && owner.uid == mount_uid)
        {
            bail!("mount StatefulSet ownership changed");
        }
        if set.spec.as_ref().and_then(|spec| spec.replicas) != Some(0) {
            let set_uid = set
                .uid()
                .ok_or_else(|| anyhow!("mount StatefulSet has no UID"))?;
            let set_version = set
                .resource_version()
                .ok_or_else(|| anyhow!("mount StatefulSet has no resourceVersion"))?;
            sets.patch(
                &name,
                &PatchParams::default(),
                &Patch::Merge(json!({
                    "metadata": {"uid":set_uid,"resourceVersion":set_version},
                    "spec": {"replicas":0}
                })),
            )
            .await?;
            bail!("waiting for original mount workload to stop");
        }
    }
    let pods = Api::<Pod>::namespaced(client.clone(), namespace)
        .list(
            &ListParams::default()
                .labels(&format!(
                    "app.kubernetes.io/name=brewfs-workspace,app.kubernetes.io/instance={}",
                    mount.name_any()
                ))
                .limit(64),
        )
        .await?;
    if pods
        .metadata
        .continue_
        .as_deref()
        .is_some_and(|value| !value.is_empty())
    {
        bail!("original mount Pod absence scan is incomplete");
    }
    if !pods.items.is_empty() {
        bail!("waiting for every original mount workload Pod to disappear");
    }
    // Also inspect the exact original Pod name; labels alone are not proof.
    if let Some(name) = mount
        .status
        .as_ref()
        .and_then(|status| status.pod_name.as_deref())
    {
        if Api::<Pod>::namespaced(client.clone(), namespace)
            .get_opt(name)
            .await?
            .is_some()
        {
            bail!("original mount Pod name remains present");
        }
    }
    Ok(())
}

fn original_reference(
    mount: &BrewFSWorkspaceMount,
) -> anyhow::Result<PackedReleasedMountReference> {
    let status = mount
        .status
        .as_ref()
        .ok_or_else(|| anyhow!("original mount status missing"))?;
    Ok(PackedReleasedMountReference {
        guard: HeadGuard {
            workspace_id: status
                .workspace_id
                .as_deref()
                .ok_or_else(|| anyhow!("original workspace missing"))?
                .parse()?,
            expected_head_layer_id: status
                .mounted_head_layer_id
                .as_deref()
                .ok_or_else(|| anyhow!("original head missing"))?
                .parse()?,
            expected_head_epoch: status
                .mounted_head_epoch
                .ok_or_else(|| anyhow!("original epoch missing"))?,
            lease_id: status
                .lease_id
                .as_deref()
                .ok_or_else(|| anyhow!("original lease missing"))?
                .parse()?,
            holder_generation: status
                .holder_generation
                .ok_or_else(|| anyhow!("original generation missing"))?,
        },
        mount_uid: Uuid::parse_str(
            &mount
                .uid()
                .ok_or_else(|| anyhow!("original mount UID missing"))?,
        )?,
        pod_uid: Uuid::parse_str(
            status
                .pod_uid
                .as_deref()
                .ok_or_else(|| anyhow!("original Pod UID missing"))?,
        )?,
    })
}

const ORIGINAL_INTENT: &str = "storage.brewfs.io/packed-original-session";
const RECOVERY_LEASE: &str = "storage.brewfs.io/packed-recovery-lease";
const RECOVERY_GENERATION: &str = "storage.brewfs.io/packed-recovery-generation";
const RECOVERY_RETRY_OF: &str = "storage.brewfs.io/packed-recovery-retry-of";

fn original_intent(original: &PackedReleasedMountReference) -> String {
    format!(
        "{}:{}:{}:{}:{}:{}:{}",
        original.guard.workspace_id,
        original.guard.expected_head_layer_id,
        original.guard.expected_head_epoch,
        original.guard.lease_id,
        original.guard.holder_generation,
        original.mount_uid,
        original.pod_uid
    )
}

fn recovery_job_name(
    mount: &BrewFSWorkspaceMount,
    original: &PackedReleasedMountReference,
    generation: u64,
) -> anyhow::Result<String> {
    let first = original
        .guard
        .holder_generation
        .checked_add(1)
        .ok_or_else(|| anyhow!("recovery generation overflow"))?;
    if generation == first {
        return Ok(format!("{}-recover", mount.name_any()));
    }
    if generation <= first {
        bail!("recovery successor generation is not strictly newer");
    }
    Ok(format!(
        "bfpr-{}",
        deterministic_uuid(
            original.mount_uid,
            &format!("packed-recovery-job-{generation}")
        )
        .simple()
    ))
}

fn recovery_lease_id(
    original: &PackedReleasedMountReference,
    generation: u64,
) -> anyhow::Result<LeaseId> {
    let first = original
        .guard
        .holder_generation
        .checked_add(1)
        .ok_or_else(|| anyhow!("recovery generation overflow"))?;
    let scope = *original.guard.lease_id.as_uuid();
    let intent = if generation == first {
        "packed-mounted-recovery".to_string()
    } else if generation > first {
        format!("packed-mounted-recovery-{generation}")
    } else {
        bail!("recovery successor generation is not strictly newer");
    };
    Ok(LeaseId::from_uuid(deterministic_uuid(scope, &intent)))
}

fn recovery_job_annotations(
    original: &PackedReleasedMountReference,
    pvc_uid: &str,
    lease: LeaseId,
    generation: u64,
) -> BTreeMap<String, String> {
    BTreeMap::from([
        (PVC_UID.into(), pvc_uid.into()),
        (ORIGINAL_INTENT.into(), original_intent(original)),
        (RECOVERY_LEASE.into(), lease.to_string()),
        (RECOVERY_GENERATION.into(), generation.to_string()),
    ])
}

fn verify_recovery_job_intent(
    job: &Job,
    mount: &BrewFSWorkspaceMount,
    expected: &BTreeMap<String, String>,
) -> anyhow::Result<()> {
    let mount_uid = mount
        .uid()
        .ok_or_else(|| anyhow!("mount UID unavailable"))?;
    if !job
        .owner_references()
        .iter()
        .any(|owner| owner.kind == "BrewFSWorkspaceMount" && owner.uid == mount_uid)
        || expected
            .iter()
            .any(|(key, value)| job.annotations().get(key) != Some(value))
    {
        bail!("recovery Job original session, owner or intent changed");
    }
    let spec = job
        .spec
        .as_ref()
        .ok_or_else(|| anyhow!("recovery Job spec unavailable"))?;
    if spec.backoff_limit != Some(0)
        || spec
            .template
            .spec
            .as_ref()
            .and_then(|spec| spec.restart_policy.as_deref())
            != Some("Never")
    {
        bail!("recovery Job can restart an old recovery owner");
    }
    Ok(())
}

fn unstarted_retry_route(job_uid: &str) -> anyhow::Result<(LeaseId, String)> {
    let scope = Uuid::parse_str(job_uid)?;
    if scope.is_nil() {
        bail!("failed recovery Job UID is nil");
    }
    Ok((
        LeaseId::from_uuid(deterministic_uuid(
            scope,
            "packed-unstarted-recovery-lease",
        )),
        format!(
            "bfpr-{}",
            deterministic_uuid(scope, "packed-unstarted-recovery-job").simple()
        ),
    ))
}

fn stably_failed_recovery_job(job: &Job) -> bool {
    job.metadata.deletion_timestamp.is_none()
        && job.status.as_ref().is_some_and(|status| {
            status.active.unwrap_or(0) == 0
                && status.conditions.as_ref().is_some_and(|conditions| {
                    conditions
                        .iter()
                        .any(|condition| condition.type_ == "Failed" && condition.status == "True")
                })
        })
}

// Each pre-CAS retry keeps its parent's generation and derives fresh IDs from
// the actual immutable Job UID. The bounded retained chain anchors at the
// ordinary deterministic generation Job; annotations alone are insufficient.
fn verify_recovery_job_route(
    job: &Job,
    all_jobs: &[Job],
    mount: &BrewFSWorkspaceMount,
    original: &PackedReleasedMountReference,
    pinned_uid: &str,
) -> anyhow::Result<(LeaseId, u64)> {
    let lease: LeaseId = job
        .annotations()
        .get(RECOVERY_LEASE)
        .ok_or_else(|| anyhow!("recovery Job lease intent missing"))?
        .parse()?;
    let generation: u64 = job
        .annotations()
        .get(RECOVERY_GENERATION)
        .ok_or_else(|| anyhow!("recovery Job generation intent missing"))?
        .parse()?;
    let mut current = job;
    let mut visited = BTreeSet::new();
    loop {
        let uid = current
            .uid()
            .ok_or_else(|| anyhow!("recovery attempt Job UID unavailable"))?;
        if !visited.insert(uid) || visited.len() > 256 {
            bail!("recovery attempt chain is invalid");
        }
        let current_lease: LeaseId = current
            .annotations()
            .get(RECOVERY_LEASE)
            .ok_or_else(|| anyhow!("recovery attempt lease intent missing"))?
            .parse()?;
        verify_recovery_job_intent(
            current,
            mount,
            &recovery_job_annotations(original, pinned_uid, current_lease, generation),
        )?;
        if let Some(parent_uid) = current.annotations().get(RECOVERY_RETRY_OF) {
            let (expected_lease, expected_name) = unstarted_retry_route(parent_uid)?;
            if current_lease != expected_lease || current.name_any() != expected_name {
                bail!("unstarted recovery attempt IDs differ from its pinned parent");
            }
            let mut parents = all_jobs
                .iter()
                .filter(|parent| parent.metadata.uid.as_deref() == Some(parent_uid.as_str()));
            let parent = parents
                .next()
                .ok_or_else(|| anyhow!("recovery retry parent Job evidence unavailable"))?;
            if parents.next().is_some() || !stably_failed_recovery_job(parent) {
                bail!("recovery retry parent is not an exact retained failed Job");
            }
            current = parent;
        } else {
            if current_lease != recovery_lease_id(original, generation)?
                || current.name_any() != recovery_job_name(mount, original, generation)?
            {
                bail!("recovery attempt chain has a different generation root");
            }
            return Ok((lease, generation));
        }
    }
}

async fn find_previous_recovery_job(
    jobs: &Api<Job>,
    mount: &BrewFSWorkspaceMount,
    original: &PackedReleasedMountReference,
    previous: &PackedReleasedMountReference,
    pinned_uid: &str,
) -> anyhow::Result<Job> {
    let listed = jobs.list(&ListParams::default().limit(256)).await?;
    if listed
        .metadata
        .continue_
        .as_deref()
        .is_some_and(|value| !value.is_empty())
    {
        bail!("previous recovery Job identity scan is incomplete");
    }
    let expected_lease = previous.guard.lease_id.to_string();
    let mut found = None;
    for job in &listed.items {
        if job.annotations().get(RECOVERY_LEASE) != Some(&expected_lease) {
            continue;
        }
        let (lease, generation) =
            verify_recovery_job_route(job, &listed.items, mount, original, pinned_uid)?;
        if lease != previous.guard.lease_id
            || generation != previous.guard.holder_generation
            || found.is_some()
        {
            bail!("previous authenticated recovery lease has ambiguous Kubernetes routing");
        }
        found = Some(job.clone());
    }
    found.ok_or_else(|| anyhow!("previous authenticated recovery Job evidence unavailable"))
}

async fn retire_unstarted_recovery_pods(
    client: &Client,
    namespace: &str,
    mount: &BrewFSWorkspaceMount,
    job: &Job,
    expected: &BTreeMap<String, String>,
) -> anyhow::Result<()> {
    if !stably_failed_recovery_job(job) {
        bail!("unstarted recovery Job is not stably failed");
    }
    let uid = job
        .uid()
        .ok_or_else(|| anyhow!("failed recovery Job UID unavailable"))?;
    let rv = job
        .resource_version()
        .ok_or_else(|| anyhow!("failed recovery Job resourceVersion unavailable"))?;
    let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
    let listed = pods.list(&ListParams::default().limit(256)).await?;
    if listed
        .metadata
        .continue_
        .as_deref()
        .is_some_and(|value| !value.is_empty())
    {
        bail!("unstarted recovery Pod absence scan is incomplete");
    }
    let mut retired = false;
    for pod in listed.items {
        if !pod
            .owner_references()
            .iter()
            .any(|owner| owner.kind == "Job" && owner.uid == uid)
        {
            continue;
        }
        retire_terminal_recovery_pod(client, namespace, mount, job, &pod, expected).await?;
        retired = true;
    }
    if retired {
        bail!("waiting for unstarted recovery Pods to disappear; status evidence is retained");
    }
    let current = Api::<Job>::namespaced(client.clone(), namespace)
        .get(&job.name_any())
        .await?;
    if current.uid().as_deref() != Some(uid.as_str())
        || current.resource_version().as_deref() != Some(rv.as_str())
    {
        bail!("unstarted recovery Job changed during its Pod absence scan");
    }
    Ok(())
}

struct UnstartedRecoveryContext<'a> {
    client: &'a Client,
    namespace: &'a str,
    mount: &'a BrewFSWorkspaceMount,
    original: &'a PackedReleasedMountReference,
    pinned_uid: &'a str,
    admin: &'a dyn WorkspaceAdmin,
}

async fn route_unstarted_recovery_attempt(
    context: UnstartedRecoveryContext<'_>,
    generation: u64,
    mut lease: LeaseId,
    mut name: String,
) -> anyhow::Result<(LeaseId, String, BTreeMap<String, String>)> {
    let UnstartedRecoveryContext {
        client,
        namespace,
        mount,
        original,
        pinned_uid,
        admin,
    } = context;
    let jobs: Api<Job> = Api::namespaced(client.clone(), namespace);
    let mut annotations = recovery_job_annotations(original, pinned_uid, lease, generation);
    for _ in 0..256 {
        let Some(job) = jobs.get_opt(&name).await? else {
            return Ok((lease, name, annotations));
        };
        verify_recovery_job_intent(&job, mount, &annotations)?;
        if job.annotations().get(RECOVERY_RETRY_OF) != annotations.get(RECOVERY_RETRY_OF) {
            bail!("recovery attempt parent intent changed");
        }
        if !stably_failed_recovery_job(&job) {
            bail!("waiting for actual packed recovery CLI Job and authenticated PMR");
        }
        let uid = job
            .uid()
            .ok_or_else(|| anyhow!("failed recovery Job UID unavailable"))?;
        let (next_lease, next_name) = unstarted_retry_route(&uid)?;
        if admin
            .unstarted_packed_mount_recovery(original.clone(), lease, next_lease)
            .await?
            != Some(generation)
        {
            bail!("failed recovery attempt acquired authority or original backend facts changed");
        }
        retire_unstarted_recovery_pods(client, namespace, mount, &job, &annotations).await?;
        // Re-authenticate after Kubernetes awaits. Only the real CLI's fresh
        // same-snapshot backend-clock CAS can create the next mutable owner.
        if admin
            .unstarted_packed_mount_recovery(original.clone(), lease, next_lease)
            .await?
            != Some(generation)
        {
            bail!("unstarted recovery backend facts changed during Pod retirement");
        }
        lease = next_lease;
        name = next_name;
        annotations = recovery_job_annotations(original, pinned_uid, lease, generation);
        annotations.insert(RECOVERY_RETRY_OF.into(), uid);
    }
    bail!("recovery attempt routing chain exceeds its bound");
}

async fn retire_terminal_recovery_pod(
    client: &Client,
    namespace: &str,
    mount: &BrewFSWorkspaceMount,
    job: &Job,
    pod: &Pod,
    expected: &BTreeMap<String, String>,
) -> anyhow::Result<()> {
    let phase = pod
        .status
        .as_ref()
        .and_then(|status| status.phase.as_deref());
    if !matches!(phase, Some("Failed" | "Succeeded")) {
        bail!("recovery Pod is still running; original writeback media is retained");
    }
    let job_uid = job
        .uid()
        .ok_or_else(|| anyhow!("recovery Job UID unavailable"))?;
    let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
    let pod_uid = pod
        .uid()
        .ok_or_else(|| anyhow!("previous recovery Pod UID unavailable"))?;
    let pod_version = pod
        .resource_version()
        .ok_or_else(|| anyhow!("previous recovery Pod resourceVersion unavailable"))?;
    let mut evidence_annotations = (*expected).clone();
    evidence_annotations.insert(
        "storage.brewfs.io/packed-recovery-job-uid".into(),
        job_uid.clone(),
    );
    evidence_annotations.insert(
        "storage.brewfs.io/packed-recovery-pod-uid".into(),
        pod_uid.clone(),
    );
    let evidence_name = format!(
        "bfpre-{}",
        deterministic_uuid(
            Uuid::parse_str(&pod_uid)?,
            "packed-failed-recovery-evidence"
        )
        .simple()
    );
    let evidence = serde_json::to_string(&json!({
        "job_uid": &job_uid, "pod_uid": &pod_uid, "pod_name": pod.name_any(),
        "job_status": &job.status, "pod_status": &pod.status,
    }))?;
    if evidence.len() > 128 * 1024 {
        bail!("previous recovery status evidence exceeds its bound");
    }
    let configs: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
    if let Some(saved) = configs.get_opt(&evidence_name).await? {
        let mount_uid = mount
            .uid()
            .ok_or_else(|| anyhow!("mount UID unavailable"))?;
        if !saved
            .owner_references()
            .iter()
            .any(|owner| owner.kind == "BrewFSWorkspaceMount" && owner.uid == mount_uid)
            || evidence_annotations
                .iter()
                .any(|(key, value)| saved.annotations().get(key) != Some(value))
        {
            bail!("previous recovery status evidence identity changed");
        }
        let bytes = saved
            .data
            .as_ref()
            .and_then(|data| data.get("recovery-evidence.json"))
            .filter(|bytes| bytes.len() <= 128 * 1024)
            .ok_or_else(|| anyhow!("previous recovery status evidence unavailable"))?;
        let recorded: serde_json::Value = serde_json::from_str(bytes)?;
        if recorded.get("job_uid").and_then(|value| value.as_str()) != Some(job_uid.as_str())
            || recorded.get("pod_uid").and_then(|value| value.as_str()) != Some(pod_uid.as_str())
        {
            bail!("previous recovery status evidence refers to another owner");
        }
    } else {
        let owner = mount
            .controller_owner_ref(&())
            .ok_or_else(|| anyhow!("mount owner missing"))?;
        configs
            .create(
                &PostParams::default(),
                &ConfigMap {
                    metadata: ObjectMeta {
                        name: Some(evidence_name),
                        owner_references: Some(vec![owner]),
                        annotations: Some(evidence_annotations),
                        ..Default::default()
                    },
                    data: Some(BTreeMap::from([(
                        "recovery-evidence.json".into(),
                        evidence,
                    )])),
                    ..Default::default()
                },
            )
            .await?;
    }
    pods.delete(
        &pod.name_any(),
        &DeleteParams {
            preconditions: Some(Preconditions {
                uid: Some(pod_uid),
                resource_version: Some(pod_version),
            }),
            ..DeleteParams::default()
        },
    )
    .await?;
    Ok(())
}

async fn retire_failed_recovery_pod(
    client: &Client,
    namespace: &str,
    mount: &BrewFSWorkspaceMount,
    original: &PackedReleasedMountReference,
    previous: &PackedReleasedMountReference,
    pinned_uid: &str,
) -> anyhow::Result<()> {
    let jobs: Api<Job> = Api::namespaced(client.clone(), namespace);
    let job = find_previous_recovery_job(&jobs, mount, original, previous, pinned_uid).await?;
    let name = job.name_any();
    let expected = recovery_job_annotations(
        original,
        pinned_uid,
        previous.guard.lease_id,
        previous.guard.holder_generation,
    );
    verify_recovery_job_intent(&job, mount, &expected)?;
    let failed = job.status.as_ref().is_some_and(|status| {
        status.active.unwrap_or(0) == 0
            && status.conditions.as_ref().is_some_and(|conditions| {
                conditions
                    .iter()
                    .any(|condition| condition.type_ == "Failed" && condition.status == "True")
            })
    });
    if !failed || job.metadata.deletion_timestamp.is_some() {
        bail!("previous authenticated recovery Job is not stably failed");
    }
    let job_uid = job
        .uid()
        .ok_or_else(|| anyhow!("previous recovery Job UID unavailable"))?;
    let job_version = job
        .resource_version()
        .ok_or_else(|| anyhow!("previous recovery Job resourceVersion unavailable"))?;
    let previous_pod_uid = previous.pod_uid.to_string();
    let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
    // Full bounded namespace scan also finds the exact old Pod if labels drift.
    // Pagination is rejected, never interpreted as absence.
    let listed = pods.list(&ListParams::default().limit(256)).await?;
    if listed
        .metadata
        .continue_
        .as_deref()
        .is_some_and(|value| !value.is_empty())
    {
        bail!("previous recovery Pod absence scan is incomplete");
    }
    let mut retired = false;
    for pod in listed.items {
        let owned = pod
            .owner_references()
            .iter()
            .any(|owner| owner.kind == "Job" && owner.uid == job_uid);
        let exact = pod.uid().as_deref() == Some(previous_pod_uid.as_str());
        if !owned && !exact {
            continue;
        }
        if !owned || !exact {
            bail!("previous recovery Job Pod differs from the authenticated PMR owner");
        }
        let phase = pod
            .status
            .as_ref()
            .and_then(|status| status.phase.as_deref());
        if !matches!(phase, Some("Failed" | "Succeeded")) {
            bail!("previous recovery Pod has not terminated");
        }
        retire_terminal_recovery_pod(client, namespace, mount, &job, &pod, &expected).await?;
        retired = true;
    }
    if retired {
        bail!("waiting for previous failed recovery Pod deletion; status evidence is retained");
    }
    let current = Api::<Job>::namespaced(client.clone(), namespace)
        .get(&name)
        .await?;
    if current.uid().as_deref() != Some(job_uid.as_str())
        || current.resource_version().as_deref() != Some(job_version.as_str())
    {
        bail!("previous recovery Job changed during its Pod absence scan");
    }
    // The old terminal Job and evidence remain. A fresh Pod/lease/generation
    // still needs the production driver's backend-clock CAS to take authority.
    Ok(())
}

async fn finish_proven_mount_cleanup(
    client: &Client,
    namespace: &str,
    mount: &BrewFSWorkspaceMount,
    exact_recovery_pod: Option<Uuid>,
) -> anyhow::Result<()> {
    let mount_uid = mount
        .uid()
        .ok_or_else(|| anyhow!("mount UID unavailable"))?;
    let jobs: Api<Job> = Api::namespaced(client.clone(), namespace);
    let listed_jobs = jobs.list(&ListParams::default().limit(256)).await?;
    if listed_jobs
        .metadata
        .continue_
        .as_deref()
        .is_some_and(|value| !value.is_empty())
    {
        bail!("recovery Job cleanup scan is incomplete");
    }
    let owned_jobs: Vec<_> = listed_jobs
        .items
        .into_iter()
        .filter(|job| {
            job.owner_references()
                .iter()
                .any(|owner| owner.kind == "BrewFSWorkspaceMount" && owner.uid == mount_uid)
        })
        .collect();
    for job in &owned_jobs {
        job.uid()
            .ok_or_else(|| anyhow!("owned recovery Job UID unavailable"))?;
        job.resource_version()
            .ok_or_else(|| anyhow!("owned recovery Job resourceVersion unavailable"))?;
        let original = original_reference(mount)?;
        let pinned_uid = mount.annotations().get(PVC_UID).ok_or_else(|| {
            anyhow!("original PVC UID pin unavailable during recovery Job cleanup")
        })?;
        let lease: LeaseId = job
            .annotations()
            .get(RECOVERY_LEASE)
            .ok_or_else(|| anyhow!("recovery Job lease intent unavailable"))?
            .parse()?;
        let generation: u64 = job
            .annotations()
            .get(RECOVERY_GENERATION)
            .ok_or_else(|| anyhow!("recovery Job generation intent unavailable"))?
            .parse()?;
        if verify_recovery_job_route(job, &owned_jobs, mount, &original, pinned_uid)?
            != (lease, generation)
        {
            bail!("recovery Job attempt route differs from this original mounted session");
        }
        let expected = recovery_job_annotations(&original, pinned_uid, lease, generation);
        verify_recovery_job_intent(job, mount, &expected)?;
        let terminal = job.status.as_ref().is_some_and(|status| {
            status.active.unwrap_or(0) == 0
                && status.conditions.as_ref().is_some_and(|conditions| {
                    conditions.iter().any(|condition| {
                        matches!(condition.type_.as_str(), "Failed" | "Complete")
                            && condition.status == "True"
                    })
                })
        });
        if !terminal {
            bail!(
                "waiting for every owned recovery Job to terminate before original media cleanup"
            );
        }
    }
    let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
    let listed_pods = pods.list(&ListParams::default().limit(256)).await?;
    if listed_pods
        .metadata
        .continue_
        .as_deref()
        .is_some_and(|value| !value.is_empty())
    {
        bail!("recovery Pod cleanup absence scan is incomplete");
    }
    let exact_uid = exact_recovery_pod.map(|uid| uid.to_string());
    let mut retired = false;
    for pod in listed_pods.items {
        let owned_job = owned_jobs.iter().find(|job| {
            pod.owner_references().iter().any(|owner| {
                owner.kind == "Job" && Some(owner.uid.as_str()) == job.metadata.uid.as_deref()
            })
        });
        let exact = exact_uid
            .as_deref()
            .is_some_and(|uid| pod.metadata.uid.as_deref() == Some(uid));
        match (owned_job, exact) {
            (None, false) => continue,
            (None, true) => bail!("completed PMR Pod remains with changed recovery Job ownership"),
            (Some(job), _) => {
                let mut expected = BTreeMap::new();
                for key in [
                    PVC_UID,
                    ORIGINAL_INTENT,
                    RECOVERY_LEASE,
                    RECOVERY_GENERATION,
                ] {
                    let value = job.annotations().get(key).ok_or_else(|| {
                        anyhow!("owned recovery Job original intent pin is unavailable")
                    })?;
                    expected.insert(key.to_string(), value.clone());
                }
                verify_recovery_job_intent(job, mount, &expected)?;
                retire_terminal_recovery_pod(client, namespace, mount, job, &pod, &expected)
                    .await?;
                retired = true;
            }
        }
    }
    if retired {
        bail!("waiting for every recovery Pod to disappear before original media cleanup");
    }
    // Detect same-name replacement or rescheduling during the absence scan.
    // Terminal Jobs and bounded status evidence stay available for review.
    for observed in &owned_jobs {
        let current = jobs.get(&observed.name_any()).await?;
        if current.metadata.uid != observed.metadata.uid
            || current.metadata.resource_version != observed.metadata.resource_version
        {
            bail!("recovery Job changed during its Pod absence scan");
        }
    }
    delete_workspace_mount_workload(client, namespace, mount).await
}

pub async fn finish_mount_workload(
    client: &Client,
    namespace: &str,
    mount: &BrewFSWorkspaceMount,
) -> anyhow::Result<()> {
    // Do not invoke the old cleanup helper until genuine backend proof exists:
    // it deletes the only writeback PVC and original configuration.
    stop_original_workload(client, namespace, mount).await?;
    let workspace = Api::<BrewFSWorkspace>::namespaced(client.clone(), namespace)
        .get(&mount.spec.workspace_ref.name)
        .await?;
    let cluster = Api::<BrewFSCluster>::namespaced(client.clone(), namespace)
        .get(&mount.spec.cluster_ref.name)
        .await?;
    if workspace.spec.cluster_ref.name != mount.spec.cluster_ref.name {
        bail!("mount and workspace cluster identity differs");
    }
    let spec = cluster
        .spec
        .workspace
        .as_ref()
        .filter(|spec| spec.enabled)
        .ok_or_else(|| anyhow!("workspace catalog disabled"))?;
    spec.validate().map_err(|error| anyhow!(error))?;
    let admin = connect_workspace_admin_for_cluster(client, &cluster, namespace, spec).await?;
    let workspace_id: WorkspaceId = workspace
        .status
        .as_ref()
        .and_then(|status| status.workspace_id.as_deref())
        .ok_or_else(|| anyhow!("workspace backend identity missing"))?
        .parse()?;
    let pvc_name = format!("{}-workspace-cache", mount.name_any());
    let current = Api::<BrewFSWorkspaceMount>::namespaced(client.clone(), namespace)
        .get(&mount.name_any())
        .await?;
    if current.uid() != mount.uid() {
        bail!("mount identity changed before release or recovery");
    }
    let pinned_uid = current.annotations().get(PVC_UID);
    let mut recovery_media_present = false;
    if mount.spec.cache.mode == WorkspaceCacheMode::WriteBack {
        let pvc = Api::<PersistentVolumeClaim>::namespaced(client.clone(), namespace)
            .get_opt(&pvc_name)
            .await?;
        match (pvc, pinned_uid) {
            (Some(pvc), Some(expected)) if pvc.uid().as_deref() == Some(expected.as_str()) => {
                recovery_media_present = true;
            }
            // A prior authenticated finalization may already have removed it.
            // Backend proof is still mandatory below; no recovery Job can use
            // this branch as permission to recover from replacement media.
            (None, _) => {}
            _ => bail!("original writeback PVC identity unavailable or changed before release"),
        }
    }
    if admin.packed_binding(workspace_id).await?.is_none() {
        admin.reap_expired_leases().await?;
        if admin
            .inspect_workspace(workspace_id)
            .await?
            .leases
            .iter()
            .any(|lease| lease.state == LeaseState::Active)
        {
            bail!("native mount lease remains active");
        }
        return finish_proven_mount_cleanup(client, namespace, mount, None).await;
    }
    // A pristine current head proves only a never-attached mount. Once any
    // original session identity is recorded, require its exact PCR/PMR below;
    // a later publication must not certify cleanup of older writeback media.
    let has_original_session = mount.status.as_ref().is_some_and(|status| {
        status.pod_uid.is_some()
            || status.mounted_head_layer_id.is_some()
            || status.mounted_head_epoch.is_some()
            || status.lease_id.is_some()
            || status.holder_generation.is_some()
    });
    if !has_original_session
        && admin
            .clean_unmounted_packed_epoch(workspace_id)
            .await?
            .is_some()
    {
        return finish_proven_mount_cleanup(client, namespace, mount, None).await;
    }
    let original = original_reference(mount)?;
    if original.guard.workspace_id != workspace_id {
        bail!("original recovery reference belongs to another workspace");
    }
    if admin
        .original_packed_mount_for_cleanup(original.clone())
        .await?
    {
        return finish_proven_mount_cleanup(client, namespace, mount, None).await;
    }
    if let Some(report) = admin
        .packed_mount_recovery_for_cleanup(original.clone())
        .await?
    {
        if report.original != original {
            bail!("completed recovered mount belongs to a different original session");
        }
        return finish_proven_mount_cleanup(
            client,
            namespace,
            mount,
            Some(report.released.pod_uid),
        )
        .await;
    }
    if mount.spec.cache.mode != WorkspaceCacheMode::WriteBack {
        bail!("packed mount recovery requires its original persistent writeback media");
    }
    if !recovery_media_present {
        bail!("packed mount recovery original media is unavailable");
    }
    let pinned_uid = pinned_uid
        .ok_or_else(|| anyhow!("original writeback PVC UID was never pinned before mount"))?;
    let (recovery_generation, recovery_lease, job_name) = if let Some((previous, next_generation)) =
        admin
            .expired_packed_mount_recovery(original.clone())
            .await?
    {
        if previous.guard.workspace_id != original.guard.workspace_id
            || previous.guard.expected_head_layer_id != original.guard.expected_head_layer_id
            || previous.guard.expected_head_epoch != original.guard.expected_head_epoch
            || previous.mount_uid != original.mount_uid
            || previous.guard.holder_generation <= original.guard.holder_generation
            || previous.guard.holder_generation.checked_add(1) != Some(next_generation)
        {
            bail!("expired recovery routing report differs from this original Job intent");
        }
        retire_failed_recovery_pod(client, namespace, mount, &original, &previous, pinned_uid)
            .await?;
        (
            next_generation,
            recovery_lease_id(&original, next_generation)?,
            recovery_job_name(mount, &original, next_generation)?,
        )
    } else {
        let first = original
            .guard
            .holder_generation
            .checked_add(1)
            .ok_or_else(|| anyhow!("recovery generation overflow"))?;
        (
            first,
            recovery_lease_id(&original, first)?,
            recovery_job_name(mount, &original, first)?,
        )
    };
    let (recovery_lease, job_name, annotations) = route_unstarted_recovery_attempt(
        UnstartedRecoveryContext {
            client,
            namespace,
            mount,
            original: &original,
            pinned_uid,
            admin: admin.as_ref(),
        },
        recovery_generation,
        recovery_lease,
        job_name,
    )
    .await?;
    let jobs: Api<Job> = Api::namespaced(client.clone(), namespace);
    let mut args = vec!["workspace".into(), "recover-packed-mount".into()];
    for (flag, value) in [
        ("--config", "/run/brewfs/config.yaml".into()),
        ("--workspace", workspace_id.to_string()),
        (
            "--head-layer",
            original.guard.expected_head_layer_id.to_string(),
        ),
        (
            "--head-epoch",
            original.guard.expected_head_epoch.to_string(),
        ),
        ("--original-lease", original.guard.lease_id.to_string()),
        (
            "--original-generation",
            original.guard.holder_generation.to_string(),
        ),
        ("--mount-uid", original.mount_uid.to_string()),
        ("--original-pod-uid", original.pod_uid.to_string()),
        ("--recovery-lease", recovery_lease.to_string()),
        ("--recovery-generation", recovery_generation.to_string()),
        ("--recovery-pod-uid", "$(RECOVERY_POD_UID)".into()),
        ("--ttl-seconds", spec.lease_ttl_seconds.to_string()),
    ] {
        args.extend([flag.into(), value]);
    }
    let mut env = vec![EnvVar {
        name: "RECOVERY_POD_UID".into(),
        value_from: Some(EnvVarSource {
            field_ref: Some(ObjectFieldSelector {
                field_path: "metadata.uid".into(),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }];
    // Recovery replays owned read/create-only flush/publication work. It does
    // not receive the operator's deletion/admin object principal.
    let (runtime_object_secret, _) = spec.object_secret_names().map_err(anyhow::Error::msg)?;
    for (name, key) in [
        ("AWS_ACCESS_KEY_ID", "accessKey"),
        ("AWS_SECRET_ACCESS_KEY", "secretKey"),
    ] {
        env.push(EnvVar {
            name: name.into(),
            value_from: Some(EnvVarSource {
                secret_key_ref: Some(SecretKeySelector {
                    name: runtime_object_secret.to_owned(),
                    key: key.into(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        });
    }
    env.push(EnvVar {
        name: "AWS_DEFAULT_REGION".into(),
        value: Some(cluster.spec.rustfs.region.clone()),
        ..Default::default()
    });
    // This delay only schedules the attempt after the old Pod is gone. The
    // actual CLI still authenticates expiration with Redis/TiKV server time;
    // elapsed wall time never grants takeover authority.
    let delay = u64::from(spec.lease_ttl_seconds) + 1;
    env.extend(super::workload::runtime_metadata_env(&cluster)?);
    let command = if spec.catalog_backend == WorkspaceCatalogBackend::Redis {
        vec!["/bin/sh".into(), "-ec".into(), format!(
            "sleep {delay}; exec /usr/local/bin/brewfs \"$@\" --meta-url \"redis://${{REDIS_USERNAME}}:${{REDIS_PASSWORD}}@{}-workspace-redis:{}/\"",
            cluster.name_any(), cluster.spec.redis.port,
        ), "packed-recovery".into()]
    } else {
        vec![
            "/bin/sh".into(),
            "-ec".into(),
            format!("sleep {delay}; exec /usr/local/bin/brewfs \"$@\""),
            "packed-recovery".into(),
        ]
    };
    let owner = mount
        .controller_owner_ref(&())
        .ok_or_else(|| anyhow!("mount owner missing"))?;
    let mut volume_mounts = vec![
        VolumeMount {
            name: "original-writeback".into(),
            mount_path: "/var/lib/brewfs/cache".into(),
            ..Default::default()
        },
        VolumeMount {
            name: "original-config".into(),
            mount_path: "/run/brewfs/config.yaml".into(),
            sub_path: Some("config.yaml".into()),
            read_only: Some(true),
            ..Default::default()
        },
    ];
    let mut volumes = vec![
        Volume {
            name: "original-writeback".into(),
            persistent_volume_claim: Some(PersistentVolumeClaimVolumeSource {
                claim_name: pvc_name,
                read_only: Some(false),
            }),
            ..Default::default()
        },
        Volume {
            name: "original-config".into(),
            config_map: Some(ConfigMapVolumeSource {
                name: format!("{}-workspace-config", mount.name_any()),
                ..Default::default()
            }),
            ..Default::default()
        },
    ];
    if let Some((volume, mount)) = super::workload::runtime_metadata_volume(&cluster)? {
        volumes.push(volume);
        volume_mounts.push(mount);
    }
    let job = Job {
        metadata: ObjectMeta {
            name: Some(job_name.clone()),
            owner_references: Some(vec![owner]),
            annotations: Some(annotations.clone()),
            ..Default::default()
        },
        spec: Some(JobSpec {
            backoff_limit: Some(0),
            active_deadline_seconds: Some(i64::from(spec.lease_ttl_seconds) + 300),
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    annotations: Some(annotations),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    restart_policy: Some("Never".into()),
                    automount_service_account_token: Some(false),
                    containers: vec![Container {
                        name: "packed-recovery".into(),
                        image: Some(mount.spec.image.clone()),
                        image_pull_policy: Some(mount.spec.image_pull_policy.clone()),
                        command: Some(command),
                        args: Some(args),
                        env: Some(env),
                        volume_mounts: Some(volume_mounts),
                        ..Default::default()
                    }],
                    volumes: Some(volumes),
                    ..Default::default()
                }),
            },
            ..Default::default()
        }),
        ..Default::default()
    };
    jobs.create(&PostParams::default(), &job)
        .await
        .context("create genuine packed recovery CLI Job")?;
    bail!("waiting for original same-PVC packed recovery and authenticated PMR");
}

#[cfg(test)]
mod unstarted_retry_tests {
    use super::*;
    use brewfs::workspace::ids::LayerId;

    fn fixture() -> (BrewFSWorkspaceMount, PackedReleasedMountReference) {
        let mut mount = BrewFSWorkspaceMount::new(
            "retry-test",
            serde_json::from_value(json!({
                "clusterRef":{"name":"cluster"}, "workspaceRef":{"name":"workspace"}
            }))
            .unwrap(),
        );
        let mount_uid = Uuid::new_v4();
        mount.metadata.uid = Some(mount_uid.to_string());
        let original = PackedReleasedMountReference {
            guard: HeadGuard {
                workspace_id: WorkspaceId::new(),
                expected_head_layer_id: LayerId::new(),
                expected_head_epoch: 1,
                lease_id: LeaseId::new(),
                holder_generation: 7,
            },
            mount_uid,
            pod_uid: Uuid::new_v4(),
        };
        (mount, original)
    }

    fn failed_job(
        mount: &BrewFSWorkspaceMount,
        name: String,
        annotations: BTreeMap<String, String>,
    ) -> Job {
        Job {
            metadata: ObjectMeta {
                name: Some(name),
                uid: Some(Uuid::new_v4().to_string()),
                resource_version: Some("1".into()),
                annotations: Some(annotations),
                owner_references: Some(vec![mount.controller_owner_ref(&()).unwrap()]),
                ..Default::default()
            },
            spec: Some(JobSpec {
                backoff_limit: Some(0),
                template: PodTemplateSpec {
                    spec: Some(PodSpec {
                        restart_policy: Some("Never".into()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                ..Default::default()
            }),
            status: Some(
                serde_json::from_value(
                    json!({"active":0,"conditions":[{"type":"Failed","status":"True"}]}),
                )
                .unwrap(),
            ),
        }
    }

    fn retry_job(
        mount: &BrewFSWorkspaceMount,
        original: &PackedReleasedMountReference,
        parent: &Job,
    ) -> Job {
        let uid = parent.uid().unwrap();
        let (lease, name) = unstarted_retry_route(&uid).unwrap();
        let mut annotations = recovery_job_annotations(original, "pvc-original", lease, 8);
        annotations.insert(RECOVERY_RETRY_OF.into(), uid);
        failed_job(mount, name, annotations)
    }

    #[test]
    fn pre_cas_retry_chain_keeps_generation_and_rejects_missing_or_live_parent() {
        let (mount, original) = fixture();
        let first_lease = recovery_lease_id(&original, 8).unwrap();
        let root = failed_job(
            &mount,
            recovery_job_name(&mount, &original, 8).unwrap(),
            recovery_job_annotations(&original, "pvc-original", first_lease, 8),
        );
        let second = retry_job(&mount, &original, &root);
        let third = retry_job(&mount, &original, &second);
        let all = vec![root.clone(), second.clone(), third.clone()];
        let (lease, generation) =
            verify_recovery_job_route(&third, &all, &mount, &original, "pvc-original").unwrap();
        assert_eq!(generation, original.guard.holder_generation + 1);
        assert_ne!(lease, first_lease);
        assert_ne!(lease.to_string(), second.annotations()[RECOVERY_LEASE]);
        assert!(verify_recovery_job_route(
            &third,
            &[second.clone(), third.clone()],
            &mount,
            &original,
            "pvc-original"
        )
        .is_err());
        let mut changed = all.clone();
        changed[0].status.as_mut().unwrap().active = Some(1);
        assert!(
            verify_recovery_job_route(&third, &changed, &mount, &original, "pvc-original").is_err()
        );
        let mut changed = all;
        changed[0]
            .metadata
            .annotations
            .as_mut()
            .unwrap()
            .insert(RECOVERY_GENERATION.into(), "9".into());
        assert!(
            verify_recovery_job_route(&third, &changed, &mount, &original, "pvc-original").is_err()
        );
    }

    #[test]
    fn pre_cas_retry_rejects_wrong_original_media_or_parent_derived_ids() {
        let (mount, original) = fixture();
        let root = failed_job(
            &mount,
            recovery_job_name(&mount, &original, 8).unwrap(),
            recovery_job_annotations(
                &original,
                "pvc-original",
                recovery_lease_id(&original, 8).unwrap(),
                8,
            ),
        );
        let retry = retry_job(&mount, &original, &root);
        assert!(verify_recovery_job_route(
            &retry,
            &[root.clone(), retry.clone()],
            &mount,
            &original,
            "pvc-replacement"
        )
        .is_err());
        let mut changed = retry.clone();
        changed
            .metadata
            .annotations
            .as_mut()
            .unwrap()
            .insert(RECOVERY_LEASE.into(), LeaseId::new().to_string());
        assert!(verify_recovery_job_route(
            &changed,
            &[root, changed.clone()],
            &mount,
            &original,
            "pvc-original"
        )
        .is_err());
    }
}
