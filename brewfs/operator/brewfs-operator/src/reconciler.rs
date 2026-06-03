use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context as _};
use chrono::{DateTime, Utc};
use k8s_openapi::api::apps::v1::{
    DaemonSet, DaemonSetSpec, Deployment, DeploymentSpec, StatefulSet, StatefulSetSpec,
};
use k8s_openapi::api::batch::v1::{Job, JobSpec};
use k8s_openapi::api::core::v1::{
    Capabilities, ConfigMap, ConfigMapEnvSource, Container, ContainerPort, EmptyDirVolumeSource,
    EnvFromSource, EnvVar, EnvVarSource, ExecAction, HTTPGetAction, HTTPHeader, Lifecycle,
    LifecycleHandler, LocalObjectReference, PersistentVolumeClaim, PersistentVolumeClaimSpec,
    PodSecurityContext, PodSpec, PodTemplateSpec, Probe, ResourceRequirements, Secret,
    SecretEnvSource, SecretKeySelector, SecurityContext, Service, ServicePort, ServiceSpec,
    TCPSocketAction, Toleration, Volume, VolumeMount,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta, OwnerReference};
use kube::api::{Api, DeleteParams, Patch, PatchParams};
use kube::runtime::controller::Action;
use kube::Resource;
use kube::ResourceExt;
use serde::de::DeserializeOwned;
use serde_json::json;
use thiserror::Error;

use crate::crd::{
    BrewFSCluster, BrewFSClusterStatus, BrewFSMount, BrewFSMountStatus, ConsumerContainerSpec,
    ConsumerEnvFromSpec, ConsumerHTTPGetActionSpec, ConsumerLifecycleSpec,
    ConsumerPodSecurityContextSpec, ConsumerPortSpec, ConsumerProbeSpec,
    ConsumerResourceRequirementsSpec, ConsumerSecurityContextSpec, ConsumerTCPSocketActionSpec,
    ConsumerVolumeMountSpec, ConsumerVolumeSpec, ConsumerWorkloadKind, MountConsumerSpec,
    MountPropagationMode, MountToleration, MountWorkloadKind,
};

#[derive(Clone)]
pub struct OperatorContext {
    pub client: kube::Client,
}

#[derive(Debug, Error)]
pub enum ReconcileError {
    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
}

pub async fn reconcile_cluster(
    cluster: Arc<BrewFSCluster>,
    ctx: Arc<OperatorContext>,
) -> Result<Action, ReconcileError> {
    let client = ctx.client.clone();
    let namespace = cluster
        .namespace()
        .ok_or_else(|| anyhow!("BrewFSCluster must be namespaced"))?;
    let owner = cluster
        .controller_owner_ref(&())
        .ok_or_else(|| anyhow!("failed to build owner reference"))?;

    apply_rustfs_secret(&client, &namespace, &cluster, &owner).await?;
    apply_rustfs_pvc(&client, &namespace, &cluster, &owner).await?;
    apply_redis_service(&client, &namespace, &cluster, &owner).await?;
    apply_redis_deployment(&client, &namespace, &cluster, &owner).await?;
    apply_rustfs_service(&client, &namespace, &cluster, &owner).await?;
    apply_rustfs_deployment(&client, &namespace, &cluster, &owner).await?;
    apply_rustfs_init_job(&client, &namespace, &cluster, &owner).await?;
    apply_brewfs_config(&client, &namespace, &cluster, &owner).await?;
    patch_cluster_status(&client, &namespace, &cluster).await?;

    Ok(Action::requeue(Duration::from_secs(300)))
}

pub async fn reconcile_mount(
    mount: Arc<BrewFSMount>,
    ctx: Arc<OperatorContext>,
) -> Result<Action, ReconcileError> {
    let client = ctx.client.clone();
    let namespace = mount
        .namespace()
        .ok_or_else(|| anyhow!("BrewFSMount must be namespaced"))?;
    let owner = mount
        .controller_owner_ref(&())
        .ok_or_else(|| anyhow!("failed to build owner reference"))?;
    let cluster_name = mount.spec.cluster_ref.name.clone();
    let workload_name = mount_workload_name(&mount.name_any());
    let default_consumer_workload_name = consumer_workload_name(&mount.name_any());
    let default_consumer_service_name = consumer_headless_service_name(&mount.name_any());
    let cluster_api: Api<BrewFSCluster> = Api::namespaced(client.clone(), &namespace);
    let config_api: Api<ConfigMap> = Api::namespaced(client.clone(), &namespace);

    let Some(cluster) = cluster_api
        .get_opt(&cluster_name)
        .await
        .with_context(|| format!("load BrewFSCluster {cluster_name}"))?
    else {
        patch_mount_status(
            &client,
            &namespace,
            &mount,
            "Pending",
            &format!("waiting for BrewFSCluster {cluster_name}"),
            None,
            mount.spec.host_mount_path.clone(),
            Some(mount.spec.mount_propagation.clone()),
            Some(mount.spec.workload_kind.clone()),
            Some(workload_name),
            None,
            None,
            mount
                .spec
                .consumer
                .as_ref()
                .map(|consumer| consumer.workload_kind.clone()),
            Some(default_consumer_workload_name.clone()),
            mount
                .spec
                .consumer
                .as_ref()
                .map(|consumer| consumer.mount_path.clone()),
            None,
            None,
        )
        .await?;
        return Ok(Action::requeue(Duration::from_secs(15)));
    };

    let config_map_name = cluster
        .status
        .as_ref()
        .and_then(|status| status.config_map.clone())
        .unwrap_or_else(|| cluster_config_map_name(&cluster.name_any()));

    if config_api
        .get_opt(&config_map_name)
        .await
        .with_context(|| format!("load ConfigMap {config_map_name}"))?
        .is_none()
    {
        patch_mount_status(
            &client,
            &namespace,
            &mount,
            "Pending",
            &format!("waiting for ConfigMap {config_map_name}"),
            Some(config_map_name),
            mount.spec.host_mount_path.clone(),
            Some(mount.spec.mount_propagation.clone()),
            Some(mount.spec.workload_kind.clone()),
            Some(workload_name),
            None,
            None,
            mount
                .spec
                .consumer
                .as_ref()
                .map(|consumer| consumer.workload_kind.clone()),
            Some(default_consumer_workload_name.clone()),
            mount
                .spec
                .consumer
                .as_ref()
                .map(|consumer| consumer.mount_path.clone()),
            None,
            None,
        )
        .await?;
        return Ok(Action::requeue(Duration::from_secs(15)));
    }

    let workload_name = apply_mount_workload(
        &client,
        &namespace,
        &mount,
        &cluster,
        &config_map_name,
        &owner,
    )
    .await?;
    let (desired_replicas, ready_replicas) =
        get_mount_workload_status(&client, &namespace, &mount, &workload_name).await?;
    let phase = if ready_replicas.unwrap_or_default() >= desired_replicas.unwrap_or_default() {
        "Ready"
    } else {
        "Progressing"
    };
    let mut phase = phase.to_string();
    let mut message = if phase == "Ready" {
        format!("mount workload {workload_name} is ready")
    } else {
        format!("mount workload {workload_name} is reconciling")
    };

    let mut consumer_workload_kind = None;
    let mut consumer_workload_name_status = None;
    let mut consumer_mount_path = None;
    let mut consumer_desired_replicas = None;
    let mut consumer_ready_replicas = None;

    if let Some(consumer) = &mount.spec.consumer {
        consumer_workload_kind = Some(consumer.workload_kind.clone());
        consumer_workload_name_status = Some(consumer_workload_name(&mount.name_any()));
        consumer_mount_path = Some(consumer.mount_path.clone());

        if let Some(host_mount_path) = &mount.spec.host_mount_path {
            let consumer_name = apply_consumer_workload(
                &client,
                &namespace,
                &mount,
                consumer,
                host_mount_path,
                &owner,
            )
            .await?;
            let (desired, ready) =
                get_consumer_workload_status(&client, &namespace, consumer, &consumer_name).await?;
            consumer_workload_name_status = Some(consumer_name.clone());
            consumer_desired_replicas = desired;
            consumer_ready_replicas = ready;

            if ready.unwrap_or_default() < desired.unwrap_or_default() {
                phase = "Progressing".to_string();
                message = format!("consumer workload {consumer_name} is reconciling");
            } else if phase == "Ready" {
                message =
                    format!("mount workload {workload_name} and consumer workload {consumer_name} are ready");
            }
        } else {
            delete_consumer_workloads_if_exist(
                &client,
                &namespace,
                &default_consumer_workload_name,
                &default_consumer_service_name,
            )
            .await?;
            if phase == "Ready" {
                phase = "Progressing".to_string();
                message = "consumer requires hostMountPath to expose the mount to workload pods"
                    .to_string();
            }
        }
    } else {
        delete_consumer_workloads_if_exist(
            &client,
            &namespace,
            &default_consumer_workload_name,
            &default_consumer_service_name,
        )
        .await?;
    }

    patch_mount_status(
        &client,
        &namespace,
        &mount,
        &phase,
        &message,
        Some(config_map_name),
        mount.spec.host_mount_path.clone(),
        Some(mount.spec.mount_propagation.clone()),
        Some(mount.spec.workload_kind.clone()),
        Some(workload_name),
        desired_replicas,
        ready_replicas,
        consumer_workload_kind,
        consumer_workload_name_status,
        consumer_mount_path,
        consumer_desired_replicas,
        consumer_ready_replicas,
    )
    .await?;

    Ok(Action::requeue(Duration::from_secs(120)))
}

pub fn error_policy_cluster(
    _cluster: Arc<BrewFSCluster>,
    _error: &ReconcileError,
    _ctx: Arc<OperatorContext>,
) -> Action {
    Action::requeue(Duration::from_secs(15))
}

pub fn error_policy_mount(
    _mount: Arc<BrewFSMount>,
    _error: &ReconcileError,
    _ctx: Arc<OperatorContext>,
) -> Action {
    Action::requeue(Duration::from_secs(15))
}

fn labels(instance: &str, component: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("app.kubernetes.io/name".to_string(), "brewfs".to_string()),
        (
            "app.kubernetes.io/instance".to_string(),
            instance.to_string(),
        ),
        (
            "app.kubernetes.io/managed-by".to_string(),
            "brewfs-operator".to_string(),
        ),
        (
            "app.kubernetes.io/component".to_string(),
            component.to_string(),
        ),
    ])
}

fn object_meta(
    name: String,
    labels: BTreeMap<String, String>,
    owner: &OwnerReference,
) -> ObjectMeta {
    object_meta_with_annotations(name, labels, BTreeMap::new(), owner)
}

fn object_meta_with_annotations(
    name: String,
    labels: BTreeMap<String, String>,
    annotations: BTreeMap<String, String>,
    owner: &OwnerReference,
) -> ObjectMeta {
    ObjectMeta {
        name: Some(name),
        labels: Some(labels),
        annotations: if annotations.is_empty() {
            None
        } else {
            Some(annotations)
        },
        owner_references: Some(vec![owner.clone()]),
        ..ObjectMeta::default()
    }
}

fn redis_name(cluster_name: &str) -> String {
    format!("{cluster_name}-redis")
}

fn rustfs_name(cluster_name: &str) -> String {
    format!("{cluster_name}-rustfs")
}

fn rustfs_secret_name(cluster_name: &str) -> String {
    format!("{cluster_name}-rustfs-credentials")
}

fn rustfs_pvc_name(cluster_name: &str) -> String {
    format!("{cluster_name}-rustfs-data")
}

fn rustfs_job_name(cluster_name: &str) -> String {
    format!("{cluster_name}-rustfs-init")
}

fn cluster_config_map_name(cluster_name: &str) -> String {
    format!("{cluster_name}-brewfs-config")
}

fn mount_workload_name(mount_name: &str) -> String {
    format!("{mount_name}-mount")
}

fn consumer_workload_name(mount_name: &str) -> String {
    format!("{mount_name}-consumer")
}

fn consumer_headless_service_name(mount_name: &str) -> String {
    format!("{mount_name}-consumer")
}

fn consumer_shared_volume_name() -> &'static str {
    "brewfs-shared"
}

async fn apply_rustfs_secret(
    client: &kube::Client,
    namespace: &str,
    cluster: &BrewFSCluster,
    owner: &OwnerReference,
) -> Result<(), anyhow::Error> {
    let api: Api<Secret> = Api::namespaced(client.clone(), namespace);
    let cluster_name = cluster.name_any();
    let name = rustfs_secret_name(&cluster_name);
    let desired = Secret {
        metadata: object_meta(name.clone(), labels(&cluster_name, "rustfs-secret"), owner),
        string_data: Some(BTreeMap::from([
            (
                "accessKey".to_string(),
                cluster.spec.rustfs.access_key.clone(),
            ),
            (
                "secretKey".to_string(),
                cluster.spec.rustfs.secret_key.clone(),
            ),
        ])),
        type_: Some("Opaque".to_string()),
        ..Secret::default()
    };
    apply(&api, &name, &desired).await
}

async fn apply_rustfs_pvc(
    client: &kube::Client,
    namespace: &str,
    cluster: &BrewFSCluster,
    owner: &OwnerReference,
) -> Result<(), anyhow::Error> {
    let api: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), namespace);
    let cluster_name = cluster.name_any();
    let name = rustfs_pvc_name(&cluster_name);
    let desired = PersistentVolumeClaim {
        metadata: object_meta(name.clone(), labels(&cluster_name, "rustfs-storage"), owner),
        spec: Some(PersistentVolumeClaimSpec {
            access_modes: Some(vec!["ReadWriteOnce".to_string()]),
            resources: Some(k8s_openapi::api::core::v1::VolumeResourceRequirements {
                requests: Some(BTreeMap::from([(
                    "storage".to_string(),
                    Quantity(cluster.spec.rustfs.storage_size.clone()),
                )])),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    };
    apply(&api, &name, &desired).await
}

async fn apply_redis_service(
    client: &kube::Client,
    namespace: &str,
    cluster: &BrewFSCluster,
    owner: &OwnerReference,
) -> Result<(), anyhow::Error> {
    let api: Api<Service> = Api::namespaced(client.clone(), namespace);
    let cluster_name = cluster.name_any();
    let name = redis_name(&cluster_name);
    let match_labels = labels(&cluster_name, "redis");
    let desired = Service {
        metadata: object_meta(name.clone(), match_labels.clone(), owner),
        spec: Some(ServiceSpec {
            selector: Some(match_labels),
            ports: Some(vec![ServicePort {
                name: Some("redis".to_string()),
                port: cluster.spec.redis.port,
                target_port: Some(
                    k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(
                        cluster.spec.redis.port,
                    ),
                ),
                ..ServicePort::default()
            }]),
            ..ServiceSpec::default()
        }),
        ..Service::default()
    };
    apply(&api, &name, &desired).await
}

async fn apply_redis_deployment(
    client: &kube::Client,
    namespace: &str,
    cluster: &BrewFSCluster,
    owner: &OwnerReference,
) -> Result<(), anyhow::Error> {
    let api: Api<Deployment> = Api::namespaced(client.clone(), namespace);
    let cluster_name = cluster.name_any();
    let name = redis_name(&cluster_name);
    let match_labels = labels(&cluster_name, "redis");
    let desired = Deployment {
        metadata: object_meta(name.clone(), match_labels.clone(), owner),
        spec: Some(DeploymentSpec {
            replicas: Some(1),
            selector: LabelSelector {
                match_labels: Some(match_labels.clone()),
                ..LabelSelector::default()
            },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(match_labels),
                    ..ObjectMeta::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![Container {
                        name: "redis".to_string(),
                        image: Some(cluster.spec.redis.image.clone()),
                        args: Some(vec![
                            "redis-server".to_string(),
                            "--appendonly".to_string(),
                            "yes".to_string(),
                            "--appendfsync".to_string(),
                            "everysec".to_string(),
                        ]),
                        ports: Some(vec![ContainerPort {
                            container_port: cluster.spec.redis.port,
                            name: Some("redis".to_string()),
                            ..ContainerPort::default()
                        }]),
                        ..Container::default()
                    }],
                    ..PodSpec::default()
                }),
            },
            ..DeploymentSpec::default()
        }),
        ..Deployment::default()
    };
    apply(&api, &name, &desired).await
}

async fn apply_rustfs_service(
    client: &kube::Client,
    namespace: &str,
    cluster: &BrewFSCluster,
    owner: &OwnerReference,
) -> Result<(), anyhow::Error> {
    let api: Api<Service> = Api::namespaced(client.clone(), namespace);
    let cluster_name = cluster.name_any();
    let name = rustfs_name(&cluster_name);
    let match_labels = labels(&cluster_name, "rustfs");
    let desired = Service {
        metadata: object_meta(name.clone(), match_labels.clone(), owner),
        spec: Some(ServiceSpec {
            selector: Some(match_labels),
            ports: Some(vec![
                ServicePort {
                    name: Some("s3".to_string()),
                    port: cluster.spec.rustfs.port,
                    target_port: Some(
                        k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(
                            cluster.spec.rustfs.port,
                        ),
                    ),
                    ..ServicePort::default()
                },
                ServicePort {
                    name: Some("console".to_string()),
                    port: cluster.spec.rustfs.console_port,
                    target_port: Some(
                        k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(
                            cluster.spec.rustfs.console_port,
                        ),
                    ),
                    ..ServicePort::default()
                },
            ]),
            ..ServiceSpec::default()
        }),
        ..Service::default()
    };
    apply(&api, &name, &desired).await
}

fn rustfs_container_args(port: i32, access_key: &str, secret_key: &str) -> Vec<String> {
    vec![
        "--address".to_string(),
        format!(":{port}"),
        "--console-enable".to_string(),
        "--access-key".to_string(),
        access_key.to_string(),
        "--secret-key".to_string(),
        secret_key.to_string(),
        "/data".to_string(),
    ]
}

async fn apply_rustfs_deployment(
    client: &kube::Client,
    namespace: &str,
    cluster: &BrewFSCluster,
    owner: &OwnerReference,
) -> Result<(), anyhow::Error> {
    let api: Api<Deployment> = Api::namespaced(client.clone(), namespace);
    let cluster_name = cluster.name_any();
    let name = rustfs_name(&cluster_name);
    let match_labels = labels(&cluster_name, "rustfs");
    let secret_name = rustfs_secret_name(&cluster_name);
    let pvc_name = rustfs_pvc_name(&cluster_name);
    let desired = Deployment {
        metadata: object_meta(name.clone(), match_labels.clone(), owner),
        spec: Some(DeploymentSpec {
            replicas: Some(1),
            selector: LabelSelector {
                match_labels: Some(match_labels.clone()),
                ..LabelSelector::default()
            },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(match_labels),
                    ..ObjectMeta::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![Container {
                        name: "rustfs".to_string(),
                        image: Some(cluster.spec.rustfs.image.clone()),
                        args: Some(rustfs_container_args(
                            cluster.spec.rustfs.port,
                            &cluster.spec.rustfs.access_key,
                            &cluster.spec.rustfs.secret_key,
                        )),
                        env: Some(vec![
                            EnvVar {
                                name: "RUSTFS_ACCESS_KEY".to_string(),
                                value_from: Some(EnvVarSource {
                                    secret_key_ref: Some(SecretKeySelector {
                                        key: "accessKey".to_string(),
                                        name: secret_name.clone(),
                                        ..SecretKeySelector::default()
                                    }),
                                    ..EnvVarSource::default()
                                }),
                                ..EnvVar::default()
                            },
                            EnvVar {
                                name: "RUSTFS_SECRET_KEY".to_string(),
                                value_from: Some(EnvVarSource {
                                    secret_key_ref: Some(SecretKeySelector {
                                        key: "secretKey".to_string(),
                                        name: secret_name.clone(),
                                        ..SecretKeySelector::default()
                                    }),
                                    ..EnvVarSource::default()
                                }),
                                ..EnvVar::default()
                            },
                        ]),
                        ports: Some(vec![
                            ContainerPort {
                                container_port: cluster.spec.rustfs.port,
                                name: Some("s3".to_string()),
                                ..ContainerPort::default()
                            },
                            ContainerPort {
                                container_port: cluster.spec.rustfs.console_port,
                                name: Some("console".to_string()),
                                ..ContainerPort::default()
                            },
                        ]),
                        volume_mounts: Some(vec![VolumeMount {
                            name: "data".to_string(),
                            mount_path: "/data".to_string(),
                            ..VolumeMount::default()
                        }]),
                        ..Container::default()
                    }],
                    volumes: Some(vec![Volume {
                        name: "data".to_string(),
                        persistent_volume_claim: Some(
                            k8s_openapi::api::core::v1::PersistentVolumeClaimVolumeSource {
                                claim_name: pvc_name,
                                ..Default::default()
                            },
                        ),
                        ..Volume::default()
                    }]),
                    ..PodSpec::default()
                }),
            },
            ..DeploymentSpec::default()
        }),
        ..Deployment::default()
    };
    apply(&api, &name, &desired).await
}

async fn apply_rustfs_init_job(
    client: &kube::Client,
    namespace: &str,
    cluster: &BrewFSCluster,
    owner: &OwnerReference,
) -> Result<(), anyhow::Error> {
    let api: Api<Job> = Api::namespaced(client.clone(), namespace);
    let cluster_name = cluster.name_any();
    let name = rustfs_job_name(&cluster_name);
    let rustfs_name = rustfs_name(&cluster_name);
    let secret_name = rustfs_secret_name(&cluster_name);
    let bucket = cluster.spec.rustfs.bucket.clone();
    let endpoint = format!("http://{}:{}", rustfs_name, cluster.spec.rustfs.port);
    let desired = Job {
        metadata: object_meta(name.clone(), labels(&cluster_name, "rustfs-init"), owner),
        spec: Some(JobSpec {
            backoff_limit: Some(3),
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(labels(&cluster_name, "rustfs-init")),
                    ..ObjectMeta::default()
                }),
                spec: Some(PodSpec {
                    restart_policy: Some("Never".to_string()),
                    containers: vec![Container {
                        name: "rustfs-init".to_string(),
                        image: Some("amazon/aws-cli:latest".to_string()),
                        command: Some(vec![
                            "/bin/sh".to_string(),
                            "-ec".to_string(),
                            format!(
                                "mkdir -p /root/.aws && \
printf '[default]\\ns3 =\\n  addressing_style = path\\n' > /root/.aws/config && \
timeout 180 sh -ec 'while true; do \
aws --endpoint-url {endpoint} s3api create-bucket --bucket {bucket} >/dev/null 2>&1 && exit 0; \
aws --endpoint-url {endpoint} s3api head-bucket --bucket {bucket} >/dev/null 2>&1 && exit 0; \
sleep 2; done'"
                            ),
                        ]),
                        env: Some(vec![
                            EnvVar {
                                name: "AWS_ACCESS_KEY_ID".to_string(),
                                value_from: Some(EnvVarSource {
                                    secret_key_ref: Some(SecretKeySelector {
                                        key: "accessKey".to_string(),
                                        name: secret_name.clone(),
                                        ..SecretKeySelector::default()
                                    }),
                                    ..EnvVarSource::default()
                                }),
                                ..EnvVar::default()
                            },
                            EnvVar {
                                name: "AWS_SECRET_ACCESS_KEY".to_string(),
                                value_from: Some(EnvVarSource {
                                    secret_key_ref: Some(SecretKeySelector {
                                        key: "secretKey".to_string(),
                                        name: secret_name,
                                        ..SecretKeySelector::default()
                                    }),
                                    ..EnvVarSource::default()
                                }),
                                ..EnvVar::default()
                            },
                            EnvVar {
                                name: "AWS_DEFAULT_REGION".to_string(),
                                value: Some(cluster.spec.rustfs.region.clone()),
                                ..EnvVar::default()
                            },
                            EnvVar {
                                name: "AWS_EC2_METADATA_DISABLED".to_string(),
                                value: Some("true".to_string()),
                                ..EnvVar::default()
                            },
                        ]),
                        ..Container::default()
                    }],
                    ..PodSpec::default()
                }),
            },
            ..JobSpec::default()
        }),
        ..Job::default()
    };
    apply(&api, &name, &desired).await
}

async fn apply_brewfs_config(
    client: &kube::Client,
    namespace: &str,
    cluster: &BrewFSCluster,
    owner: &OwnerReference,
) -> Result<(), anyhow::Error> {
    let api: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
    let cluster_name = cluster.name_any();
    let name = cluster_config_map_name(&cluster_name);
    let desired = ConfigMap {
        metadata: object_meta(name.clone(), labels(&cluster_name, "config"), owner),
        data: Some(BTreeMap::from([(
            "config.yaml".to_string(),
            render_config(cluster),
        )])),
        ..ConfigMap::default()
    };
    apply(&api, &name, &desired).await
}

async fn apply_mount_workload(
    client: &kube::Client,
    namespace: &str,
    mount: &BrewFSMount,
    cluster: &BrewFSCluster,
    config_map_name: &str,
    owner: &OwnerReference,
) -> Result<String, anyhow::Error> {
    let mount_name = mount.name_any();
    let workload_name = mount_workload_name(&mount_name);
    let match_labels = labels(&mount_name, "mount");
    let template = build_mount_pod_template(mount, cluster, config_map_name, match_labels.clone());

    match mount.spec.workload_kind {
        MountWorkloadKind::Deployment => {
            delete_daemonset_if_exists(client, namespace, &workload_name).await?;
            let api: Api<Deployment> = Api::namespaced(client.clone(), namespace);
            let desired = Deployment {
                metadata: object_meta(workload_name.clone(), match_labels.clone(), owner),
                spec: Some(DeploymentSpec {
                    replicas: Some(mount.spec.replicas),
                    selector: LabelSelector {
                        match_labels: Some(match_labels),
                        ..LabelSelector::default()
                    },
                    template,
                    ..DeploymentSpec::default()
                }),
                ..Deployment::default()
            };
            apply(&api, &workload_name, &desired).await?;
        }
        MountWorkloadKind::DaemonSet => {
            delete_deployment_if_exists(client, namespace, &workload_name).await?;
            let api: Api<DaemonSet> = Api::namespaced(client.clone(), namespace);
            let desired = DaemonSet {
                metadata: object_meta(workload_name.clone(), match_labels.clone(), owner),
                spec: Some(DaemonSetSpec {
                    selector: LabelSelector {
                        match_labels: Some(match_labels),
                        ..LabelSelector::default()
                    },
                    template,
                    ..DaemonSetSpec::default()
                }),
                ..DaemonSet::default()
            };
            apply(&api, &workload_name, &desired).await?;
        }
    }

    Ok(workload_name)
}

async fn apply_consumer_workload(
    client: &kube::Client,
    namespace: &str,
    mount: &BrewFSMount,
    consumer: &MountConsumerSpec,
    host_mount_path: &str,
    owner: &OwnerReference,
) -> Result<String, anyhow::Error> {
    let mount_name = mount.name_any();
    let workload_name = consumer_workload_name(&mount_name);
    let match_labels = labels(&mount_name, "consumer");
    let mut workload_labels = match_labels.clone();
    workload_labels.extend(consumer.workload_labels.clone());
    let workload_annotations = consumer.workload_annotations.clone();
    let template = build_consumer_pod_template(consumer, host_mount_path, match_labels.clone());
    let headless_service_name = consumer_headless_service_name(&mount_name);

    match consumer.workload_kind {
        ConsumerWorkloadKind::Deployment => {
            delete_daemonset_if_exists(client, namespace, &workload_name).await?;
            delete_statefulset_if_exists(client, namespace, &workload_name).await?;
            delete_service_if_exists(client, namespace, &headless_service_name).await?;
            let api: Api<Deployment> = Api::namespaced(client.clone(), namespace);
            let desired = Deployment {
                metadata: object_meta_with_annotations(
                    workload_name.clone(),
                    workload_labels,
                    workload_annotations,
                    owner,
                ),
                spec: Some(DeploymentSpec {
                    replicas: Some(consumer.replicas),
                    selector: LabelSelector {
                        match_labels: Some(match_labels),
                        ..LabelSelector::default()
                    },
                    template,
                    ..DeploymentSpec::default()
                }),
                ..Deployment::default()
            };
            apply(&api, &workload_name, &desired).await?;
        }
        ConsumerWorkloadKind::DaemonSet => {
            delete_deployment_if_exists(client, namespace, &workload_name).await?;
            delete_statefulset_if_exists(client, namespace, &workload_name).await?;
            delete_service_if_exists(client, namespace, &headless_service_name).await?;
            let api: Api<DaemonSet> = Api::namespaced(client.clone(), namespace);
            let desired = DaemonSet {
                metadata: object_meta_with_annotations(
                    workload_name.clone(),
                    workload_labels,
                    workload_annotations,
                    owner,
                ),
                spec: Some(DaemonSetSpec {
                    selector: LabelSelector {
                        match_labels: Some(match_labels),
                        ..LabelSelector::default()
                    },
                    template,
                    ..DaemonSetSpec::default()
                }),
                ..DaemonSet::default()
            };
            apply(&api, &workload_name, &desired).await?;
        }
        ConsumerWorkloadKind::StatefulSet => {
            delete_deployment_if_exists(client, namespace, &workload_name).await?;
            delete_daemonset_if_exists(client, namespace, &workload_name).await?;
            apply_consumer_headless_service(
                client,
                namespace,
                &headless_service_name,
                &workload_labels,
                &match_labels,
                owner,
            )
            .await?;
            let api: Api<StatefulSet> = Api::namespaced(client.clone(), namespace);
            let desired = StatefulSet {
                metadata: object_meta_with_annotations(
                    workload_name.clone(),
                    workload_labels,
                    workload_annotations,
                    owner,
                ),
                spec: Some(StatefulSetSpec {
                    service_name: headless_service_name,
                    replicas: Some(consumer.replicas),
                    selector: LabelSelector {
                        match_labels: Some(match_labels),
                        ..LabelSelector::default()
                    },
                    template,
                    ..StatefulSetSpec::default()
                }),
                ..StatefulSet::default()
            };
            apply(&api, &workload_name, &desired).await?;
        }
    }

    Ok(workload_name)
}

async fn apply_consumer_headless_service(
    client: &kube::Client,
    namespace: &str,
    name: &str,
    metadata_labels: &BTreeMap<String, String>,
    selector_labels: &BTreeMap<String, String>,
    owner: &OwnerReference,
) -> Result<(), anyhow::Error> {
    let api: Api<Service> = Api::namespaced(client.clone(), namespace);
    let desired = Service {
        metadata: object_meta(name.to_string(), metadata_labels.clone(), owner),
        spec: Some(ServiceSpec {
            cluster_ip: Some("None".to_string()),
            publish_not_ready_addresses: Some(true),
            selector: Some(selector_labels.clone()),
            ports: Some(vec![ServicePort {
                name: Some("identity".to_string()),
                port: 80,
                target_port: Some(
                    k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(80),
                ),
                ..ServicePort::default()
            }]),
            ..ServiceSpec::default()
        }),
        ..Service::default()
    };
    apply(&api, name, &desired).await
}

fn build_mount_pod_template(
    mount: &BrewFSMount,
    cluster: &BrewFSCluster,
    config_map_name: &str,
    match_labels: BTreeMap<String, String>,
) -> PodTemplateSpec {
    let cluster_name = cluster.name_any();
    let mount_command = format!(
        "mkdir -p {mount_path} {state_path} && exec /usr/local/bin/brewfs mount --privileged --config {config_path} {mount_path}",
        mount_path = mount.spec.mount_path,
        state_path = mount.spec.state_path,
        config_path = mount.spec.config_path,
    );

    let mut pod_annotations = BTreeMap::from([
        (
            "storage.brewfs.io/cluster".to_string(),
            cluster_name.clone(),
        ),
        (
            "storage.brewfs.io/config-map".to_string(),
            config_map_name.to_string(),
        ),
    ]);
    if let Some(generation) = cluster.metadata.generation {
        pod_annotations.insert(
            "storage.brewfs.io/cluster-generation".to_string(),
            generation.to_string(),
        );
    }

    if let Some(host_mount_path) = &mount.spec.host_mount_path {
        pod_annotations.insert(
            "storage.brewfs.io/host-mount-path".to_string(),
            host_mount_path.clone(),
        );
        pod_annotations.insert(
            "storage.brewfs.io/mount-propagation".to_string(),
            mount_propagation_value(&mount.spec.mount_propagation).to_string(),
        );
    }

    let mut volume_mounts = vec![
        VolumeMount {
            name: "fuse-device".to_string(),
            mount_path: "/dev/fuse".to_string(),
            ..VolumeMount::default()
        },
        VolumeMount {
            name: "config".to_string(),
            mount_path: mount.spec.config_path.clone(),
            sub_path: Some("config.yaml".to_string()),
            read_only: Some(true),
            ..VolumeMount::default()
        },
        VolumeMount {
            name: "state".to_string(),
            mount_path: mount.spec.state_path.clone(),
            ..VolumeMount::default()
        },
    ];

    let mut volumes = vec![
        Volume {
            name: "fuse-device".to_string(),
            host_path: Some(k8s_openapi::api::core::v1::HostPathVolumeSource {
                path: "/dev/fuse".to_string(),
                type_: Some("CharDevice".to_string()),
            }),
            ..Volume::default()
        },
        Volume {
            name: "config".to_string(),
            config_map: Some(k8s_openapi::api::core::v1::ConfigMapVolumeSource {
                name: config_map_name.to_string(),
                ..Default::default()
            }),
            ..Volume::default()
        },
        mount_state_volume(mount),
    ];

    if let Some(host_mount_path) = &mount.spec.host_mount_path {
        volume_mounts.push(VolumeMount {
            name: "mount-path".to_string(),
            mount_path: mount.spec.mount_path.clone(),
            mount_propagation: mount_propagation_option(&mount.spec.mount_propagation),
            ..VolumeMount::default()
        });
        volumes.push(Volume {
            name: "mount-path".to_string(),
            host_path: Some(k8s_openapi::api::core::v1::HostPathVolumeSource {
                path: host_mount_path.clone(),
                type_: Some("DirectoryOrCreate".to_string()),
            }),
            ..Volume::default()
        });
    }

    PodTemplateSpec {
        metadata: Some(ObjectMeta {
            labels: Some(match_labels),
            annotations: Some(pod_annotations),
            ..ObjectMeta::default()
        }),
        spec: Some(PodSpec {
            service_account_name: mount.spec.service_account_name.clone(),
            node_selector: if mount.spec.node_selector.is_empty() {
                None
            } else {
                Some(mount.spec.node_selector.clone())
            },
            tolerations: mount_tolerations(&mount.spec.tolerations),
            containers: vec![Container {
                name: "brewfs".to_string(),
                image: Some(mount.spec.image.clone()),
                image_pull_policy: Some(mount.spec.image_pull_policy.clone()),
                command: Some(vec!["/bin/sh".to_string(), "-ec".to_string()]),
                args: Some(vec![mount_command]),
                env: Some(vec![
                    EnvVar {
                        name: "RUST_LOG".to_string(),
                        value: Some(mount.spec.log_level.clone()),
                        ..EnvVar::default()
                    },
                    EnvVar {
                        name: "AWS_ACCESS_KEY_ID".to_string(),
                        value_from: Some(EnvVarSource {
                            secret_key_ref: Some(SecretKeySelector {
                                key: "accessKey".to_string(),
                                name: rustfs_secret_name(&cluster_name),
                                ..SecretKeySelector::default()
                            }),
                            ..EnvVarSource::default()
                        }),
                        ..EnvVar::default()
                    },
                    EnvVar {
                        name: "AWS_SECRET_ACCESS_KEY".to_string(),
                        value_from: Some(EnvVarSource {
                            secret_key_ref: Some(SecretKeySelector {
                                key: "secretKey".to_string(),
                                name: rustfs_secret_name(&cluster_name),
                                ..SecretKeySelector::default()
                            }),
                            ..EnvVarSource::default()
                        }),
                        ..EnvVar::default()
                    },
                    EnvVar {
                        name: "AWS_DEFAULT_REGION".to_string(),
                        value: Some(cluster.spec.rustfs.region.clone()),
                        ..EnvVar::default()
                    },
                    EnvVar {
                        name: "BREWFS_CONFIG_PATH".to_string(),
                        value: Some(mount.spec.config_path.clone()),
                        ..EnvVar::default()
                    },
                    EnvVar {
                        name: "BREWFS_HOME".to_string(),
                        value: Some(mount.spec.state_path.clone()),
                        ..EnvVar::default()
                    },
                    EnvVar {
                        name: "BREWFS_MOUNT_POINT".to_string(),
                        value: Some(mount.spec.mount_path.clone()),
                        ..EnvVar::default()
                    },
                ]),
                security_context: Some(SecurityContext {
                    privileged: Some(true),
                    allow_privilege_escalation: Some(true),
                    capabilities: Some(Capabilities {
                        add: Some(vec!["SYS_ADMIN".to_string()]),
                        ..Capabilities::default()
                    }),
                    ..SecurityContext::default()
                }),
                volume_mounts: Some(volume_mounts),
                ..Container::default()
            }],
            volumes: Some(volumes),
            ..PodSpec::default()
        }),
    }
}

fn build_consumer_pod_template(
    consumer: &MountConsumerSpec,
    host_mount_path: &str,
    match_labels: BTreeMap<String, String>,
) -> PodTemplateSpec {
    let mut labels = match_labels;
    labels.extend(consumer.pod_labels.clone());

    let mut annotations = consumer.pod_annotations.clone();
    annotations.insert(
        "storage.brewfs.io/consumer-host-mount-path".to_string(),
        host_mount_path.to_string(),
    );

    let shared_volume_name = consumer_shared_volume_name();
    let shared_mount = Volume {
        name: shared_volume_name.to_string(),
        host_path: Some(k8s_openapi::api::core::v1::HostPathVolumeSource {
            path: host_mount_path.to_string(),
            type_: Some("DirectoryOrCreate".to_string()),
        }),
        ..Volume::default()
    };

    let mut volumes = vec![shared_mount];
    volumes.extend(consumer_extra_volumes(&consumer.volumes));

    let containers = consumer_rendered_containers(consumer, shared_volume_name);
    let init_containers = consumer_rendered_init_containers(consumer, shared_volume_name);

    PodTemplateSpec {
        metadata: Some(ObjectMeta {
            labels: Some(labels),
            annotations: Some(annotations),
            ..ObjectMeta::default()
        }),
        spec: Some(PodSpec {
            service_account_name: consumer.service_account_name.clone(),
            image_pull_secrets: consumer_image_pull_secrets(&consumer.image_pull_secrets),
            node_selector: if consumer.node_selector.is_empty() {
                None
            } else {
                Some(consumer.node_selector.clone())
            },
            priority_class_name: consumer.priority_class_name.clone(),
            host_network: Some(consumer.host_network),
            dns_policy: consumer.dns_policy.clone(),
            termination_grace_period_seconds: consumer.termination_grace_period_seconds,
            security_context: consumer_pod_security_context(&consumer.pod_security_context),
            tolerations: mount_tolerations(&consumer.tolerations),
            init_containers,
            containers,
            volumes: Some(volumes),
            ..PodSpec::default()
        }),
    }
}

fn mount_state_volume(mount: &BrewFSMount) -> Volume {
    let mut volume = Volume {
        name: "state".to_string(),
        ..Volume::default()
    };

    if let Some(claim_name) = &mount.spec.state_pvc_name {
        volume.persistent_volume_claim = Some(
            k8s_openapi::api::core::v1::PersistentVolumeClaimVolumeSource {
                claim_name: claim_name.clone(),
                ..Default::default()
            },
        );
    } else {
        volume.empty_dir = Some(EmptyDirVolumeSource::default());
    }

    volume
}

fn mount_tolerations(tolerations: &[MountToleration]) -> Option<Vec<Toleration>> {
    if tolerations.is_empty() {
        return None;
    }

    Some(
        tolerations
            .iter()
            .map(|item| Toleration {
                effect: item.effect.clone(),
                key: item.key.clone(),
                operator: item.operator.clone(),
                toleration_seconds: item.toleration_seconds,
                value: item.value.clone(),
            })
            .collect(),
    )
}

fn consumer_env_vars(env: &BTreeMap<String, String>) -> Option<Vec<EnvVar>> {
    if env.is_empty() {
        return None;
    }

    Some(
        env.iter()
            .map(|(name, value)| EnvVar {
                name: name.clone(),
                value: Some(value.clone()),
                ..EnvVar::default()
            })
            .collect(),
    )
}

fn consumer_image_pull_secrets(names: &[String]) -> Option<Vec<LocalObjectReference>> {
    if names.is_empty() {
        return None;
    }

    Some(
        names
            .iter()
            .map(|name| LocalObjectReference { name: name.clone() })
            .collect(),
    )
}

fn consumer_env_from_sources(specs: &[ConsumerEnvFromSpec]) -> Option<Vec<EnvFromSource>> {
    if specs.is_empty() {
        return None;
    }

    Some(
        specs
            .iter()
            .filter_map(|spec| {
                if let Some(name) = &spec.config_map_name {
                    Some(EnvFromSource {
                        config_map_ref: Some(ConfigMapEnvSource {
                            name: name.clone(),
                            optional: spec.optional,
                        }),
                        prefix: spec.prefix.clone(),
                        ..EnvFromSource::default()
                    })
                } else {
                    spec.secret_name.as_ref().map(|name| EnvFromSource {
                        secret_ref: Some(SecretEnvSource {
                            name: name.clone(),
                            optional: spec.optional,
                        }),
                        prefix: spec.prefix.clone(),
                        ..EnvFromSource::default()
                    })
                }
            })
            .collect(),
    )
}

fn consumer_ports(specs: &[ConsumerPortSpec]) -> Option<Vec<ContainerPort>> {
    if specs.is_empty() {
        return None;
    }

    Some(
        specs
            .iter()
            .map(|spec| ContainerPort {
                container_port: spec.container_port,
                name: spec.name.clone(),
                protocol: spec.protocol.clone(),
                ..ContainerPort::default()
            })
            .collect(),
    )
}

fn consumer_resource_requirements(
    spec: &Option<ConsumerResourceRequirementsSpec>,
) -> Option<ResourceRequirements> {
    let Some(spec) = spec else {
        return None;
    };

    let limits = quantity_map(&spec.limits);
    let requests = quantity_map(&spec.requests);

    if limits.is_none() && requests.is_none() {
        return None;
    }

    Some(ResourceRequirements {
        limits,
        requests,
        ..ResourceRequirements::default()
    })
}

fn quantity_map(values: &BTreeMap<String, String>) -> Option<BTreeMap<String, Quantity>> {
    if values.is_empty() {
        return None;
    }

    Some(
        values
            .iter()
            .map(|(name, value)| (name.clone(), Quantity(value.clone())))
            .collect(),
    )
}

fn consumer_security_context(
    spec: &Option<ConsumerSecurityContextSpec>,
) -> Option<SecurityContext> {
    let Some(spec) = spec else {
        return None;
    };

    let capabilities = if spec.capabilities_add.is_empty() && spec.capabilities_drop.is_empty() {
        None
    } else {
        Some(Capabilities {
            add: if spec.capabilities_add.is_empty() {
                None
            } else {
                Some(spec.capabilities_add.clone())
            },
            drop: if spec.capabilities_drop.is_empty() {
                None
            } else {
                Some(spec.capabilities_drop.clone())
            },
        })
    };

    Some(SecurityContext {
        privileged: spec.privileged,
        allow_privilege_escalation: spec.allow_privilege_escalation,
        read_only_root_filesystem: spec.read_only_root_filesystem,
        run_as_user: spec.run_as_user,
        run_as_group: spec.run_as_group,
        run_as_non_root: spec.run_as_non_root,
        capabilities,
        ..SecurityContext::default()
    })
}

fn consumer_pod_security_context(
    spec: &Option<ConsumerPodSecurityContextSpec>,
) -> Option<PodSecurityContext> {
    let Some(spec) = spec else {
        return None;
    };

    let supplemental_groups = if spec.supplemental_groups.is_empty() {
        None
    } else {
        Some(spec.supplemental_groups.clone())
    };

    Some(PodSecurityContext {
        run_as_user: spec.run_as_user,
        run_as_group: spec.run_as_group,
        run_as_non_root: spec.run_as_non_root,
        fs_group: spec.fs_group,
        supplemental_groups,
        ..PodSecurityContext::default()
    })
}

fn consumer_probe(spec: &Option<ConsumerProbeSpec>) -> Option<Probe> {
    let Some(spec) = spec else {
        return None;
    };

    Some(Probe {
        exec: if spec.exec_command.is_empty() {
            None
        } else {
            Some(ExecAction {
                command: Some(spec.exec_command.clone()),
            })
        },
        http_get: spec.http_get.as_ref().map(consumer_http_get_action),
        tcp_socket: spec.tcp_socket.as_ref().map(consumer_tcp_socket_action),
        initial_delay_seconds: spec.initial_delay_seconds,
        period_seconds: spec.period_seconds,
        timeout_seconds: spec.timeout_seconds,
        success_threshold: spec.success_threshold,
        failure_threshold: spec.failure_threshold,
        ..Probe::default()
    })
}

fn consumer_lifecycle(spec: &Option<ConsumerLifecycleSpec>) -> Option<Lifecycle> {
    let Some(spec) = spec else {
        return None;
    };

    let post_start = spec.post_start.as_ref().map(consumer_lifecycle_handler);
    let pre_stop = spec.pre_stop.as_ref().map(consumer_lifecycle_handler);

    if post_start.is_none() && pre_stop.is_none() {
        return None;
    }

    Some(Lifecycle {
        post_start,
        pre_stop,
    })
}

fn consumer_lifecycle_handler(spec: &crate::crd::ConsumerLifecycleHandlerSpec) -> LifecycleHandler {
    LifecycleHandler {
        exec: if spec.exec_command.is_empty() {
            None
        } else {
            Some(ExecAction {
                command: Some(spec.exec_command.clone()),
            })
        },
        http_get: spec.http_get.as_ref().map(consumer_http_get_action),
        tcp_socket: spec.tcp_socket.as_ref().map(consumer_tcp_socket_action),
        ..LifecycleHandler::default()
    }
}

fn consumer_http_get_action(spec: &ConsumerHTTPGetActionSpec) -> HTTPGetAction {
    HTTPGetAction {
        path: spec.path.clone(),
        host: spec.host.clone(),
        scheme: spec.scheme.clone(),
        port: k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(spec.port),
        http_headers: if spec.http_headers.is_empty() {
            None
        } else {
            Some(
                spec.http_headers
                    .iter()
                    .map(|header| HTTPHeader {
                        name: header.name.clone(),
                        value: header.value.clone(),
                    })
                    .collect(),
            )
        },
    }
}

fn consumer_tcp_socket_action(spec: &ConsumerTCPSocketActionSpec) -> TCPSocketAction {
    TCPSocketAction {
        host: spec.host.clone(),
        port: k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(spec.port),
    }
}

fn consumer_rendered_init_containers(
    consumer: &MountConsumerSpec,
    shared_volume_name: &str,
) -> Option<Vec<Container>> {
    if consumer.init_containers.is_empty() {
        return None;
    }

    Some(
        consumer
            .init_containers
            .iter()
            .map(|container| {
                render_consumer_container(container, &consumer.mount_path, shared_volume_name)
            })
            .collect(),
    )
}

fn consumer_rendered_containers(
    consumer: &MountConsumerSpec,
    shared_volume_name: &str,
) -> Vec<Container> {
    if consumer.containers.is_empty() {
        let default_container = ConsumerContainerSpec {
            name: "consumer".to_string(),
            image: consumer.image.clone(),
            image_pull_policy: consumer.image_pull_policy.clone(),
            command: consumer.command.clone(),
            args: consumer.args.clone(),
            env: consumer.env.clone(),
            working_dir: None,
            mount_path: Some(consumer.mount_path.clone()),
            volume_mounts: Vec::new(),
            ports: Vec::new(),
            env_from: Vec::new(),
            resources: None,
            security_context: None,
            liveness_probe: None,
            readiness_probe: None,
            startup_probe: None,
            lifecycle: None,
            termination_message_policy: None,
            stdin: false,
            tty: false,
        };
        vec![render_consumer_container(
            &default_container,
            &consumer.mount_path,
            shared_volume_name,
        )]
    } else {
        consumer
            .containers
            .iter()
            .map(|container| {
                render_consumer_container(container, &consumer.mount_path, shared_volume_name)
            })
            .collect()
    }
}

fn render_consumer_container(
    container: &ConsumerContainerSpec,
    default_mount_path: &str,
    shared_volume_name: &str,
) -> Container {
    let mut volume_mounts = vec![VolumeMount {
        name: shared_volume_name.to_string(),
        mount_path: container
            .mount_path
            .clone()
            .unwrap_or_else(|| default_mount_path.to_string()),
        mount_propagation: mount_propagation_option(&MountPropagationMode::HostToContainer),
        ..VolumeMount::default()
    }];
    volume_mounts.extend(
        container
            .volume_mounts
            .iter()
            .map(render_consumer_volume_mount),
    );

    Container {
        name: container.name.clone(),
        image: Some(container.image.clone()),
        image_pull_policy: Some(container.image_pull_policy.clone()),
        command: if container.command.is_empty() {
            None
        } else {
            Some(container.command.clone())
        },
        args: if container.args.is_empty() {
            None
        } else {
            Some(container.args.clone())
        },
        env: consumer_env_vars(&container.env),
        env_from: consumer_env_from_sources(&container.env_from),
        ports: consumer_ports(&container.ports),
        resources: consumer_resource_requirements(&container.resources),
        security_context: consumer_security_context(&container.security_context),
        liveness_probe: consumer_probe(&container.liveness_probe),
        readiness_probe: consumer_probe(&container.readiness_probe),
        startup_probe: consumer_probe(&container.startup_probe),
        lifecycle: consumer_lifecycle(&container.lifecycle),
        termination_message_policy: container.termination_message_policy.clone(),
        stdin: Some(container.stdin),
        tty: Some(container.tty),
        working_dir: container.working_dir.clone(),
        volume_mounts: Some(volume_mounts),
        ..Container::default()
    }
}

fn render_consumer_volume_mount(spec: &ConsumerVolumeMountSpec) -> VolumeMount {
    VolumeMount {
        name: spec.name.clone(),
        mount_path: spec.mount_path.clone(),
        read_only: Some(spec.read_only),
        sub_path: spec.sub_path.clone(),
        ..VolumeMount::default()
    }
}

fn consumer_extra_volumes(specs: &[ConsumerVolumeSpec]) -> Vec<Volume> {
    specs.iter().map(render_consumer_volume).collect()
}

fn render_consumer_volume(spec: &ConsumerVolumeSpec) -> Volume {
    let mut volume = Volume {
        name: spec.name.clone(),
        ..Volume::default()
    };

    if let Some(host_path) = &spec.host_path {
        volume.host_path = Some(k8s_openapi::api::core::v1::HostPathVolumeSource {
            path: host_path.clone(),
            type_: spec.host_path_type.clone(),
        });
    } else if let Some(config_map_name) = &spec.config_map_name {
        volume.config_map = Some(k8s_openapi::api::core::v1::ConfigMapVolumeSource {
            name: config_map_name.clone(),
            ..Default::default()
        });
    } else if let Some(secret_name) = &spec.secret_name {
        volume.secret = Some(k8s_openapi::api::core::v1::SecretVolumeSource {
            secret_name: Some(secret_name.clone()),
            ..Default::default()
        });
    } else if spec.empty_dir {
        volume.empty_dir = Some(EmptyDirVolumeSource::default());
    }

    volume
}

fn mount_propagation_option(mode: &MountPropagationMode) -> Option<String> {
    match mode {
        MountPropagationMode::None => None,
        MountPropagationMode::HostToContainer => Some("HostToContainer".to_string()),
        MountPropagationMode::Bidirectional => Some("Bidirectional".to_string()),
    }
}

fn mount_propagation_value(mode: &MountPropagationMode) -> &'static str {
    match mode {
        MountPropagationMode::None => "None",
        MountPropagationMode::HostToContainer => "HostToContainer",
        MountPropagationMode::Bidirectional => "Bidirectional",
    }
}

async fn get_mount_workload_status(
    client: &kube::Client,
    namespace: &str,
    mount: &BrewFSMount,
    name: &str,
) -> Result<(Option<i32>, Option<i32>), anyhow::Error> {
    match mount.spec.workload_kind {
        MountWorkloadKind::Deployment => {
            let api: Api<Deployment> = Api::namespaced(client.clone(), namespace);
            let deployment = api
                .get_opt(name)
                .await
                .with_context(|| format!("load Deployment {name}"))?;
            let desired = deployment
                .as_ref()
                .and_then(|d| d.spec.as_ref().and_then(|spec| spec.replicas));
            let ready = deployment.and_then(|d| d.status.and_then(|status| status.ready_replicas));
            Ok((desired, ready))
        }
        MountWorkloadKind::DaemonSet => {
            let api: Api<DaemonSet> = Api::namespaced(client.clone(), namespace);
            let daemonset = api
                .get_opt(name)
                .await
                .with_context(|| format!("load DaemonSet {name}"))?;
            let desired = daemonset.as_ref().and_then(|d| {
                d.status
                    .as_ref()
                    .map(|status| status.desired_number_scheduled)
            });
            let ready = daemonset.and_then(|d| d.status.map(|status| status.number_ready));
            Ok((desired, ready))
        }
    }
}

async fn get_consumer_workload_status(
    client: &kube::Client,
    namespace: &str,
    consumer: &MountConsumerSpec,
    name: &str,
) -> Result<(Option<i32>, Option<i32>), anyhow::Error> {
    match consumer.workload_kind {
        ConsumerWorkloadKind::Deployment => {
            let api: Api<Deployment> = Api::namespaced(client.clone(), namespace);
            let deployment = api
                .get_opt(name)
                .await
                .with_context(|| format!("load consumer Deployment {name}"))?;
            let desired = deployment
                .as_ref()
                .and_then(|d| d.spec.as_ref().and_then(|spec| spec.replicas));
            let ready = deployment.and_then(|d| d.status.and_then(|status| status.ready_replicas));
            Ok((desired, ready))
        }
        ConsumerWorkloadKind::DaemonSet => {
            let api: Api<DaemonSet> = Api::namespaced(client.clone(), namespace);
            let daemonset = api
                .get_opt(name)
                .await
                .with_context(|| format!("load consumer DaemonSet {name}"))?;
            let desired = daemonset.as_ref().and_then(|d| {
                d.status
                    .as_ref()
                    .map(|status| status.desired_number_scheduled)
            });
            let ready = daemonset.and_then(|d| d.status.map(|status| status.number_ready));
            Ok((desired, ready))
        }
        ConsumerWorkloadKind::StatefulSet => {
            let api: Api<StatefulSet> = Api::namespaced(client.clone(), namespace);
            let statefulset = api
                .get_opt(name)
                .await
                .with_context(|| format!("load consumer StatefulSet {name}"))?;
            let desired = statefulset
                .as_ref()
                .and_then(|s| s.spec.as_ref().and_then(|spec| spec.replicas));
            let ready = statefulset.and_then(|s| s.status.and_then(|status| status.ready_replicas));
            Ok((desired, ready))
        }
    }
}

async fn delete_deployment_if_exists(
    client: &kube::Client,
    namespace: &str,
    name: &str,
) -> Result<(), anyhow::Error> {
    let api: Api<Deployment> = Api::namespaced(client.clone(), namespace);
    if api.get_opt(name).await?.is_some() {
        api.delete(name, &DeleteParams::default())
            .await
            .with_context(|| format!("delete stale Deployment {name}"))?;
    }
    Ok(())
}

async fn delete_daemonset_if_exists(
    client: &kube::Client,
    namespace: &str,
    name: &str,
) -> Result<(), anyhow::Error> {
    let api: Api<DaemonSet> = Api::namespaced(client.clone(), namespace);
    if api.get_opt(name).await?.is_some() {
        api.delete(name, &DeleteParams::default())
            .await
            .with_context(|| format!("delete stale DaemonSet {name}"))?;
    }
    Ok(())
}

async fn delete_statefulset_if_exists(
    client: &kube::Client,
    namespace: &str,
    name: &str,
) -> Result<(), anyhow::Error> {
    let api: Api<StatefulSet> = Api::namespaced(client.clone(), namespace);
    if api.get_opt(name).await?.is_some() {
        api.delete(name, &DeleteParams::default())
            .await
            .with_context(|| format!("delete stale StatefulSet {name}"))?;
    }
    Ok(())
}

async fn delete_service_if_exists(
    client: &kube::Client,
    namespace: &str,
    name: &str,
) -> Result<(), anyhow::Error> {
    let api: Api<Service> = Api::namespaced(client.clone(), namespace);
    if api.get_opt(name).await?.is_some() {
        api.delete(name, &DeleteParams::default())
            .await
            .with_context(|| format!("delete stale Service {name}"))?;
    }
    Ok(())
}

async fn delete_consumer_workloads_if_exist(
    client: &kube::Client,
    namespace: &str,
    name: &str,
    service_name: &str,
) -> Result<(), anyhow::Error> {
    delete_deployment_if_exists(client, namespace, name).await?;
    delete_daemonset_if_exists(client, namespace, name).await?;
    delete_statefulset_if_exists(client, namespace, name).await?;
    delete_service_if_exists(client, namespace, service_name).await?;
    Ok(())
}

fn render_config(cluster: &BrewFSCluster) -> String {
    let cluster_name = cluster.name_any();
    let redis_service = redis_name(&cluster_name);
    let rustfs_service = rustfs_name(&cluster_name);
    format!(
        "mount_point: {mount_point}\n\n\
data:\n  backend: s3\n  s3:\n    bucket: {bucket}\n    region: {region}\n    part_size: {part_size}\n    max_concurrency: {max_concurrency}\n    force_path_style: {force_path_style}\n    endpoint: http://{rustfs_service}:{rustfs_port}\n\n\
meta:\n  backend: redis\n  redis:\n    url: \"redis://{redis_service}:{redis_port}/0\"\n\n\
layout:\n  chunk_size: {chunk_size}\n  block_size: {block_size}\n",
        mount_point = cluster.spec.mount_config.mount_point,
        bucket = cluster.spec.rustfs.bucket,
        region = cluster.spec.rustfs.region,
        part_size = cluster.spec.mount_config.part_size,
        max_concurrency = cluster.spec.mount_config.max_concurrency,
        force_path_style = cluster.spec.mount_config.force_path_style,
        rustfs_service = rustfs_service,
        rustfs_port = cluster.spec.rustfs.port,
        redis_service = redis_service,
        redis_port = cluster.spec.redis.port,
        chunk_size = cluster.spec.mount_config.chunk_size,
        block_size = cluster.spec.mount_config.block_size,
    )
}

fn cluster_ready_status(
    cluster: &BrewFSCluster,
    last_reconciled_at: Option<DateTime<Utc>>,
) -> BrewFSClusterStatus {
    let cluster_name = cluster.name_any();
    BrewFSClusterStatus {
        observed_generation: cluster.metadata.generation,
        phase: "Ready".to_string(),
        message: "Backend resources reconciled".to_string(),
        redis_service: Some(redis_name(&cluster_name)),
        rustfs_service: Some(rustfs_name(&cluster_name)),
        bucket: Some(cluster.spec.rustfs.bucket.clone()),
        config_map: Some(cluster_config_map_name(&cluster_name)),
        last_reconciled_at,
    }
}

fn cluster_status_semantically_equal(
    current: &BrewFSClusterStatus,
    desired: &BrewFSClusterStatus,
) -> bool {
    current.observed_generation == desired.observed_generation
        && current.phase == desired.phase
        && current.message == desired.message
        && current.redis_service == desired.redis_service
        && current.rustfs_service == desired.rustfs_service
        && current.bucket == desired.bucket
        && current.config_map == desired.config_map
}

fn mount_status_semantically_equal(
    current: &BrewFSMountStatus,
    desired: &BrewFSMountStatus,
) -> bool {
    current.observed_generation == desired.observed_generation
        && current.phase == desired.phase
        && current.message == desired.message
        && current.cluster == desired.cluster
        && current.workload_kind == desired.workload_kind
        && current.workload_name == desired.workload_name
        && current.config_map == desired.config_map
        && current.host_mount_path == desired.host_mount_path
        && current.mount_propagation == desired.mount_propagation
        && current.desired_replicas == desired.desired_replicas
        && current.ready_replicas == desired.ready_replicas
        && current.consumer_workload_kind == desired.consumer_workload_kind
        && current.consumer_workload_name == desired.consumer_workload_name
        && current.consumer_mount_path == desired.consumer_mount_path
        && current.consumer_desired_replicas == desired.consumer_desired_replicas
        && current.consumer_ready_replicas == desired.consumer_ready_replicas
}

async fn patch_cluster_status(
    client: &kube::Client,
    namespace: &str,
    cluster: &BrewFSCluster,
) -> Result<(), anyhow::Error> {
    let api: Api<BrewFSCluster> = Api::namespaced(client.clone(), namespace);
    let cluster_name = cluster.name_any();
    let desired = cluster_ready_status(cluster, None);
    if cluster
        .status
        .as_ref()
        .is_some_and(|current| cluster_status_semantically_equal(current, &desired))
    {
        return Ok(());
    }

    let status = cluster_ready_status(cluster, Some(Utc::now()));

    let patch = json!({
        "apiVersion": "storage.brewfs.io/v1alpha1",
        "kind": "BrewFSCluster",
        "status": status,
    });
    api.patch_status(
        &cluster_name,
        &PatchParams::apply("brewfs-operator").force(),
        &Patch::Apply(&patch),
    )
    .await
    .with_context(|| format!("patch status for {cluster_name}"))?;
    Ok(())
}

async fn patch_mount_status(
    client: &kube::Client,
    namespace: &str,
    mount: &BrewFSMount,
    phase: &str,
    message: &str,
    config_map: Option<String>,
    host_mount_path: Option<String>,
    mount_propagation: Option<MountPropagationMode>,
    workload_kind: Option<MountWorkloadKind>,
    workload_name: Option<String>,
    desired_replicas: Option<i32>,
    ready_replicas: Option<i32>,
    consumer_workload_kind: Option<ConsumerWorkloadKind>,
    consumer_workload_name: Option<String>,
    consumer_mount_path: Option<String>,
    consumer_desired_replicas: Option<i32>,
    consumer_ready_replicas: Option<i32>,
) -> Result<(), anyhow::Error> {
    let api: Api<BrewFSMount> = Api::namespaced(client.clone(), namespace);
    let mount_name = mount.name_any();
    let desired = BrewFSMountStatus {
        observed_generation: mount.metadata.generation,
        phase: phase.to_string(),
        message: message.to_string(),
        cluster: Some(mount.spec.cluster_ref.name.clone()),
        workload_kind,
        workload_name,
        config_map,
        host_mount_path,
        mount_propagation,
        desired_replicas,
        ready_replicas,
        consumer_workload_kind,
        consumer_workload_name,
        consumer_mount_path,
        consumer_desired_replicas,
        consumer_ready_replicas,
        last_reconciled_at: None,
    };
    if mount
        .status
        .as_ref()
        .is_some_and(|current| mount_status_semantically_equal(current, &desired))
    {
        return Ok(());
    }

    let status = BrewFSMountStatus {
        last_reconciled_at: Some(Utc::now()),
        ..desired
    };

    let patch = json!({
        "apiVersion": "storage.brewfs.io/v1alpha1",
        "kind": "BrewFSMount",
        "status": status,
    });
    api.patch_status(
        &mount_name,
        &PatchParams::apply("brewfs-operator").force(),
        &Patch::Apply(&patch),
    )
    .await
    .with_context(|| format!("patch status for {mount_name}"))?;
    Ok(())
}

async fn apply<K>(api: &Api<K>, name: &str, desired: &K) -> Result<(), anyhow::Error>
where
    K: Clone + serde::Serialize + DeserializeOwned + Resource + Send + Sync + Debug,
    <K as Resource>::DynamicType: Default,
{
    api.patch(
        name,
        &PatchParams::apply("brewfs-operator").force(),
        &Patch::Apply(desired),
    )
    .await
    .with_context(|| format!("apply resource {name}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::crd::{
        BrewFSClusterRef, BrewFSClusterSpec, BrewFSMountSpec, MountConfigSpec, RedisSpec,
        RustFsSpec,
    };

    use super::*;

    #[test]
    fn rustfs_container_args_do_not_force_virtual_host_domains() {
        let args = rustfs_container_args(9000, "access", "secret");

        assert_eq!(
            args,
            vec![
                "--address",
                ":9000",
                "--console-enable",
                "--access-key",
                "access",
                "--secret-key",
                "secret",
                "/data",
            ]
        );
        assert!(!args.iter().any(|arg| arg == "--server-domains"));
    }

    #[test]
    fn mount_pod_uses_privileged_brewfs_mount_mode() {
        let cluster = BrewFSCluster {
            metadata: ObjectMeta {
                name: Some("demo".to_string()),
                generation: Some(1),
                ..ObjectMeta::default()
            },
            spec: BrewFSClusterSpec {
                redis: RedisSpec::default(),
                rustfs: RustFsSpec::default(),
                mount_config: MountConfigSpec::default(),
            },
            status: None,
        };
        let mount = BrewFSMount {
            metadata: ObjectMeta {
                name: Some("demo-mount".to_string()),
                ..ObjectMeta::default()
            },
            spec: BrewFSMountSpec {
                cluster_ref: BrewFSClusterRef {
                    name: "demo".to_string(),
                },
                workload_kind: MountWorkloadKind::DaemonSet,
                image: "brewfs:local".to_string(),
                image_pull_policy: "IfNotPresent".to_string(),
                replicas: 1,
                mount_path: "/mnt/brewfs".to_string(),
                host_mount_path: Some("/var/lib/brewfs/mounts/demo".to_string()),
                mount_propagation: MountPropagationMode::Bidirectional,
                config_path: "/run/brewfs/config.yaml".to_string(),
                state_path: "/var/lib/brewfs".to_string(),
                log_level: "brewfs=info".to_string(),
                service_account_name: None,
                node_selector: BTreeMap::new(),
                tolerations: Vec::new(),
                state_pvc_name: None,
                consumer: None,
            },
            status: None,
        };

        let template = build_mount_pod_template(&mount, &cluster, "demo-config", BTreeMap::new());
        let command = template
            .spec
            .as_ref()
            .and_then(|spec| spec.containers.first())
            .and_then(|container| container.args.as_ref())
            .and_then(|args| args.first())
            .expect("mount pod command");

        assert!(command.contains("/usr/local/bin/brewfs mount --privileged --config"));
    }

    #[test]
    fn mount_pod_injects_rustfs_s3_credentials() {
        let cluster = BrewFSCluster {
            metadata: ObjectMeta {
                name: Some("demo".to_string()),
                generation: Some(1),
                ..ObjectMeta::default()
            },
            spec: BrewFSClusterSpec {
                redis: RedisSpec::default(),
                rustfs: RustFsSpec::default(),
                mount_config: MountConfigSpec::default(),
            },
            status: None,
        };
        let mount = BrewFSMount {
            metadata: ObjectMeta {
                name: Some("demo-mount".to_string()),
                ..ObjectMeta::default()
            },
            spec: BrewFSMountSpec {
                cluster_ref: BrewFSClusterRef {
                    name: "demo".to_string(),
                },
                workload_kind: MountWorkloadKind::DaemonSet,
                image: "brewfs:local".to_string(),
                image_pull_policy: "IfNotPresent".to_string(),
                replicas: 1,
                mount_path: "/mnt/brewfs".to_string(),
                host_mount_path: Some("/var/lib/brewfs/mounts/demo".to_string()),
                mount_propagation: MountPropagationMode::Bidirectional,
                config_path: "/run/brewfs/config.yaml".to_string(),
                state_path: "/var/lib/brewfs".to_string(),
                log_level: "brewfs=info".to_string(),
                service_account_name: None,
                node_selector: BTreeMap::new(),
                tolerations: Vec::new(),
                state_pvc_name: None,
                consumer: None,
            },
            status: None,
        };

        let template = build_mount_pod_template(&mount, &cluster, "demo-config", BTreeMap::new());
        let env = template
            .spec
            .as_ref()
            .and_then(|spec| spec.containers.first())
            .and_then(|container| container.env.as_ref())
            .expect("mount pod env");
        let access_key = env
            .iter()
            .find(|var| var.name == "AWS_ACCESS_KEY_ID")
            .expect("AWS_ACCESS_KEY_ID env var");
        let secret_key = env
            .iter()
            .find(|var| var.name == "AWS_SECRET_ACCESS_KEY")
            .expect("AWS_SECRET_ACCESS_KEY env var");
        let region = env
            .iter()
            .find(|var| var.name == "AWS_DEFAULT_REGION")
            .expect("AWS_DEFAULT_REGION env var");

        let access_ref = access_key
            .value_from
            .as_ref()
            .and_then(|source| source.secret_key_ref.as_ref())
            .expect("access key secret ref");
        let secret_ref = secret_key
            .value_from
            .as_ref()
            .and_then(|source| source.secret_key_ref.as_ref())
            .expect("secret key secret ref");

        assert_eq!(access_ref.name, "demo-rustfs-credentials");
        assert_eq!(access_ref.key, "accessKey");
        assert_eq!(secret_ref.name, "demo-rustfs-credentials");
        assert_eq!(secret_ref.key, "secretKey");
        assert_eq!(region.value.as_deref(), Some("us-east-1"));
    }

    #[test]
    fn cluster_status_semantic_comparison_ignores_last_reconciled_at() {
        let current = BrewFSClusterStatus {
            observed_generation: Some(1),
            phase: "Ready".to_string(),
            message: "Backend resources reconciled".to_string(),
            redis_service: Some("demo-redis".to_string()),
            rustfs_service: Some("demo-rustfs".to_string()),
            bucket: Some("brewfs-data".to_string()),
            config_map: Some("demo-brewfs-config".to_string()),
            last_reconciled_at: Some(Utc::now()),
        };
        let mut desired = current.clone();
        desired.last_reconciled_at = None;

        assert!(cluster_status_semantically_equal(&current, &desired));

        desired.phase = "Pending".to_string();
        assert!(!cluster_status_semantically_equal(&current, &desired));
    }

    #[test]
    fn mount_status_semantic_comparison_ignores_last_reconciled_at() {
        let current = BrewFSMountStatus {
            observed_generation: Some(2),
            phase: "Running".to_string(),
            message: "mount workload reconciled".to_string(),
            cluster: Some("demo".to_string()),
            workload_kind: Some(MountWorkloadKind::DaemonSet),
            workload_name: Some("demo-mount".to_string()),
            config_map: Some("demo-brewfs-config".to_string()),
            host_mount_path: Some("/var/lib/brewfs/mounts/demo".to_string()),
            mount_propagation: Some(MountPropagationMode::Bidirectional),
            desired_replicas: None,
            ready_replicas: Some(1),
            consumer_workload_kind: Some(ConsumerWorkloadKind::StatefulSet),
            consumer_workload_name: Some("demo-mount-consumer".to_string()),
            consumer_mount_path: Some("/data".to_string()),
            consumer_desired_replicas: Some(1),
            consumer_ready_replicas: Some(1),
            last_reconciled_at: Some(Utc::now()),
        };
        let mut desired = current.clone();
        desired.last_reconciled_at = None;

        assert!(mount_status_semantically_equal(&current, &desired));

        desired.ready_replicas = Some(0);
        assert!(!mount_status_semantically_equal(&current, &desired));
    }
}
