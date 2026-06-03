use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "storage.brewfs.io",
    version = "v1alpha1",
    kind = "BrewFSCluster",
    plural = "brewfsclusters",
    namespaced,
    status = "BrewFSClusterStatus",
    shortname = "sfs"
)]
pub struct BrewFSClusterSpec {
    #[serde(default)]
    pub redis: RedisSpec,
    #[serde(default)]
    pub rustfs: RustFsSpec,
    #[serde(default, rename = "mountConfig")]
    pub mount_config: MountConfigSpec,
}

#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "storage.brewfs.io",
    version = "v1alpha1",
    kind = "BrewFSMount",
    plural = "brewfsmounts",
    namespaced,
    status = "BrewFSMountStatus",
    shortname = "sfsm"
)]
#[serde(rename_all = "camelCase")]
pub struct BrewFSMountSpec {
    #[serde(rename = "clusterRef")]
    pub cluster_ref: BrewFSClusterRef,
    #[serde(default)]
    pub workload_kind: MountWorkloadKind,
    #[serde(default = "default_brewfs_image")]
    pub image: String,
    #[serde(default = "default_image_pull_policy")]
    pub image_pull_policy: String,
    #[serde(default = "default_mount_replicas")]
    pub replicas: i32,
    #[serde(default = "default_mount_runtime_path")]
    pub mount_path: String,
    #[serde(default)]
    pub host_mount_path: Option<String>,
    #[serde(default)]
    pub mount_propagation: MountPropagationMode,
    #[serde(default = "default_config_path")]
    pub config_path: String,
    #[serde(default = "default_state_path")]
    pub state_path: String,
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(default)]
    pub service_account_name: Option<String>,
    #[serde(default)]
    pub node_selector: BTreeMap<String, String>,
    #[serde(default)]
    pub tolerations: Vec<MountToleration>,
    #[serde(default)]
    pub state_pvc_name: Option<String>,
    #[serde(default)]
    pub consumer: Option<MountConsumerSpec>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BrewFSClusterStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    pub phase: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub redis_service: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rustfs_service: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bucket: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config_map: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_reconciled_at: Option<DateTime<Utc>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BrewFSMountStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    pub phase: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cluster: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workload_kind: Option<MountWorkloadKind>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workload_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config_map: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_mount_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mount_propagation: Option<MountPropagationMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub desired_replicas: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ready_replicas: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consumer_workload_kind: Option<ConsumerWorkloadKind>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consumer_workload_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consumer_mount_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consumer_desired_replicas: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consumer_ready_replicas: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_reconciled_at: Option<DateTime<Utc>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BrewFSClusterRef {
    pub name: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "PascalCase")]
pub enum MountWorkloadKind {
    #[default]
    Deployment,
    DaemonSet,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "PascalCase")]
pub enum MountPropagationMode {
    #[default]
    None,
    HostToContainer,
    Bidirectional,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "PascalCase")]
pub enum ConsumerWorkloadKind {
    #[default]
    Deployment,
    DaemonSet,
    StatefulSet,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MountConsumerSpec {
    #[serde(default)]
    pub workload_kind: ConsumerWorkloadKind,
    #[serde(default)]
    pub workload_labels: BTreeMap<String, String>,
    #[serde(default)]
    pub workload_annotations: BTreeMap<String, String>,
    #[serde(default = "default_consumer_image")]
    pub image: String,
    #[serde(default = "default_image_pull_policy")]
    pub image_pull_policy: String,
    #[serde(default = "default_consumer_replicas")]
    pub replicas: i32,
    #[serde(default = "default_consumer_mount_path")]
    pub mount_path: String,
    #[serde(default = "default_consumer_command")]
    pub command: Vec<String>,
    #[serde(default = "default_consumer_args")]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub service_account_name: Option<String>,
    #[serde(default)]
    pub image_pull_secrets: Vec<String>,
    #[serde(default)]
    pub node_selector: BTreeMap<String, String>,
    #[serde(default)]
    pub tolerations: Vec<MountToleration>,
    #[serde(default)]
    pub priority_class_name: Option<String>,
    #[serde(default)]
    pub host_network: bool,
    #[serde(default)]
    pub dns_policy: Option<String>,
    #[serde(default)]
    pub termination_grace_period_seconds: Option<i64>,
    #[serde(default)]
    pub pod_security_context: Option<ConsumerPodSecurityContextSpec>,
    #[serde(default)]
    pub pod_labels: BTreeMap<String, String>,
    #[serde(default)]
    pub pod_annotations: BTreeMap<String, String>,
    #[serde(default)]
    pub init_containers: Vec<ConsumerContainerSpec>,
    #[serde(default)]
    pub containers: Vec<ConsumerContainerSpec>,
    #[serde(default)]
    pub volumes: Vec<ConsumerVolumeSpec>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MountToleration {
    #[serde(default)]
    pub key: Option<String>,
    #[serde(default)]
    pub operator: Option<String>,
    #[serde(default)]
    pub value: Option<String>,
    #[serde(default)]
    pub effect: Option<String>,
    #[serde(default)]
    pub toleration_seconds: Option<i64>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerContainerSpec {
    pub name: String,
    pub image: String,
    #[serde(default = "default_image_pull_policy")]
    pub image_pull_policy: String,
    #[serde(default)]
    pub command: Vec<String>,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub working_dir: Option<String>,
    #[serde(default)]
    pub mount_path: Option<String>,
    #[serde(default)]
    pub volume_mounts: Vec<ConsumerVolumeMountSpec>,
    #[serde(default)]
    pub ports: Vec<ConsumerPortSpec>,
    #[serde(default)]
    pub env_from: Vec<ConsumerEnvFromSpec>,
    #[serde(default)]
    pub resources: Option<ConsumerResourceRequirementsSpec>,
    #[serde(default)]
    pub security_context: Option<ConsumerSecurityContextSpec>,
    #[serde(default)]
    pub liveness_probe: Option<ConsumerProbeSpec>,
    #[serde(default)]
    pub readiness_probe: Option<ConsumerProbeSpec>,
    #[serde(default)]
    pub startup_probe: Option<ConsumerProbeSpec>,
    #[serde(default)]
    pub lifecycle: Option<ConsumerLifecycleSpec>,
    #[serde(default)]
    pub termination_message_policy: Option<String>,
    #[serde(default)]
    pub stdin: bool,
    #[serde(default)]
    pub tty: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerVolumeMountSpec {
    pub name: String,
    pub mount_path: String,
    #[serde(default)]
    pub read_only: bool,
    #[serde(default)]
    pub sub_path: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerPortSpec {
    pub container_port: i32,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub protocol: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerEnvFromSpec {
    #[serde(default)]
    pub config_map_name: Option<String>,
    #[serde(default)]
    pub secret_name: Option<String>,
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub optional: Option<bool>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerResourceRequirementsSpec {
    #[serde(default)]
    pub limits: BTreeMap<String, String>,
    #[serde(default)]
    pub requests: BTreeMap<String, String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerSecurityContextSpec {
    #[serde(default)]
    pub privileged: Option<bool>,
    #[serde(default)]
    pub allow_privilege_escalation: Option<bool>,
    #[serde(default)]
    pub read_only_root_filesystem: Option<bool>,
    #[serde(default)]
    pub run_as_user: Option<i64>,
    #[serde(default)]
    pub run_as_group: Option<i64>,
    #[serde(default)]
    pub run_as_non_root: Option<bool>,
    #[serde(default)]
    pub capabilities_add: Vec<String>,
    #[serde(default)]
    pub capabilities_drop: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerPodSecurityContextSpec {
    #[serde(default)]
    pub run_as_user: Option<i64>,
    #[serde(default)]
    pub run_as_group: Option<i64>,
    #[serde(default)]
    pub run_as_non_root: Option<bool>,
    #[serde(default)]
    pub fs_group: Option<i64>,
    #[serde(default)]
    pub supplemental_groups: Vec<i64>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerLifecycleSpec {
    #[serde(default)]
    pub post_start: Option<ConsumerLifecycleHandlerSpec>,
    #[serde(default)]
    pub pre_stop: Option<ConsumerLifecycleHandlerSpec>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerLifecycleHandlerSpec {
    #[serde(default)]
    pub exec_command: Vec<String>,
    #[serde(default)]
    pub http_get: Option<ConsumerHTTPGetActionSpec>,
    #[serde(default)]
    pub tcp_socket: Option<ConsumerTCPSocketActionSpec>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerProbeSpec {
    #[serde(default)]
    pub exec_command: Vec<String>,
    #[serde(default)]
    pub http_get: Option<ConsumerHTTPGetActionSpec>,
    #[serde(default)]
    pub tcp_socket: Option<ConsumerTCPSocketActionSpec>,
    #[serde(default)]
    pub initial_delay_seconds: Option<i32>,
    #[serde(default)]
    pub period_seconds: Option<i32>,
    #[serde(default)]
    pub timeout_seconds: Option<i32>,
    #[serde(default)]
    pub success_threshold: Option<i32>,
    #[serde(default)]
    pub failure_threshold: Option<i32>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerHTTPGetActionSpec {
    pub port: i32,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default)]
    pub scheme: Option<String>,
    #[serde(default)]
    pub http_headers: Vec<ConsumerHTTPHeaderSpec>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerHTTPHeaderSpec {
    pub name: String,
    pub value: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerTCPSocketActionSpec {
    pub port: i32,
    #[serde(default)]
    pub host: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerVolumeSpec {
    pub name: String,
    #[serde(default)]
    pub empty_dir: bool,
    #[serde(default)]
    pub config_map_name: Option<String>,
    #[serde(default)]
    pub secret_name: Option<String>,
    #[serde(default)]
    pub host_path: Option<String>,
    #[serde(default)]
    pub host_path_type: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RedisSpec {
    #[serde(default = "default_redis_image")]
    pub image: String,
    #[serde(default = "default_redis_port")]
    pub port: i32,
}

impl Default for RedisSpec {
    fn default() -> Self {
        Self {
            image: default_redis_image(),
            port: default_redis_port(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RustFsSpec {
    #[serde(default = "default_rustfs_image")]
    pub image: String,
    #[serde(default = "default_bucket")]
    pub bucket: String,
    #[serde(default = "default_region")]
    pub region: String,
    #[serde(default = "default_access_key")]
    pub access_key: String,
    #[serde(default = "default_secret_key")]
    pub secret_key: String,
    #[serde(default = "default_rustfs_port")]
    pub port: i32,
    #[serde(default = "default_rustfs_console_port")]
    pub console_port: i32,
    #[serde(default = "default_storage_size")]
    pub storage_size: String,
}

impl Default for RustFsSpec {
    fn default() -> Self {
        Self {
            image: default_rustfs_image(),
            bucket: default_bucket(),
            region: default_region(),
            access_key: default_access_key(),
            secret_key: default_secret_key(),
            port: default_rustfs_port(),
            console_port: default_rustfs_console_port(),
            storage_size: default_storage_size(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MountConfigSpec {
    #[serde(default = "default_mount_point")]
    pub mount_point: String,
    #[serde(default = "default_chunk_size")]
    pub chunk_size: u64,
    #[serde(default = "default_block_size")]
    pub block_size: u32,
    #[serde(default = "default_part_size")]
    pub part_size: usize,
    #[serde(default = "default_max_concurrency")]
    pub max_concurrency: usize,
    #[serde(default = "default_force_path_style")]
    pub force_path_style: bool,
}

impl Default for MountConfigSpec {
    fn default() -> Self {
        Self {
            mount_point: default_mount_point(),
            chunk_size: default_chunk_size(),
            block_size: default_block_size(),
            part_size: default_part_size(),
            max_concurrency: default_max_concurrency(),
            force_path_style: default_force_path_style(),
        }
    }
}

fn default_redis_image() -> String {
    "redis:7.2-alpine".to_string()
}

fn default_redis_port() -> i32 {
    6379
}

fn default_rustfs_image() -> String {
    "rustfs/rustfs:latest".to_string()
}

fn default_bucket() -> String {
    "brewfs-data".to_string()
}

fn default_region() -> String {
    "us-east-1".to_string()
}

fn default_access_key() -> String {
    "rustfsadmin".to_string()
}

fn default_secret_key() -> String {
    "rustfsadmin".to_string()
}

fn default_rustfs_port() -> i32 {
    9000
}

fn default_rustfs_console_port() -> i32 {
    9001
}

fn default_storage_size() -> String {
    "20Gi".to_string()
}

fn default_mount_point() -> String {
    "/mnt/brewfs".to_string()
}

fn default_brewfs_image() -> String {
    "brewfs:local".to_string()
}

fn default_image_pull_policy() -> String {
    "IfNotPresent".to_string()
}

fn default_mount_replicas() -> i32 {
    1
}

fn default_mount_runtime_path() -> String {
    "/mnt/brewfs".to_string()
}

fn default_config_path() -> String {
    "/run/brewfs/config.yaml".to_string()
}

fn default_state_path() -> String {
    "/var/lib/brewfs".to_string()
}

fn default_log_level() -> String {
    "brewfs=info".to_string()
}

fn default_consumer_image() -> String {
    "busybox:1.36".to_string()
}

fn default_consumer_replicas() -> i32 {
    1
}

fn default_consumer_mount_path() -> String {
    "/data".to_string()
}

fn default_consumer_command() -> Vec<String> {
    vec!["/bin/sh".to_string(), "-ec".to_string()]
}

fn default_consumer_args() -> Vec<String> {
    vec!["sleep infinity".to_string()]
}

fn default_chunk_size() -> u64 {
    64 * 1024 * 1024
}

fn default_block_size() -> u32 {
    4 * 1024 * 1024
}

fn default_part_size() -> usize {
    16 * 1024 * 1024
}

fn default_max_concurrency() -> usize {
    8
}

fn default_force_path_style() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::BrewFSMountStatus;

    #[test]
    fn mount_status_omits_absent_optional_fields() {
        let value = serde_json::to_value(BrewFSMountStatus {
            observed_generation: None,
            phase: "Ready".to_string(),
            message: "ready".to_string(),
            cluster: None,
            workload_kind: None,
            workload_name: None,
            config_map: None,
            host_mount_path: None,
            mount_propagation: None,
            desired_replicas: None,
            ready_replicas: None,
            consumer_workload_kind: None,
            consumer_workload_name: None,
            consumer_mount_path: None,
            consumer_desired_replicas: None,
            consumer_ready_replicas: None,
            last_reconciled_at: None,
        })
        .expect("serialize BrewFSMountStatus");

        let Value::Object(status) = value else {
            panic!("status should serialize as an object");
        };

        assert_eq!(
            status.get("phase"),
            Some(&Value::String("Ready".to_string()))
        );
        assert_eq!(
            status.get("message"),
            Some(&Value::String("ready".to_string()))
        );
        assert!(!status.contains_key("consumerWorkloadKind"));
        assert!(!status.contains_key("observedGeneration"));
        assert!(!status.contains_key("lastReconciledAt"));
    }
}
