use super::api::CsiResourceQuery;
use async_trait::async_trait;
use serde::Serialize;
use std::{fmt, sync::Arc};

pub const DEFAULT_DRIVER_NAME: &str = "csi.brewfs.io";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CsiSummary {
    pub storageclasses: usize,
    pub persistentvolumes: usize,
    pub persistentvolumeclaims: usize,
    pub pods: usize,
    pub unhealthy_mounts: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct CsiResourceList {
    pub items: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CsiAdapterError {
    Disabled,
    Unsupported(&'static str),
}

#[async_trait]
pub trait CsiAdapter: fmt::Debug + Send + Sync {
    async fn summary(&self) -> Result<CsiSummary, CsiAdapterError>;

    async fn storageclasses(&self) -> Result<CsiResourceList, CsiAdapterError>;

    async fn persistentvolumes(&self) -> Result<CsiResourceList, CsiAdapterError>;

    async fn persistentvolumeclaims(
        &self,
        query: &CsiResourceQuery,
    ) -> Result<CsiResourceList, CsiAdapterError>;

    async fn pods(&self, query: &CsiResourceQuery) -> Result<CsiResourceList, CsiAdapterError>;
}

pub fn default_csi_adapter(config: super::ConsoleCsiConfig) -> Arc<dyn CsiAdapter> {
    Arc::new(UnsupportedCsiAdapter { config })
}

#[derive(Debug)]
struct UnsupportedCsiAdapter {
    config: super::ConsoleCsiConfig,
}

impl UnsupportedCsiAdapter {
    fn unavailable_or_unsupported<T>(&self, message: &'static str) -> Result<T, CsiAdapterError> {
        if self.config.enabled {
            Err(CsiAdapterError::Unsupported(message))
        } else {
            Err(CsiAdapterError::Disabled)
        }
    }
}

#[async_trait]
impl CsiAdapter for UnsupportedCsiAdapter {
    async fn summary(&self) -> Result<CsiSummary, CsiAdapterError> {
        self.unavailable_or_unsupported("CSI dashboard adapter is not implemented yet")
    }

    async fn storageclasses(&self) -> Result<CsiResourceList, CsiAdapterError> {
        self.unavailable_or_unsupported("CSI StorageClass adapter is not implemented yet")
    }

    async fn persistentvolumes(&self) -> Result<CsiResourceList, CsiAdapterError> {
        self.unavailable_or_unsupported("CSI PersistentVolume adapter is not implemented yet")
    }

    async fn persistentvolumeclaims(
        &self,
        _query: &CsiResourceQuery,
    ) -> Result<CsiResourceList, CsiAdapterError> {
        self.unavailable_or_unsupported("CSI PersistentVolumeClaim adapter is not implemented yet")
    }

    async fn pods(&self, _query: &CsiResourceQuery) -> Result<CsiResourceList, CsiAdapterError> {
        self.unavailable_or_unsupported("CSI Pod adapter is not implemented yet")
    }
}

#[derive(Debug, Clone, Default)]
pub struct CsiResourceSnapshot {
    pub storageclasses: Vec<serde_json::Value>,
    pub persistentvolumes: Vec<serde_json::Value>,
    pub persistentvolumeclaims: Vec<serde_json::Value>,
    pub pods: Vec<serde_json::Value>,
}

#[derive(Debug, Clone)]
pub struct SnapshotCsiAdapter {
    driver_name: String,
    snapshot: CsiResourceSnapshot,
}

impl SnapshotCsiAdapter {
    pub fn new(driver_name: impl Into<String>, snapshot: CsiResourceSnapshot) -> Self {
        Self {
            driver_name: driver_name.into(),
            snapshot,
        }
    }

    fn brewfs_storageclass_names(&self) -> Vec<String> {
        self.snapshot
            .storageclasses
            .iter()
            .filter(|item| self.storageclass_matches(item))
            .filter_map(resource_name)
            .map(ToOwned::to_owned)
            .collect()
    }

    fn brewfs_pv_names(&self) -> Vec<String> {
        self.snapshot
            .persistentvolumes
            .iter()
            .filter(|item| self.pv_matches(item))
            .filter_map(resource_name)
            .map(ToOwned::to_owned)
            .collect()
    }

    fn brewfs_pvc_names(&self, namespace: Option<&str>) -> Vec<String> {
        let storageclasses = self.brewfs_storageclass_names();
        let pvs = self.brewfs_pv_names();
        self.snapshot
            .persistentvolumeclaims
            .iter()
            .filter(|item| namespace_matches(item, namespace))
            .filter(|item| {
                let storageclass = item
                    .pointer("/spec/storageClassName")
                    .and_then(|value| value.as_str());
                let volume_name = item
                    .pointer("/spec/volumeName")
                    .and_then(|value| value.as_str());
                self.resource_has_brewfs_marker(item)
                    || storageclass
                        .is_some_and(|name| storageclasses.iter().any(|entry| entry == name))
                    || volume_name.is_some_and(|name| pvs.iter().any(|entry| entry == name))
            })
            .filter_map(resource_name)
            .map(ToOwned::to_owned)
            .collect()
    }

    fn brewfs_pods(&self, query: &CsiResourceQuery) -> Vec<serde_json::Value> {
        self.snapshot
            .pods
            .iter()
            .filter(|pod| namespace_matches(pod, query.namespace.as_deref()))
            .filter(|pod| self.pod_uses_brewfs_volume(pod, query))
            .cloned()
            .collect()
    }

    fn pod_uses_brewfs_volume(&self, pod: &serde_json::Value, query: &CsiResourceQuery) -> bool {
        let namespace = pod
            .pointer("/metadata/namespace")
            .and_then(|value| value.as_str());
        let pvc_names = self.brewfs_pvc_names(namespace);
        pod.pointer("/spec/volumes")
            .and_then(|value| value.as_array())
            .is_some_and(|volumes| {
                volumes.iter().any(|volume| {
                    let volume_name = volume.pointer("/name").and_then(|value| value.as_str());
                    let inline_brewfs = volume
                        .pointer("/csi/driver")
                        .and_then(|value| value.as_str())
                        == Some(self.driver_name.as_str());
                    let pvc_name = volume
                        .pointer("/persistentVolumeClaim/claimName")
                        .and_then(|value| value.as_str());
                    let brewfs_pvc =
                        pvc_name.is_some_and(|name| pvc_names.iter().any(|entry| entry == name));
                    let volume_matches = query.volume.as_deref().is_none_or(|filter| {
                        pvc_name == Some(filter) || volume_name == Some(filter)
                    });

                    (inline_brewfs || brewfs_pvc) && volume_matches
                })
            })
    }

    fn storageclass_matches(&self, item: &serde_json::Value) -> bool {
        item.pointer("/provisioner")
            .and_then(|value| value.as_str())
            == Some(self.driver_name.as_str())
            || self.resource_has_brewfs_marker(item)
    }

    fn pv_matches(&self, item: &serde_json::Value) -> bool {
        item.pointer("/spec/csi/driver")
            .and_then(|value| value.as_str())
            == Some(self.driver_name.as_str())
            || metadata_value(item, "annotations", "csi.brewfs.io/driver")
                == Some(self.driver_name.as_str())
            || self.resource_has_brewfs_marker(item)
    }

    fn resource_has_brewfs_marker(&self, item: &serde_json::Value) -> bool {
        metadata_value(item, "labels", "app.kubernetes.io/name") == Some("brewfs")
            || metadata_value(item, "labels", "brewfs.io/filesystem").is_some()
            || metadata_value(item, "annotations", "brewfs.io/filesystem").is_some()
    }
}

#[async_trait]
impl CsiAdapter for SnapshotCsiAdapter {
    async fn summary(&self) -> Result<CsiSummary, CsiAdapterError> {
        let pods = self.brewfs_pods(&CsiResourceQuery {
            namespace: None,
            volume: None,
        });
        Ok(CsiSummary {
            storageclasses: self.brewfs_storageclass_names().len(),
            persistentvolumes: self.brewfs_pv_names().len(),
            persistentvolumeclaims: self.brewfs_pvc_names(None).len(),
            unhealthy_mounts: pods.iter().filter(|pod| !pod_ready(pod)).count(),
            pods: pods.len(),
        })
    }

    async fn storageclasses(&self) -> Result<CsiResourceList, CsiAdapterError> {
        Ok(CsiResourceList {
            items: self
                .snapshot
                .storageclasses
                .iter()
                .filter(|item| self.storageclass_matches(item))
                .cloned()
                .collect(),
        })
    }

    async fn persistentvolumes(&self) -> Result<CsiResourceList, CsiAdapterError> {
        Ok(CsiResourceList {
            items: self
                .snapshot
                .persistentvolumes
                .iter()
                .filter(|item| self.pv_matches(item))
                .cloned()
                .collect(),
        })
    }

    async fn persistentvolumeclaims(
        &self,
        query: &CsiResourceQuery,
    ) -> Result<CsiResourceList, CsiAdapterError> {
        let names = self.brewfs_pvc_names(query.namespace.as_deref());
        Ok(CsiResourceList {
            items: self
                .snapshot
                .persistentvolumeclaims
                .iter()
                .filter(|item| namespace_matches(item, query.namespace.as_deref()))
                .filter(|item| {
                    resource_name(item).is_some_and(|name| names.iter().any(|entry| entry == name))
                })
                .cloned()
                .collect(),
        })
    }

    async fn pods(&self, query: &CsiResourceQuery) -> Result<CsiResourceList, CsiAdapterError> {
        Ok(CsiResourceList {
            items: self.brewfs_pods(query),
        })
    }
}

fn resource_name(item: &serde_json::Value) -> Option<&str> {
    item.pointer("/metadata/name")
        .and_then(|value| value.as_str())
}

fn namespace_matches(item: &serde_json::Value, namespace: Option<&str>) -> bool {
    namespace.is_none_or(|expected| {
        item.pointer("/metadata/namespace")
            .and_then(|value| value.as_str())
            == Some(expected)
    })
}

fn metadata_value<'a>(item: &'a serde_json::Value, section: &str, key: &str) -> Option<&'a str> {
    item.pointer("/metadata")
        .and_then(|metadata| metadata.get(section))
        .and_then(|section| section.get(key))
        .and_then(|value| value.as_str())
}

fn pod_ready(pod: &serde_json::Value) -> bool {
    let phase = pod
        .pointer("/status/phase")
        .and_then(|value| value.as_str());
    let ready = pod
        .pointer("/status/conditions")
        .and_then(|value| value.as_array())
        .and_then(|conditions| {
            conditions.iter().find(|condition| {
                condition.pointer("/type").and_then(|value| value.as_str()) == Some("Ready")
            })
        })
        .and_then(|condition| {
            condition
                .pointer("/status")
                .and_then(|value| value.as_str())
        });

    phase == Some("Running") && ready.is_none_or(|status| status == "True")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn snapshot_adapter_classifies_brewfs_resources_and_filters_pods() {
        let snapshot = CsiResourceSnapshot {
            storageclasses: vec![
                json!({
                    "metadata": { "name": "brewfs-sc" },
                    "provisioner": DEFAULT_DRIVER_NAME
                }),
                json!({
                    "metadata": { "name": "other-sc" },
                    "provisioner": "other.example.com"
                }),
            ],
            persistentvolumes: vec![
                json!({
                    "metadata": { "name": "pv-data" },
                    "spec": {
                        "storageClassName": "brewfs-sc",
                        "csi": {
                            "driver": DEFAULT_DRIVER_NAME,
                            "volumeHandle": "data"
                        }
                    }
                }),
                json!({
                    "metadata": { "name": "pv-cache" },
                    "spec": {
                        "storageClassName": "other-sc",
                        "csi": { "driver": "other.example.com" }
                    }
                }),
            ],
            persistentvolumeclaims: vec![
                json!({
                    "metadata": { "name": "data", "namespace": "prod" },
                    "spec": {
                        "storageClassName": "brewfs-sc",
                        "volumeName": "pv-data"
                    }
                }),
                json!({
                    "metadata": { "name": "cache", "namespace": "prod" },
                    "spec": {
                        "storageClassName": "other-sc",
                        "volumeName": "pv-cache"
                    }
                }),
            ],
            pods: vec![
                json!({
                    "metadata": { "name": "api", "namespace": "prod" },
                    "spec": {
                        "volumes": [
                            { "name": "data", "persistentVolumeClaim": { "claimName": "data" } }
                        ]
                    },
                    "status": {
                        "phase": "Running",
                        "conditions": [{ "type": "Ready", "status": "True" }]
                    }
                }),
                json!({
                    "metadata": { "name": "stuck", "namespace": "prod" },
                    "spec": {
                        "volumes": [
                            { "name": "inline", "csi": { "driver": DEFAULT_DRIVER_NAME } }
                        ]
                    },
                    "status": {
                        "phase": "Pending",
                        "conditions": [{ "type": "Ready", "status": "False" }]
                    }
                }),
                json!({
                    "metadata": { "name": "worker", "namespace": "prod" },
                    "spec": {
                        "volumes": [
                            { "name": "cache", "persistentVolumeClaim": { "claimName": "cache" } }
                        ]
                    },
                    "status": { "phase": "Running" }
                }),
            ],
        };
        let adapter = SnapshotCsiAdapter::new(DEFAULT_DRIVER_NAME, snapshot);

        let summary = adapter.summary().await.unwrap();
        assert_eq!(
            summary,
            CsiSummary {
                storageclasses: 1,
                persistentvolumes: 1,
                persistentvolumeclaims: 1,
                pods: 2,
                unhealthy_mounts: 1,
            }
        );
        assert_eq!(
            resource_names(adapter.storageclasses().await.unwrap().items.as_slice()),
            vec!["brewfs-sc"]
        );
        assert_eq!(
            resource_names(adapter.persistentvolumes().await.unwrap().items.as_slice()),
            vec!["pv-data"]
        );
        assert_eq!(
            resource_names(
                adapter
                    .persistentvolumeclaims(&CsiResourceQuery {
                        namespace: Some("prod".to_string()),
                        volume: None,
                    })
                    .await
                    .unwrap()
                    .items
                    .as_slice()
            ),
            vec!["data"]
        );
        assert_eq!(
            resource_names(
                adapter
                    .pods(&CsiResourceQuery {
                        namespace: Some("prod".to_string()),
                        volume: Some("data".to_string()),
                    })
                    .await
                    .unwrap()
                    .items
                    .as_slice()
            ),
            vec!["api"]
        );
    }

    #[tokio::test]
    async fn snapshot_adapter_discovers_label_and_annotation_marked_resources() {
        let snapshot = CsiResourceSnapshot {
            storageclasses: vec![json!({
                "metadata": {
                    "name": "labeled-sc",
                    "labels": { "app.kubernetes.io/name": "brewfs" }
                },
                "provisioner": "external.example.com"
            })],
            persistentvolumes: vec![json!({
                "metadata": {
                    "name": "annotated-pv",
                    "annotations": { "csi.brewfs.io/driver": DEFAULT_DRIVER_NAME }
                },
                "spec": { "storageClassName": "external-sc" }
            })],
            persistentvolumeclaims: vec![json!({
                "metadata": {
                    "name": "marked-claim",
                    "namespace": "prod",
                    "labels": { "brewfs.io/filesystem": "reports" }
                },
                "spec": { "storageClassName": "external-sc" }
            })],
            pods: vec![json!({
                "metadata": { "name": "reader", "namespace": "prod" },
                "spec": {
                    "volumes": [
                        {
                            "name": "reports",
                            "persistentVolumeClaim": { "claimName": "marked-claim" }
                        }
                    ]
                },
                "status": {
                    "phase": "Running",
                    "conditions": [{ "type": "Ready", "status": "True" }]
                }
            })],
        };
        let adapter = SnapshotCsiAdapter::new(DEFAULT_DRIVER_NAME, snapshot);

        assert_eq!(
            resource_names(adapter.storageclasses().await.unwrap().items.as_slice()),
            vec!["labeled-sc"]
        );
        assert_eq!(
            resource_names(adapter.persistentvolumes().await.unwrap().items.as_slice()),
            vec!["annotated-pv"]
        );
        assert_eq!(
            resource_names(
                adapter
                    .persistentvolumeclaims(&CsiResourceQuery {
                        namespace: Some("prod".to_string()),
                        volume: None,
                    })
                    .await
                    .unwrap()
                    .items
                    .as_slice()
            ),
            vec!["marked-claim"]
        );
        assert_eq!(
            resource_names(
                adapter
                    .pods(&CsiResourceQuery {
                        namespace: Some("prod".to_string()),
                        volume: Some("marked-claim".to_string()),
                    })
                    .await
                    .unwrap()
                    .items
                    .as_slice()
            ),
            vec!["reader"]
        );
    }

    fn resource_names(items: &[serde_json::Value]) -> Vec<&str> {
        items
            .iter()
            .filter_map(|item| item.pointer("/metadata/name")?.as_str())
            .collect()
    }
}
