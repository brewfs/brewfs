//! Feature-local workspace metrics. Flat `.stats` remains unchanged.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

#[derive(Default)]
pub struct WorkspaceMetrics {
    mount_ok: AtomicU64,
    mount_err: AtomicU64,
    fork_ok: AtomicU64,
    fork_err: AtomicU64,
    seal_ok: AtomicU64,
    seal_err: AtomicU64,
    publish_ok: AtomicU64,
    publish_err: AtomicU64,
    fork_control_latency_ns: AtomicU64,
    fork_drain_latency_ns: AtomicU64,
    quiesce_latency_ns: AtomicU64,
    seal_pending_bytes: AtomicU64,
    publish_changed_paths: AtomicU64,
    layer_depth: AtomicU64,
    resolver_steps: AtomicU64,
    extent_plan_segments: AtomicU64,
    private_bytes_written: AtomicU64,
    parent_bytes_read: AtomicU64,
    shared_cache_hits: AtomicU64,
    fenced_writes: AtomicU64,
    active_leases: AtomicU64,
    gc_reachable_layers: AtomicU64,
    gc_orphan_bytes: AtomicU64,
    compaction_bytes: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WorkspaceMetricsSnapshot {
    pub mount_ok: u64,
    pub mount_err: u64,
    pub fork_ok: u64,
    pub fork_err: u64,
    pub seal_ok: u64,
    pub seal_err: u64,
    pub publish_ok: u64,
    pub publish_err: u64,
    pub fork_control_latency_ns: u64,
    pub fork_drain_latency_ns: u64,
    pub quiesce_latency_ns: u64,
    pub seal_pending_bytes: u64,
    pub publish_changed_paths: u64,
    pub layer_depth: u64,
    pub resolver_steps: u64,
    pub extent_plan_segments: u64,
    pub private_bytes_written: u64,
    pub parent_bytes_read: u64,
    pub shared_cache_hits: u64,
    pub fenced_writes: u64,
    pub active_leases: u64,
    pub gc_reachable_layers: u64,
    pub gc_orphan_bytes: u64,
    pub compaction_bytes: u64,
}

impl WorkspaceMetrics {
    pub fn record_mount(&self, ok: bool) {
        counter(ok, &self.mount_ok, &self.mount_err);
    }
    pub fn record_fork(&self, ok: bool) {
        counter(ok, &self.fork_ok, &self.fork_err);
    }
    pub fn record_seal(&self, ok: bool) {
        counter(ok, &self.seal_ok, &self.seal_err);
    }
    pub fn record_publish(&self, ok: bool) {
        counter(ok, &self.publish_ok, &self.publish_err);
    }
    pub fn add_fork_control_latency_ns(&self, value: u64) {
        self.fork_control_latency_ns
            .fetch_add(value, Ordering::Relaxed);
    }
    pub fn add_fork_drain_latency_ns(&self, value: u64) {
        self.fork_drain_latency_ns
            .fetch_add(value, Ordering::Relaxed);
    }
    pub fn add_quiesce_latency_ns(&self, value: u64) {
        self.quiesce_latency_ns.fetch_add(value, Ordering::Relaxed);
    }
    pub fn set_seal_pending_bytes(&self, value: u64) {
        self.seal_pending_bytes.store(value, Ordering::Relaxed);
    }
    pub fn add_publish_changed_paths(&self, value: u64) {
        self.publish_changed_paths
            .fetch_add(value, Ordering::Relaxed);
    }
    pub fn set_layer_depth(&self, value: u64) {
        self.layer_depth.store(value, Ordering::Relaxed);
    }
    pub fn add_resolver_steps(&self, value: u64) {
        self.resolver_steps.fetch_add(value, Ordering::Relaxed);
    }
    pub fn add_extent_plan_segments(&self, value: u64) {
        self.extent_plan_segments
            .fetch_add(value, Ordering::Relaxed);
    }
    pub fn add_private_bytes_written(&self, value: u64) {
        self.private_bytes_written
            .fetch_add(value, Ordering::Relaxed);
    }
    pub fn add_parent_bytes_read(&self, value: u64) {
        self.parent_bytes_read.fetch_add(value, Ordering::Relaxed);
    }
    pub fn record_shared_cache_hit(&self) {
        self.shared_cache_hits.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_fenced_write(&self) {
        self.fenced_writes.fetch_add(1, Ordering::Relaxed);
    }
    pub fn add_active_lease(&self) {
        self.active_leases.fetch_add(1, Ordering::Relaxed);
    }
    pub fn remove_active_lease(&self) {
        let _ = self
            .active_leases
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                Some(value.saturating_sub(1))
            });
    }
    pub fn set_active_leases(&self, value: u64) {
        self.active_leases.store(value, Ordering::Relaxed);
    }
    pub fn set_gc(&self, reachable_layers: u64, orphan_bytes: u64) {
        self.gc_reachable_layers
            .store(reachable_layers, Ordering::Relaxed);
        self.gc_orphan_bytes.store(orphan_bytes, Ordering::Relaxed);
    }
    pub fn add_compaction_bytes(&self, value: u64) {
        self.compaction_bytes.fetch_add(value, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> WorkspaceMetricsSnapshot {
        macro_rules! load {
            ($field:ident) => {
                self.$field.load(Ordering::Relaxed)
            };
        }
        WorkspaceMetricsSnapshot {
            mount_ok: load!(mount_ok),
            mount_err: load!(mount_err),
            fork_ok: load!(fork_ok),
            fork_err: load!(fork_err),
            seal_ok: load!(seal_ok),
            seal_err: load!(seal_err),
            publish_ok: load!(publish_ok),
            publish_err: load!(publish_err),
            fork_control_latency_ns: load!(fork_control_latency_ns),
            fork_drain_latency_ns: load!(fork_drain_latency_ns),
            quiesce_latency_ns: load!(quiesce_latency_ns),
            seal_pending_bytes: load!(seal_pending_bytes),
            publish_changed_paths: load!(publish_changed_paths),
            layer_depth: load!(layer_depth),
            resolver_steps: load!(resolver_steps),
            extent_plan_segments: load!(extent_plan_segments),
            private_bytes_written: load!(private_bytes_written),
            parent_bytes_read: load!(parent_bytes_read),
            shared_cache_hits: load!(shared_cache_hits),
            fenced_writes: load!(fenced_writes),
            active_leases: load!(active_leases),
            gc_reachable_layers: load!(gc_reachable_layers),
            gc_orphan_bytes: load!(gc_orphan_bytes),
            compaction_bytes: load!(compaction_bytes),
        }
    }

    pub fn render_prometheus(&self) -> String {
        let value = self.snapshot();
        let mut lines = vec![
            metric_label("brewfs_workspace_mount_total", "ok", value.mount_ok),
            metric_label("brewfs_workspace_mount_total", "error", value.mount_err),
            metric_label("brewfs_workspace_fork_total", "ok", value.fork_ok),
            metric_label("brewfs_workspace_fork_total", "error", value.fork_err),
            metric_label("brewfs_workspace_seal_total", "ok", value.seal_ok),
            metric_label("brewfs_workspace_seal_total", "error", value.seal_err),
            metric_label("brewfs_workspace_publish_total", "ok", value.publish_ok),
            metric_label("brewfs_workspace_publish_total", "error", value.publish_err),
            metric_seconds(
                "brewfs_workspace_fork_control_latency_seconds",
                value.fork_control_latency_ns,
            ),
            metric_seconds(
                "brewfs_workspace_fork_drain_latency_seconds",
                value.fork_drain_latency_ns,
            ),
            metric_seconds(
                "brewfs_workspace_quiesce_latency_seconds",
                value.quiesce_latency_ns,
            ),
            metric(
                "brewfs_workspace_seal_pending_bytes",
                value.seal_pending_bytes,
            ),
            metric(
                "brewfs_workspace_publish_changed_paths",
                value.publish_changed_paths,
            ),
            metric("brewfs_workspace_layer_depth", value.layer_depth),
            metric("brewfs_workspace_resolver_steps", value.resolver_steps),
            metric(
                "brewfs_workspace_extent_plan_segments",
                value.extent_plan_segments,
            ),
            metric(
                "brewfs_workspace_private_bytes_written",
                value.private_bytes_written,
            ),
            metric(
                "brewfs_workspace_parent_bytes_read",
                value.parent_bytes_read,
            ),
            metric(
                "brewfs_workspace_shared_cache_hits",
                value.shared_cache_hits,
            ),
            metric("brewfs_workspace_active_leases", value.active_leases),
            metric("brewfs_workspace_fenced_writes_total", value.fenced_writes),
            metric(
                "brewfs_workspace_gc_reachable_layers",
                value.gc_reachable_layers,
            ),
            metric("brewfs_workspace_gc_orphan_bytes", value.gc_orphan_bytes),
            metric("brewfs_workspace_compaction_bytes", value.compaction_bytes),
        ];
        lines.push(String::new());
        lines.join("\n")
    }
}

pub fn global_workspace_metrics() -> Arc<WorkspaceMetrics> {
    static METRICS: OnceLock<Arc<WorkspaceMetrics>> = OnceLock::new();
    METRICS
        .get_or_init(|| Arc::new(WorkspaceMetrics::default()))
        .clone()
}

fn counter(ok: bool, success: &AtomicU64, error: &AtomicU64) {
    if ok { success } else { error }.fetch_add(1, Ordering::Relaxed);
}

fn metric(name: &str, value: u64) -> String {
    format!("{name} {value}")
}

fn metric_label(name: &str, result: &str, value: u64) -> String {
    format!(r#"{name}{{result="{result}"}} {value}"#)
}

fn metric_seconds(name: &str, nanoseconds: u64) -> String {
    format!("{name} {}", nanoseconds as f64 / 1_000_000_000.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_are_feature_local_and_render_all_stable_names() {
        let metrics = WorkspaceMetrics::default();
        metrics.record_mount(true);
        metrics.record_fenced_write();
        let rendered = metrics.render_prometheus();
        assert!(rendered.contains("brewfs_workspace_mount_total{result=\"ok\"} 1"));
        assert!(rendered.contains("brewfs_workspace_fenced_writes_total 1"));
        for name in [
            "brewfs_workspace_fork_control_latency_seconds",
            "brewfs_workspace_fork_drain_latency_seconds",
            "brewfs_workspace_quiesce_latency_seconds",
            "brewfs_workspace_publish_changed_paths",
            "brewfs_workspace_shared_cache_hits",
        ] {
            assert!(rendered.contains(name), "missing {name}");
        }
    }
}
