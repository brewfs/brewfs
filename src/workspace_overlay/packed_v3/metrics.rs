use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use super::catalog::RemoteGroupCatalog;
use crate::cadapter::client::ObjectBackend;

pub const SIZE_CLASS_COUNT: usize = 4;

#[derive(Default)]
pub struct PackedRuntimeMetrics {
    data_range_gets: AtomicU64,
    data_range_bytes: AtomicU64,
    logical_bytes: AtomicU64,
    overscan_bytes: AtomicU64,
    frames_decoded: AtomicU64,
    coalesced_ranges: AtomicU64,
    inflight_singleflight: AtomicU64,
    pipeline_current: AtomicU64,
    pipeline_peak: AtomicU64,
    prefetched_logical_bytes: AtomicU64,
    data_cache_hits: AtomicU64,
    decoded_frame_cache_hits: AtomicU64,
    decoded_frame_cache_misses: AtomicU64,
    decoded_frame_cache_evictions: AtomicU64,
    window_cache_hits: AtomicU64,
    window_cache_misses: AtomicU64,
    window_remote_fetches: AtomicU64,
    size_class_frames: [AtomicU64; SIZE_CLASS_COUNT],
    size_class_raw_bytes: [AtomicU64; SIZE_CLASS_COUNT],
    size_class_overscan_bytes: [AtomicU64; SIZE_CLASS_COUNT],
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PackedRuntimeMetricsSnapshot {
    pub data_range_gets: u64,
    pub data_range_bytes: u64,
    pub logical_bytes: u64,
    pub overscan_bytes: u64,
    pub frames_decoded: u64,
    pub coalesced_ranges: u64,
    pub inflight_singleflight: u64,
    pub pipeline_bytes_current: u64,
    pub pipeline_bytes_peak: u64,
    pub prefetched_logical_bytes: u64,
    pub data_cache_hits: u64,
    pub decoded_frame_cache_configured_bytes: u64,
    pub decoded_frame_cache_entries: u64,
    pub decoded_frame_cache_resident_bytes: u64,
    pub decoded_frame_cache_hits: u64,
    pub decoded_frame_cache_misses: u64,
    pub decoded_frame_cache_evictions: u64,
    pub window_cache_hits: u64,
    pub window_cache_misses: u64,
    pub window_remote_fetches: u64,
    pub frames_by_size_class: [u64; SIZE_CLASS_COUNT],
    pub frame_raw_bytes_by_size_class: [u64; SIZE_CLASS_COUNT],
    pub overscan_by_size_class: [u64; SIZE_CLASS_COUNT],
}

impl PackedRuntimeMetrics {
    pub fn record_data_range(&self, bytes: u64) {
        self.data_range_gets.fetch_add(1, Ordering::Relaxed);
        self.data_range_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_data_cache_hit(&self) {
        self.data_cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_logical_bytes(&self, logical_bytes: u64) {
        self.logical_bytes
            .fetch_add(logical_bytes, Ordering::Relaxed);
    }

    pub fn record_overscan(&self, overscan_bytes: u64, class: usize) {
        self.overscan_bytes
            .fetch_add(overscan_bytes, Ordering::Relaxed);
        let class = class.min(SIZE_CLASS_COUNT - 1);
        self.size_class_overscan_bytes[class].fetch_add(overscan_bytes, Ordering::Relaxed);
    }

    pub fn record_frame(&self, class: usize, raw_bytes: u64) {
        self.frames_decoded.fetch_add(1, Ordering::Relaxed);
        let class = class.min(SIZE_CLASS_COUNT - 1);
        self.size_class_frames[class].fetch_add(1, Ordering::Relaxed);
        self.size_class_raw_bytes[class].fetch_add(raw_bytes, Ordering::Relaxed);
    }

    pub fn record_coalesced_ranges(&self, count: u64) {
        self.coalesced_ranges.fetch_add(count, Ordering::Relaxed);
    }

    pub fn record_singleflight(&self, count: u64) {
        self.inflight_singleflight
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn pipeline_acquire(&self, bytes: u64) {
        let current = self.pipeline_current.fetch_add(bytes, Ordering::Relaxed) + bytes;
        self.pipeline_peak.fetch_max(current, Ordering::Relaxed);
    }

    pub fn pipeline_release(&self, bytes: u64) {
        let _ = self
            .pipeline_current
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(bytes))
            });
    }

    pub fn record_prefetched_logical_bytes(&self, bytes: u64) {
        self.prefetched_logical_bytes
            .fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_decoded_frame_cache_hit(&self) {
        self.decoded_frame_cache_hits
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_decoded_frame_cache_miss(&self) {
        self.decoded_frame_cache_misses
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_decoded_frame_cache_eviction(&self) {
        self.decoded_frame_cache_evictions
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_window_hit(&self) {
        self.window_cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_window_miss(&self) {
        self.window_cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_window_fetch(&self, bytes: u64) {
        self.window_remote_fetches.fetch_add(1, Ordering::Relaxed);
        self.record_data_range(bytes);
    }

    pub fn snapshot(&self) -> PackedRuntimeMetricsSnapshot {
        let load = |value: &AtomicU64| value.load(Ordering::Relaxed);
        PackedRuntimeMetricsSnapshot {
            data_range_gets: load(&self.data_range_gets),
            data_range_bytes: load(&self.data_range_bytes),
            logical_bytes: load(&self.logical_bytes),
            overscan_bytes: load(&self.overscan_bytes),
            frames_decoded: load(&self.frames_decoded),
            coalesced_ranges: load(&self.coalesced_ranges),
            inflight_singleflight: load(&self.inflight_singleflight),
            pipeline_bytes_current: load(&self.pipeline_current),
            pipeline_bytes_peak: load(&self.pipeline_peak),
            prefetched_logical_bytes: load(&self.prefetched_logical_bytes),
            data_cache_hits: load(&self.data_cache_hits),
            decoded_frame_cache_configured_bytes: 0,
            decoded_frame_cache_entries: 0,
            decoded_frame_cache_resident_bytes: 0,
            decoded_frame_cache_hits: load(&self.decoded_frame_cache_hits),
            decoded_frame_cache_misses: load(&self.decoded_frame_cache_misses),
            decoded_frame_cache_evictions: load(&self.decoded_frame_cache_evictions),
            window_cache_hits: load(&self.window_cache_hits),
            window_cache_misses: load(&self.window_cache_misses),
            window_remote_fetches: load(&self.window_remote_fetches),
            frames_by_size_class: std::array::from_fn(|index| load(&self.size_class_frames[index])),
            frame_raw_bytes_by_size_class: std::array::from_fn(|index| {
                load(&self.size_class_raw_bytes[index])
            }),
            overscan_by_size_class: std::array::from_fn(|index| {
                load(&self.size_class_overscan_bytes[index])
            }),
        }
    }
}

/// A weak reference keeps `.stats` from extending the catalog's lifetime or
/// retaining a second copy of its metadata/payload caches.
pub struct PackedStatsExtension<B: ObjectBackend + Clone> {
    catalog: Weak<RemoteGroupCatalog<B>>,
}

impl<B: ObjectBackend + Clone> std::fmt::Debug for PackedStatsExtension<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PackedStatsExtension")
            .finish_non_exhaustive()
    }
}

impl<B: ObjectBackend + Clone> PackedStatsExtension<B> {
    pub fn new(catalog: &Arc<RemoteGroupCatalog<B>>) -> Self {
        Self {
            catalog: Arc::downgrade(catalog),
        }
    }
}

impl<B: ObjectBackend + Clone + 'static> crate::vfs::stats::FsStatsExtension
    for PackedStatsExtension<B>
{
    fn render_into(&self, output: &mut dyn std::fmt::Write) {
        let Some(catalog) = self.catalog.upgrade() else {
            return;
        };
        let metadata = catalog.metadata_cache_stats();
        let runtime = catalog.packed_runtime_metrics();
        macro_rules! metric {
            ($name:literal, $value:expr) => {
                let _ = writeln!(output, concat!("brewfs_packed_v3_", $name, " {}"), $value);
            };
        }
        metric!("group_index_gets_total", metadata.group_index_remote_gets);
        metric!("inode_index_gets_total", metadata.inode_index_remote_gets);
        metric!("group_meta_gets_total", metadata.group_meta_remote_gets);
        // These legacy metadata counters measure declared range bytes, not
        // partially received failure bodies. Name them explicitly until the
        // transport observation supplies actual consumed-byte counters.
        metric!(
            "group_index_requested_bytes_total",
            metadata.group_index_remote_bytes
        );
        metric!(
            "inode_index_requested_bytes_total",
            metadata.inode_index_remote_bytes
        );
        metric!(
            "group_meta_requested_bytes_total",
            metadata.group_meta_remote_bytes
        );
        metric!(
            "frame_directory_gets_total",
            metadata.frame_directory_remote_gets
        );
        metric!(
            "frame_directory_requested_bytes_total",
            metadata.frame_directory_remote_bytes
        );
        metric!(
            "frame_descriptor_gets_total",
            metadata.frame_descriptor_remote_gets
        );
        metric!(
            "frame_descriptor_requested_bytes_total",
            metadata.frame_descriptor_remote_bytes
        );
        metric!("metadata_cache_hit_total", metadata.hits);
        metric!("metadata_cache_miss_total", metadata.misses);
        metric!("metadata_index_cache_hit_total", metadata.index_hits);
        metric!("metadata_index_cache_miss_total", metadata.index_misses);
        metric!("metadata_group_cache_hit_total", metadata.group_meta_hits);
        metric!(
            "metadata_group_cache_miss_total",
            metadata.group_meta_misses
        );
        metric!("metadata_locator_cache_hit_total", metadata.locator_hits);
        metric!("metadata_locator_cache_miss_total", metadata.locator_misses);
        metric!("metadata_inode_cache_hit_total", metadata.inode_entry_hits);
        metric!(
            "metadata_inode_cache_miss_total",
            metadata.inode_entry_misses
        );
        metric!("group_index_resident_bytes", metadata.group_index_bytes);
        metric!("inode_index_resident_bytes", metadata.inode_index_bytes);
        metric!("group_meta_resident_bytes", metadata.group_meta_bytes);
        metric!("inode_entry_resident_bytes", metadata.inode_entry_bytes);
        metric!("file_locator_resident_bytes", metadata.file_locator_bytes);
        metric!(
            "frame_directory_resident_bytes",
            metadata.frame_directory_bytes
        );
        metric!(
            "frame_descriptor_resident_bytes",
            metadata.frame_descriptor_bytes
        );
        metric!("data_range_gets_total", runtime.data_range_gets);
        metric!("data_range_requested_bytes_total", runtime.data_range_bytes);
        metric!("logical_bytes_total", runtime.logical_bytes);
        metric!("overscan_bytes_total", runtime.overscan_bytes);
        metric!("frames_decoded_total", runtime.frames_decoded);
        metric!("coalesced_ranges_total", runtime.coalesced_ranges);
        metric!("inflight_singleflight_total", runtime.inflight_singleflight);
        metric!("pipeline_bytes_current", runtime.pipeline_bytes_current);
        metric!("pipeline_bytes_peak", runtime.pipeline_bytes_peak);
        metric!(
            "prefetched_logical_bytes_total",
            runtime.prefetched_logical_bytes
        );
        metric!("data_cache_hit_total", runtime.data_cache_hits);
        metric!(
            "decoded_frame_cache_configured_bytes",
            runtime.decoded_frame_cache_configured_bytes
        );
        metric!(
            "decoded_frame_cache_resident_bytes",
            runtime.decoded_frame_cache_resident_bytes
        );
        metric!(
            "decoded_frame_cache_hit_total",
            runtime.decoded_frame_cache_hits
        );
        metric!(
            "decoded_frame_cache_miss_total",
            runtime.decoded_frame_cache_misses
        );
        metric!(
            "decoded_frame_cache_evictions_total",
            runtime.decoded_frame_cache_evictions
        );
        metric!("window_cache_hit_total", runtime.window_cache_hits);
        metric!("window_cache_miss_total", runtime.window_cache_misses);
        metric!("window_remote_fetches_total", runtime.window_remote_fetches);
        for class in 0..SIZE_CLASS_COUNT {
            let _ = writeln!(
                output,
                "brewfs_packed_v3_frames_by_size_class_total{{class=\"{class}\"}} {}",
                runtime.frames_by_size_class[class]
            );
            let _ = writeln!(
                output,
                "brewfs_packed_v3_frame_raw_bytes_by_size_class_total{{class=\"{class}\"}} {}",
                runtime.frame_raw_bytes_by_size_class[class]
            );
            let _ = writeln!(
                output,
                "brewfs_packed_v3_overscan_by_size_class_total{{class=\"{class}\"}} {}",
                runtime.overscan_by_size_class[class]
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_metrics_track_pipeline_and_size_classes() {
        let metrics = PackedRuntimeMetrics::default();
        metrics.record_data_range(128);
        metrics.record_logical_bytes(100);
        metrics.record_overscan(28, 2);
        metrics.record_frame(2, 100);
        metrics.record_coalesced_ranges(1);
        metrics.pipeline_acquire(128);
        metrics.pipeline_release(128);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.data_range_gets, 1);
        assert_eq!(snapshot.data_range_bytes, 128);
        assert_eq!(snapshot.logical_bytes, 100);
        assert_eq!(snapshot.overscan_bytes, 28);
        assert_eq!(snapshot.frames_by_size_class[2], 1);
        assert_eq!(snapshot.pipeline_bytes_peak, 128);
        assert_eq!(snapshot.pipeline_bytes_current, 0);
    }
}
