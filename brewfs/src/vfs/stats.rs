//! Real-time performance statistics collector for BrewFS.
//!
//! Provides atomic counters for FUSE operations, metadata ops, S3 object
//! traffic, and buffer usage. Metrics are exposed via a `.stats` virtual
//! file at the mount root (similar to JuiceFS) in a Prometheus-compatible
//! text format.
//!
//! The `stats` CLI tool reads this file periodically to display real-time
//! throughput and latency in the terminal.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Relaxed ordering is sufficient for stats counters — we only need eventual
/// visibility, not happens-before relationships.
const ORD: Ordering = Ordering::Relaxed;

/// Point-in-time copy of the counters exposed through `.stats`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FsStatsSnapshot {
    pub uptime_seconds: u64,
    pub fuse_read_ops: u64,
    pub fuse_read_bytes: u64,
    pub fuse_read_lat_us: u64,
    pub fuse_write_ops: u64,
    pub fuse_write_bytes: u64,
    pub fuse_write_lat_us: u64,
    pub fuse_lookup_ops: u64,
    pub fuse_lookup_lat_us: u64,
    pub fuse_getattr_ops: u64,
    pub fuse_getattr_lat_us: u64,
    pub fuse_open_ops: u64,
    pub fuse_create_ops: u64,
    pub fuse_unlink_ops: u64,
    pub fuse_readdir_ops: u64,
    pub fuse_flush_ops: u64,
    pub fuse_flush_lat_us: u64,
    pub meta_ops: u64,
    pub meta_lat_us: u64,
    pub meta_txn_ops: u64,
    pub meta_txn_lat_us: u64,
    pub vfs_create_total_ops: u64,
    pub vfs_create_total_lat_us: u64,
    pub vfs_create_meta_ops: u64,
    pub vfs_create_meta_lat_us: u64,
    pub vfs_unlink_total_ops: u64,
    pub vfs_unlink_total_lat_us: u64,
    pub vfs_unlink_lookup_ops: u64,
    pub vfs_unlink_lookup_lat_us: u64,
    pub vfs_unlink_stat_ops: u64,
    pub vfs_unlink_stat_lat_us: u64,
    pub vfs_unlink_meta_ops: u64,
    pub vfs_unlink_meta_lat_us: u64,
    pub vfs_unlink_recent_ops: u64,
    pub vfs_unlink_recent_lat_us: u64,
    pub vfs_setattr_recent_remove_ops: u64,
    pub vfs_setattr_recent_remove_lat_us: u64,
    pub vfs_setattr_recent_get_mut_ops: u64,
    pub vfs_setattr_recent_get_mut_lat_us: u64,
    pub vfs_read_dirty_probe_ops: u64,
    pub vfs_read_dirty_probe_lat_us: u64,
    pub vfs_read_handle_ops: u64,
    pub vfs_read_handle_lat_us: u64,
    pub vfs_read_overlay_ops: u64,
    pub vfs_read_overlay_lat_us: u64,
    pub s3_get_ops: u64,
    pub s3_get_bytes: u64,
    pub s3_get_lat_us: u64,
    pub s3_put_ops: u64,
    pub s3_put_bytes: u64,
    pub s3_put_lat_us: u64,
    pub s3_put_prepare_lat_us: u64,
    pub s3_put_cache_lat_us: u64,
    pub s3_del_ops: u64,
    pub buf_dirty_bytes: u64,
    pub buf_read_bytes: u64,
    pub writeback_live_dirty_bytes: u64,
    pub writeback_live_slices: u64,
    pub writeback_recent_pending_upload_bytes: u64,
    pub writeback_recent_pending_upload_slices: u64,
    pub writeback_recent_uploaded_bytes: u64,
    pub writeback_recent_uploaded_slices: u64,
    pub writeback_backpressure_soft_sleep_ops: u64,
    pub writeback_backpressure_soft_sleep_us: u64,
    pub writeback_backpressure_hard_wait_ops: u64,
    pub writeback_backpressure_hard_wait_us: u64,
    pub writeback_stage_inflight_bytes: u64,
    pub writeback_remote_upload_inflight_bytes: u64,
    pub writeback_stage_ops: u64,
    pub writeback_stage_bytes: u64,
    pub writeback_stage_lat_us: u64,
    pub writeback_stage_failures: u64,
    pub writeback_commit_before_stage_ops: u64,
    pub writeback_slice_create_ops: u64,
    pub writeback_slice_reuse_ops: u64,
    pub writeback_slice_reject_older_unique_ops: u64,
    pub writeback_slice_reject_dispatched_prefix_ops: u64,
    pub writeback_freeze_size_ops: u64,
    pub writeback_freeze_size_bytes: u64,
    pub writeback_freeze_max_unflushed_ops: u64,
    pub writeback_freeze_max_unflushed_bytes: u64,
    pub writeback_freeze_explicit_flush_ops: u64,
    pub writeback_freeze_explicit_flush_bytes: u64,
    pub writeback_freeze_auto_ops: u64,
    pub writeback_freeze_auto_bytes: u64,
    pub writeback_freeze_commit_age_ops: u64,
    pub writeback_freeze_commit_age_bytes: u64,
    pub writeback_upload_batch_ops: u64,
    pub writeback_upload_batch_bytes: u64,
    pub writeback_upload_batch_blocks: u64,
    pub writeback_upload_partial_tail_ops: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub read_block_cache_hits: u64,
    pub read_page_cache_hits: u64,
    pub read_page_cache_misses: u64,
    pub read_range_gets: u64,
    pub read_full_gets: u64,
    pub read_piggyback_full: u64,
    pub read_background_prefetches: u64,
    pub read_background_prefetch_dropped: u64,
    pub meta_stat_cache_hit: u64,
    pub meta_stat_cache_miss: u64,
    pub meta_stat_fresh_store_hit: u64,
    pub meta_lookup_cache_hit: u64,
    pub meta_lookup_cache_miss: u64,
    pub meta_get_slices_cache_hit: u64,
    pub meta_get_slices_cache_miss: u64,
    pub meta_open_fresh_stat: u64,
    pub meta_open_file_cache_hit: u64,
    pub meta_open_file_cache_miss: u64,
    pub meta_lookup_attr_fused_hit: u64,
    pub meta_lookup_attr_fused_miss: u64,
    pub meta_lookup_attr_fused_error: u64,
}

impl FsStatsSnapshot {
    pub fn cache_requests(&self) -> u64 {
        self.cache_hits + self.cache_misses
    }

    pub fn cache_hit_ratio(&self) -> f64 {
        ratio(self.cache_hits, self.cache_requests())
    }

    pub fn avg_fuse_read_lat_us(&self) -> f64 {
        ratio(self.fuse_read_lat_us, self.fuse_read_ops)
    }

    pub fn avg_fuse_write_lat_us(&self) -> f64 {
        ratio(self.fuse_write_lat_us, self.fuse_write_ops)
    }

    pub fn avg_fuse_flush_lat_us(&self) -> f64 {
        ratio(self.fuse_flush_lat_us, self.fuse_flush_ops)
    }

    pub fn avg_s3_get_lat_us(&self) -> f64 {
        ratio(self.s3_get_lat_us, self.s3_get_ops)
    }

    pub fn avg_s3_put_lat_us(&self) -> f64 {
        ratio(self.s3_put_lat_us, self.s3_put_ops)
    }

    pub fn avg_s3_put_prepare_lat_us(&self) -> f64 {
        ratio(self.s3_put_prepare_lat_us, self.s3_put_ops)
    }

    pub fn avg_s3_put_cache_lat_us(&self) -> f64 {
        ratio(self.s3_put_cache_lat_us, self.s3_put_ops)
    }
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

/// Global filesystem statistics, designed for lock-free concurrent updates.
#[derive(Debug)]
pub struct FsStats {
    pub start_time: Instant,

    // ─── FUSE layer ───────────────────────────────────────────────
    /// Total FUSE read operations
    pub fuse_read_ops: AtomicU64,
    /// Total bytes read via FUSE
    pub fuse_read_bytes: AtomicU64,
    /// Total FUSE read latency in microseconds
    pub fuse_read_lat_us: AtomicU64,
    /// Total FUSE write operations
    pub fuse_write_ops: AtomicU64,
    /// Total bytes written via FUSE
    pub fuse_write_bytes: AtomicU64,
    /// Total FUSE write latency in microseconds
    pub fuse_write_lat_us: AtomicU64,
    /// Total FUSE lookup operations
    pub fuse_lookup_ops: AtomicU64,
    /// Total FUSE lookup latency in microseconds
    pub fuse_lookup_lat_us: AtomicU64,
    /// Total FUSE getattr operations
    pub fuse_getattr_ops: AtomicU64,
    /// Total FUSE getattr latency in microseconds
    pub fuse_getattr_lat_us: AtomicU64,
    /// Total FUSE open operations
    pub fuse_open_ops: AtomicU64,
    /// Total FUSE create operations
    pub fuse_create_ops: AtomicU64,
    /// Total FUSE unlink/rmdir operations
    pub fuse_unlink_ops: AtomicU64,
    /// Total FUSE readdir operations
    pub fuse_readdir_ops: AtomicU64,
    /// Total FUSE flush/fsync operations
    pub fuse_flush_ops: AtomicU64,
    /// Total FUSE flush/fsync latency in microseconds
    pub fuse_flush_lat_us: AtomicU64,

    // ─── Meta layer ──────────────────────────────────────────────
    /// Total metadata operations (get_node, lookup, etc.)
    pub meta_ops: AtomicU64,
    /// Total metadata operation latency in microseconds
    pub meta_lat_us: AtomicU64,
    /// Total metadata transaction (write/commit) operations
    pub meta_txn_ops: AtomicU64,
    /// Total metadata transaction latency in microseconds
    pub meta_txn_lat_us: AtomicU64,

    // ─── VFS diagnostic timing ───────────────────────────────────
    /// Total VFS create_file_at operations timed by the optional diagnostic path
    pub vfs_create_total_ops: AtomicU64,
    /// Total VFS create_file_at latency in microseconds
    pub vfs_create_total_lat_us: AtomicU64,
    /// Total metadata create calls inside create_file_at
    pub vfs_create_meta_ops: AtomicU64,
    /// Total metadata create latency inside create_file_at in microseconds
    pub vfs_create_meta_lat_us: AtomicU64,
    /// Total VFS unlink_at operations timed by the optional diagnostic path
    pub vfs_unlink_total_ops: AtomicU64,
    /// Total VFS unlink_at latency in microseconds
    pub vfs_unlink_total_lat_us: AtomicU64,
    /// Total lookup calls inside unlink_at
    pub vfs_unlink_lookup_ops: AtomicU64,
    /// Total lookup latency inside unlink_at in microseconds
    pub vfs_unlink_lookup_lat_us: AtomicU64,
    /// Total stat calls inside unlink_at
    pub vfs_unlink_stat_ops: AtomicU64,
    /// Total stat latency inside unlink_at in microseconds
    pub vfs_unlink_stat_lat_us: AtomicU64,
    /// Total metadata unlink calls inside unlink_at
    pub vfs_unlink_meta_ops: AtomicU64,
    /// Total metadata unlink latency inside unlink_at in microseconds
    pub vfs_unlink_meta_lat_us: AtomicU64,
    /// Total recently-unlinked map updates inside unlink_at
    pub vfs_unlink_recent_ops: AtomicU64,
    /// Total recently-unlinked map update latency inside unlink_at in microseconds
    pub vfs_unlink_recent_lat_us: AtomicU64,
    /// Total remove-first deleted-inode setattr map probes
    pub vfs_setattr_recent_remove_ops: AtomicU64,
    /// Total remove-first deleted-inode setattr map latency in microseconds
    pub vfs_setattr_recent_remove_lat_us: AtomicU64,
    /// Total get_mut deleted-inode setattr map probes
    pub vfs_setattr_recent_get_mut_ops: AtomicU64,
    /// Total get_mut deleted-inode setattr map latency in microseconds
    pub vfs_setattr_recent_get_mut_lat_us: AtomicU64,
    /// Total dirty-overlay probes before VFS reads committed data
    pub vfs_read_dirty_probe_ops: AtomicU64,
    /// Total dirty-overlay probe latency before VFS reads committed data
    pub vfs_read_dirty_probe_lat_us: AtomicU64,
    /// Total handle.read calls inside VFS read
    pub vfs_read_handle_ops: AtomicU64,
    /// Total handle.read latency inside VFS read
    pub vfs_read_handle_lat_us: AtomicU64,
    /// Total post-read dirty overlay calls inside VFS read
    pub vfs_read_overlay_ops: AtomicU64,
    /// Total post-read dirty overlay latency inside VFS read
    pub vfs_read_overlay_lat_us: AtomicU64,

    // ─── Object storage (S3) layer ───────────────────────────────
    /// Total S3 GET requests
    pub s3_get_ops: AtomicU64,
    /// Total bytes fetched from S3
    pub s3_get_bytes: AtomicU64,
    /// Total S3 GET latency in microseconds
    pub s3_get_lat_us: AtomicU64,
    /// Total S3 PUT requests
    pub s3_put_ops: AtomicU64,
    /// Total bytes uploaded to S3
    pub s3_put_bytes: AtomicU64,
    /// Total S3 PUT latency in microseconds
    pub s3_put_lat_us: AtomicU64,
    /// Total block preparation latency before S3 PUT in microseconds
    pub s3_put_prepare_lat_us: AtomicU64,
    /// Total write-cache population latency after S3 PUT in microseconds
    pub s3_put_cache_lat_us: AtomicU64,
    /// Total S3 DELETE requests
    pub s3_del_ops: AtomicU64,

    // ─── Buffer/cache usage ──────────────────────────────────────
    /// Current dirty write buffer bytes
    pub buf_dirty_bytes: AtomicU64,
    /// Current reader cache bytes
    pub buf_read_bytes: AtomicU64,
    /// Dirty bytes in active writeback slices.
    pub writeback_live_dirty_bytes: AtomicU64,
    /// Active writeback slices currently visible to overlay/commit.
    pub writeback_live_slices: AtomicU64,
    /// Recently committed bytes still waiting for S3 upload completion.
    pub writeback_recent_pending_upload_bytes: AtomicU64,
    /// Recently committed slices still waiting for S3 upload completion.
    pub writeback_recent_pending_upload_slices: AtomicU64,
    /// Recently committed bytes already uploaded to S3 but kept for overlay grace.
    pub writeback_recent_uploaded_bytes: AtomicU64,
    /// Recently committed slices already uploaded to S3 but kept for overlay grace.
    pub writeback_recent_uploaded_slices: AtomicU64,
    /// Soft backpressure sleeps on committed-but-not-uploaded writeback bytes.
    pub writeback_backpressure_soft_sleep_ops: AtomicU64,
    /// Total soft backpressure sleep duration in microseconds.
    pub writeback_backpressure_soft_sleep_us: AtomicU64,
    /// Hard backpressure waits on committed-but-not-uploaded writeback bytes.
    pub writeback_backpressure_hard_wait_ops: AtomicU64,
    /// Total hard backpressure wait duration in microseconds.
    pub writeback_backpressure_hard_wait_us: AtomicU64,
    /// Bytes currently being persisted to the local writeback stage.
    pub writeback_stage_inflight_bytes: AtomicU64,
    /// Bytes currently being uploaded from the writer pipeline to object storage.
    pub writeback_remote_upload_inflight_bytes: AtomicU64,
    /// Total local writeback stage operations.
    pub writeback_stage_ops: AtomicU64,
    /// Total bytes persisted to the local writeback stage.
    pub writeback_stage_bytes: AtomicU64,
    /// Total local writeback stage latency in microseconds.
    pub writeback_stage_lat_us: AtomicU64,
    /// Total local writeback stage failures.
    pub writeback_stage_failures: AtomicU64,
    /// Metadata commits that happened before local staging finished.
    pub writeback_commit_before_stage_ops: AtomicU64,
    /// Newly-created writer slices.
    pub writeback_slice_create_ops: AtomicU64,
    /// Writes that reused an existing writer slice.
    pub writeback_slice_reuse_ops: AtomicU64,
    /// Slice reuse rejects caused by older FUSE unique ordering.
    pub writeback_slice_reject_older_unique_ops: AtomicU64,
    /// Slice reuse rejects caused by already-dispatched/uploaded block prefixes.
    pub writeback_slice_reject_dispatched_prefix_ops: AtomicU64,
    /// Slices frozen because they reached target size or chunk end.
    pub writeback_freeze_size_ops: AtomicU64,
    /// Bytes in slices frozen because they reached target size or chunk end.
    pub writeback_freeze_size_bytes: AtomicU64,
    /// Slices frozen to cap per-chunk unflushed slice count.
    pub writeback_freeze_max_unflushed_ops: AtomicU64,
    /// Bytes in slices frozen to cap per-chunk unflushed slice count.
    pub writeback_freeze_max_unflushed_bytes: AtomicU64,
    /// Slices frozen by explicit flush/fsync/close.
    pub writeback_freeze_explicit_flush_ops: AtomicU64,
    /// Bytes in slices frozen by explicit flush/fsync/close.
    pub writeback_freeze_explicit_flush_bytes: AtomicU64,
    /// Slices frozen by auto-flush, memory pressure, or background pressure flush.
    pub writeback_freeze_auto_ops: AtomicU64,
    /// Bytes in slices frozen by auto-flush, memory pressure, or background pressure flush.
    pub writeback_freeze_auto_bytes: AtomicU64,
    /// Slices frozen by commit-loop age safety.
    pub writeback_freeze_commit_age_ops: AtomicU64,
    /// Bytes in slices frozen by commit-loop age safety.
    pub writeback_freeze_commit_age_bytes: AtomicU64,
    /// Upload batches dispatched from writer slices.
    pub writeback_upload_batch_ops: AtomicU64,
    /// Bytes included in writer upload batches.
    pub writeback_upload_batch_bytes: AtomicU64,
    /// Blocks included in writer upload batches.
    pub writeback_upload_batch_blocks: AtomicU64,
    /// Upload batches that included a frozen partial tail block.
    pub writeback_upload_partial_tail_ops: AtomicU64,
    /// Block cache hit count
    pub cache_hits: AtomicU64,
    /// Block cache miss count
    pub cache_misses: AtomicU64,
    /// Full-block cache read hits
    pub read_block_cache_hits: AtomicU64,
    /// Page-cache read hits
    pub read_page_cache_hits: AtomicU64,
    /// Page-cache read misses
    pub read_page_cache_misses: AtomicU64,
    /// Foreground range GET requests
    pub read_range_gets: AtomicU64,
    /// Foreground full-block GET requests
    pub read_full_gets: AtomicU64,
    /// Reads piggybacked onto an in-flight full-block GET
    pub read_piggyback_full: AtomicU64,
    /// Background full-block prefetches scheduled by range reads
    pub read_background_prefetches: AtomicU64,
    /// Background full-block prefetches dropped before execution
    pub read_background_prefetch_dropped: AtomicU64,
    /// Metadata stat cache hits
    pub meta_stat_cache_hit: AtomicU64,
    /// Metadata stat cache misses
    pub meta_stat_cache_miss: AtomicU64,
    /// Fresh stat store hits
    pub meta_stat_fresh_store_hit: AtomicU64,
    /// Metadata lookup cache hits, including cached negative answers
    pub meta_lookup_cache_hit: AtomicU64,
    /// Metadata lookup cache misses
    pub meta_lookup_cache_miss: AtomicU64,
    /// Metadata slice cache hits
    pub meta_get_slices_cache_hit: AtomicU64,
    /// Metadata slice cache misses
    pub meta_get_slices_cache_miss: AtomicU64,
    /// Fresh stat calls issued for close-to-open/open refresh
    pub meta_open_fresh_stat: AtomicU64,
    /// Open-file scoped metadata cache hits
    pub meta_open_file_cache_hit: AtomicU64,
    /// Open-file scoped metadata cache misses
    pub meta_open_file_cache_miss: AtomicU64,
    /// Fused lookup-with-attr cache hits
    pub meta_lookup_attr_fused_hit: AtomicU64,
    /// Fused lookup-with-attr backend misses
    pub meta_lookup_attr_fused_miss: AtomicU64,
    /// Fused lookup-with-attr backend errors
    pub meta_lookup_attr_fused_error: AtomicU64,
}

impl FsStats {
    pub fn new() -> Self {
        Self {
            start_time: Instant::now(),
            fuse_read_ops: AtomicU64::new(0),
            fuse_read_bytes: AtomicU64::new(0),
            fuse_read_lat_us: AtomicU64::new(0),
            fuse_write_ops: AtomicU64::new(0),
            fuse_write_bytes: AtomicU64::new(0),
            fuse_write_lat_us: AtomicU64::new(0),
            fuse_lookup_ops: AtomicU64::new(0),
            fuse_lookup_lat_us: AtomicU64::new(0),
            fuse_getattr_ops: AtomicU64::new(0),
            fuse_getattr_lat_us: AtomicU64::new(0),
            fuse_open_ops: AtomicU64::new(0),
            fuse_create_ops: AtomicU64::new(0),
            fuse_unlink_ops: AtomicU64::new(0),
            fuse_readdir_ops: AtomicU64::new(0),
            fuse_flush_ops: AtomicU64::new(0),
            fuse_flush_lat_us: AtomicU64::new(0),
            meta_ops: AtomicU64::new(0),
            meta_lat_us: AtomicU64::new(0),
            meta_txn_ops: AtomicU64::new(0),
            meta_txn_lat_us: AtomicU64::new(0),
            vfs_create_total_ops: AtomicU64::new(0),
            vfs_create_total_lat_us: AtomicU64::new(0),
            vfs_create_meta_ops: AtomicU64::new(0),
            vfs_create_meta_lat_us: AtomicU64::new(0),
            vfs_unlink_total_ops: AtomicU64::new(0),
            vfs_unlink_total_lat_us: AtomicU64::new(0),
            vfs_unlink_lookup_ops: AtomicU64::new(0),
            vfs_unlink_lookup_lat_us: AtomicU64::new(0),
            vfs_unlink_stat_ops: AtomicU64::new(0),
            vfs_unlink_stat_lat_us: AtomicU64::new(0),
            vfs_unlink_meta_ops: AtomicU64::new(0),
            vfs_unlink_meta_lat_us: AtomicU64::new(0),
            vfs_unlink_recent_ops: AtomicU64::new(0),
            vfs_unlink_recent_lat_us: AtomicU64::new(0),
            vfs_setattr_recent_remove_ops: AtomicU64::new(0),
            vfs_setattr_recent_remove_lat_us: AtomicU64::new(0),
            vfs_setattr_recent_get_mut_ops: AtomicU64::new(0),
            vfs_setattr_recent_get_mut_lat_us: AtomicU64::new(0),
            vfs_read_dirty_probe_ops: AtomicU64::new(0),
            vfs_read_dirty_probe_lat_us: AtomicU64::new(0),
            vfs_read_handle_ops: AtomicU64::new(0),
            vfs_read_handle_lat_us: AtomicU64::new(0),
            vfs_read_overlay_ops: AtomicU64::new(0),
            vfs_read_overlay_lat_us: AtomicU64::new(0),
            s3_get_ops: AtomicU64::new(0),
            s3_get_bytes: AtomicU64::new(0),
            s3_get_lat_us: AtomicU64::new(0),
            s3_put_ops: AtomicU64::new(0),
            s3_put_bytes: AtomicU64::new(0),
            s3_put_lat_us: AtomicU64::new(0),
            s3_put_prepare_lat_us: AtomicU64::new(0),
            s3_put_cache_lat_us: AtomicU64::new(0),
            s3_del_ops: AtomicU64::new(0),
            buf_dirty_bytes: AtomicU64::new(0),
            buf_read_bytes: AtomicU64::new(0),
            writeback_live_dirty_bytes: AtomicU64::new(0),
            writeback_live_slices: AtomicU64::new(0),
            writeback_recent_pending_upload_bytes: AtomicU64::new(0),
            writeback_recent_pending_upload_slices: AtomicU64::new(0),
            writeback_recent_uploaded_bytes: AtomicU64::new(0),
            writeback_recent_uploaded_slices: AtomicU64::new(0),
            writeback_backpressure_soft_sleep_ops: AtomicU64::new(0),
            writeback_backpressure_soft_sleep_us: AtomicU64::new(0),
            writeback_backpressure_hard_wait_ops: AtomicU64::new(0),
            writeback_backpressure_hard_wait_us: AtomicU64::new(0),
            writeback_stage_inflight_bytes: AtomicU64::new(0),
            writeback_remote_upload_inflight_bytes: AtomicU64::new(0),
            writeback_stage_ops: AtomicU64::new(0),
            writeback_stage_bytes: AtomicU64::new(0),
            writeback_stage_lat_us: AtomicU64::new(0),
            writeback_stage_failures: AtomicU64::new(0),
            writeback_commit_before_stage_ops: AtomicU64::new(0),
            writeback_slice_create_ops: AtomicU64::new(0),
            writeback_slice_reuse_ops: AtomicU64::new(0),
            writeback_slice_reject_older_unique_ops: AtomicU64::new(0),
            writeback_slice_reject_dispatched_prefix_ops: AtomicU64::new(0),
            writeback_freeze_size_ops: AtomicU64::new(0),
            writeback_freeze_size_bytes: AtomicU64::new(0),
            writeback_freeze_max_unflushed_ops: AtomicU64::new(0),
            writeback_freeze_max_unflushed_bytes: AtomicU64::new(0),
            writeback_freeze_explicit_flush_ops: AtomicU64::new(0),
            writeback_freeze_explicit_flush_bytes: AtomicU64::new(0),
            writeback_freeze_auto_ops: AtomicU64::new(0),
            writeback_freeze_auto_bytes: AtomicU64::new(0),
            writeback_freeze_commit_age_ops: AtomicU64::new(0),
            writeback_freeze_commit_age_bytes: AtomicU64::new(0),
            writeback_upload_batch_ops: AtomicU64::new(0),
            writeback_upload_batch_bytes: AtomicU64::new(0),
            writeback_upload_batch_blocks: AtomicU64::new(0),
            writeback_upload_partial_tail_ops: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            read_block_cache_hits: AtomicU64::new(0),
            read_page_cache_hits: AtomicU64::new(0),
            read_page_cache_misses: AtomicU64::new(0),
            read_range_gets: AtomicU64::new(0),
            read_full_gets: AtomicU64::new(0),
            read_piggyback_full: AtomicU64::new(0),
            read_background_prefetches: AtomicU64::new(0),
            read_background_prefetch_dropped: AtomicU64::new(0),
            meta_stat_cache_hit: AtomicU64::new(0),
            meta_stat_cache_miss: AtomicU64::new(0),
            meta_stat_fresh_store_hit: AtomicU64::new(0),
            meta_lookup_cache_hit: AtomicU64::new(0),
            meta_lookup_cache_miss: AtomicU64::new(0),
            meta_get_slices_cache_hit: AtomicU64::new(0),
            meta_get_slices_cache_miss: AtomicU64::new(0),
            meta_open_fresh_stat: AtomicU64::new(0),
            meta_open_file_cache_hit: AtomicU64::new(0),
            meta_open_file_cache_miss: AtomicU64::new(0),
            meta_lookup_attr_fused_hit: AtomicU64::new(0),
            meta_lookup_attr_fused_miss: AtomicU64::new(0),
            meta_lookup_attr_fused_error: AtomicU64::new(0),
        }
    }

    pub fn snapshot(&self) -> FsStatsSnapshot {
        FsStatsSnapshot {
            uptime_seconds: self.start_time.elapsed().as_secs(),
            fuse_read_ops: self.fuse_read_ops.load(ORD),
            fuse_read_bytes: self.fuse_read_bytes.load(ORD),
            fuse_read_lat_us: self.fuse_read_lat_us.load(ORD),
            fuse_write_ops: self.fuse_write_ops.load(ORD),
            fuse_write_bytes: self.fuse_write_bytes.load(ORD),
            fuse_write_lat_us: self.fuse_write_lat_us.load(ORD),
            fuse_lookup_ops: self.fuse_lookup_ops.load(ORD),
            fuse_lookup_lat_us: self.fuse_lookup_lat_us.load(ORD),
            fuse_getattr_ops: self.fuse_getattr_ops.load(ORD),
            fuse_getattr_lat_us: self.fuse_getattr_lat_us.load(ORD),
            fuse_open_ops: self.fuse_open_ops.load(ORD),
            fuse_create_ops: self.fuse_create_ops.load(ORD),
            fuse_unlink_ops: self.fuse_unlink_ops.load(ORD),
            fuse_readdir_ops: self.fuse_readdir_ops.load(ORD),
            fuse_flush_ops: self.fuse_flush_ops.load(ORD),
            fuse_flush_lat_us: self.fuse_flush_lat_us.load(ORD),
            meta_ops: self.meta_ops.load(ORD),
            meta_lat_us: self.meta_lat_us.load(ORD),
            meta_txn_ops: self.meta_txn_ops.load(ORD),
            meta_txn_lat_us: self.meta_txn_lat_us.load(ORD),
            vfs_create_total_ops: self.vfs_create_total_ops.load(ORD),
            vfs_create_total_lat_us: self.vfs_create_total_lat_us.load(ORD),
            vfs_create_meta_ops: self.vfs_create_meta_ops.load(ORD),
            vfs_create_meta_lat_us: self.vfs_create_meta_lat_us.load(ORD),
            vfs_unlink_total_ops: self.vfs_unlink_total_ops.load(ORD),
            vfs_unlink_total_lat_us: self.vfs_unlink_total_lat_us.load(ORD),
            vfs_unlink_lookup_ops: self.vfs_unlink_lookup_ops.load(ORD),
            vfs_unlink_lookup_lat_us: self.vfs_unlink_lookup_lat_us.load(ORD),
            vfs_unlink_stat_ops: self.vfs_unlink_stat_ops.load(ORD),
            vfs_unlink_stat_lat_us: self.vfs_unlink_stat_lat_us.load(ORD),
            vfs_unlink_meta_ops: self.vfs_unlink_meta_ops.load(ORD),
            vfs_unlink_meta_lat_us: self.vfs_unlink_meta_lat_us.load(ORD),
            vfs_unlink_recent_ops: self.vfs_unlink_recent_ops.load(ORD),
            vfs_unlink_recent_lat_us: self.vfs_unlink_recent_lat_us.load(ORD),
            vfs_setattr_recent_remove_ops: self.vfs_setattr_recent_remove_ops.load(ORD),
            vfs_setattr_recent_remove_lat_us: self.vfs_setattr_recent_remove_lat_us.load(ORD),
            vfs_setattr_recent_get_mut_ops: self.vfs_setattr_recent_get_mut_ops.load(ORD),
            vfs_setattr_recent_get_mut_lat_us: self.vfs_setattr_recent_get_mut_lat_us.load(ORD),
            vfs_read_dirty_probe_ops: self.vfs_read_dirty_probe_ops.load(ORD),
            vfs_read_dirty_probe_lat_us: self.vfs_read_dirty_probe_lat_us.load(ORD),
            vfs_read_handle_ops: self.vfs_read_handle_ops.load(ORD),
            vfs_read_handle_lat_us: self.vfs_read_handle_lat_us.load(ORD),
            vfs_read_overlay_ops: self.vfs_read_overlay_ops.load(ORD),
            vfs_read_overlay_lat_us: self.vfs_read_overlay_lat_us.load(ORD),
            s3_get_ops: self.s3_get_ops.load(ORD),
            s3_get_bytes: self.s3_get_bytes.load(ORD),
            s3_get_lat_us: self.s3_get_lat_us.load(ORD),
            s3_put_ops: self.s3_put_ops.load(ORD),
            s3_put_bytes: self.s3_put_bytes.load(ORD),
            s3_put_lat_us: self.s3_put_lat_us.load(ORD),
            s3_put_prepare_lat_us: self.s3_put_prepare_lat_us.load(ORD),
            s3_put_cache_lat_us: self.s3_put_cache_lat_us.load(ORD),
            s3_del_ops: self.s3_del_ops.load(ORD),
            buf_dirty_bytes: self.buf_dirty_bytes.load(ORD),
            buf_read_bytes: self.buf_read_bytes.load(ORD),
            writeback_live_dirty_bytes: self.writeback_live_dirty_bytes.load(ORD),
            writeback_live_slices: self.writeback_live_slices.load(ORD),
            writeback_recent_pending_upload_bytes: self
                .writeback_recent_pending_upload_bytes
                .load(ORD),
            writeback_recent_pending_upload_slices: self
                .writeback_recent_pending_upload_slices
                .load(ORD),
            writeback_recent_uploaded_bytes: self.writeback_recent_uploaded_bytes.load(ORD),
            writeback_recent_uploaded_slices: self.writeback_recent_uploaded_slices.load(ORD),
            writeback_backpressure_soft_sleep_ops: self
                .writeback_backpressure_soft_sleep_ops
                .load(ORD),
            writeback_backpressure_soft_sleep_us: self
                .writeback_backpressure_soft_sleep_us
                .load(ORD),
            writeback_backpressure_hard_wait_ops: self
                .writeback_backpressure_hard_wait_ops
                .load(ORD),
            writeback_backpressure_hard_wait_us: self.writeback_backpressure_hard_wait_us.load(ORD),
            writeback_stage_inflight_bytes: self.writeback_stage_inflight_bytes.load(ORD),
            writeback_remote_upload_inflight_bytes: self
                .writeback_remote_upload_inflight_bytes
                .load(ORD),
            writeback_stage_ops: self.writeback_stage_ops.load(ORD),
            writeback_stage_bytes: self.writeback_stage_bytes.load(ORD),
            writeback_stage_lat_us: self.writeback_stage_lat_us.load(ORD),
            writeback_stage_failures: self.writeback_stage_failures.load(ORD),
            writeback_commit_before_stage_ops: self.writeback_commit_before_stage_ops.load(ORD),
            writeback_slice_create_ops: self.writeback_slice_create_ops.load(ORD),
            writeback_slice_reuse_ops: self.writeback_slice_reuse_ops.load(ORD),
            writeback_slice_reject_older_unique_ops: self
                .writeback_slice_reject_older_unique_ops
                .load(ORD),
            writeback_slice_reject_dispatched_prefix_ops: self
                .writeback_slice_reject_dispatched_prefix_ops
                .load(ORD),
            writeback_freeze_size_ops: self.writeback_freeze_size_ops.load(ORD),
            writeback_freeze_size_bytes: self.writeback_freeze_size_bytes.load(ORD),
            writeback_freeze_max_unflushed_ops: self.writeback_freeze_max_unflushed_ops.load(ORD),
            writeback_freeze_max_unflushed_bytes: self
                .writeback_freeze_max_unflushed_bytes
                .load(ORD),
            writeback_freeze_explicit_flush_ops: self.writeback_freeze_explicit_flush_ops.load(ORD),
            writeback_freeze_explicit_flush_bytes: self
                .writeback_freeze_explicit_flush_bytes
                .load(ORD),
            writeback_freeze_auto_ops: self.writeback_freeze_auto_ops.load(ORD),
            writeback_freeze_auto_bytes: self.writeback_freeze_auto_bytes.load(ORD),
            writeback_freeze_commit_age_ops: self.writeback_freeze_commit_age_ops.load(ORD),
            writeback_freeze_commit_age_bytes: self.writeback_freeze_commit_age_bytes.load(ORD),
            writeback_upload_batch_ops: self.writeback_upload_batch_ops.load(ORD),
            writeback_upload_batch_bytes: self.writeback_upload_batch_bytes.load(ORD),
            writeback_upload_batch_blocks: self.writeback_upload_batch_blocks.load(ORD),
            writeback_upload_partial_tail_ops: self.writeback_upload_partial_tail_ops.load(ORD),
            cache_hits: self.cache_hits.load(ORD),
            cache_misses: self.cache_misses.load(ORD),
            read_block_cache_hits: self.read_block_cache_hits.load(ORD),
            read_page_cache_hits: self.read_page_cache_hits.load(ORD),
            read_page_cache_misses: self.read_page_cache_misses.load(ORD),
            read_range_gets: self.read_range_gets.load(ORD),
            read_full_gets: self.read_full_gets.load(ORD),
            read_piggyback_full: self.read_piggyback_full.load(ORD),
            read_background_prefetches: self.read_background_prefetches.load(ORD),
            read_background_prefetch_dropped: self.read_background_prefetch_dropped.load(ORD),
            meta_stat_cache_hit: self.meta_stat_cache_hit.load(ORD),
            meta_stat_cache_miss: self.meta_stat_cache_miss.load(ORD),
            meta_stat_fresh_store_hit: self.meta_stat_fresh_store_hit.load(ORD),
            meta_lookup_cache_hit: self.meta_lookup_cache_hit.load(ORD),
            meta_lookup_cache_miss: self.meta_lookup_cache_miss.load(ORD),
            meta_get_slices_cache_hit: self.meta_get_slices_cache_hit.load(ORD),
            meta_get_slices_cache_miss: self.meta_get_slices_cache_miss.load(ORD),
            meta_open_fresh_stat: self.meta_open_fresh_stat.load(ORD),
            meta_open_file_cache_hit: self.meta_open_file_cache_hit.load(ORD),
            meta_open_file_cache_miss: self.meta_open_file_cache_miss.load(ORD),
            meta_lookup_attr_fused_hit: self.meta_lookup_attr_fused_hit.load(ORD),
            meta_lookup_attr_fused_miss: self.meta_lookup_attr_fused_miss.load(ORD),
            meta_lookup_attr_fused_error: self.meta_lookup_attr_fused_error.load(ORD),
        }
    }

    pub fn sync_cache_counters(&self, hits: u64, misses: u64) {
        self.cache_hits.store(hits, ORD);
        self.cache_misses.store(misses, ORD);
    }

    pub fn sync_buffer_bytes(&self, dirty_bytes: u64, read_bytes: u64) {
        self.buf_dirty_bytes.store(dirty_bytes, ORD);
        self.buf_read_bytes.store(read_bytes, ORD);
    }

    pub fn sync_writeback_dirty_breakdown(
        &self,
        live_bytes: u64,
        live_slices: u64,
        recent_pending_upload_bytes: u64,
        recent_pending_upload_slices: u64,
        recent_uploaded_bytes: u64,
        recent_uploaded_slices: u64,
    ) {
        self.writeback_live_dirty_bytes.store(live_bytes, ORD);
        self.writeback_live_slices.store(live_slices, ORD);
        self.writeback_recent_pending_upload_bytes
            .store(recent_pending_upload_bytes, ORD);
        self.writeback_recent_pending_upload_slices
            .store(recent_pending_upload_slices, ORD);
        self.writeback_recent_uploaded_bytes
            .store(recent_uploaded_bytes, ORD);
        self.writeback_recent_uploaded_slices
            .store(recent_uploaded_slices, ORD);
    }

    pub fn add_writeback_backpressure_soft_sleep(&self, duration: Duration) {
        self.writeback_backpressure_soft_sleep_ops.fetch_add(1, ORD);
        self.writeback_backpressure_soft_sleep_us
            .fetch_add(duration.as_micros().min(u128::from(u64::MAX)) as u64, ORD);
    }

    pub fn add_writeback_backpressure_hard_wait(&self, duration: Duration) {
        self.writeback_backpressure_hard_wait_ops.fetch_add(1, ORD);
        self.writeback_backpressure_hard_wait_us
            .fetch_add(duration.as_micros().min(u128::from(u64::MAX)) as u64, ORD);
    }

    pub fn sync_writeback_backpressure_metrics(
        &self,
        soft_sleep_ops: u64,
        soft_sleep_us: u64,
        hard_wait_ops: u64,
        hard_wait_us: u64,
    ) {
        self.writeback_backpressure_soft_sleep_ops
            .store(soft_sleep_ops, ORD);
        self.writeback_backpressure_soft_sleep_us
            .store(soft_sleep_us, ORD);
        self.writeback_backpressure_hard_wait_ops
            .store(hard_wait_ops, ORD);
        self.writeback_backpressure_hard_wait_us
            .store(hard_wait_us, ORD);
    }

    #[allow(clippy::too_many_arguments)]
    pub fn sync_writeback_phase_metrics(
        &self,
        stage_inflight_bytes: u64,
        remote_upload_inflight_bytes: u64,
        stage_ops: u64,
        stage_bytes: u64,
        stage_lat_us: u64,
        stage_failures: u64,
        commit_before_stage_ops: u64,
    ) {
        self.writeback_stage_inflight_bytes
            .store(stage_inflight_bytes, ORD);
        self.writeback_remote_upload_inflight_bytes
            .store(remote_upload_inflight_bytes, ORD);
        self.writeback_stage_ops.store(stage_ops, ORD);
        self.writeback_stage_bytes.store(stage_bytes, ORD);
        self.writeback_stage_lat_us.store(stage_lat_us, ORD);
        self.writeback_stage_failures.store(stage_failures, ORD);
        self.writeback_commit_before_stage_ops
            .store(commit_before_stage_ops, ORD);
    }

    pub fn sync_writeback_slice_selection_metrics(
        &self,
        create_ops: u64,
        reuse_ops: u64,
        reject_older_unique_ops: u64,
        reject_dispatched_prefix_ops: u64,
    ) {
        self.writeback_slice_create_ops.store(create_ops, ORD);
        self.writeback_slice_reuse_ops.store(reuse_ops, ORD);
        self.writeback_slice_reject_older_unique_ops
            .store(reject_older_unique_ops, ORD);
        self.writeback_slice_reject_dispatched_prefix_ops
            .store(reject_dispatched_prefix_ops, ORD);
    }

    #[allow(clippy::too_many_arguments)]
    pub fn sync_writeback_freeze_metrics(
        &self,
        size_ops: u64,
        size_bytes: u64,
        max_unflushed_ops: u64,
        max_unflushed_bytes: u64,
        explicit_flush_ops: u64,
        explicit_flush_bytes: u64,
        auto_ops: u64,
        auto_bytes: u64,
        commit_age_ops: u64,
        commit_age_bytes: u64,
    ) {
        self.writeback_freeze_size_ops.store(size_ops, ORD);
        self.writeback_freeze_size_bytes.store(size_bytes, ORD);
        self.writeback_freeze_max_unflushed_ops
            .store(max_unflushed_ops, ORD);
        self.writeback_freeze_max_unflushed_bytes
            .store(max_unflushed_bytes, ORD);
        self.writeback_freeze_explicit_flush_ops
            .store(explicit_flush_ops, ORD);
        self.writeback_freeze_explicit_flush_bytes
            .store(explicit_flush_bytes, ORD);
        self.writeback_freeze_auto_ops.store(auto_ops, ORD);
        self.writeback_freeze_auto_bytes.store(auto_bytes, ORD);
        self.writeback_freeze_commit_age_ops
            .store(commit_age_ops, ORD);
        self.writeback_freeze_commit_age_bytes
            .store(commit_age_bytes, ORD);
    }

    pub fn sync_writeback_upload_batch_metrics(
        &self,
        ops: u64,
        bytes: u64,
        blocks: u64,
        partial_tail_ops: u64,
    ) {
        self.writeback_upload_batch_ops.store(ops, ORD);
        self.writeback_upload_batch_bytes.store(bytes, ORD);
        self.writeback_upload_batch_blocks.store(blocks, ORD);
        self.writeback_upload_partial_tail_ops
            .store(partial_tail_ops, ORD);
    }

    #[allow(clippy::too_many_arguments)]
    pub fn sync_object_store_metrics(
        &self,
        get_ops: u64,
        get_bytes: u64,
        get_lat_us: u64,
        put_ops: u64,
        put_bytes: u64,
        put_lat_us: u64,
        put_prepare_lat_us: u64,
        put_cache_lat_us: u64,
        del_ops: u64,
    ) {
        self.s3_get_ops.store(get_ops, ORD);
        self.s3_get_bytes.store(get_bytes, ORD);
        self.s3_get_lat_us.store(get_lat_us, ORD);
        self.s3_put_ops.store(put_ops, ORD);
        self.s3_put_bytes.store(put_bytes, ORD);
        self.s3_put_lat_us.store(put_lat_us, ORD);
        self.s3_put_prepare_lat_us.store(put_prepare_lat_us, ORD);
        self.s3_put_cache_lat_us.store(put_cache_lat_us, ORD);
        self.s3_del_ops.store(del_ops, ORD);
    }

    #[allow(clippy::too_many_arguments)]
    pub fn sync_read_strategy_metrics(
        &self,
        block_cache_hits: u64,
        page_cache_hits: u64,
        page_cache_misses: u64,
        range_gets: u64,
        full_gets: u64,
        piggyback_full: u64,
        background_prefetches: u64,
        background_prefetch_dropped: u64,
    ) {
        self.read_block_cache_hits.store(block_cache_hits, ORD);
        self.read_page_cache_hits.store(page_cache_hits, ORD);
        self.read_page_cache_misses.store(page_cache_misses, ORD);
        self.read_range_gets.store(range_gets, ORD);
        self.read_full_gets.store(full_gets, ORD);
        self.read_piggyback_full.store(piggyback_full, ORD);
        self.read_background_prefetches
            .store(background_prefetches, ORD);
        self.read_background_prefetch_dropped
            .store(background_prefetch_dropped, ORD);
    }

    #[allow(clippy::too_many_arguments)]
    pub fn sync_meta_client_metrics(
        &self,
        stat_cache_hit: u64,
        stat_cache_miss: u64,
        stat_fresh_store_hit: u64,
        lookup_cache_hit: u64,
        lookup_cache_miss: u64,
        get_slices_cache_hit: u64,
        get_slices_cache_miss: u64,
        open_fresh_stat: u64,
        open_file_cache_hit: u64,
        open_file_cache_miss: u64,
        lookup_attr_fused_hit: u64,
        lookup_attr_fused_miss: u64,
        lookup_attr_fused_error: u64,
    ) {
        self.meta_stat_cache_hit.store(stat_cache_hit, ORD);
        self.meta_stat_cache_miss.store(stat_cache_miss, ORD);
        self.meta_stat_fresh_store_hit
            .store(stat_fresh_store_hit, ORD);
        self.meta_lookup_cache_hit.store(lookup_cache_hit, ORD);
        self.meta_lookup_cache_miss.store(lookup_cache_miss, ORD);
        self.meta_get_slices_cache_hit
            .store(get_slices_cache_hit, ORD);
        self.meta_get_slices_cache_miss
            .store(get_slices_cache_miss, ORD);
        self.meta_open_fresh_stat.store(open_fresh_stat, ORD);
        self.meta_open_file_cache_hit
            .store(open_file_cache_hit, ORD);
        self.meta_open_file_cache_miss
            .store(open_file_cache_miss, ORD);
        self.meta_lookup_attr_fused_hit
            .store(lookup_attr_fused_hit, ORD);
        self.meta_lookup_attr_fused_miss
            .store(lookup_attr_fused_miss, ORD);
        self.meta_lookup_attr_fused_error
            .store(lookup_attr_fused_error, ORD);
    }

    /// Render all counters in Prometheus text format (one metric per line).
    /// Format: `metric_name value\n`
    pub fn render(&self) -> String {
        let snapshot = self.snapshot();
        let mut out = String::with_capacity(4096);

        // System
        out.push_str(&format!(
            "brewfs_uptime_seconds {}\n",
            snapshot.uptime_seconds
        ));

        // FUSE
        out.push_str(&format!(
            "brewfs_fuse_read_ops_total {}\n",
            self.fuse_read_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_fuse_read_bytes_total {}\n",
            self.fuse_read_bytes.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_fuse_read_lat_us_total {}\n",
            self.fuse_read_lat_us.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_fuse_write_ops_total {}\n",
            self.fuse_write_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_fuse_write_bytes_total {}\n",
            self.fuse_write_bytes.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_fuse_write_lat_us_total {}\n",
            self.fuse_write_lat_us.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_fuse_lookup_ops_total {}\n",
            self.fuse_lookup_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_fuse_lookup_lat_us_total {}\n",
            self.fuse_lookup_lat_us.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_fuse_getattr_ops_total {}\n",
            self.fuse_getattr_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_fuse_getattr_lat_us_total {}\n",
            self.fuse_getattr_lat_us.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_fuse_open_ops_total {}\n",
            self.fuse_open_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_fuse_create_ops_total {}\n",
            self.fuse_create_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_fuse_unlink_ops_total {}\n",
            self.fuse_unlink_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_fuse_readdir_ops_total {}\n",
            self.fuse_readdir_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_fuse_flush_ops_total {}\n",
            self.fuse_flush_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_fuse_flush_lat_us_total {}\n",
            self.fuse_flush_lat_us.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_fuse_read_avg_lat_us {:.6}\n",
            snapshot.avg_fuse_read_lat_us()
        ));
        out.push_str(&format!(
            "brewfs_fuse_write_avg_lat_us {:.6}\n",
            snapshot.avg_fuse_write_lat_us()
        ));
        out.push_str(&format!(
            "brewfs_fuse_flush_avg_lat_us {:.6}\n",
            snapshot.avg_fuse_flush_lat_us()
        ));

        // Meta
        out.push_str(&format!(
            "brewfs_meta_ops_total {}\n",
            self.meta_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_meta_lat_us_total {}\n",
            self.meta_lat_us.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_meta_txn_ops_total {}\n",
            self.meta_txn_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_meta_txn_lat_us_total {}\n",
            self.meta_txn_lat_us.load(ORD)
        ));

        // VFS diagnostic timing
        out.push_str(&format!(
            "brewfs_vfs_create_total_ops_total {}\n",
            self.vfs_create_total_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_vfs_create_total_lat_us_total {}\n",
            self.vfs_create_total_lat_us.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_vfs_create_meta_ops_total {}\n",
            self.vfs_create_meta_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_vfs_create_meta_lat_us_total {}\n",
            self.vfs_create_meta_lat_us.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_vfs_unlink_total_ops_total {}\n",
            self.vfs_unlink_total_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_vfs_unlink_total_lat_us_total {}\n",
            self.vfs_unlink_total_lat_us.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_vfs_unlink_lookup_ops_total {}\n",
            self.vfs_unlink_lookup_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_vfs_unlink_lookup_lat_us_total {}\n",
            self.vfs_unlink_lookup_lat_us.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_vfs_unlink_stat_ops_total {}\n",
            self.vfs_unlink_stat_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_vfs_unlink_stat_lat_us_total {}\n",
            self.vfs_unlink_stat_lat_us.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_vfs_unlink_meta_ops_total {}\n",
            self.vfs_unlink_meta_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_vfs_unlink_meta_lat_us_total {}\n",
            self.vfs_unlink_meta_lat_us.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_vfs_unlink_recent_ops_total {}\n",
            self.vfs_unlink_recent_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_vfs_unlink_recent_lat_us_total {}\n",
            self.vfs_unlink_recent_lat_us.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_vfs_setattr_recent_remove_ops_total {}\n",
            self.vfs_setattr_recent_remove_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_vfs_setattr_recent_remove_lat_us_total {}\n",
            self.vfs_setattr_recent_remove_lat_us.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_vfs_setattr_recent_get_mut_ops_total {}\n",
            self.vfs_setattr_recent_get_mut_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_vfs_setattr_recent_get_mut_lat_us_total {}\n",
            self.vfs_setattr_recent_get_mut_lat_us.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_vfs_read_dirty_probe_ops_total {}\n",
            self.vfs_read_dirty_probe_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_vfs_read_dirty_probe_lat_us_total {}\n",
            self.vfs_read_dirty_probe_lat_us.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_vfs_read_handle_ops_total {}\n",
            self.vfs_read_handle_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_vfs_read_handle_lat_us_total {}\n",
            self.vfs_read_handle_lat_us.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_vfs_read_overlay_ops_total {}\n",
            self.vfs_read_overlay_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_vfs_read_overlay_lat_us_total {}\n",
            self.vfs_read_overlay_lat_us.load(ORD)
        ));

        // Object storage
        out.push_str(&format!(
            "brewfs_s3_get_ops_total {}\n",
            self.s3_get_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_s3_get_bytes_total {}\n",
            self.s3_get_bytes.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_s3_get_lat_us_total {}\n",
            self.s3_get_lat_us.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_s3_put_ops_total {}\n",
            self.s3_put_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_s3_put_bytes_total {}\n",
            self.s3_put_bytes.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_s3_put_lat_us_total {}\n",
            self.s3_put_lat_us.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_s3_put_prepare_lat_us_total {}\n",
            self.s3_put_prepare_lat_us.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_s3_put_cache_lat_us_total {}\n",
            self.s3_put_cache_lat_us.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_s3_del_ops_total {}\n",
            self.s3_del_ops.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_s3_get_avg_lat_us {:.6}\n",
            snapshot.avg_s3_get_lat_us()
        ));
        out.push_str(&format!(
            "brewfs_s3_put_avg_lat_us {:.6}\n",
            snapshot.avg_s3_put_lat_us()
        ));
        out.push_str(&format!(
            "brewfs_s3_put_prepare_avg_lat_us {:.6}\n",
            snapshot.avg_s3_put_prepare_lat_us()
        ));
        out.push_str(&format!(
            "brewfs_s3_put_cache_avg_lat_us {:.6}\n",
            snapshot.avg_s3_put_cache_lat_us()
        ));

        // Buffer/cache
        out.push_str(&format!(
            "brewfs_buffer_dirty_bytes {}\n",
            self.buf_dirty_bytes.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_buffer_read_bytes {}\n",
            self.buf_read_bytes.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_writeback_dirty_bytes {}\n",
            snapshot.buf_dirty_bytes
        ));
        out.push_str(&format!(
            "brewfs_writeback_live_dirty_bytes {}\n",
            snapshot.writeback_live_dirty_bytes
        ));
        out.push_str(&format!(
            "brewfs_writeback_live_slices {}\n",
            snapshot.writeback_live_slices
        ));
        out.push_str(&format!(
            "brewfs_writeback_recent_pending_upload_bytes {}\n",
            snapshot.writeback_recent_pending_upload_bytes
        ));
        out.push_str(&format!(
            "brewfs_writeback_recent_pending_upload_slices {}\n",
            snapshot.writeback_recent_pending_upload_slices
        ));
        out.push_str(&format!(
            "brewfs_writeback_recent_uploaded_bytes {}\n",
            snapshot.writeback_recent_uploaded_bytes
        ));
        out.push_str(&format!(
            "brewfs_writeback_recent_uploaded_slices {}\n",
            snapshot.writeback_recent_uploaded_slices
        ));
        out.push_str(&format!(
            "brewfs_writeback_backpressure_soft_sleep_ops {}\n",
            snapshot.writeback_backpressure_soft_sleep_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_backpressure_soft_sleep_us {}\n",
            snapshot.writeback_backpressure_soft_sleep_us
        ));
        out.push_str(&format!(
            "brewfs_writeback_backpressure_hard_wait_ops {}\n",
            snapshot.writeback_backpressure_hard_wait_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_backpressure_hard_wait_us {}\n",
            snapshot.writeback_backpressure_hard_wait_us
        ));
        out.push_str(&format!(
            "brewfs_writeback_stage_inflight_bytes {}\n",
            snapshot.writeback_stage_inflight_bytes
        ));
        out.push_str(&format!(
            "brewfs_writeback_remote_upload_inflight_bytes {}\n",
            snapshot.writeback_remote_upload_inflight_bytes
        ));
        out.push_str(&format!(
            "brewfs_writeback_stage_ops_total {}\n",
            snapshot.writeback_stage_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_stage_bytes_total {}\n",
            snapshot.writeback_stage_bytes
        ));
        out.push_str(&format!(
            "brewfs_writeback_stage_lat_us_total {}\n",
            snapshot.writeback_stage_lat_us
        ));
        out.push_str(&format!(
            "brewfs_writeback_stage_failures_total {}\n",
            snapshot.writeback_stage_failures
        ));
        out.push_str(&format!(
            "brewfs_writeback_commit_before_stage_ops_total {}\n",
            snapshot.writeback_commit_before_stage_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_slice_create_ops_total {}\n",
            snapshot.writeback_slice_create_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_slice_reuse_ops_total {}\n",
            snapshot.writeback_slice_reuse_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_slice_reject_older_unique_ops_total {}\n",
            snapshot.writeback_slice_reject_older_unique_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_slice_reject_dispatched_prefix_ops_total {}\n",
            snapshot.writeback_slice_reject_dispatched_prefix_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_freeze_size_ops_total {}\n",
            snapshot.writeback_freeze_size_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_freeze_size_bytes_total {}\n",
            snapshot.writeback_freeze_size_bytes
        ));
        out.push_str(&format!(
            "brewfs_writeback_freeze_max_unflushed_ops_total {}\n",
            snapshot.writeback_freeze_max_unflushed_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_freeze_max_unflushed_bytes_total {}\n",
            snapshot.writeback_freeze_max_unflushed_bytes
        ));
        out.push_str(&format!(
            "brewfs_writeback_freeze_explicit_flush_ops_total {}\n",
            snapshot.writeback_freeze_explicit_flush_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_freeze_explicit_flush_bytes_total {}\n",
            snapshot.writeback_freeze_explicit_flush_bytes
        ));
        out.push_str(&format!(
            "brewfs_writeback_freeze_auto_ops_total {}\n",
            snapshot.writeback_freeze_auto_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_freeze_auto_bytes_total {}\n",
            snapshot.writeback_freeze_auto_bytes
        ));
        out.push_str(&format!(
            "brewfs_writeback_freeze_commit_age_ops_total {}\n",
            snapshot.writeback_freeze_commit_age_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_freeze_commit_age_bytes_total {}\n",
            snapshot.writeback_freeze_commit_age_bytes
        ));
        out.push_str(&format!(
            "brewfs_writeback_upload_batch_ops_total {}\n",
            snapshot.writeback_upload_batch_ops
        ));
        out.push_str(&format!(
            "brewfs_writeback_upload_batch_bytes_total {}\n",
            snapshot.writeback_upload_batch_bytes
        ));
        out.push_str(&format!(
            "brewfs_writeback_upload_batch_blocks_total {}\n",
            snapshot.writeback_upload_batch_blocks
        ));
        out.push_str(&format!(
            "brewfs_writeback_upload_partial_tail_ops_total {}\n",
            snapshot.writeback_upload_partial_tail_ops
        ));
        out.push_str(&format!(
            "brewfs_reader_buffer_bytes {}\n",
            snapshot.buf_read_bytes
        ));
        out.push_str(&format!(
            "brewfs_cache_hits_total {}\n",
            self.cache_hits.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_cache_misses_total {}\n",
            self.cache_misses.load(ORD)
        ));
        out.push_str(&format!(
            "brewfs_cache_requests_total {}\n",
            snapshot.cache_requests()
        ));
        out.push_str(&format!(
            "brewfs_cache_hit_ratio {:.6}\n",
            snapshot.cache_hit_ratio()
        ));
        out.push_str(&format!(
            "brewfs_read_block_cache_hits_total {}\n",
            snapshot.read_block_cache_hits
        ));
        out.push_str(&format!(
            "brewfs_read_page_cache_hits_total {}\n",
            snapshot.read_page_cache_hits
        ));
        out.push_str(&format!(
            "brewfs_read_page_cache_misses_total {}\n",
            snapshot.read_page_cache_misses
        ));
        out.push_str(&format!(
            "brewfs_read_range_gets_total {}\n",
            snapshot.read_range_gets
        ));
        out.push_str(&format!(
            "brewfs_read_full_gets_total {}\n",
            snapshot.read_full_gets
        ));
        out.push_str(&format!(
            "brewfs_read_piggyback_full_total {}\n",
            snapshot.read_piggyback_full
        ));
        out.push_str(&format!(
            "brewfs_read_background_prefetch_total {}\n",
            snapshot.read_background_prefetches
        ));
        out.push_str(&format!(
            "brewfs_read_background_prefetch_dropped_total {}\n",
            snapshot.read_background_prefetch_dropped
        ));
        out.push_str(&format!(
            "brewfs_meta_stat_cache_hit_total {}\n",
            snapshot.meta_stat_cache_hit
        ));
        out.push_str(&format!(
            "brewfs_meta_stat_cache_miss_total {}\n",
            snapshot.meta_stat_cache_miss
        ));
        out.push_str(&format!(
            "brewfs_meta_stat_fresh_store_hit_total {}\n",
            snapshot.meta_stat_fresh_store_hit
        ));
        out.push_str(&format!(
            "brewfs_meta_lookup_cache_hit_total {}\n",
            snapshot.meta_lookup_cache_hit
        ));
        out.push_str(&format!(
            "brewfs_meta_lookup_cache_miss_total {}\n",
            snapshot.meta_lookup_cache_miss
        ));
        out.push_str(&format!(
            "brewfs_meta_get_slices_cache_hit_total {}\n",
            snapshot.meta_get_slices_cache_hit
        ));
        out.push_str(&format!(
            "brewfs_meta_get_slices_cache_miss_total {}\n",
            snapshot.meta_get_slices_cache_miss
        ));
        out.push_str(&format!(
            "brewfs_meta_open_fresh_stat_total {}\n",
            snapshot.meta_open_fresh_stat
        ));
        out.push_str(&format!(
            "brewfs_meta_open_file_cache_hit_total {}\n",
            snapshot.meta_open_file_cache_hit
        ));
        out.push_str(&format!(
            "brewfs_meta_open_file_cache_miss_total {}\n",
            snapshot.meta_open_file_cache_miss
        ));
        out.push_str(&format!(
            "brewfs_meta_lookup_attr_fused_hit_total {}\n",
            snapshot.meta_lookup_attr_fused_hit
        ));
        out.push_str(&format!(
            "brewfs_meta_lookup_attr_fused_miss_total {}\n",
            snapshot.meta_lookup_attr_fused_miss
        ));
        out.push_str(&format!(
            "brewfs_meta_lookup_attr_fused_error_total {}\n",
            snapshot.meta_lookup_attr_fused_error
        ));

        out
    }

    pub fn record_duration(ops_counter: &AtomicU64, lat_counter: &AtomicU64, duration: Duration) {
        let elapsed_us = duration.as_micros() as u64;
        ops_counter.fetch_add(1, ORD);
        lat_counter.fetch_add(elapsed_us, ORD);
    }
}

impl Default for FsStats {
    fn default() -> Self {
        Self::new()
    }
}

/// RAII guard for timing an operation and recording latency + count.
/// Records stats on drop, so it can be used as `let _timer = OpTimer::new(...)`.
pub struct OpTimer<'a> {
    start: Instant,
    ops_counter: &'a AtomicU64,
    lat_counter: &'a AtomicU64,
}

impl<'a> OpTimer<'a> {
    pub fn new(ops_counter: &'a AtomicU64, lat_counter: &'a AtomicU64) -> Self {
        Self {
            start: Instant::now(),
            ops_counter,
            lat_counter,
        }
    }

    /// Finish timing and record the operation (consumes self).
    pub fn finish(self) {
        // Drop impl handles the actual recording.
    }
}

impl<'a> Drop for OpTimer<'a> {
    fn drop(&mut self) {
        FsStats::record_duration(self.ops_counter, self.lat_counter, self.start.elapsed());
    }
}

/// Optional timer for diagnostic hot-path stats. Disabled timers avoid
/// `Instant::now()` so production hot paths only pay a cheap branch.
pub struct MaybeOpTimer<'a> {
    start: Option<Instant>,
    ops_counter: &'a AtomicU64,
    lat_counter: &'a AtomicU64,
}

impl<'a> MaybeOpTimer<'a> {
    pub fn new(enabled: bool, ops_counter: &'a AtomicU64, lat_counter: &'a AtomicU64) -> Self {
        Self {
            start: enabled.then(Instant::now),
            ops_counter,
            lat_counter,
        }
    }
}

impl<'a> Drop for MaybeOpTimer<'a> {
    fn drop(&mut self) {
        if let Some(start) = self.start {
            FsStats::record_duration(self.ops_counter, self.lat_counter, start.elapsed());
        }
    }
}

/// Convenience macro for timing an async operation and recording stats.
///
/// Usage:
/// ```ignore
/// let result = timed_op!(stats.fuse_read_ops, stats.fuse_read_lat_us, {
///     handle.read(offset, len).await
/// });
/// ```
#[macro_export]
macro_rules! timed_op {
    ($ops:expr, $lat:expr, $body:expr) => {{
        let __start = std::time::Instant::now();
        let __result = $body;
        let __elapsed_us = __start.elapsed().as_micros() as u64;
        $ops.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        $lat.fetch_add(__elapsed_us, std::sync::atomic::Ordering::Relaxed);
        __result
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_contains_all_metrics() {
        let stats = FsStats::new();
        stats.fuse_read_ops.store(42, ORD);
        stats.fuse_read_bytes.store(1024 * 1024, ORD);
        stats.fuse_read_lat_us.store(840, ORD);
        stats.s3_put_ops.store(10, ORD);
        stats.s3_put_lat_us.store(250, ORD);
        stats.s3_put_prepare_lat_us.store(500, ORD);
        stats.s3_put_cache_lat_us.store(100, ORD);
        stats.sync_cache_counters(8, 2);
        stats.sync_buffer_bytes(4096, 8192);
        stats.sync_writeback_dirty_breakdown(1024, 2, 2048, 3, 512, 4);
        stats.add_writeback_backpressure_soft_sleep(Duration::from_micros(12));
        stats.add_writeback_backpressure_hard_wait(Duration::from_micros(34));
        stats.sync_writeback_phase_metrics(1, 2, 3, 4, 5, 6, 7);
        stats.sync_writeback_slice_selection_metrics(8, 9, 10, 11);
        stats.sync_writeback_freeze_metrics(12, 1024, 13, 2048, 14, 4096, 15, 8192, 16, 16384);
        stats.sync_writeback_upload_batch_metrics(17, 32768, 18, 19);

        let output = stats.render();
        assert!(output.contains("brewfs_fuse_read_ops_total 42"));
        assert!(output.contains("brewfs_fuse_read_bytes_total 1048576"));
        assert!(output.contains("brewfs_fuse_read_avg_lat_us 20.000000"));
        assert!(output.contains("brewfs_s3_put_ops_total 10"));
        assert!(output.contains("brewfs_s3_put_avg_lat_us 25.000000"));
        assert!(output.contains("brewfs_s3_put_prepare_avg_lat_us 50.000000"));
        assert!(output.contains("brewfs_s3_put_cache_avg_lat_us 10.000000"));
        assert!(output.contains("brewfs_uptime_seconds"));
        assert!(output.contains("brewfs_cache_hits_total 8"));
        assert!(output.contains("brewfs_cache_misses_total 2"));
        assert!(output.contains("brewfs_cache_requests_total 10"));
        assert!(output.contains("brewfs_cache_hit_ratio 0.800000"));
        assert!(output.contains("brewfs_read_block_cache_hits_total 0"));
        assert!(output.contains("brewfs_read_page_cache_hits_total 0"));
        assert!(output.contains("brewfs_read_page_cache_misses_total 0"));
        assert!(output.contains("brewfs_read_range_gets_total 0"));
        assert!(output.contains("brewfs_read_full_gets_total 0"));
        assert!(output.contains("brewfs_read_piggyback_full_total 0"));
        assert!(output.contains("brewfs_read_background_prefetch_total 0"));
        assert!(output.contains("brewfs_read_background_prefetch_dropped_total 0"));
        assert!(output.contains("brewfs_meta_stat_cache_hit_total 0"));
        assert!(output.contains("brewfs_meta_stat_cache_miss_total 0"));
        assert!(output.contains("brewfs_meta_stat_fresh_store_hit_total 0"));
        assert!(output.contains("brewfs_meta_lookup_cache_hit_total 0"));
        assert!(output.contains("brewfs_meta_lookup_cache_miss_total 0"));
        assert!(output.contains("brewfs_meta_get_slices_cache_hit_total 0"));
        assert!(output.contains("brewfs_meta_get_slices_cache_miss_total 0"));
        assert!(output.contains("brewfs_meta_open_fresh_stat_total 0"));
        assert!(output.contains("brewfs_meta_open_file_cache_hit_total 0"));
        assert!(output.contains("brewfs_meta_open_file_cache_miss_total 0"));
        assert!(output.contains("brewfs_meta_lookup_attr_fused_hit_total 0"));
        assert!(output.contains("brewfs_meta_lookup_attr_fused_miss_total 0"));
        assert!(output.contains("brewfs_meta_lookup_attr_fused_error_total 0"));
        assert!(output.contains("brewfs_writeback_dirty_bytes 4096"));
        assert!(output.contains("brewfs_writeback_live_dirty_bytes 1024"));
        assert!(output.contains("brewfs_writeback_live_slices 2"));
        assert!(output.contains("brewfs_writeback_recent_pending_upload_bytes 2048"));
        assert!(output.contains("brewfs_writeback_recent_pending_upload_slices 3"));
        assert!(output.contains("brewfs_writeback_recent_uploaded_bytes 512"));
        assert!(output.contains("brewfs_writeback_recent_uploaded_slices 4"));
        assert!(output.contains("brewfs_writeback_backpressure_soft_sleep_ops 1"));
        assert!(output.contains("brewfs_writeback_backpressure_soft_sleep_us 12"));
        assert!(output.contains("brewfs_writeback_backpressure_hard_wait_ops 1"));
        assert!(output.contains("brewfs_writeback_backpressure_hard_wait_us 34"));
        assert!(output.contains("brewfs_writeback_stage_inflight_bytes 1"));
        assert!(output.contains("brewfs_writeback_remote_upload_inflight_bytes 2"));
        assert!(output.contains("brewfs_writeback_stage_ops_total 3"));
        assert!(output.contains("brewfs_writeback_stage_bytes_total 4"));
        assert!(output.contains("brewfs_writeback_stage_lat_us_total 5"));
        assert!(output.contains("brewfs_writeback_stage_failures_total 6"));
        assert!(output.contains("brewfs_writeback_commit_before_stage_ops_total 7"));
        assert!(output.contains("brewfs_writeback_slice_create_ops_total 8"));
        assert!(output.contains("brewfs_writeback_slice_reuse_ops_total 9"));
        assert!(output.contains("brewfs_writeback_slice_reject_older_unique_ops_total 10"));
        assert!(output.contains("brewfs_writeback_slice_reject_dispatched_prefix_ops_total 11"));
        assert!(output.contains("brewfs_writeback_freeze_size_ops_total 12"));
        assert!(output.contains("brewfs_writeback_freeze_size_bytes_total 1024"));
        assert!(output.contains("brewfs_writeback_freeze_max_unflushed_ops_total 13"));
        assert!(output.contains("brewfs_writeback_freeze_max_unflushed_bytes_total 2048"));
        assert!(output.contains("brewfs_writeback_freeze_explicit_flush_ops_total 14"));
        assert!(output.contains("brewfs_writeback_freeze_explicit_flush_bytes_total 4096"));
        assert!(output.contains("brewfs_writeback_freeze_auto_ops_total 15"));
        assert!(output.contains("brewfs_writeback_freeze_auto_bytes_total 8192"));
        assert!(output.contains("brewfs_writeback_freeze_commit_age_ops_total 16"));
        assert!(output.contains("brewfs_writeback_freeze_commit_age_bytes_total 16384"));
        assert!(output.contains("brewfs_writeback_upload_batch_ops_total 17"));
        assert!(output.contains("brewfs_writeback_upload_batch_bytes_total 32768"));
        assert!(output.contains("brewfs_writeback_upload_batch_blocks_total 18"));
        assert!(output.contains("brewfs_writeback_upload_partial_tail_ops_total 19"));
        assert!(output.contains("brewfs_reader_buffer_bytes 8192"));
        assert!(output.contains("brewfs_vfs_create_total_ops_total 0"));
        assert!(output.contains("brewfs_vfs_unlink_lookup_lat_us_total 0"));
        assert!(output.contains("brewfs_vfs_unlink_recent_ops_total 0"));
        assert!(output.contains("brewfs_vfs_setattr_recent_remove_lat_us_total 0"));
        assert!(output.contains("brewfs_vfs_read_dirty_probe_ops_total 0"));
        assert!(output.contains("brewfs_vfs_read_handle_ops_total 0"));
        assert!(output.contains("brewfs_vfs_read_overlay_ops_total 0"));
    }

    #[test]
    fn snapshot_exposes_derived_values_without_divide_by_zero() {
        let stats = FsStats::new();
        let empty = stats.snapshot();
        assert_eq!(empty.cache_requests(), 0);
        assert_eq!(empty.cache_hit_ratio(), 0.0);
        assert_eq!(empty.avg_fuse_read_lat_us(), 0.0);

        stats.fuse_write_ops.store(4, ORD);
        stats.fuse_write_lat_us.store(1000, ORD);
        stats.sync_cache_counters(3, 1);
        stats.sync_writeback_dirty_breakdown(11, 2, 22, 3, 33, 4);
        stats.add_writeback_backpressure_soft_sleep(Duration::from_micros(44));
        stats.add_writeback_backpressure_hard_wait(Duration::from_micros(55));
        stats.sync_writeback_backpressure_metrics(66, 77, 88, 99);
        stats.sync_writeback_phase_metrics(111, 222, 333, 444, 555, 666, 777);
        stats.sync_writeback_slice_selection_metrics(888, 999, 1000, 1001);
        stats.sync_writeback_freeze_metrics(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        stats.sync_writeback_upload_batch_metrics(11, 12, 13, 14);
        stats.sync_object_store_metrics(2, 8192, 50, 1, 4096, 25, 75, 125, 3);

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.cache_requests(), 4);
        assert_eq!(snapshot.cache_hit_ratio(), 0.75);
        assert_eq!(snapshot.avg_fuse_write_lat_us(), 250.0);
        assert_eq!(snapshot.s3_get_ops, 2);
        assert_eq!(snapshot.s3_get_bytes, 8192);
        assert_eq!(snapshot.avg_s3_get_lat_us(), 25.0);
        assert_eq!(snapshot.s3_put_ops, 1);
        assert_eq!(snapshot.s3_put_bytes, 4096);
        assert_eq!(snapshot.avg_s3_put_lat_us(), 25.0);
        assert_eq!(snapshot.avg_s3_put_prepare_lat_us(), 75.0);
        assert_eq!(snapshot.avg_s3_put_cache_lat_us(), 125.0);
        assert_eq!(snapshot.s3_del_ops, 3);
        assert_eq!(snapshot.writeback_live_dirty_bytes, 11);
        assert_eq!(snapshot.writeback_live_slices, 2);
        assert_eq!(snapshot.writeback_recent_pending_upload_bytes, 22);
        assert_eq!(snapshot.writeback_recent_pending_upload_slices, 3);
        assert_eq!(snapshot.writeback_recent_uploaded_bytes, 33);
        assert_eq!(snapshot.writeback_recent_uploaded_slices, 4);
        assert_eq!(snapshot.writeback_backpressure_soft_sleep_ops, 66);
        assert_eq!(snapshot.writeback_backpressure_soft_sleep_us, 77);
        assert_eq!(snapshot.writeback_backpressure_hard_wait_ops, 88);
        assert_eq!(snapshot.writeback_backpressure_hard_wait_us, 99);
        assert_eq!(snapshot.writeback_stage_inflight_bytes, 111);
        assert_eq!(snapshot.writeback_remote_upload_inflight_bytes, 222);
        assert_eq!(snapshot.writeback_stage_ops, 333);
        assert_eq!(snapshot.writeback_stage_bytes, 444);
        assert_eq!(snapshot.writeback_stage_lat_us, 555);
        assert_eq!(snapshot.writeback_stage_failures, 666);
        assert_eq!(snapshot.writeback_commit_before_stage_ops, 777);
        assert_eq!(snapshot.writeback_slice_create_ops, 888);
        assert_eq!(snapshot.writeback_slice_reuse_ops, 999);
        assert_eq!(snapshot.writeback_slice_reject_older_unique_ops, 1000);
        assert_eq!(snapshot.writeback_slice_reject_dispatched_prefix_ops, 1001);
        assert_eq!(snapshot.writeback_freeze_size_ops, 1);
        assert_eq!(snapshot.writeback_freeze_size_bytes, 2);
        assert_eq!(snapshot.writeback_freeze_max_unflushed_ops, 3);
        assert_eq!(snapshot.writeback_freeze_max_unflushed_bytes, 4);
        assert_eq!(snapshot.writeback_freeze_explicit_flush_ops, 5);
        assert_eq!(snapshot.writeback_freeze_explicit_flush_bytes, 6);
        assert_eq!(snapshot.writeback_freeze_auto_ops, 7);
        assert_eq!(snapshot.writeback_freeze_auto_bytes, 8);
        assert_eq!(snapshot.writeback_freeze_commit_age_ops, 9);
        assert_eq!(snapshot.writeback_freeze_commit_age_bytes, 10);
        assert_eq!(snapshot.writeback_upload_batch_ops, 11);
        assert_eq!(snapshot.writeback_upload_batch_bytes, 12);
        assert_eq!(snapshot.writeback_upload_batch_blocks, 13);
        assert_eq!(snapshot.writeback_upload_partial_tail_ops, 14);
    }

    #[test]
    fn op_timer_records_latency() {
        let ops = AtomicU64::new(0);
        let lat = AtomicU64::new(0);

        let timer = OpTimer::new(&ops, &lat);
        std::thread::sleep(std::time::Duration::from_millis(1));
        timer.finish();

        assert_eq!(ops.load(ORD), 1);
        assert!(lat.load(ORD) >= 1000); // at least 1ms = 1000us
    }
}
