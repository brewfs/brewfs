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
    pub s3_get_ops: u64,
    pub s3_get_bytes: u64,
    pub s3_get_lat_us: u64,
    pub s3_put_ops: u64,
    pub s3_put_bytes: u64,
    pub s3_put_lat_us: u64,
    pub s3_del_ops: u64,
    pub buf_dirty_bytes: u64,
    pub buf_read_bytes: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
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
    /// Total S3 DELETE requests
    pub s3_del_ops: AtomicU64,

    // ─── Buffer/cache usage ──────────────────────────────────────
    /// Current dirty write buffer bytes
    pub buf_dirty_bytes: AtomicU64,
    /// Current reader cache bytes
    pub buf_read_bytes: AtomicU64,
    /// Block cache hit count
    pub cache_hits: AtomicU64,
    /// Block cache miss count
    pub cache_misses: AtomicU64,
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
            s3_get_ops: AtomicU64::new(0),
            s3_get_bytes: AtomicU64::new(0),
            s3_get_lat_us: AtomicU64::new(0),
            s3_put_ops: AtomicU64::new(0),
            s3_put_bytes: AtomicU64::new(0),
            s3_put_lat_us: AtomicU64::new(0),
            s3_del_ops: AtomicU64::new(0),
            buf_dirty_bytes: AtomicU64::new(0),
            buf_read_bytes: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
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
            s3_get_ops: self.s3_get_ops.load(ORD),
            s3_get_bytes: self.s3_get_bytes.load(ORD),
            s3_get_lat_us: self.s3_get_lat_us.load(ORD),
            s3_put_ops: self.s3_put_ops.load(ORD),
            s3_put_bytes: self.s3_put_bytes.load(ORD),
            s3_put_lat_us: self.s3_put_lat_us.load(ORD),
            s3_del_ops: self.s3_del_ops.load(ORD),
            buf_dirty_bytes: self.buf_dirty_bytes.load(ORD),
            buf_read_bytes: self.buf_read_bytes.load(ORD),
            cache_hits: self.cache_hits.load(ORD),
            cache_misses: self.cache_misses.load(ORD),
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

    pub fn sync_object_store_metrics(
        &self,
        get_ops: u64,
        get_bytes: u64,
        get_lat_us: u64,
        put_ops: u64,
        put_bytes: u64,
        put_lat_us: u64,
        del_ops: u64,
    ) {
        self.s3_get_ops.store(get_ops, ORD);
        self.s3_get_bytes.store(get_bytes, ORD);
        self.s3_get_lat_us.store(get_lat_us, ORD);
        self.s3_put_ops.store(put_ops, ORD);
        self.s3_put_bytes.store(put_bytes, ORD);
        self.s3_put_lat_us.store(put_lat_us, ORD);
        self.s3_del_ops.store(del_ops, ORD);
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
        stats.sync_cache_counters(8, 2);
        stats.sync_buffer_bytes(4096, 8192);

        let output = stats.render();
        assert!(output.contains("brewfs_fuse_read_ops_total 42"));
        assert!(output.contains("brewfs_fuse_read_bytes_total 1048576"));
        assert!(output.contains("brewfs_fuse_read_avg_lat_us 20.000000"));
        assert!(output.contains("brewfs_s3_put_ops_total 10"));
        assert!(output.contains("brewfs_s3_put_avg_lat_us 25.000000"));
        assert!(output.contains("brewfs_uptime_seconds"));
        assert!(output.contains("brewfs_cache_hits_total 8"));
        assert!(output.contains("brewfs_cache_misses_total 2"));
        assert!(output.contains("brewfs_cache_requests_total 10"));
        assert!(output.contains("brewfs_cache_hit_ratio 0.800000"));
        assert!(output.contains("brewfs_writeback_dirty_bytes 4096"));
        assert!(output.contains("brewfs_reader_buffer_bytes 8192"));
        assert!(output.contains("brewfs_vfs_create_total_ops_total 0"));
        assert!(output.contains("brewfs_vfs_unlink_lookup_lat_us_total 0"));
        assert!(output.contains("brewfs_vfs_unlink_recent_ops_total 0"));
        assert!(output.contains("brewfs_vfs_setattr_recent_remove_lat_us_total 0"));
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
        stats.sync_object_store_metrics(2, 8192, 50, 1, 4096, 25, 3);

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
        assert_eq!(snapshot.s3_del_ops, 3);
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
