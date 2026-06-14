//! Global memory coordination for read and write buffers.
//!
//! Provides a shared `MemoryBudget` that tracks total memory usage across the
//! reader prefetch pool and writer dirty buffers. Watermark-based pressure levels
//! allow subsystems to adapt their behavior (reduce prefetch, force-flush writes).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tracing::{debug, trace, warn};

/// Memory pressure watermarks (fraction of total budget)
const WATERMARK_LOW: f64 = 0.60;
const WATERMARK_HIGH: f64 = 0.80;
const WATERMARK_CRITICAL: f64 = 0.95;

/// Pressure level indicating how close we are to the memory budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PressureLevel {
    /// Below 60% — plenty of headroom
    Low,
    /// 60-80% — normal operation, no action needed
    Normal,
    /// 80-95% — reduce prefetch window, slow new allocations
    High,
    /// Above 95% — force-flush oldest dirty slices, reject new prefetch
    Critical,
}

/// Shared memory budget for coordinating read and write buffer usage.
///
/// Both the reader (prefetch buffers) and writer (dirty slice buffers)
/// register their allocations here. The budget provides pressure feedback
/// so subsystems can adapt.
#[derive(Clone)]
pub struct MemoryBudget {
    inner: Arc<Inner>,
}

struct Inner {
    /// Total budget in bytes
    total_bytes: u64,
    /// Current allocated bytes (reader + writer combined)
    used_bytes: AtomicU64,
    /// Bytes allocated by reader prefetch
    reader_bytes: AtomicU64,
    /// Bytes allocated by writer buffers
    writer_bytes: AtomicU64,
}

impl MemoryBudget {
    /// Create a new memory budget with the given total capacity.
    pub fn new(total_bytes: u64) -> Self {
        debug!(
            "MemoryBudget created: total={:.1} MiB",
            total_bytes as f64 / 1048576.0
        );
        Self {
            inner: Arc::new(Inner {
                total_bytes,
                used_bytes: AtomicU64::new(0),
                reader_bytes: AtomicU64::new(0),
                writer_bytes: AtomicU64::new(0),
            }),
        }
    }

    /// Total budget in bytes
    pub fn total_bytes(&self) -> u64 {
        self.inner.total_bytes
    }

    /// Current total used bytes
    pub fn used_bytes(&self) -> u64 {
        self.inner.used_bytes.load(Ordering::Relaxed)
    }

    /// Current reader (prefetch) bytes
    pub fn reader_bytes(&self) -> u64 {
        self.inner.reader_bytes.load(Ordering::Relaxed)
    }

    /// Current writer (dirty buffer) bytes
    pub fn writer_bytes(&self) -> u64 {
        self.inner.writer_bytes.load(Ordering::Relaxed)
    }

    /// Current memory pressure as a fraction (0.0 to 1.0+)
    pub fn pressure(&self) -> f64 {
        let used = self.inner.used_bytes.load(Ordering::Relaxed) as f64;
        let total = self.inner.total_bytes as f64;
        if total == 0.0 {
            return 0.0;
        }
        used / total
    }

    /// Current pressure level
    pub fn pressure_level(&self) -> PressureLevel {
        let p = self.pressure();
        if p >= WATERMARK_CRITICAL {
            PressureLevel::Critical
        } else if p >= WATERMARK_HIGH {
            PressureLevel::High
        } else if p >= WATERMARK_LOW {
            PressureLevel::Normal
        } else {
            PressureLevel::Low
        }
    }

    fn fetch_sub_saturating(counter: &AtomicU64, bytes: u64) {
        let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            Some(current.saturating_sub(bytes))
        });
    }

    fn alloc_reader(&self, bytes: u64) -> PressureLevel {
        self.inner.used_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.inner.reader_bytes.fetch_add(bytes, Ordering::Relaxed);
        let level = self.pressure_level();
        if level >= PressureLevel::High {
            debug!(
                "Reader alloc under pressure: +{:.1} MiB, total={:.1}/{:.1} MiB, level={:?}",
                bytes as f64 / 1048576.0,
                self.used_bytes() as f64 / 1048576.0,
                self.inner.total_bytes as f64 / 1048576.0,
                level
            );
        }
        level
    }

    /// Try to allocate bytes for the reader (prefetch).
    /// Returns true if allocation succeeded (pressure < critical), false otherwise.
    pub fn try_alloc_reader(&self, bytes: u64) -> bool {
        let critical_limit = (self.inner.total_bytes as f64 * WATERMARK_CRITICAL) as u64;
        loop {
            let current = self.inner.used_bytes.load(Ordering::Relaxed);
            if current.saturating_add(bytes) > critical_limit {
                trace!(
                    "Reader alloc rejected: used={:.1} MiB + request={:.1} MiB > critical={:.1} MiB",
                    current as f64 / 1048576.0,
                    bytes as f64 / 1048576.0,
                    critical_limit as f64 / 1048576.0
                );
                return false;
            }
            // CAS: atomically reserve the bytes to avoid TOCTOU races
            if self
                .inner
                .used_bytes
                .compare_exchange_weak(
                    current,
                    current + bytes,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                self.inner.reader_bytes.fetch_add(bytes, Ordering::Relaxed);
                return true;
            }
            // CAS failed (contention) — retry
        }
    }

    /// Allocate bytes for the writer (dirty buffers).
    /// Writers always succeed but return the pressure level so caller can decide to flush.
    pub fn alloc_writer(&self, bytes: u64) -> PressureLevel {
        self.inner.used_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.inner.writer_bytes.fetch_add(bytes, Ordering::Relaxed);
        let level = self.pressure_level();
        if level >= PressureLevel::High {
            debug!(
                "Writer alloc under pressure: +{:.1} MiB, total={:.1}/{:.1} MiB, level={:?}",
                bytes as f64 / 1048576.0,
                self.used_bytes() as f64 / 1048576.0,
                self.inner.total_bytes as f64 / 1048576.0,
                level
            );
        }
        level
    }

    /// Free reader bytes (prefetch buffer released).
    pub fn free_reader(&self, bytes: u64) {
        Self::fetch_sub_saturating(&self.inner.reader_bytes, bytes);
        Self::fetch_sub_saturating(&self.inner.used_bytes, bytes);
        trace!("Reader free: -{:.1} MiB", bytes as f64 / 1048576.0);
    }

    /// Free writer bytes (slice uploaded/committed).
    pub fn free_writer(&self, bytes: u64) {
        Self::fetch_sub_saturating(&self.inner.writer_bytes, bytes);
        Self::fetch_sub_saturating(&self.inner.used_bytes, bytes);
        trace!("Writer free: -{:.1} MiB", bytes as f64 / 1048576.0);
    }

    /// Get the recommended readahead window multiplier based on pressure.
    /// Returns 1.0 at low pressure, scaling down to 0.25 at critical.
    pub fn readahead_factor(&self) -> f64 {
        match self.pressure_level() {
            PressureLevel::Low => 1.0,
            PressureLevel::Normal => 1.0,
            PressureLevel::High => 0.5,
            PressureLevel::Critical => 0.25,
        }
    }

    /// Whether the writer should force-flush oldest slices
    pub fn should_force_flush(&self) -> bool {
        self.pressure_level() >= PressureLevel::Critical
    }

    /// Log current memory state (for diagnostics)
    pub fn log_state(&self) {
        let used = self.used_bytes();
        let reader = self.reader_bytes();
        let writer = self.writer_bytes();
        let total = self.inner.total_bytes;
        let level = self.pressure_level();
        if level >= PressureLevel::High {
            warn!(
                "Memory pressure {:?}: {:.1}/{:.1} MiB (reader={:.1} MiB, writer={:.1} MiB)",
                level,
                used as f64 / 1048576.0,
                total as f64 / 1048576.0,
                reader as f64 / 1048576.0,
                writer as f64 / 1048576.0,
            );
        } else {
            debug!(
                "Memory budget: {:.1}/{:.1} MiB ({:.0}%, reader={:.1} MiB, writer={:.1} MiB)",
                used as f64 / 1048576.0,
                total as f64 / 1048576.0,
                self.pressure() * 100.0,
                reader as f64 / 1048576.0,
                writer as f64 / 1048576.0,
            );
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum MemoryConsumer {
    Reader,
    Writer,
}

pub(crate) struct MemoryUsageGuard {
    budget: MemoryBudget,
    consumer: MemoryConsumer,
    bytes: u64,
}

impl MemoryUsageGuard {
    pub(crate) fn new(budget: MemoryBudget, consumer: MemoryConsumer) -> Self {
        Self {
            budget,
            consumer,
            bytes: 0,
        }
    }

    pub(crate) fn update_bytes(&mut self, bytes: u64) {
        match bytes.cmp(&self.bytes) {
            std::cmp::Ordering::Greater => {
                let delta = bytes - self.bytes;
                match self.consumer {
                    MemoryConsumer::Reader => {
                        self.budget.alloc_reader(delta);
                    }
                    MemoryConsumer::Writer => {
                        self.budget.alloc_writer(delta);
                    }
                }
            }
            std::cmp::Ordering::Less => {
                let delta = self.bytes - bytes;
                match self.consumer {
                    MemoryConsumer::Reader => self.budget.free_reader(delta),
                    MemoryConsumer::Writer => self.budget.free_writer(delta),
                }
            }
            std::cmp::Ordering::Equal => {}
        }
        self.bytes = bytes;
    }
}

impl Drop for MemoryUsageGuard {
    fn drop(&mut self) {
        self.update_bytes(0);
    }
}

impl std::fmt::Debug for MemoryBudget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryBudget")
            .field("total_mib", &(self.inner.total_bytes / 1048576))
            .field("used_mib", &(self.used_bytes() / 1048576))
            .field("reader_mib", &(self.reader_bytes() / 1048576))
            .field("writer_mib", &(self.writer_bytes() / 1048576))
            .field("pressure", &format!("{:.1}%", self.pressure() * 100.0))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pressure_levels() {
        let budget = MemoryBudget::new(1000);

        assert_eq!(budget.pressure_level(), PressureLevel::Low);

        budget.alloc_writer(600);
        assert_eq!(budget.pressure_level(), PressureLevel::Normal);

        budget.alloc_writer(250);
        assert_eq!(budget.pressure_level(), PressureLevel::High);

        budget.alloc_writer(100);
        assert_eq!(budget.pressure_level(), PressureLevel::Critical);
    }

    #[test]
    fn test_reader_alloc_rejected_at_critical() {
        let budget = MemoryBudget::new(1000);
        budget.alloc_writer(950); // 95% used
        assert!(!budget.try_alloc_reader(100)); // rejected
    }

    #[test]
    fn test_free_reduces_pressure() {
        let budget = MemoryBudget::new(1000);
        budget.alloc_writer(900);
        assert_eq!(budget.pressure_level(), PressureLevel::High);
        budget.free_writer(500);
        assert_eq!(budget.pressure_level(), PressureLevel::Low);
    }

    #[test]
    fn test_readahead_factor() {
        let budget = MemoryBudget::new(1000);
        assert_eq!(budget.readahead_factor(), 1.0);

        budget.alloc_writer(850);
        assert_eq!(budget.readahead_factor(), 0.5);

        budget.alloc_writer(100);
        assert_eq!(budget.readahead_factor(), 0.25);
    }

    #[test]
    fn test_should_force_flush() {
        let budget = MemoryBudget::new(1000);
        assert!(!budget.should_force_flush());
        budget.alloc_writer(960);
        assert!(budget.should_force_flush());
    }

    #[test]
    fn test_usage_guard_tracks_and_frees_reader_bytes() {
        let budget = MemoryBudget::new(1000);
        {
            let mut guard = MemoryUsageGuard::new(budget.clone(), MemoryConsumer::Reader);
            guard.update_bytes(400);
            assert_eq!(budget.reader_bytes(), 400);
            assert_eq!(budget.used_bytes(), 400);

            guard.update_bytes(250);
            assert_eq!(budget.reader_bytes(), 250);
            assert_eq!(budget.used_bytes(), 250);
        }

        assert_eq!(budget.reader_bytes(), 0);
        assert_eq!(budget.used_bytes(), 0);
    }

    #[test]
    fn test_usage_guard_tracks_and_frees_writer_bytes() {
        let budget = MemoryBudget::new(1000);
        {
            let mut guard = MemoryUsageGuard::new(budget.clone(), MemoryConsumer::Writer);
            guard.update_bytes(600);
            assert_eq!(budget.writer_bytes(), 600);
            assert_eq!(budget.pressure_level(), PressureLevel::Normal);
        }

        assert_eq!(budget.writer_bytes(), 0);
        assert_eq!(budget.used_bytes(), 0);
        assert_eq!(budget.pressure_level(), PressureLevel::Low);
    }
}
