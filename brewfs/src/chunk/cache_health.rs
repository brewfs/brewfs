use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug)]
pub struct DiskHealth {
    error_count: AtomicU64,
    success_count: AtomicU64,
    bypassed: AtomicBool,
    last_error_time: AtomicU64,
    error_threshold: u64,
    recovery_threshold: u64,
    reset_window_secs: u64,
}

impl Default for DiskHealth {
    fn default() -> Self {
        Self::new()
    }
}

impl DiskHealth {
    pub fn new() -> Self {
        Self {
            error_count: AtomicU64::new(0),
            success_count: AtomicU64::new(0),
            bypassed: AtomicBool::new(false),
            last_error_time: AtomicU64::new(0),
            error_threshold: 3,
            recovery_threshold: 10,
            reset_window_secs: 60,
        }
    }

    pub fn is_bypassed(&self) -> bool {
        if let Ok(now) = SystemTime::now().duration_since(UNIX_EPOCH) {
            let last = self.last_error_time.load(Ordering::Relaxed);
            if last > 0 && now.as_secs().saturating_sub(last) > self.reset_window_secs {
                self.error_count.store(0, Ordering::Relaxed);
            }
        }
        self.bypassed.load(Ordering::Relaxed)
    }

    pub fn record_error(&self) {
        if let Ok(now) = SystemTime::now().duration_since(UNIX_EPOCH) {
            self.last_error_time.store(now.as_secs(), Ordering::Relaxed);
        }
        self.success_count.store(0, Ordering::Relaxed);
        let count = self.error_count.fetch_add(1, Ordering::Relaxed) + 1;
        if count >= self.error_threshold {
            self.bypassed.store(true, Ordering::Relaxed);
            tracing::warn!(
                errors = count,
                "disk cache entering bypassed mode after repeated I/O errors"
            );
        }
    }

    pub fn record_success(&self) {
        let count = self.success_count.fetch_add(1, Ordering::Relaxed) + 1;
        if count >= self.recovery_threshold && self.bypassed.load(Ordering::Relaxed) {
            self.bypassed.store(false, Ordering::Relaxed);
            self.error_count.store(0, Ordering::Relaxed);
            self.success_count.store(0, Ordering::Relaxed);
            tracing::info!(successes = count, "disk cache recovered from bypassed mode");
        }
    }
}
