//! Network bandwidth rate limiting for upload and download operations.
//!
//! Uses a token-bucket algorithm to enforce configurable bandwidth caps.
//! When limits are set, callers must acquire tokens before transferring data,
//! which throttles throughput to the configured rate.

use std::num::NonZeroU32;
use std::sync::Arc;

use governor::{Quota, RateLimiter, clock::DefaultClock, state::InMemoryState, state::NotKeyed};
use tracing::{debug, trace};

type Limiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock>;

/// Configuration for bandwidth rate limiting.
#[derive(Debug, Clone, Default)]
pub struct BandwidthConfig {
    /// Maximum upload bandwidth in MiB/s. None = unlimited.
    pub upload_limit_mibps: Option<u64>,
    /// Maximum download bandwidth in MiB/s. None = unlimited.
    pub download_limit_mibps: Option<u64>,
}

/// Rate limiter for upload and download bandwidth.
///
/// Each direction has an independent token bucket. Tokens represent bytes.
/// The bucket refills at the configured rate (MiB/s), and callers must
/// acquire tokens proportional to the data size before transferring.
#[derive(Clone)]
pub struct BandwidthLimiter {
    upload: Option<Arc<Limiter>>,
    download: Option<Arc<Limiter>>,
}

impl BandwidthLimiter {
    /// Create a new bandwidth limiter from config.
    pub fn new(config: &BandwidthConfig) -> Self {
        let upload = config.upload_limit_mibps.and_then(|mibps| {
            if mibps == 0 {
                return None;
            }
            // Token unit = 64KB chunk. Rate = mibps * 16 tokens/sec (1 MiB = 16 × 64KB)
            let tokens_per_sec = (mibps * 16).max(1);
            let burst = tokens_per_sec.max(4); // allow small bursts
            let quota = Quota::per_second(NonZeroU32::new(tokens_per_sec as u32)?);
            let quota = quota.allow_burst(NonZeroU32::new(burst as u32)?);
            debug!(
                "Upload bandwidth limiter: {} MiB/s ({} tokens/s)",
                mibps, tokens_per_sec
            );
            Some(Arc::new(RateLimiter::direct(quota)))
        });

        let download = config.download_limit_mibps.and_then(|mibps| {
            if mibps == 0 {
                return None;
            }
            let tokens_per_sec = (mibps * 16).max(1);
            let burst = tokens_per_sec.max(4);
            let quota = Quota::per_second(NonZeroU32::new(tokens_per_sec as u32)?);
            let quota = quota.allow_burst(NonZeroU32::new(burst as u32)?);
            debug!(
                "Download bandwidth limiter: {} MiB/s ({} tokens/s)",
                mibps, tokens_per_sec
            );
            Some(Arc::new(RateLimiter::direct(quota)))
        });

        Self { upload, download }
    }

    /// Unlimited bandwidth (no rate limiting).
    pub fn unlimited() -> Self {
        Self {
            upload: None,
            download: None,
        }
    }

    /// Acquire upload bandwidth for `bytes` of data.
    /// Blocks until enough tokens are available.
    pub async fn acquire_upload(&self, bytes: usize) {
        if let Some(limiter) = &self.upload {
            // Each token represents 64KB
            let tokens = bytes.div_ceil(65536).max(1) as u32;
            self.acquire_tokens(limiter, tokens, "upload").await;
        }
    }

    /// Acquire download bandwidth for `bytes` of data.
    /// Blocks until enough tokens are available.
    pub async fn acquire_download(&self, bytes: usize) {
        if let Some(limiter) = &self.download {
            let tokens = bytes.div_ceil(65536).max(1) as u32;
            self.acquire_tokens(limiter, tokens, "download").await;
        }
    }

    /// Internal: acquire N tokens, waiting if necessary.
    async fn acquire_tokens(&self, limiter: &Limiter, tokens: u32, direction: &str) {
        // governor's `until_n_ready` can only request up to burst size at once,
        // so we loop requesting in smaller batches if needed.
        let mut remaining = tokens;
        while remaining > 0 {
            let batch = remaining.min(32); // max 32 tokens per wait (2MB)
            match NonZeroU32::new(batch) {
                Some(n) => {
                    let _ = limiter.until_n_ready(n).await;
                    trace!(
                        "{} rate limit: acquired {} tokens ({} remaining)",
                        direction,
                        batch,
                        remaining - batch
                    );
                }
                None => break,
            }
            remaining -= batch;
        }
    }

    /// Check if upload limiting is active
    #[allow(dead_code)]
    pub fn has_upload_limit(&self) -> bool {
        self.upload.is_some()
    }

    /// Check if download limiting is active
    #[allow(dead_code)]
    pub fn has_download_limit(&self) -> bool {
        self.download.is_some()
    }
}

impl std::fmt::Debug for BandwidthLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BandwidthLimiter")
            .field("upload_limited", &self.upload.is_some())
            .field("download_limited", &self.download.is_some())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_unlimited_does_not_block() {
        let limiter = BandwidthLimiter::unlimited();
        // Should return immediately
        limiter.acquire_upload(4 * 1024 * 1024).await;
        limiter.acquire_download(4 * 1024 * 1024).await;
    }

    #[tokio::test]
    async fn test_limiter_creation() {
        let config = BandwidthConfig {
            upload_limit_mibps: Some(100),
            download_limit_mibps: Some(200),
        };
        let limiter = BandwidthLimiter::new(&config);
        assert!(limiter.has_upload_limit());
        assert!(limiter.has_download_limit());
    }

    #[tokio::test]
    async fn test_zero_limit_means_unlimited() {
        let config = BandwidthConfig {
            upload_limit_mibps: Some(0),
            download_limit_mibps: Some(0),
        };
        let limiter = BandwidthLimiter::new(&config);
        assert!(!limiter.has_upload_limit());
        assert!(!limiter.has_download_limit());
    }

    #[tokio::test]
    async fn test_acquire_small_data() {
        let config = BandwidthConfig {
            upload_limit_mibps: Some(1000), // 1000 MiB/s — fast enough to not actually block
            download_limit_mibps: None,
        };
        let limiter = BandwidthLimiter::new(&config);
        limiter.acquire_upload(64 * 1024).await; // 64KB = 1 token
        limiter.acquire_upload(128 * 1024).await; // 128KB = 2 tokens
    }
}
