//! Fixed buckets; updates touch one disjoint bucket, exposition is cumulative.
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Duration;

pub const READ: &[u64] = &[
    100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000,
    1_000_000,
];
pub const WRITE: &[u64] = &[
    1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000, 1_000_000, 2_500_000,
    5_000_000, 10_000_000,
];
pub const META: &[u64] = &[
    50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000,
];

pub struct Histogram {
    pub bounds: &'static [u64],
    buckets: Vec<AtomicU64>,
    nanos: AtomicU64,
}
impl Histogram {
    pub fn new(bounds: &'static [u64]) -> Self {
        Self {
            bounds,
            buckets: (0..=bounds.len()).map(|_| AtomicU64::new(0)).collect(),
            nanos: AtomicU64::new(0),
        }
    }
    pub fn observe(&self, duration: Duration) {
        let nanos = duration.as_nanos().min(u64::MAX as u128) as u64;
        let bucket = self
            .bounds
            .partition_point(|bound| bound.saturating_mul(1_000) < nanos);
        self.nanos.fetch_add(nanos, Relaxed);
        self.buckets[bucket].fetch_add(1, Relaxed);
    }
    pub fn snapshot(&self) -> (Vec<u64>, f64) {
        let mut sum = 0;
        let buckets = self
            .buckets
            .iter()
            .map(|bucket| {
                sum += bucket.load(Relaxed);
                sum
            })
            .collect();
        (buckets, self.nanos.load(Relaxed) as f64 / 1e9)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn hundred_known_latencies_have_exact_cumulative_buckets() {
        for bounds in [READ, WRITE, META] {
            let histogram = Histogram::new(bounds);
            for _ in 0..25 {
                histogram.observe(Duration::ZERO);
                histogram.observe(Duration::from_micros(bounds[0]));
                histogram.observe(Duration::from_nanos(bounds[0] * 1_000 + 1));
                histogram.observe(Duration::from_secs(20));
            }
            let (buckets, sum) = histogram.snapshot();
            assert_eq!(buckets[0], 50);
            assert!(buckets[1..bounds.len()].iter().all(|n| *n == 75));
            assert_eq!(buckets[bounds.len()], 100);
            let expected = 500.0 + 50.0 * bounds[0] as f64 / 1e6 + 25e-9;
            assert!((sum - expected).abs() < 1e-9);
        }
    }
}
