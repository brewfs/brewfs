//! Compatibility types for legacy diagnostic call sites. Unit tests retain
//! their existing counter assertions; production builds require the feature.
#[cfg(any(test, feature = "observability"))]
pub use std::{sync::atomic::AtomicU64 as Counter, time::Instant};

#[cfg(not(any(test, feature = "observability")))]
mod disabled {
    use std::{sync::atomic::Ordering, time::Duration};
    #[derive(Debug, Default)]
    pub struct Counter;
    impl Counter {
        #[inline(always)]
        pub const fn new(_: u64) -> Self {
            Self
        }
        #[inline(always)]
        pub fn load(&self, _: Ordering) -> u64 {
            0
        }
        #[inline(always)]
        pub fn store(&self, _: u64, _: Ordering) {}
        #[inline(always)]
        pub fn fetch_add(&self, _: u64, _: Ordering) -> u64 {
            0
        }
        #[inline(always)]
        pub fn fetch_sub(&self, _: u64, _: Ordering) -> u64 {
            0
        }
    }
    #[derive(Debug, Clone, Copy)]
    pub struct Instant;
    impl Instant {
        #[inline(always)]
        pub fn now() -> Self {
            Self
        }
        #[inline(always)]
        pub fn elapsed(&self) -> Duration {
            Duration::ZERO
        }
    }
}
#[cfg(not(any(test, feature = "observability")))]
pub use disabled::{Counter, Instant};
