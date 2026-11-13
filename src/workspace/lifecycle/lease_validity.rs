//! Local proof that this mount still holds its exclusive workspace lease.
//!
//! Each successful renewal (or the initial acquisition) records a local
//! deadline: the renewal's start instant plus the lease TTL. The lease is
//! exclusive and every upper mutation re-authenticates it in its CAS, so
//! before that deadline no other holder can have changed upper rows. A failed
//! renewal revokes the proof permanently; a late or missed renewal lets the
//! deadline pass. Caches that serve upper metadata without a per-operation
//! remote fence must stop once `authoritative()` is false.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct LeaseValidity {
    origin: Instant,
    /// Nanoseconds after `origin` until which the lease is known held.
    until_ns: AtomicU64,
    revoked: AtomicBool,
}

impl LeaseValidity {
    /// `ttl` from `started`, the instant just before the acquisition request.
    pub fn new(started: Instant, ttl: Duration) -> Self {
        let validity = Self {
            origin: started,
            until_ns: AtomicU64::new(0),
            revoked: AtomicBool::new(false),
        };
        validity.extend(started, ttl);
        validity
    }

    /// A renewal that was sent at `started` succeeded with this `ttl`.
    pub fn extend(&self, started: Instant, ttl: Duration) {
        let until = started
            .saturating_duration_since(self.origin)
            .saturating_add(ttl);
        let until = u64::try_from(until.as_nanos()).unwrap_or(u64::MAX);
        self.until_ns.fetch_max(until, Ordering::AcqRel);
    }

    /// The lease is lost or can no longer be proven; never authoritative again.
    pub fn revoke(&self) {
        self.revoked.store(true, Ordering::Release);
    }

    pub fn authoritative(&self) -> bool {
        if self.revoked.load(Ordering::Acquire) {
            return false;
        }
        let now = Instant::now().saturating_duration_since(self.origin);
        u64::try_from(now.as_nanos()).unwrap_or(u64::MAX) < self.until_ns.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deadline_extends_on_renewal_and_revocation_is_permanent() {
        let start = Instant::now();
        let validity = LeaseValidity::new(start, Duration::from_millis(40));
        assert!(validity.authoritative());
        std::thread::sleep(Duration::from_millis(60));
        assert!(
            !validity.authoritative(),
            "missed renewal lets the deadline pass"
        );
        validity.extend(Instant::now(), Duration::from_secs(30));
        assert!(validity.authoritative());
        // An older renewal can never shorten a later deadline.
        validity.extend(start, Duration::from_millis(1));
        assert!(validity.authoritative());
        validity.revoke();
        assert!(!validity.authoritative());
        validity.extend(Instant::now(), Duration::from_secs(30));
        assert!(!validity.authoritative(), "revocation is permanent");
    }
}
