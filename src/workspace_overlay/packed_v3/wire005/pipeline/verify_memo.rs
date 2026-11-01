//! Mount-local evidence about immutable frame identities, never payload bytes.
//!
//! This relies on the object store preserving immutable objects after their
//! first successful verification. It cannot detect a same-length overwrite or
//! corrupt later response for an identity already in the memo. Set
//! BREWFS_PACKED_VERIFY_MEMO=0 to authenticate every response instead.

use super::{PackedResult, V3FrameDemand, invalid};
use lru::LruCache;
use std::num::NonZeroUsize;
use std::sync::Mutex;
use std::time::Instant;

pub(super) const CAPACITY: usize = 8192;
// 8192 nodes (64-byte key + timestamp + links + allocator slack, <=128 each),
// a <=16384-bucket pointer table/control bytes, sentinels and mutex fit below
// 1.5 MiB. Reserve before allocation, within the 256 MiB Metadata budget.
pub(super) const RESERVATION_BYTES: u64 = 1536 << 10;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) struct FrameIdentity {
    container: [u8; 32],
    digest: [u8; 16],
    offset: u64,
    ordinal: u32,
    stored_len: u32,
}

impl FrameIdentity {
    pub(super) fn from_demand(demand: &V3FrameDemand) -> Self {
        // Different physical keys can claim the same authenticated digest.
        // Evidence for one key must not skip the first verification of another.
        // Hash only this bounded identity, never the stored/decoded payload.
        let mut container = blake3::Hasher::new();
        container.update(&demand.container.digest);
        container.update(demand.container.key.as_bytes());
        Self {
            container: container.finalize().into(),
            digest: demand.descriptor.frame_digest,
            offset: demand.descriptor.object_offset,
            ordinal: demand.descriptor.frame_ordinal,
            stored_len: demand.descriptor.stored_len,
        }
    }
}

pub(super) struct VerifyMemo {
    entries: Option<Mutex<LruCache<FrameIdentity, Instant>>>,
}

impl VerifyMemo {
    pub(super) fn enabled_from_env() -> PackedResult<bool> {
        match std::env::var("BREWFS_PACKED_VERIFY_MEMO") {
            Ok(value) => Self::parse_switch(Some(&value)),
            Err(std::env::VarError::NotPresent) => Self::parse_switch(None),
            Err(_) => Err(invalid("BREWFS_PACKED_VERIFY_MEMO must be 0 or 1")),
        }
    }

    fn parse_switch(value: Option<&str>) -> PackedResult<bool> {
        match value {
            None | Some("1") => Ok(true),
            Some("0") => Ok(false),
            _ => Err(invalid("BREWFS_PACKED_VERIFY_MEMO must be 0 or 1")),
        }
    }

    pub(super) fn new(enabled: bool) -> Self {
        Self::with_capacity(if enabled { CAPACITY } else { 0 })
    }

    fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: NonZeroUsize::new(capacity)
                .map(|capacity| Mutex::new(LruCache::new(capacity))),
        }
    }

    pub(super) fn contains_verified(&self, identity: &FrameIdentity) -> bool {
        self.entries.as_ref().is_some_and(|entries| {
            let mut entries = entries.lock().unwrap();
            if let Some(accessed) = entries.get_mut(identity) {
                *accessed = Instant::now();
                true
            } else {
                false
            }
        })
    }

    /// Call only after comparing the actual stored bytes' digest successfully.
    /// A concurrent miss may hash twice; no pending/unverified entry is shared.
    pub(super) fn remember_verified(&self, identity: FrameIdentity) {
        if let Some(entries) = &self.entries {
            entries.lock().unwrap().put(identity, Instant::now());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(ordinal: u32) -> FrameIdentity {
        FrameIdentity {
            container: [1; 32],
            digest: [2; 16],
            offset: 4096,
            ordinal,
            stored_len: 64,
        }
    }

    #[test]
    fn switch_defaults_on_and_rejects_ambiguous_values() {
        assert!(VerifyMemo::parse_switch(None).unwrap());
        assert!(VerifyMemo::parse_switch(Some("1")).unwrap());
        assert!(!VerifyMemo::parse_switch(Some("0")).unwrap());
        for value in ["", "false", "true", "2", " 1"] {
            assert!(VerifyMemo::parse_switch(Some(value)).is_err());
        }
    }

    #[test]
    fn lru_is_bounded_and_requires_successful_verification_after_eviction() {
        let memo = VerifyMemo::with_capacity(2);
        assert!(!memo.contains_verified(&identity(0)));
        memo.remember_verified(identity(0));
        memo.remember_verified(identity(1));
        assert!(memo.contains_verified(&identity(0)));
        memo.remember_verified(identity(2));
        assert!(!memo.contains_verified(&identity(1)));
        assert!(memo.contains_verified(&identity(0)));
        assert!(memo.contains_verified(&identity(2)));
        for ordinal in 3..10_000 {
            memo.remember_verified(identity(ordinal));
        }
        assert_eq!(memo.entries.as_ref().unwrap().lock().unwrap().len(), 2);
        assert!(!memo.contains_verified(&identity(0)));
    }

    #[test]
    fn disabled_memo_retains_no_entries_and_mounts_do_not_share_evidence() {
        let disabled = VerifyMemo::new(false);
        disabled.remember_verified(identity(0));
        assert!(!disabled.contains_verified(&identity(0)));
        assert!(disabled.entries.is_none());
        let first = VerifyMemo::new(true);
        first.remember_verified(identity(0));
        assert!(first.contains_verified(&identity(0)));
        assert!(!VerifyMemo::new(true).contains_verified(&identity(0)));
    }
}
