//! Writer-lease fencing and the confirmed-write boundary (KV-004/KV-005,
//! WRITE-007).
//!
//! A lease is only meaningful when every writer agrees on the clock that
//! decides it, and that clock belongs to the backend.  A skewed client clock
//! may neither extend a lease nor expire one, so every validity decision here
//! asks a [`LeaseClock`] and refuses any clock whose source is not the
//! backend.  A fenced writer produces no metadata: the commit it would have
//! driven is refused before its transaction runs.
//!
//! [`DurabilityProfile`] reports what a successful reply actually confirms and
//! what may still be lost if the process, the host or the backend fails next.
//! The boundary is verified, not asserted: the confirmed stages must be a
//! prefix of the write stages, so no profile can claim a later stage while
//! leaving an earlier one lossy.

use crate::native_base::wire::bnct::Id16;
use crate::native_base::write::domain::HeadGuard;
use crate::native_base::write::error::WriteError;
use crate::native_base::write::records::HeadState;

/// Where a lease timestamp came from.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimeSource {
    /// The storage backend's own clock (Redis `TIME`, TiKV TSO, ...).
    Backend,
    /// The calling process's clock; never trusted for lease validity.
    Client,
}

/// The clock a coordinator reads lease time from.
pub trait LeaseClock {
    fn source(&self) -> TimeSource;
    fn now_ns(&self) -> u64;
}

/// A clock with a fixed reading, for tests and for replaying a decision.
#[derive(Clone, Copy, Debug)]
pub struct FixedClock {
    pub source: TimeSource,
    pub now_ns: u64,
}

impl FixedClock {
    pub fn backend(now_ns: u64) -> Self {
        Self {
            source: TimeSource::Backend,
            now_ns,
        }
    }

    /// A deliberately skewed client clock: whatever it says must never change
    /// a lease decision.
    pub fn skewed_client(now_ns: u64) -> Self {
        Self {
            source: TimeSource::Client,
            now_ns,
        }
    }
}

impl LeaseClock for FixedClock {
    fn source(&self) -> TimeSource {
        self.source
    }

    fn now_ns(&self) -> u64 {
        self.now_ns
    }
}

/// A granted writer lease: who owns which generation, from when, for how long
/// — all in backend time.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LeaseGrant {
    pub workspace_id: Id16,
    pub owner_generation: u64,
    pub granted_at_ns: u64,
    pub ttl_ns: u64,
}

/// Why a lease is or is not usable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LeaseFence {
    Held,
    Superseded { held: u64, current: u64 },
    Expired { now_ns: u64, deadline_ns: u64 },
}

impl LeaseGrant {
    pub fn deadline_ns(&self) -> u64 {
        self.granted_at_ns.saturating_add(self.ttl_ns)
    }

    /// Decide the lease against the *backend* clock and the head generation.
    /// A clock that is not the backend's is refused rather than consulted: a
    /// client whose clock is an hour ahead could otherwise extend its lease,
    /// and one an hour behind could expire a healthy one.
    pub fn evaluate(
        &self,
        clock: &dyn LeaseClock,
        current_generation: u64,
    ) -> Result<LeaseFence, WriteError> {
        if clock.source() != TimeSource::Backend {
            return Err(WriteError::LeaseFence(
                "lease validity must be decided on backend time".into(),
            ));
        }
        if self.owner_generation != current_generation {
            return Ok(LeaseFence::Superseded {
                held: self.owner_generation,
                current: current_generation,
            });
        }
        let now_ns = clock.now_ns();
        if now_ns >= self.deadline_ns() {
            return Ok(LeaseFence::Expired {
                now_ns,
                deadline_ns: self.deadline_ns(),
            });
        }
        Ok(LeaseFence::Held)
    }

    /// The commit guard this lease authorises.  A superseded or expired lease
    /// yields no guard at all, so the fenced write cannot even be attempted.
    pub fn guard(&self, head: &HeadState, clock: &dyn LeaseClock) -> Result<HeadGuard, WriteError> {
        match self.evaluate(clock, head.writer_generation)? {
            LeaseFence::Held => Ok(HeadGuard {
                workspace_id: self.workspace_id,
                expected_head: head.clone(),
            }),
            LeaseFence::Superseded { held, current } => Err(WriteError::LeaseFence(format!(
                "generation {held} was superseded by {current}"
            ))),
            LeaseFence::Expired {
                now_ns,
                deadline_ns,
            } => Err(WriteError::LeaseFence(format!(
                "lease expired: backend time {now_ns} passed deadline {deadline_ns}"
            ))),
        }
    }
}

/// The observable stages of one native write.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum WriteStage {
    /// The object PUT was acknowledged and verified by the backend.
    UploadVerified,
    /// The durable receipt set (spec 07 §2) was written.
    ReceiptDurable,
    /// The mutation transaction committed extent/placement/inode/head.
    MutationCommitted,
    /// Metadata storage confirmed the commit is on stable media.
    MetadataSynced,
}

/// Every stage in order; confirmation is only meaningful as a prefix.
pub const WRITE_STAGES: [WriteStage; 4] = [
    WriteStage::UploadVerified,
    WriteStage::ReceiptDurable,
    WriteStage::MutationCommitted,
    WriteStage::MetadataSynced,
];

/// What the storage actually confirms once it has acknowledged a write.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DurabilityProfile {
    /// Only in-memory state: an acknowledgement confirms nothing durable.
    VolatileOnly,
    /// Backend acknowledged the uploaded object; the receipt and the commit
    /// are still pending and may be lost.
    BackendAck,
    /// Backend acknowledged the object and the receipt set; the mutation
    /// commit may still be lost.
    BackendAckWithReceipt,
    /// Metadata storage confirms the commit on stable media.
    FsyncConfirmed,
}

impl DurabilityProfile {
    pub const ALL: [DurabilityProfile; 4] = [
        DurabilityProfile::VolatileOnly,
        DurabilityProfile::BackendAck,
        DurabilityProfile::BackendAckWithReceipt,
        DurabilityProfile::FsyncConfirmed,
    ];

    pub fn as_u8(self) -> u8 {
        match self {
            Self::VolatileOnly => 0,
            Self::BackendAck => 1,
            Self::BackendAckWithReceipt => 2,
            Self::FsyncConfirmed => 3,
        }
    }

    pub fn from_u8(code: u8) -> Result<Self, WriteError> {
        match code {
            0 => Ok(Self::VolatileOnly),
            1 => Ok(Self::BackendAck),
            2 => Ok(Self::BackendAckWithReceipt),
            3 => Ok(Self::FsyncConfirmed),
            other => Err(WriteError::Record(format!(
                "unknown durability profile {other}"
            ))),
        }
    }
}

/// What a successful reply confirms and what may still be lost.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurabilityBoundary {
    pub profile: DurabilityProfile,
    pub confirmed: Vec<WriteStage>,
    pub may_be_lost: Vec<WriteStage>,
}

impl DurabilityBoundary {
    /// A profile may not confirm a later stage while leaving an earlier one
    /// lossy, and the two halves must partition the stages exactly.
    pub fn verify(&self) -> Result<(), WriteError> {
        let mut joined: Vec<WriteStage> = self
            .confirmed
            .iter()
            .chain(self.may_be_lost.iter())
            .copied()
            .collect();
        joined.sort();
        if joined != WRITE_STAGES {
            return Err(WriteError::Record(
                "durability boundary does not partition the write stages".into(),
            ));
        }
        if !self.confirmed.is_empty() || !self.may_be_lost.is_empty() {
            let split = self.confirmed.len();
            if WRITE_STAGES[..split] != self.confirmed[..]
                || WRITE_STAGES[split..] != self.may_be_lost[..]
            {
                return Err(WriteError::Record(
                    "confirmed writes must be a prefix of the write stages".into(),
                ));
            }
        }
        Ok(())
    }

    pub fn confirms(&self, stage: WriteStage) -> bool {
        self.confirmed.contains(&stage)
    }
}

/// Report the boundary of a profile.  The result is verified before it is
/// returned, so a caller can never read a claim the storage does not support.
pub fn durability_boundary(profile: DurabilityProfile) -> Result<DurabilityBoundary, WriteError> {
    let split = match profile {
        DurabilityProfile::VolatileOnly => 0,
        DurabilityProfile::BackendAck => 1,
        DurabilityProfile::BackendAckWithReceipt => 2,
        DurabilityProfile::FsyncConfirmed => WRITE_STAGES.len(),
    };
    let boundary = DurabilityBoundary {
        profile,
        confirmed: WRITE_STAGES[..split].to_vec(),
        may_be_lost: WRITE_STAGES[split..].to_vec(),
    };
    boundary.verify()?;
    Ok(boundary)
}

/// A persisted profile code is decoded fail-closed: an unknown code is
/// refused instead of being rounded to the nearest known durability.
pub fn durability_boundary_for_code(code: u8) -> Result<DurabilityBoundary, WriteError> {
    durability_boundary(DurabilityProfile::from_u8(code)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    const WORKSPACE: Id16 = [1u8; 16];

    fn grant() -> LeaseGrant {
        LeaseGrant {
            workspace_id: WORKSPACE,
            owner_generation: 4,
            granted_at_ns: 1_000_000,
            ttl_ns: 5_000_000,
        }
    }

    /// KV-004 / INV-08: a lease is decided on backend time.  A client clock is
    /// refused, and an arbitrarily skewed one cannot change the verdict the
    /// backend clock produces.
    #[test]
    fn lease_validity_comes_from_the_backend_clock_and_ignores_client_skew() {
        let grant = grant();
        assert_eq!(grant.deadline_ns(), 6_000_000);
        let held = FixedClock::backend(1_200_000);
        assert_eq!(grant.evaluate(&held, 4).unwrap(), LeaseFence::Held);
        assert!(matches!(
            grant.evaluate(&FixedClock::backend(6_000_000), 4).unwrap(),
            LeaseFence::Expired { .. }
        ));
        assert_eq!(
            grant.evaluate(&FixedClock::backend(0), 4).unwrap(),
            LeaseFence::Held
        );

        for reading in [0u64, 1_200_000, 60 * 60 * 1_000_000_000] {
            let error = grant
                .evaluate(&FixedClock::skewed_client(reading), 4)
                .unwrap_err();
            assert!(matches!(error, WriteError::LeaseFence(_)));
            assert!(error.to_string().contains("backend time"), "{error}");
            assert!(!error.is_retryable(), "a fence is not retried as-is");
        }

        // The generation decides before the clock, so a superseded lease is
        // reported as superseded even while its ttl is still running.
        assert_eq!(
            grant.evaluate(&held, 5).unwrap(),
            LeaseFence::Superseded {
                held: 4,
                current: 5
            }
        );
    }

    /// KV-005 / INV-07: the durability profile reports which stages a
    /// successful reply confirms and which may still be lost, and no profile
    /// may claim a later stage while leaving an earlier one lossy.
    #[test]
    fn durability_profiles_report_their_lossy_boundary_and_verify() {
        for profile in DurabilityProfile::ALL {
            let boundary = durability_boundary(profile).unwrap();
            boundary.verify().unwrap();
            assert_eq!(boundary.profile, profile);
            assert_eq!(
                boundary.confirmed.len() + boundary.may_be_lost.len(),
                WRITE_STAGES.len()
            );
            assert!(
                !boundary.confirms(WriteStage::MutationCommitted)
                    || boundary.confirms(WriteStage::UploadVerified),
                "confirmation is a prefix"
            );
            assert_eq!(
                durability_boundary_for_code(profile.as_u8()).unwrap(),
                boundary
            );
        }
        let volatile = durability_boundary(DurabilityProfile::VolatileOnly).unwrap();
        assert!(volatile.confirmed.is_empty());
        assert_eq!(volatile.may_be_lost, WRITE_STAGES.to_vec());
        let acknowledged = durability_boundary(DurabilityProfile::BackendAck).unwrap();
        assert_eq!(acknowledged.confirmed, vec![WriteStage::UploadVerified]);
        assert!(
            acknowledged
                .may_be_lost
                .contains(&WriteStage::ReceiptDurable)
                && acknowledged
                    .may_be_lost
                    .contains(&WriteStage::MutationCommitted)
        );
        let fsync = durability_boundary(DurabilityProfile::FsyncConfirmed).unwrap();
        assert_eq!(fsync.confirmed, WRITE_STAGES.to_vec());
        assert!(fsync.may_be_lost.is_empty());

        let error = durability_boundary_for_code(9).unwrap_err();
        assert!(
            error.to_string().contains("unknown durability profile"),
            "{error}"
        );

        let bogus = DurabilityBoundary {
            profile: DurabilityProfile::FsyncConfirmed,
            confirmed: vec![WriteStage::MutationCommitted],
            may_be_lost: vec![
                WriteStage::UploadVerified,
                WriteStage::ReceiptDurable,
                WriteStage::MetadataSynced,
            ],
        };
        assert!(bogus.verify().is_err());
    }
}
