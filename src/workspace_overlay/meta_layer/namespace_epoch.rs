//! A local namespace fence, usable only alongside an exclusive lease proof.
//! Data/attribute commits do not change directory cookies. Namespace commits
//! fence readers before dispatch and publish a new generation on completion.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

#[derive(Default)]
pub(super) struct NamespaceEpoch {
    generation: AtomicU64,
    writers: AtomicU64,
    uncertain: AtomicBool,
}

pub(super) struct NamespaceSnapshot {
    epoch: Arc<NamespaceEpoch>,
    generation: u64,
}

impl NamespaceEpoch {
    pub(super) fn capture(self: &Arc<Self>) -> Option<NamespaceSnapshot> {
        if self.uncertain.load(Ordering::SeqCst) {
            return None;
        }
        Some(NamespaceSnapshot {
            epoch: self.clone(),
            generation: self.generation.load(Ordering::SeqCst),
        })
    }

    pub(super) fn begin_write(self: &Arc<Self>) -> NamespaceWriteGuard {
        self.writers.fetch_add(1, Ordering::SeqCst);
        NamespaceWriteGuard {
            epoch: self.clone(),
            outcome: WriteOutcome::Uncertain,
        }
    }

    pub(super) fn invalidate(&self) {
        self.generation.fetch_add(1, Ordering::SeqCst);
    }
}

impl NamespaceSnapshot {
    pub(super) fn unchanged(&self) -> bool {
        // Two generation reads bracket the other observations. A writer that
        // finishes during validation cannot make its new rows look unchanged.
        self.epoch.generation.load(Ordering::SeqCst) == self.generation
            && self.epoch.writers.load(Ordering::SeqCst) == 0
            && !self.epoch.uncertain.load(Ordering::SeqCst)
            && self.epoch.generation.load(Ordering::SeqCst) == self.generation
    }
}

enum WriteOutcome {
    Uncertain,
    Committed,
    Unchanged,
}

pub(super) struct NamespaceWriteGuard {
    epoch: Arc<NamespaceEpoch>,
    outcome: WriteOutcome,
}

impl NamespaceWriteGuard {
    pub(super) fn committed(mut self) {
        self.outcome = WriteOutcome::Committed;
    }

    /// Only a definite conditional rejection guarantees that no row changed.
    pub(super) fn unchanged(mut self) {
        self.outcome = WriteOutcome::Unchanged;
    }
}

impl Drop for NamespaceWriteGuard {
    fn drop(&mut self) {
        match self.outcome {
            WriteOutcome::Uncertain => {
                // Cancellation can leave an owned backend commit running.
                // No later snapshot may use local authority in this mount.
                self.epoch.uncertain.store(true, Ordering::SeqCst);
                self.epoch.invalidate();
            }
            WriteOutcome::Committed => self.epoch.invalidate(),
            WriteOutcome::Unchanged => {}
        }
        self.epoch.writers.fetch_sub(1, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readers_are_fenced_during_and_after_a_committed_write() {
        let epoch = Arc::new(NamespaceEpoch::default());
        let before = epoch.capture().unwrap();
        assert!(before.unchanged());
        let writer = epoch.begin_write();
        let during = epoch.capture().unwrap();
        assert!(!before.unchanged());
        assert!(!during.unchanged());
        writer.committed();
        assert!(!before.unchanged());
        assert!(!during.unchanged());
        assert!(epoch.capture().unwrap().unchanged());
    }

    #[test]
    fn rejected_cas_does_not_change_cookie_generation() {
        let epoch = Arc::new(NamespaceEpoch::default());
        let before = epoch.capture().unwrap();
        let first = epoch.begin_write();
        let second = epoch.begin_write();
        let during = epoch.capture().unwrap();
        first.unchanged();
        assert!(!before.unchanged());
        second.unchanged();
        assert!(before.unchanged());
        assert!(during.unchanged());
    }

    #[test]
    fn cancellation_permanently_disables_local_namespace_proof() {
        let epoch = Arc::new(NamespaceEpoch::default());
        let before = epoch.capture().unwrap();
        let canceled = epoch.begin_write();
        let other = epoch.begin_write();
        drop(canceled);
        other.committed();
        assert!(!before.unchanged());
        assert!(epoch.capture().is_none());
        epoch.begin_write().unchanged();
        assert!(epoch.capture().is_none());
    }

    #[test]
    fn view_invalidation_fences_existing_snapshots() {
        let epoch = Arc::new(NamespaceEpoch::default());
        let before = epoch.capture().unwrap();
        epoch.invalidate();
        assert!(!before.unchanged());
        assert!(epoch.capture().unwrap().unchanged());
    }
}
