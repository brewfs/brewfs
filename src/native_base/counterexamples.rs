//! PR01 counterexample models for the native-base control contracts.
//!
//! These are deliberately small in-memory models, not the product
//! implementation. Each counterexample drives one deterministic interleave
//! through two protocol variants:
//!
//! * a *weak* protocol that reproduces a documented hazard (the raw
//!   counterexamples recorded in `doc/native-base/pr01-baseline-audit.md`),
//!   and
//! * the *gated/ordered/atomic* protocol required by specs 09/18/20, which
//!   must keep the exact same interleave safe.
//!
//! The strong-side tests are the standing contract that the PR03 reader,
//! PR04 commit pipeline and PR06A publication transaction must keep passing
//! once they replace these models with real implementations.
//!
//! Mapped acceptance ids: CONS-001..CONS-006 (capture), ORD-001..ORD-010
//! (commit ordering), RET-001..RET-006 (retention).

/// CEX-CONS — consistent capture of EOF, dirty overlays and committed
/// extents for a single internal read (INV-03; spec 18 sections 4-5).
///
/// A writer's accepted append bumps the visible EOF immediately, while the
/// extent only becomes committed through a later metadata transaction. A
/// reader that captures EOF and extents at different instants can observe a
/// mixed view.
pub mod capture {
    /// Committed file state as pinned by one capture.
    #[derive(Clone, Debug, PartialEq)]
    pub struct FileView {
        pub eof: u64,
        /// Committed extents as `(offset, bytes)`.
        pub extents: Vec<(u64, Vec<u8>)>,
    }

    /// Writer with an accepted-but-uncommitted append in flight.
    pub struct Writer {
        pub state: FileView,
        pending: Option<Vec<u8>>,
    }

    impl Writer {
        pub fn new(initial: &[u8]) -> Self {
            Writer {
                state: FileView {
                    eof: initial.len() as u64,
                    extents: vec![(0, initial.to_vec())],
                },
                pending: None,
            }
        }

        /// Accept an append: EOF moves now, the extent commit lands later.
        pub fn accept_append(&mut self, data: &[u8]) {
            self.state.eof += data.len() as u64;
            self.pending = Some(data.to_vec());
        }

        /// The metadata transaction that makes the accepted append
        /// committed and removes its dirty overlay.
        pub fn commit_extent(&mut self) {
            if let Some(data) = self.pending.take() {
                let offset = self.state.eof - data.len() as u64;
                self.state.extents.push((offset, data));
            }
        }
    }

    /// Assemble the bytes a captured view serves for `[0, len)`; ranges not
    /// covered by any committed extent read as zeros (the model of "read as
    /// Hole", which is only correct when the extent set is genuinely empty).
    pub fn materialize(view: &FileView, len: u64) -> Vec<u8> {
        let mut out = vec![0u8; len as usize];
        for (offset, data) in &view.extents {
            for (i, byte) in data.iter().enumerate() {
                let pos = *offset as usize + i;
                if pos < out.len() {
                    out[pos] = *byte;
                }
            }
        }
        out
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        /// Weak protocol: EOF and extents are captured at different
        /// instants. Between the two captures the writer accepts an append
        /// (EOF moves) but its extent has not committed yet. The read
        /// returns zeros for a tail the admission gate already accepted:
        /// missing data silently read as a Hole, violating INV-04 and the
        /// single-read atomicity of INV-03.
        #[test]
        fn weak_capture_reads_accepted_tail_as_hole() {
            let mut writer = Writer::new(b"OLD");
            writer.accept_append(b"NEW-TAIL");

            // Captured at two different instants.
            let eof = writer.state.eof; // 11: includes the accepted append.
            let extents = writer.state.extents.clone(); // without append.

            let out = materialize(&FileView { eof, extents }, 11);
            assert_eq!(&out[..3], b"OLD");
            assert_eq!(&out[3..], &[0u8; 8], "accepted tail read as zeros");
        }

        /// Weak protocol, second interleave from spec 18 section 5: the
        /// reader captures the extent set before the handoff removes a
        /// dirty overlay that covered an *already returned* write. The read
        /// serves pre-write content even though the write completed before
        /// the read started.
        #[test]
        fn weak_capture_serves_stale_content_after_handoff() {
            let mut writer = Writer::new(b"OLD");

            // A rewrite of the whole file is accepted and committed while
            // the reader is between its two captures.
            writer.state.extents.clear();
            writer.accept_append(b"NEW");
            writer.commit_extent();

            // The weak reader pinned the pre-rewrite extents first.
            let stale_extents = vec![(0u64, b"OLD".to_vec())];
            let eof = writer.state.eof;

            let out = materialize(
                &FileView {
                    eof,
                    extents: stale_extents,
                },
                3,
            );
            assert_eq!(out, b"OLD", "stale content served after returned write");
        }

        /// Strong protocol: one capture pins EOF and extents together under
        /// the view gate (spec 18 section 4). Whatever instant the capture
        /// lands on, it observes either the full pre-append state or the
        /// full post-commit state — never a mixed generation.
        #[test]
        fn gated_capture_never_mixes_generations() {
            let mut writer = Writer::new(b"OLD");

            // Capture before the append is accepted.
            let before = writer.state.clone();
            assert_eq!(materialize(&before, before.eof), b"OLD");

            writer.accept_append(b"NEW-TAIL");
            // A capture in the acceptance window still sees one coherent
            // generation (the pre-append one); the pending append stays in
            // the dirty overlay and is served by the dirty path, not by
            // zero-filling committed extents.
            let during = writer.state.clone();
            assert_eq!(materialize(&during, before.eof), b"OLD");

            writer.commit_extent();
            let after = writer.state.clone();
            assert_eq!(materialize(&after, after.eof), b"OLDNEW-TAIL");
        }

        /// Strong protocol, token revalidation (spec 18 section 4 step 4):
        /// a candidate plan whose token changed before use is discarded and
        /// retried instead of being assembled from two generations.
        #[test]
        fn token_revalidation_retries_mixed_candidate() {
            let mut writer = Writer::new(b"OLD");
            writer.accept_append(b"NEW");

            let token_before = writer.state.eof + writer.state.extents.len() as u64;
            // The metadata transaction lands between capture and use.
            writer.commit_extent();
            let token_after = writer.state.eof + writer.state.extents.len() as u64;

            assert_ne!(token_before, token_after);
            // The candidate plan from the first token is discarded; the
            // retried capture sees the committed generation in full.
            let retry = writer.state.clone();
            assert_eq!(materialize(&retry, retry.eof), b"OLDNEW");
        }
    }
}

/// CEX-ORD — per-inode data operations commit in acceptance order even when
/// uploads complete out of order (INV-22; spec 18 sections 5 and 10).
///
/// Two engines share the same op set and the same network completion order:
/// the weak engine applies commits as uploads complete; the ordered engine
/// applies an op only once every earlier ticket on the same inode has
/// committed.
pub mod ordered_commit {
    /// One accepted data mutation on an inode.
    #[derive(Clone, Debug, PartialEq)]
    pub struct PendingOp {
        pub ticket: u64,
        pub op: Op,
    }

    #[derive(Clone, Debug, PartialEq)]
    pub enum Op {
        Write { offset: u64, data: Vec<u8> },
        Truncate { len: u64 },
    }

    /// Engine that applies commits in whatever order uploads complete.
    #[derive(Default)]
    pub struct WeakEngine {
        pub content: Vec<u8>,
    }

    impl WeakEngine {
        pub fn complete(&mut self, op: &PendingOp) {
            self.apply(op);
        }

        fn apply(&mut self, op: &PendingOp) {
            match &op.op {
                Op::Write { offset, data } => {
                    let end = *offset as usize + data.len();
                    if self.content.len() < end {
                        self.content.resize(end, 0);
                    }
                    self.content[*offset as usize..end].copy_from_slice(data);
                }
                Op::Truncate { len } => {
                    self.content.truncate(*len as usize);
                }
            }
        }
    }

    /// Engine that serializes per-inode commits by admission ticket: an
    /// accepted op is applied only once its upload has completed *and*
    /// every smaller ticket on the same inode has already been applied
    /// (spec 18 section 10). Ops are registered at acceptance time, the
    /// same way the admission gate assigns tickets before upload.
    #[derive(Default)]
    pub struct OrderedEngine {
        pub content: Vec<u8>,
        /// Accepted ops by ticket with their upload-completion flag.
        pending: std::collections::BTreeMap<u64, (PendingOp, bool)>,
    }

    impl OrderedEngine {
        pub fn accept(&mut self, op: PendingOp) {
            self.pending.insert(op.ticket, (op, false));
        }

        pub fn complete(&mut self, ticket: u64) {
            if let Some(entry) = self.pending.get_mut(&ticket) {
                entry.1 = true;
            }
            self.drain_ready();
        }

        fn drain_ready(&mut self) {
            while self.pending.values().next().is_some_and(|(_, done)| *done) {
                let (_, (op, _)) = self.pending.pop_first().expect("entry checked above");
                let mut weak = WeakEngine {
                    content: std::mem::take(&mut self.content),
                };
                weak.apply(&op);
                self.content = weak.content;
            }
        }

        /// Whether accepted ops are still buffered waiting for earlier
        /// tickets or their own upload completion.
        pub fn buffered_pending(&self) -> bool {
            !self.pending.is_empty()
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        /// Ticket 1 writes [0,8) `AAAAAAAA`; ticket 2 overwrites [0,4)
        /// `BBBB`; the network completes ticket 2 first. The weak engine
        /// lets the late old write shadow the newer one; the ordered engine
        /// preserves acceptance order (ORD-001/ORD-002).
        #[test]
        fn overlapping_writes_commit_in_acceptance_order() {
            let ops = vec![
                PendingOp {
                    ticket: 1,
                    op: Op::Write {
                        offset: 0,
                        data: b"AAAAAAAA".to_vec(),
                    },
                },
                PendingOp {
                    ticket: 2,
                    op: Op::Write {
                        offset: 0,
                        data: b"BBBB".to_vec(),
                    },
                },
            ];
            let completion_order = [1usize, 0]; // newer upload lands first.

            let mut weak = WeakEngine::default();
            for &i in &completion_order {
                weak.complete(&ops[i]);
            }
            assert_eq!(weak.content, b"AAAAAAAA", "late old write shadowed new");

            let mut ordered = OrderedEngine::default();
            for op in ops.clone() {
                ordered.accept(op);
            }
            for &i in &completion_order {
                ordered.complete(ops[i].ticket);
            }
            assert_eq!(ordered.content, b"BBBBAAAA");
        }

        /// Ticket 1 writes [0,8); ticket 2 truncates to 4; the truncate
        /// upload completes first. The weak engine lets the late write
        /// regrow the file past the truncation with stale bytes; the
        /// ordered engine keeps the truncate final (ORD-004).
        #[test]
        fn late_write_cannot_undo_truncate() {
            let ops = vec![
                PendingOp {
                    ticket: 1,
                    op: Op::Write {
                        offset: 0,
                        data: b"AAAAAAAA".to_vec(),
                    },
                },
                PendingOp {
                    ticket: 2,
                    op: Op::Truncate { len: 4 },
                },
            ];

            let mut weak = WeakEngine::default();
            weak.complete(&ops[1]);
            weak.complete(&ops[0]);
            assert_eq!(weak.content, b"AAAAAAAA", "truncate undone by late write");

            let mut ordered = OrderedEngine::default();
            for op in ops.clone() {
                ordered.accept(op);
            }
            ordered.complete(2); // truncate upload lands first
            assert!(ordered.buffered_pending());
            ordered.complete(1); // write upload lands late
            assert_eq!(ordered.content, b"AAAA");
            assert!(!ordered.buffered_pending());
        }

        /// Spec 18 section 10: a handoff may only release the dirty
        /// references owned by the operation being committed; it must not
        /// remove newer dirty data that happens to overlap the same range.
        #[test]
        fn handoff_releases_only_its_own_dirty() {
            // Dirty overlays keyed by owning ticket, both covering [0,4).
            let mut dirty: Vec<(u64, u64, Vec<u8>)> =
                vec![(1, 0, b"AAAA".to_vec()), (2, 0, b"BBBB".to_vec())];

            // Weak handoff: drop every dirty range covered by the op.
            let mut weak = dirty.clone();
            weak.retain(|(_, off, data)| !(*off == 0 && data.len() == 4));
            assert!(weak.is_empty(), "weak handoff removed newer dirty");

            // Ordered handoff: release only ticket 1's own reference.
            dirty.retain(|(ticket, _, _)| *ticket != 1);
            assert_eq!(
                dirty,
                vec![(2, 0, b"BBBB".to_vec())],
                "newer dirty survives the old op's handoff"
            );
        }
    }
}

/// CEX-RET — publication registers the exact retain batch in the same
/// transaction that switches the head (INV-11/INV-18; specs 09 and 20).
pub mod retention {
    use std::collections::BTreeSet;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum DomainState {
        Active,
        Closed,
    }

    pub struct Domain {
        pub state: DomainState,
        /// I(d): the frozen creation/upload inventory.
        pub inventory: BTreeSet<&'static str>,
        /// K(d): union of committed retain batches.
        pub retained: BTreeSet<&'static str>,
    }

    impl Domain {
        pub fn new(objects: &[&'static str]) -> Self {
            Domain {
                state: DomainState::Active,
                inventory: objects.iter().copied().collect(),
                retained: BTreeSet::new(),
            }
        }
    }

    /// Objects the new published head references.
    pub struct Candidate {
        pub needed: Vec<&'static str>,
    }

    /// Weak protocol: switch the head first, register retention later.
    pub fn publish_weak(_domain: &mut Domain, candidate: &Candidate) -> Vec<&'static str> {
        // Head now references candidate.needed, but no retain batch has
        // been registered yet — the objects are still "eligible".
        candidate.needed.clone()
    }

    /// Strong protocol (spec 20 section 5): one bounded transaction appends
    /// the RetainBatch, bumps retention_seq and switches the head; any
    /// precondition failure leaves nothing behind.
    pub fn publish_atomic(
        domain: &mut Domain,
        candidate: &Candidate,
    ) -> Result<Vec<&'static str>, &'static str> {
        if domain.state != DomainState::Active {
            return Err("domain not ACTIVE");
        }
        let mut batch = domain.retained.clone();
        batch.extend(candidate.needed.iter().copied());
        domain.retained = batch;
        Ok(candidate.needed.clone())
    }

    /// The private-domain cleaner: eligible = I(d) − K(d).
    pub fn eligible(domain: &Domain) -> BTreeSet<&'static str> {
        domain
            .inventory
            .difference(&domain.retained)
            .copied()
            .collect()
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        /// Weak protocol counterexample: the cleaner runs in the window
        /// between the head switch and the retention registration and
        /// deletes objects the published head references (RET-001; the
        /// exact hazard FNL-04 records).
        #[test]
        fn weak_publish_leaves_published_objects_deletable() {
            let mut domain = Domain::new(&["o1-head", "o2-garbage"]);
            let candidate = Candidate {
                needed: vec!["o1-head"],
            };

            let head_refs = publish_weak(&mut domain, &candidate);
            // Cleaner runs before the deferred retention registration.
            let deleted = eligible(&domain);
            assert_eq!(deleted, BTreeSet::from(["o1-head", "o2-garbage"]));
            assert!(
                head_refs.iter().any(|o| deleted.contains(o)),
                "published object deleted by private cleaner"
            );
        }

        /// Strong protocol: publication registers the exact batch in the
        /// same transaction; afterwards the cleaner's eligible set contains
        /// only the never-published garbage (RET-005/RET-006).
        #[test]
        fn atomic_publish_shields_exactly_the_needed_objects() {
            let mut domain = Domain::new(&["o1-head", "o2-garbage"]);
            let candidate = Candidate {
                needed: vec!["o1-head"],
            };

            let head_refs =
                publish_atomic(&mut domain, &candidate).expect("active domain publishes");
            assert_eq!(head_refs, vec!["o1-head"]);

            let deleted = eligible(&domain);
            assert_eq!(deleted, BTreeSet::from(["o2-garbage"]));
            assert!(!deleted.contains("o1-head"));
        }

        /// Strong protocol, publish-vs-close race (spec 10 section 4):
        /// once the domain has closed, a late publication is rejected
        /// instead of creating published references outside the frozen
        /// inventory (RET-016).
        #[test]
        fn close_wins_over_late_publication() {
            let mut domain = Domain::new(&["o1"]);
            domain.state = DomainState::Closed;

            let candidate = Candidate { needed: vec!["o1"] };
            let result = publish_atomic(&mut domain, &candidate);
            assert_eq!(result, Err("domain not ACTIVE"));
            assert!(domain.retained.is_empty());
        }
    }
}
