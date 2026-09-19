//! Training-mode sampler hints (spec 06 §8, OPT-006/INV-02).
//!
//! Training mode may accept explicit sample or byte-range hints so that the
//! *issue* order of the underlying reads can be made friendlier (say,
//! sequential inside one file).  Hints are only ever allowed to reorder:
//!
//! - the sampler's sample set and its distribution are inputs, not outputs:
//!   every draw is issued exactly once -- duplicates included -- and nothing
//!   is added, dropped or merged;
//! - the semantic order the application observes is the sampler's order and
//!   never changes;
//! - a byte-range hint must name a drawn sample exactly, so a hint can never
//!   widen or narrow the sample set.
//!
//! `SampleIssuePlan::semantic_order` is therefore always `0..n`, while
//! `issue_order` is a permutation of it: the application's order and the order
//! the requests are issued in are deliberately separable, which is the only
//! freedom spec 06 §8 grants.

use std::collections::BTreeSet;

/// One draw of the training sampler: a byte range in one file, in the
/// sampler's semantic position.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Sample {
    pub file: u64,
    pub offset: u64,
    pub len: u64,
}

/// A hint the caller may supply in training mode.  None of these can change
/// which samples are read; they only say something about the issue order (or,
/// for [`SampleHint::ByteRange`], assert that a range really was drawn).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SampleHint {
    /// Issue the draw at this semantic index first.  Several hints are applied
    /// in the order they appear; a repeated index is idempotent.
    Prioritise { draw: usize },
    /// Issue this file's draws in ascending byte order.  Their slots in the
    /// issue order are kept, so other files do not move.
    SequentialFile { file: u64 },
    /// Assert that this exact range is one of the sampler's draws.  This is
    /// the sample-set check: a hint that names a range the sampler never drew
    /// is an error, not a new sample.
    ByteRange { file: u64, offset: u64, len: u64 },
}

#[derive(Debug, thiserror::Error)]
pub enum SamplerError {
    #[error("byte-range hint ({file}, {offset}, +{len}) is not one of the sampler's draws")]
    HintNotASample { file: u64, offset: u64, len: u64 },
    #[error("sample hint names draw {draw} but the sampler drew {draws} samples")]
    HintOutOfRange { draw: usize, draws: usize },
}

/// The issue plan for one training batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SampleIssuePlan {
    /// The sampler's draws, verbatim: the set *and* the distribution.
    pub samples: Vec<Sample>,
    /// The order the application consumes results in: the sampler's order.
    pub semantic_order: Vec<usize>,
    /// The order the underlying requests are issued in: a permutation of
    /// `semantic_order`.
    pub issue_order: Vec<usize>,
    /// Draws a hint touched (reordered or asserted).
    pub hinted_draws: usize,
}

impl SampleIssuePlan {
    /// Samples in the order the application observes them.
    pub fn applied_samples(&self) -> Vec<Sample> {
        self.semantic_order
            .iter()
            .map(|index| self.samples[*index])
            .collect()
    }

    /// Samples in the order the requests go out.
    pub fn issued_samples(&self) -> Vec<Sample> {
        self.issue_order
            .iter()
            .map(|index| self.samples[*index])
            .collect()
    }

    pub fn issued_bytes(&self) -> u64 {
        self.samples.iter().map(|sample| sample.len).sum()
    }

    /// Every draw appears exactly once in the issue order.
    pub fn covers_every_draw(&self) -> bool {
        let mut seen = self.issue_order.clone();
        seen.sort_unstable();
        seen == (0..self.samples.len()).collect::<Vec<_>>()
    }
}

/// Turn the sampler's draws plus the caller's hints into an issue plan.
///
/// The plan always reads the sampler's samples: hints reorder the issue order
/// at most, and every hint that would change the sample set is refused.
pub fn plan_sample_issue_order(
    draws: &[Sample],
    hints: &[SampleHint],
) -> Result<SampleIssuePlan, SamplerError> {
    let mut issue_order: Vec<usize> = (0..draws.len()).collect();
    let mut hinted: BTreeSet<usize> = BTreeSet::new();

    // Byte-range hints are assertions about the sample set: each must name a
    // draw exactly.  A near miss is an error, never a substitute sample.
    for hint in hints {
        if let SampleHint::ByteRange { file, offset, len } = hint {
            let range = Sample {
                file: *file,
                offset: *offset,
                len: *len,
            };
            match draws.iter().position(|drawn| *drawn == range) {
                Some(index) => {
                    hinted.insert(index);
                }
                None => {
                    return Err(SamplerError::HintNotASample {
                        file: *file,
                        offset: *offset,
                        len: *len,
                    });
                }
            }
        }
    }

    // Per-file sequentialisation: the draws that live in this file's slots are
    // re-issued in ascending byte order, and no other slot moves.
    let mut sequentialised: BTreeSet<u64> = BTreeSet::new();
    for hint in hints {
        if let SampleHint::SequentialFile { file } = hint {
            if !sequentialised.insert(*file) {
                continue;
            }
            let slots: Vec<usize> = issue_order
                .iter()
                .enumerate()
                .filter(|(_, index)| draws[**index].file == *file)
                .map(|(slot, _)| slot)
                .collect();
            let mut indices: Vec<usize> = slots.iter().map(|slot| issue_order[*slot]).collect();
            indices.sort_by_key(|index| (draws[*index].offset, draws[*index].len));
            for (slot, index) in slots.into_iter().zip(indices) {
                issue_order[slot] = index;
                hinted.insert(index);
            }
        }
    }

    // Explicit prioritisation: the named draws move to the front in hint
    // order, and everything else keeps its relative order.
    let mut front: Vec<usize> = Vec::new();
    let mut promoted: BTreeSet<usize> = BTreeSet::new();
    for hint in hints {
        if let SampleHint::Prioritise { draw } = hint {
            if *draw >= draws.len() {
                return Err(SamplerError::HintOutOfRange {
                    draw: *draw,
                    draws: draws.len(),
                });
            }
            if promoted.insert(*draw) {
                front.push(*draw);
                hinted.insert(*draw);
            }
        }
    }
    if !front.is_empty() {
        front.extend(
            issue_order
                .iter()
                .copied()
                .filter(|index| !promoted.contains(index)),
        );
        issue_order = front;
    }

    let plan = SampleIssuePlan {
        samples: draws.to_vec(),
        semantic_order: (0..draws.len()).collect(),
        issue_order,
        hinted_draws: hinted.len(),
    };
    debug_assert!(
        plan.covers_every_draw(),
        "a hint must never add, drop or duplicate a draw"
    );
    Ok(plan)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A sampler batch over three files: file 1 is drawn in *descending* byte
    /// order, and one range is drawn twice (a distribution that repeats).
    fn draws() -> Vec<Sample> {
        vec![
            Sample {
                file: 1,
                offset: 8192,
                len: 4096,
            },
            Sample {
                file: 2,
                offset: 0,
                len: 4096,
            },
            Sample {
                file: 1,
                offset: 0,
                len: 8192,
            },
            Sample {
                file: 3,
                offset: 4096,
                len: 4096,
            },
            Sample {
                file: 2,
                offset: 0,
                len: 4096,
            },
        ]
    }

    fn sample(file: u64, offset: u64, len: u64) -> Sample {
        Sample { file, offset, len }
    }

    fn samples_of(plan: &SampleIssuePlan, order: &[usize]) -> Vec<Sample> {
        order.iter().map(|index| plan.samples[*index]).collect()
    }

    #[test]
    fn hints_reorder_the_issue_order_without_touching_the_sampler_order() {
        let draws = draws();
        let plan = plan_sample_issue_order(&draws, &[SampleHint::SequentialFile { file: 1 }])
            .expect("a per-file hint is valid");

        // The application still sees exactly the sampler's order, including
        // the repeated draw at its original position.
        assert_eq!(plan.semantic_order, vec![0, 1, 2, 3, 4]);
        assert_eq!(plan.applied_samples(), draws);
        // The issue order moved only file 1's draws, which now ascend.
        assert_eq!(
            samples_of(&plan, &plan.issue_order),
            vec![
                sample(1, 0, 8192),
                sample(2, 0, 4096),
                sample(1, 8192, 4096),
                sample(3, 4096, 4096),
                sample(2, 0, 4096),
            ]
        );
        assert!(plan.covers_every_draw());
        // Same set, same distribution, same bytes: only the order moved.
        let mut issued = plan.issued_samples();
        let mut applied = plan.applied_samples();
        issued.sort_unstable();
        applied.sort_unstable();
        assert_eq!(issued, applied, "the sample multiset must not change");
        assert_eq!(plan.issued_bytes(), 24576);
        assert_eq!(plan.hinted_draws, 2);
    }

    #[test]
    fn a_prioritised_draw_moves_to_the_front_and_keeps_the_rest_stable() {
        let draws = draws();
        let plan = plan_sample_issue_order(
            &draws,
            &[
                SampleHint::Prioritise { draw: 3 },
                SampleHint::Prioritise { draw: 3 },
            ],
        )
        .expect("a drawn index is valid");
        assert_eq!(plan.issue_order, vec![3, 0, 1, 2, 4]);
        assert_eq!(plan.applied_samples(), draws);
        assert!(plan.covers_every_draw());
        // Repeating the same hint is idempotent, not a second promotion.
        assert_eq!(plan.hinted_draws, 1);

        assert!(matches!(
            plan_sample_issue_order(&draws, &[SampleHint::Prioritise { draw: 5 }]),
            Err(SamplerError::HintOutOfRange { draw: 5, draws: 5 })
        ));
    }

    #[test]
    fn a_byte_range_hint_must_name_a_drawn_sample_exactly() {
        let draws = draws();
        let plan = plan_sample_issue_order(
            &draws,
            &[SampleHint::ByteRange {
                file: 3,
                offset: 4096,
                len: 4096,
            }],
        )
        .expect("this range is drawn");
        assert_eq!(plan.issue_order, vec![0, 1, 2, 3, 4]);
        assert_eq!(plan.hinted_draws, 1);

        // A near miss in any of the three coordinates is refused, so a hint
        // can never widen or narrow the sample set.
        for (file, offset, len) in [(3, 4096, 8192), (3, 0, 4096), (4, 4096, 4096)] {
            assert!(
                matches!(
                    plan_sample_issue_order(&draws, &[SampleHint::ByteRange { file, offset, len }]),
                    Err(SamplerError::HintNotASample { .. })
                ),
                "({file}, {offset}, +{len}) is not a draw"
            );
        }
    }

    #[test]
    fn duplicate_draws_keep_their_multiplicity_under_every_hint() {
        let draws = draws();
        let plan = plan_sample_issue_order(
            &draws,
            &[
                SampleHint::SequentialFile { file: 2 },
                SampleHint::Prioritise { draw: 4 },
            ],
        )
        .expect("the hints are valid");
        // The two identical draws of file 2 are two distinct slots: the
        // sequential hint must not merge them into one sample.
        let repeated = plan
            .issued_samples()
            .into_iter()
            .filter(|drawn| *drawn == sample(2, 0, 4096))
            .count();
        assert_eq!(repeated, 2, "a hint must not collapse a repeated draw");
        assert_eq!(plan.issue_order[0], 4, "the prioritised draw goes first");
        assert!(plan.covers_every_draw());
    }

    #[test]
    fn an_empty_batch_plans_nothing_and_refuses_any_hint() {
        let plan = plan_sample_issue_order(&[], &[]).expect("an empty batch is valid");
        assert!(plan.samples.is_empty());
        assert!(plan.issue_order.is_empty());
        assert!(plan.covers_every_draw());
        assert_eq!(plan.issued_bytes(), 0);
        assert!(matches!(
            plan_sample_issue_order(&[], &[SampleHint::Prioritise { draw: 0 }]),
            Err(SamplerError::HintOutOfRange { draw: 0, draws: 0 })
        ));
        assert!(matches!(
            plan_sample_issue_order(
                &[],
                &[SampleHint::ByteRange {
                    file: 1,
                    offset: 0,
                    len: 1
                }]
            ),
            Err(SamplerError::HintNotASample { .. })
        ));
    }
}
