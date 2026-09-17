//! Explicit, additive layout variants (PR12).
//!
//! A variant is never an in-place repack and never removes a published
//! dependency.  Validation therefore proves semantic identity, checks the
//! caller's extra-byte budget, and requires a quiescent head for an optional
//! explicit apply.

use std::collections::BTreeSet;

use crate::native_base::wire::refs::{Hash32, ObjectId};

use super::{LifecycleError, LifecycleResult};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LayoutIdentity {
    pub logical_revision: Hash32,
    pub binding_digest: Hash32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LayoutVariantRequest {
    pub source: LayoutIdentity,
    pub candidate: LayoutIdentity,
    pub source_objects: BTreeSet<ObjectId>,
    pub candidate_objects: BTreeSet<ObjectId>,
    pub candidate_new_bytes: u64,
    pub max_extra_bytes: u64,
    pub apply: bool,
    pub expected_view_matches: bool,
    pub head_has_visible_delta: bool,
    pub open_orphan_count: u64,
    pub valid_writer_present: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LayoutVariantPlan {
    /// Both sets stay retained forever. This is intentionally a union rather
    /// than a replacement set.
    pub permanent_objects: BTreeSet<ObjectId>,
    pub added_objects: BTreeSet<ObjectId>,
    pub added_bytes: u64,
    pub apply: bool,
}

pub fn validate_layout_variant(
    request: &LayoutVariantRequest,
) -> LifecycleResult<LayoutVariantPlan> {
    if request.source != request.candidate {
        return Err(LifecycleError::Retention(
            "layout variant changes logical revision or block bindings".into(),
        ));
    }
    if request.candidate_new_bytes > request.max_extra_bytes {
        return Err(LifecycleError::LimitExceeded(format!(
            "layout variant requires {} bytes, budget is {}",
            request.candidate_new_bytes, request.max_extra_bytes
        )));
    }
    if request.apply
        && (!request.expected_view_matches
            || request.head_has_visible_delta
            || request.open_orphan_count != 0
            || request.valid_writer_present)
    {
        return Err(LifecycleError::InvalidState(
            "layout variant apply requires matching, clean, unmounted head".into(),
        ));
    }
    let added_objects = request
        .candidate_objects
        .difference(&request.source_objects)
        .copied()
        .collect::<BTreeSet<_>>();
    let permanent_objects = request
        .source_objects
        .union(&request.candidate_objects)
        .copied()
        .collect();
    Ok(LayoutVariantPlan {
        permanent_objects,
        added_objects,
        added_bytes: request.candidate_new_bytes,
        apply: request.apply,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> LayoutVariantRequest {
        LayoutVariantRequest {
            source: LayoutIdentity {
                logical_revision: [1; 32],
                binding_digest: [2; 32],
            },
            candidate: LayoutIdentity {
                logical_revision: [1; 32],
                binding_digest: [2; 32],
            },
            source_objects: [[3; 16], [4; 16]].into_iter().collect(),
            candidate_objects: [[4; 16], [5; 16]].into_iter().collect(),
            candidate_new_bytes: 100,
            max_extra_bytes: 100,
            apply: false,
            expected_view_matches: true,
            head_has_visible_delta: false,
            open_orphan_count: 0,
            valid_writer_present: false,
        }
    }

    #[test]
    fn variant_retains_old_and_new_dependencies() {
        let plan = validate_layout_variant(&request()).unwrap();
        assert_eq!(plan.permanent_objects.len(), 3);
        assert!(plan.permanent_objects.contains(&[3; 16]));
        assert!(plan.added_objects.contains(&[5; 16]));
    }

    #[test]
    fn semantic_change_budget_overrun_and_busy_apply_fail_closed() {
        let mut changed = request();
        changed.candidate.binding_digest = [9; 32];
        assert!(validate_layout_variant(&changed).is_err());

        let mut over = request();
        over.candidate_new_bytes = 101;
        assert!(validate_layout_variant(&over).is_err());

        let mut busy = request();
        busy.apply = true;
        busy.open_orphan_count = 1;
        assert!(validate_layout_variant(&busy).is_err());
    }
}
