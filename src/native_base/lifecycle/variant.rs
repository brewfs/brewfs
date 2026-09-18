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
    /// Space the published source already occupies.  A variant never rewrites
    /// it, so it stays booked against the published revision.
    pub source_bytes: u64,
    /// Space this variant adds on top of the source.
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
    /// Footprint of the untouched published source.
    pub source_bytes: u64,
    /// New space this variant adds; booked separately from the source so a
    /// repack can never hide its cost inside the old revision.
    pub added_bytes: u64,
    /// `source_bytes + added_bytes`: both live forever.
    pub permanent_bytes: u64,
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
    let permanent_bytes = request
        .source_bytes
        .checked_add(request.candidate_new_bytes)
        .ok_or_else(|| LifecycleError::LimitExceeded("variant footprint overflow".into()))?;
    Ok(LayoutVariantPlan {
        permanent_objects,
        added_objects,
        source_bytes: request.source_bytes,
        added_bytes: request.candidate_new_bytes,
        permanent_bytes,
        apply: request.apply,
    })
}

/// Outputs a terminated, unpublished variant build domain may clean (CLN-023).
///
/// `retained_objects` is what the published revision already retains: the
/// [`LayoutVariantPlan::permanent_objects`] union once a variant was applied,
/// or the variant's own `source_objects` when it never published.  A failed
/// repack publishes nothing, so its build domain is terminated and only the
/// domain's own unretained outputs are delete-eligible — everything retained is
/// excluded here, exactly as the private cleaner excludes it from
/// `I(d) - (K(d) ∪ C(d))`.
pub fn abandoned_variant_outputs(
    domain_objects: &BTreeSet<ObjectId>,
    retained_objects: &BTreeSet<ObjectId>,
) -> BTreeSet<ObjectId> {
    domain_objects
        .difference(retained_objects)
        .copied()
        .collect()
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
            source_bytes: 4096,
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
        assert_eq!(plan.added_bytes, 100);
        assert_eq!(plan.source_bytes, 4096);
        assert_eq!(plan.permanent_bytes, 4196);
    }

    /// CLN-022 / INV-11, INV-17, INV-18, INV-21, INV-24: an explicit repack
    /// variant succeeds only as an addition.  The new space is booked
    /// separately from the untouched source footprint, and every old official
    /// pack stays in the permanent set.
    #[test]
    fn explicit_variant_books_new_space_separately_and_keeps_old_packs() {
        let mut applied = request();
        applied.apply = true;
        let plan = validate_layout_variant(&applied).unwrap();
        assert!(plan.apply);
        assert_eq!(plan.added_bytes, plan.permanent_bytes - plan.source_bytes);
        assert_eq!(plan.added_objects, BTreeSet::from([[5; 16]]));
        assert_eq!(
            plan.permanent_objects,
            BTreeSet::from([[3; 16], [4; 16], [5; 16]])
        );
        for old_pack in [[3; 16], [4; 16]] {
            assert!(
                plan.permanent_objects.contains(&old_pack),
                "the old official pack {old_pack:02x?} stays permanent"
            );
            assert!(!plan.added_objects.contains(&old_pack));
        }

        let mut overflow = request();
        overflow.source_bytes = u64::MAX;
        overflow.candidate_new_bytes = 1;
        assert!(
            validate_layout_variant(&overflow)
                .unwrap_err()
                .to_string()
                .contains("variant footprint overflow")
        );
    }

    /// CLN-023 / INV-11, INV-17, INV-18, INV-21, INV-24: a repack that was not
    /// published leaves only its own unretained build outputs delete-eligible.
    /// Retained and source objects are never in the cleanup set, whether the
    /// variant failed semantically or simply was never applied.
    #[test]
    fn an_unpublished_variant_cleans_only_its_unretained_outputs() {
        let domain_objects: BTreeSet<ObjectId> = [[4; 16], [5; 16], [6; 16]].into_iter().collect();
        let plan = validate_layout_variant(&request()).unwrap();
        assert_eq!(
            abandoned_variant_outputs(&domain_objects, &plan.permanent_objects),
            BTreeSet::from([[6; 16]])
        );

        let mut failed = request();
        failed.candidate.binding_digest = [9; 32];
        assert!(validate_layout_variant(&failed).is_err());
        // A failed variant publishes nothing, so only the published source
        // stays retained and the whole build output set becomes cleanable.
        assert_eq!(
            abandoned_variant_outputs(&domain_objects, &failed.source_objects),
            BTreeSet::from([[5; 16], [6; 16]])
        );
        assert!(
            !abandoned_variant_outputs(&domain_objects, &failed.source_objects).contains(&[3; 16])
        );
        assert!(
            !abandoned_variant_outputs(&domain_objects, &failed.source_objects).contains(&[4; 16])
        );
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
