//! Epoch- and inode-version-scoped resolver caches.

use std::ops::Range;
use std::sync::Arc;

use moka::sync::Cache;

use crate::chunk::read_plan::{ReadPlanSegment, ResolvedReadPlan};
use crate::meta::store::MetaError;

use super::ids::WorkspaceId;

/// Maximum weighted size of the resolver cache. Each entry is weighted by its
/// plan segment count, with one additional unit for the cache entry itself.
const DEFAULT_READ_PLAN_CACHE_MAX_WEIGHT: u64 = 65_536;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ReadPlanCacheKey {
    pub workspace_id: WorkspaceId,
    pub head_epoch: u64,
    pub ino: i64,
    pub chunk_index: u64,
    pub inode_data_version: u64,
    pub range_start: u64,
    pub range_end: u64,
}

pub struct WorkspaceResolverCache {
    read_plans: Cache<ReadPlanCacheKey, Arc<ResolvedReadPlan>>,
}

impl Default for WorkspaceResolverCache {
    fn default() -> Self {
        Self::with_capacity(DEFAULT_READ_PLAN_CACHE_MAX_WEIGHT)
    }
}

impl WorkspaceResolverCache {
    pub fn with_capacity(max_weight: u64) -> Self {
        let read_plans = Cache::builder()
            .max_capacity(max_weight.max(1))
            .support_invalidation_closures()
            .weigher(|_key: &ReadPlanCacheKey, plan: &Arc<ResolvedReadPlan>| {
                plan.segments
                    .len()
                    .saturating_add(1)
                    .try_into()
                    .unwrap_or(u32::MAX)
            })
            .build();
        Self { read_plans }
    }

    pub fn get_read_plan(&self, key: &ReadPlanCacheKey) -> Option<Arc<ResolvedReadPlan>> {
        self.read_plans.get(key)
    }

    pub fn insert_read_plan(&self, key: ReadPlanCacheKey, plan: ResolvedReadPlan) {
        self.read_plans.insert(key, Arc::new(plan));
    }

    pub fn invalidate_inode(&self, workspace_id: WorkspaceId, ino: i64) {
        self.invalidate_matching(move |key| key.workspace_id == workspace_id && key.ino == ino);
    }

    pub fn invalidate_workspace(&self, workspace_id: WorkspaceId) {
        self.invalidate_matching(move |key| key.workspace_id == workspace_id);
    }

    fn invalidate_matching(
        &self,
        predicate: impl Fn(&ReadPlanCacheKey) -> bool + Send + Sync + 'static,
    ) {
        self.read_plans
            .invalidate_entries_if(move |key, _| predicate(key))
            .expect("read-plan cache invalidation predicates are enabled");
        self.read_plans.run_pending_tasks();
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.read_plans.run_pending_tasks();
        self.read_plans.entry_count() as usize
    }

    pub fn is_empty(&self) -> bool {
        self.read_plans.run_pending_tasks();
        self.read_plans.entry_count() == 0
    }
}

/// Clip the sorted, non-overlapping output of the extent resolver. A small
/// read only copies its intersecting segments, even in a fragmented chunk.
pub(super) fn clip_read_plan(
    plan: &ResolvedReadPlan,
    requested: Range<u64>,
) -> Result<ResolvedReadPlan, MetaError> {
    if requested.start > requested.end {
        return Err(MetaError::Internal("read-plan range is reversed".into()));
    }
    if requested.is_empty() {
        return Ok(ResolvedReadPlan::default());
    }
    let bounds = |segment: &ReadPlanSegment| match segment {
        ReadPlanSegment::Data {
            logical_offset,
            length,
            ..
        }
        | ReadPlanSegment::Zero {
            logical_offset,
            length,
        } => (*logical_offset, *length),
    };
    let first = plan.segments.partition_point(|segment| {
        let (offset, length) = bounds(segment);
        offset
            .checked_add(length)
            .is_some_and(|end| end <= requested.start)
    });
    let mut segments = Vec::new();
    for segment in &plan.segments[first..] {
        let (offset, length) = bounds(segment);
        if offset >= requested.end {
            break;
        }
        let start = offset.max(requested.start);
        let end = offset
            .checked_add(length)
            .ok_or_else(|| MetaError::Internal("read-plan segment overflows".into()))?
            .min(requested.end);
        if end <= start {
            return Err(MetaError::Internal("empty read-plan intersection".into()));
        }
        segments.push(match segment {
            ReadPlanSegment::Data {
                slice_id,
                slice_offset,
                ..
            } => ReadPlanSegment::Data {
                logical_offset: start,
                length: end - start,
                slice_id: *slice_id,
                slice_offset: slice_offset.checked_add(start - offset).ok_or_else(|| {
                    MetaError::Internal("read-plan slice offset overflows".into())
                })?,
            },
            ReadPlanSegment::Zero { .. } => ReadPlanSegment::Zero {
                logical_offset: start,
                length: end - start,
            },
        });
    }
    Ok(ResolvedReadPlan { segments })
}

#[cfg(test)]
mod tests {
    use crate::chunk::read_plan::ReadPlanSegment;

    use super::*;

    fn key(workspace_id: WorkspaceId, ino: i64, range_start: u64) -> ReadPlanCacheKey {
        ReadPlanCacheKey {
            workspace_id,
            head_epoch: 1,
            ino,
            chunk_index: 0,
            inode_data_version: 1,
            range_start,
            range_end: range_start + 1,
        }
    }

    fn plan(slice_id: u64) -> ResolvedReadPlan {
        ResolvedReadPlan {
            segments: vec![ReadPlanSegment::Data {
                logical_offset: 0,
                length: 1,
                slice_id,
                slice_offset: 0,
            }],
        }
    }

    #[test]
    fn read_plan_cache_bounds_unique_ranges_and_preserves_invalidation() {
        let cache = WorkspaceResolverCache::default();
        let workspace_id = WorkspaceId::new();
        let other_workspace_id = WorkspaceId::new();
        let max_one_segment_entries = (DEFAULT_READ_PLAN_CACHE_MAX_WEIGHT / 2) as usize;
        let keys: Vec<_> = (0..=max_one_segment_entries)
            .map(|index| key(workspace_id, 7, index as u64))
            .collect();

        for (index, cache_key) in keys.iter().copied().enumerate() {
            cache.insert_read_plan(cache_key, plan(index as u64));
        }

        assert!(
            cache.len() <= max_one_segment_entries,
            "cache retained {} unique ranges above its {}-entry weighted limit",
            cache.len(),
            max_one_segment_entries
        );

        let survivor = keys.iter().enumerate().find_map(|(index, cache_key)| {
            cache.get_read_plan(cache_key).map(|cached| (index, cached))
        });
        let (survivor_index, survivor_plan) = survivor.expect("at least one cached read plan");
        assert_eq!(*survivor_plan, plan(survivor_index as u64));

        let invalidation_cache = WorkspaceResolverCache::with_capacity(8);
        let stale_key = key(workspace_id, 7, 0);
        let other_key = key(other_workspace_id, 8, 0);
        invalidation_cache.insert_read_plan(stale_key, plan(0));
        let other_plan = plan(u64::MAX);
        invalidation_cache.insert_read_plan(other_key, other_plan.clone());
        invalidation_cache.invalidate_inode(workspace_id, 7);
        assert!(
            invalidation_cache.get_read_plan(&stale_key).is_none(),
            "inode invalidation must remove the stale inode plan"
        );
        assert_eq!(
            invalidation_cache.get_read_plan(&other_key),
            Some(Arc::new(other_plan))
        );

        invalidation_cache.invalidate_workspace(other_workspace_id);
        assert!(invalidation_cache.get_read_plan(&other_key).is_none());
        assert!(invalidation_cache.is_empty());
    }

    #[test]
    fn read_plan_cache_charges_every_segment_in_a_shared_chunk() {
        let cache = WorkspaceResolverCache::with_capacity(4);
        let cache_key = key(WorkspaceId::new(), 7, 0);
        let fragmented = ResolvedReadPlan {
            segments: (0..4)
                .map(|offset| ReadPlanSegment::Data {
                    logical_offset: offset,
                    length: 1,
                    slice_id: offset,
                    slice_offset: 0,
                })
                .collect(),
        };
        // Four segments plus the entry exceed the four-unit budget. Sharing
        // the plan must not turn its weight into a single pointer-sized entry.
        cache.insert_read_plan(cache_key, fragmented);
        assert_eq!(cache.len(), 0);
        assert!(cache.get_read_plan(&cache_key).is_none());
        cache.insert_read_plan(cache_key, plan(9));
        assert_eq!(cache.len(), 1);
        assert_eq!(*cache.get_read_plan(&cache_key).unwrap(), plan(9));
    }
}
