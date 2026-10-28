//! Epoch- and inode-version-scoped resolver caches.

use std::sync::Arc;

use moka::sync::Cache;

use crate::chunk::read_plan::ResolvedReadPlan;

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

/// One immutable upper extent view for a complete chunk.  The request range
/// is deliberately absent: callers clip the resolved plan in memory so
/// adjacent/random reads of the same chunk share one backend extent scan.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ExtentSnapshotCacheKey {
    pub workspace_id: WorkspaceId,
    pub head_epoch: u64,
    pub ino: i64,
    pub chunk_index: u64,
    pub inode_data_version: u64,
}

pub struct WorkspaceResolverCache {
    read_plans: Cache<ReadPlanCacheKey, ResolvedReadPlan>,
    extent_snapshots: Cache<ExtentSnapshotCacheKey, Arc<ResolvedReadPlan>>,
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
            .weigher(|_key: &ReadPlanCacheKey, plan: &ResolvedReadPlan| {
                plan.segments
                    .len()
                    .saturating_add(1)
                    .try_into()
                    .unwrap_or(u32::MAX)
            })
            .build();
        let extent_snapshots = Cache::builder()
            .max_capacity(max_weight.max(1))
            .support_invalidation_closures()
            .weigher(
                |_key: &ExtentSnapshotCacheKey, plan: &Arc<ResolvedReadPlan>| {
                    plan.segments
                        .len()
                        .saturating_add(1)
                        .try_into()
                        .unwrap_or(u32::MAX)
                },
            )
            .build();
        Self {
            read_plans,
            extent_snapshots,
        }
    }

    pub fn get_read_plan(&self, key: &ReadPlanCacheKey) -> Option<ResolvedReadPlan> {
        self.read_plans.get(key)
    }

    pub fn insert_read_plan(&self, key: ReadPlanCacheKey, plan: ResolvedReadPlan) {
        self.read_plans.insert(key, plan);
    }

    pub fn get_extent_snapshot(
        &self,
        key: &ExtentSnapshotCacheKey,
    ) -> Option<Arc<ResolvedReadPlan>> {
        self.extent_snapshots.get(key)
    }

    pub fn insert_extent_snapshot(&self, key: ExtentSnapshotCacheKey, plan: Arc<ResolvedReadPlan>) {
        self.extent_snapshots.insert(key, plan);
    }

    pub fn invalidate_inode(&self, workspace_id: WorkspaceId, ino: i64) {
        self.invalidate_matching(move |key| key.workspace_id == workspace_id && key.ino == ino);
        self.invalidate_extent_matching(move |key| {
            key.workspace_id == workspace_id && key.ino == ino
        });
    }

    pub fn invalidate_workspace(&self, workspace_id: WorkspaceId) {
        self.invalidate_matching(move |key| key.workspace_id == workspace_id);
        self.invalidate_extent_matching(move |key| key.workspace_id == workspace_id);
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

    fn invalidate_extent_matching(
        &self,
        predicate: impl Fn(&ExtentSnapshotCacheKey) -> bool + Send + Sync + 'static,
    ) {
        self.extent_snapshots
            .invalidate_entries_if(move |key, _| predicate(key))
            .expect("extent snapshot cache invalidation predicates are enabled");
        self.extent_snapshots.run_pending_tasks();
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.read_plans.run_pending_tasks();
        self.read_plans.entry_count() as usize
    }

    #[cfg(test)]
    pub fn extent_snapshot_len(&self) -> usize {
        self.extent_snapshots.run_pending_tasks();
        self.extent_snapshots.entry_count() as usize
    }

    pub fn is_empty(&self) -> bool {
        self.read_plans.run_pending_tasks();
        self.extent_snapshots.run_pending_tasks();
        self.read_plans.entry_count() == 0 && self.extent_snapshots.entry_count() == 0
    }
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

    fn snapshot_key(workspace_id: WorkspaceId, ino: i64) -> ExtentSnapshotCacheKey {
        ExtentSnapshotCacheKey {
            workspace_id,
            head_epoch: 1,
            ino,
            chunk_index: 0,
            inode_data_version: 1,
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
        assert_eq!(survivor_plan, plan(survivor_index as u64));

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
            Some(other_plan)
        );

        invalidation_cache.invalidate_workspace(other_workspace_id);
        assert!(invalidation_cache.get_read_plan(&other_key).is_none());
        assert!(invalidation_cache.is_empty());
    }

    #[test]
    fn extent_snapshot_cache_is_inode_scoped_and_invalidated_with_the_view() {
        let cache = WorkspaceResolverCache::with_capacity(8);
        let workspace_id = WorkspaceId::new();
        let other_workspace_id = WorkspaceId::new();
        let key = snapshot_key(workspace_id, 7);
        let other_key = snapshot_key(other_workspace_id, 8);
        let cached = Arc::new(plan(17));
        cache.insert_extent_snapshot(key, cached.clone());
        cache.insert_extent_snapshot(other_key, Arc::new(plan(18)));

        assert_eq!(cache.get_extent_snapshot(&key), Some(cached));
        assert_eq!(cache.extent_snapshot_len(), 2);
        cache.invalidate_inode(workspace_id, 7);
        assert!(cache.get_extent_snapshot(&key).is_none());
        assert!(cache.get_extent_snapshot(&other_key).is_some());

        cache.invalidate_workspace(other_workspace_id);
        assert!(cache.is_empty());
    }
}
