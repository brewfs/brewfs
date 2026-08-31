//! Epoch- and inode-version-scoped resolver caches.

use dashmap::DashMap;

use crate::chunk::read_plan::ResolvedReadPlan;

use super::ids::WorkspaceId;

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

#[derive(Default)]
pub struct WorkspaceResolverCache {
    read_plans: DashMap<ReadPlanCacheKey, ResolvedReadPlan>,
}

impl WorkspaceResolverCache {
    pub fn get_read_plan(&self, key: &ReadPlanCacheKey) -> Option<ResolvedReadPlan> {
        self.read_plans.get(key).map(|entry| entry.clone())
    }

    pub fn insert_read_plan(&self, key: ReadPlanCacheKey, plan: ResolvedReadPlan) {
        self.read_plans.insert(key, plan);
    }

    pub fn invalidate_inode(&self, workspace_id: WorkspaceId, ino: i64) {
        self.read_plans
            .retain(|key, _| key.workspace_id != workspace_id || key.ino != ino);
    }

    pub fn invalidate_workspace(&self, workspace_id: WorkspaceId) {
        self.read_plans
            .retain(|key, _| key.workspace_id != workspace_id);
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.read_plans.len()
    }

    pub fn is_empty(&self) -> bool {
        self.read_plans.is_empty()
    }
}
