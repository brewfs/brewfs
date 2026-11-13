//! Native capture requires Linux. No non-Linux constructor grants authority.
use super::*;
pub(crate) struct PackedInitialBootstrapAuthority<B> {
    store: Arc<KvWorkspaceStore<B>>,
    budget: Arc<SnapshotMountBudget>,
    guard: HeadGuard,
}
impl<B: WorkspaceKvBackend> PackedInitialBootstrapAuthority<B> {
    pub(crate) fn belongs_to_store(&self, store: &Arc<KvWorkspaceStore<B>>) -> bool {
        Arc::ptr_eq(&self.store, store)
    }
    pub(crate) fn mount_budget(&self) -> &Arc<SnapshotMountBudget> {
        &self.budget
    }
    pub(crate) fn guard(&self) -> &HeadGuard {
        &self.guard
    }
    pub(crate) async fn authority_checks_before(
        &self,
    ) -> Result<(Vec<KvCheck>, i64), WorkspaceError> {
        Err(WorkspaceError::UnsupportedCapability(
            "Linux native packed bootstrap capture",
        ))
    }
}
impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    pub(in crate::workspace::stores::kv_store) async fn retained_original_initial_seed_recovery_checks(
        self: &Arc<Self>,
        _: &crate::workspace::stores::kv_store::packed_native_freeze::PackedNativePlannedRotation,
        _: &PackedLowerBindingRecord,
        _: u64,
        _: &SnapshotOpenRecord,
        _: &SnapshotLease,
        _: &Arc<SnapshotMountBudget>,
    ) -> Result<Option<(Vec<KvCheck>, Arc<PackedInitialBootstrapAuthority<B>>)>, WorkspaceError>
    {
        Ok(None)
    }
    pub(crate) async fn restore_initial_native_origin(
        self: &Arc<Self>,
        _: &crate::workspace::stores::kv_store::packed_native_freeze::PackedNativeQuiesceFence<B>,
    ) -> Result<Option<Arc<PackedInitialBootstrapAuthority<B>>>, WorkspaceError> {
        Ok(None)
    }
}
