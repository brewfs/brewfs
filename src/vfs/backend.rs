use crate::chunk::BlockStore;
use crate::meta::MetaLayer;
use std::sync::Arc;

pub(crate) struct Backend<B, M> {
    store: Arc<B>,
    meta: Arc<M>,
    #[cfg(feature = "workspace-overlay")]
    workspace_read_plan: Option<Arc<dyn crate::chunk::read_plan::WorkspaceReadPlanProvider>>,
}

impl<B, M> Backend<B, M>
where
    B: BlockStore,
    M: MetaLayer,
{
    pub(crate) fn new(store: Arc<B>, meta: Arc<M>) -> Self {
        Self {
            store,
            meta,
            #[cfg(feature = "workspace-overlay")]
            workspace_read_plan: None,
        }
    }

    #[cfg(feature = "workspace-overlay")]
    pub(crate) fn new_workspace(
        store: Arc<B>,
        meta: Arc<M>,
        workspace_read_plan: Arc<dyn crate::chunk::read_plan::WorkspaceReadPlanProvider>,
    ) -> Self {
        Self {
            store,
            meta,
            workspace_read_plan: Some(workspace_read_plan),
        }
    }

    pub(crate) fn meta(&self) -> &M {
        &self.meta
    }

    pub(crate) fn store(&self) -> &B {
        &self.store
    }

    #[cfg(feature = "workspace-overlay")]
    pub(crate) fn workspace_read_plan(
        &self,
    ) -> Option<&dyn crate::chunk::read_plan::WorkspaceReadPlanProvider> {
        self.workspace_read_plan.as_deref()
    }
}
