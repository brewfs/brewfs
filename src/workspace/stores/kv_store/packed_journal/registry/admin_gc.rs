//! Bounded admin routing from durable facts to the existing private collectors.
//! A route is an identity hint. Every destructive driver authenticates again.

use super::*;

#[path = "admin_gc/facade.rs"]
pub(crate) mod facade;

pub(crate) struct PackedHistoryVersionRetirementOptions {
    pub workspace_id: WorkspaceId,
    pub binding_version: u64,
    pub grace_ns: u64,
    pub max_native_holds: u64,
    pub max_current_bindings: u64,
    pub max_work: u64,
    pub cancel: CancellationToken,
}

impl<B: WorkspaceKvBackend + 'static> KvWorkspaceStore<B> {
    /// Packed GC requires the immutable header sidecar. Do not fall back to
    /// loading the ordinary catalog's unbounded topology document.
    pub(crate) async fn validate_packed_gc_volume_header(
        &self,
        budget: &Arc<SnapshotMountBudget>,
    ) -> Result<(), WorkspaceError> {
        self.configure_packed_reader_pin_budget(budget.clone())?;
        let _owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, OPERATION_BYTES)])
            .map_err(journal_budget_error)?;
        if budget.state().closed {
            return Err(WorkspaceError::Busy);
        }
        let (rows, _) = self
            .backend
            .get_many_consistent_with_time_bounded(
                &[VOLUME_HEADER_KEY.to_vec()],
                KvReadLimits {
                    max_records: 1,
                    max_key_bytes: 64,
                    max_value_bytes: 512,
                    max_total_bytes: 4096,
                    max_response_bytes: 16 << 10,
                    max_data_requests: 1,
                },
            )
            .await?;
        if rows.len() != 1 || budget.state().closed {
            return Err(WorkspaceError::Busy);
        }
        let header: VolumeHeader =
            decode_open_value(rows[0].as_deref().ok_or(WorkspaceError::Fenced)?, 512)?;
        if header.volume_format != "workspace" {
            return Err(WorkspaceError::UnsupportedVolumeFormat(
                header.volume_format,
            ));
        }
        if header.schema_version != WORKSPACE_SCHEMA_VERSION {
            return Err(WorkspaceError::UnsupportedSchemaVersion(
                header.schema_version,
            ));
        }
        if header.volume_id.is_nil() || header.created_at_ns <= 0 {
            return Err(WorkspaceError::Fenced);
        }
        Ok(())
    }

    pub(crate) async fn retire_packed_binding_history_version<O>(
        self: &Arc<Self>,
        client: ObjectClient<O>,
        budget: Arc<SnapshotMountBudget>,
        options: PackedHistoryVersionRetirementOptions,
    ) -> Result<PackedHistoryRetirementReport, WorkspaceError>
    where
        O: ObjectBackend + Clone + 'static,
    {
        self.configure_packed_reader_pin_budget(budget.clone())?;
        if options.workspace_id.as_uuid().is_nil() || options.binding_version == 0 {
            return Err(WorkspaceError::Fenced);
        }
        let incarnation = {
            let _owner = budget
                .admit(&[(SnapshotBudgetPool::Metadata, OPERATION_BYTES)])
                .map_err(journal_budget_error)?;
            if budget.state().closed || options.cancel.is_cancelled() {
                return Err(WorkspaceError::Busy);
            }
            let key = format!(
                "packed/registry/history-root/{}/{:016x}",
                options.workspace_id, options.binding_version
            )
            .into_bytes();
            let raw = self
                .packed_journal_value(key.clone())
                .await?
                .ok_or(WorkspaceError::Fenced)?;
            let root = RootRow::decode(&raw)?;
            let binding = root.binding.as_ref().ok_or(WorkspaceError::Fenced)?;
            if binding.workspace_id != options.workspace_id
                || binding.binding.binding_version != options.binding_version
                || registry_history_root_key(binding) != key
                || !matches!(
                    root.state,
                    RootState::BindingHistory | RootState::Retiring | RootState::Retired
                )
                || budget.state().closed
                || options.cancel.is_cancelled()
            {
                return Err(WorkspaceError::Fenced);
            }
            root.incarnation
        };
        // The route does not authorize retirement. The owned driver reads its
        // actual root, mapping, history/current pair, pins, holds and backend
        // time before observing grace or reserving any physical DELETE.
        self.retire_packed_binding_history(
            client,
            budget,
            PackedHistoryRetirementOptions {
                incarnation,
                grace_ns: options.grace_ns,
                max_native_holds: options.max_native_holds,
                max_current_bindings: options.max_current_bindings,
                max_work: options.max_work,
                cancel: options.cancel,
            },
        )
        .await
    }

    pub(crate) async fn collect_aborted_packed_journal<O: ObjectBackend + Clone>(
        &self,
        journal_id: JournalId,
        client: &ObjectClient<O>,
        budget: &Arc<SnapshotMountBudget>,
        max_members: u64,
        cancel: CancellationToken,
    ) -> Result<u64, WorkspaceError> {
        self.configure_packed_reader_pin_budget(budget.clone())?;
        let owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, OPERATION_BYTES)])
            .map_err(journal_budget_error)?;
        if journal_id.as_uuid().is_nil() || budget.state().closed || cancel.is_cancelled() {
            return Err(WorkspaceError::Fenced);
        }
        let raw = self
            .packed_journal_value(journal_key(journal_id))
            .await?
            .ok_or(WorkspaceError::Fenced)?;
        let actual = PackedJournalRecord::decode(&raw)?;
        if actual.journal_id != journal_id || actual.phase != PackedJournalPhase::Aborted {
            return Err(WorkspaceError::Fenced);
        }
        drop(raw);
        let actual = actual.retain(owner)?;
        // No terminal state, source proof or completed PUT is supplied by the
        // caller. The collector consumes exact persisted journal/root checks.
        self.collect_aborted_packed_graph(&actual, client, budget, max_members, cancel)
            .await
    }
}
