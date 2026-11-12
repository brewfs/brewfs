//! Bounded history for one explicitly named packed workspace. Lease rows are
//! entity authority; the catalog epoch closes membership changes across pages.

use super::*;

pub(in crate::workspace::stores::kv_store) fn append_workspace_history_keys(
    keys: &mut Vec<Vec<u8>>,
    history: &[KvCheck],
    cap: usize,
) -> Result<(), WorkspaceError> {
    for check in history {
        if !keys.contains(&check.key) {
            if keys.len() >= cap {
                return Err(WorkspaceError::Busy);
            }
            keys.push(check.key.clone());
        }
    }
    Ok(())
}

pub(in crate::workspace::stores::kv_store) fn authenticate_workspace_history_values(
    keys: &[Vec<u8>],
    values: &[Option<Vec<u8>>],
    history: &[KvCheck],
) -> Result<(), WorkspaceError> {
    if keys.len() != values.len() {
        return Err(WorkspaceError::Fenced);
    }
    for check in history {
        let index = keys
            .iter()
            .position(|key| key == &check.key)
            .ok_or(WorkspaceError::Fenced)?;
        if values[index] != check.expected {
            return Err(WorkspaceError::Busy);
        }
    }
    Ok(())
}

impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    pub(in crate::workspace::stores::kv_store) async fn read_workspace_lease_history_checks(
        &self,
        workspace: WorkspaceId,
        max_rows: usize,
        limits: KvReadLimits,
    ) -> Result<Vec<KvCheck>, WorkspaceError> {
        limits.validate()?;
        if workspace.as_uuid().is_nil()
            || limits.max_records == 0
            || max_rows >= limits.max_records
            || limits.max_data_requests == 0
        {
            return Err(WorkspaceError::InvalidReadPlan(
                "packed workspace lease history plan".into(),
            ));
        }
        let key = TOPOLOGY_GENERATION_KEY.to_vec();
        let (values, _) = self
            .backend
            .get_many_consistent_with_time_bounded(
                std::slice::from_ref(&key),
                KvReadLimits {
                    max_records: 1,
                    ..limits
                },
            )
            .await?;
        if values.len() != 1 {
            return Err(WorkspaceError::Fenced);
        }
        layer_inventory_generation(&values[0])?;
        let fence = KvCheck {
            key,
            expected: values[0].clone(),
        };
        let mut checks = vec![fence.clone()];
        let prefix = [HOT_LEASE_PREFIX, format!("{workspace}/").as_bytes()].concat();
        let mut after = None;
        let mut total = fence.key.len() + fence.expected.as_ref().map_or(0, Vec::len);
        let mut rows = 0usize;
        let mut calls = 0usize;
        loop {
            if !self
                .backend
                .compare_and_swap(std::slice::from_ref(&fence), &[])
                .await?
            {
                return Err(WorkspaceError::Busy);
            }
            calls = calls.checked_add(1).ok_or(WorkspaceError::Busy)?;
            if calls > limits.max_data_requests {
                return Err(WorkspaceError::Busy);
            }
            let page = self
                .backend
                .scan_prefix_page_with_byte_limits(
                    &prefix,
                    after.as_deref(),
                    KvReadLimits {
                        // A single row fits even the largest admitted point
                        // value in the fixed decoder tier. Read an additional
                        // page to distinguish quota exhaustion from completion.
                        max_records: 1,
                        ..limits
                    },
                )
                .await?;
            if !self
                .backend
                .compare_and_swap(std::slice::from_ref(&fence), &[])
                .await?
            {
                return Err(WorkspaceError::Busy);
            }
            if page.is_empty() {
                break;
            }
            for entry in page {
                if !entry.key.starts_with(&prefix)
                    || after.as_ref().is_some_and(|last| entry.key <= *last)
                    || entry.key.len() > limits.max_key_bytes
                    || entry.value.len() > limits.max_value_bytes
                {
                    return Err(WorkspaceError::Fenced);
                }
                let row: SnapshotLease = decode_open_value(&entry.value, limits.max_value_bytes)?;
                if row.workspace_id != workspace
                    || entry.key != hot_lease_key(workspace, row.lease_id)
                {
                    return Err(WorkspaceError::Fenced);
                }
                rows = rows.checked_add(1).ok_or(WorkspaceError::Busy)?;
                total = total
                    .checked_add(entry.key.len())
                    .and_then(|bytes| bytes.checked_add(entry.value.len()))
                    .ok_or(WorkspaceError::Busy)?;
                if rows > max_rows || total > limits.max_total_bytes {
                    return Err(WorkspaceError::Busy);
                }
                after = Some(entry.key.clone());
                checks.push(KvCheck {
                    key: entry.key,
                    expected: Some(entry.value),
                });
            }
        }
        if !self.backend.compare_and_swap(&checks, &[]).await? {
            return Err(WorkspaceError::Busy);
        }
        Ok(checks)
    }
}
