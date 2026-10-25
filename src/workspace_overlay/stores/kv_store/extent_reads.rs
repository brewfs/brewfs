//! Ordinary paged reads must not mix histories from either side of compaction.
use super::*;
use crate::workspace_overlay::packed_v3::wire005::V3BudgetPool;

// Each attempt retains the ordinary row/byte limits. Under sustained writes,
// return a retryable error instead of scanning without a finite work bound.
const MAX_EXTENT_READ_ATTEMPTS: usize = 3;

impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    async fn extent_read_generations(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>, WorkspaceError> {
        let mut generations = Vec::with_capacity(keys.len());
        for window in keys.chunks(32) {
            let (values, _) = self
                .backend
                .get_many_consistent_with_time_bounded(
                    window,
                    KvReadLimits {
                        max_records: window.len(),
                        max_key_bytes: 128,
                        max_value_bytes: 16,
                        max_total_bytes: 8 << 10,
                        max_response_bytes: 16 << 10,
                        max_data_requests: window.len(),
                    },
                )
                .await?;
            if values.len() != window.len() {
                return Err(WorkspaceError::Fenced);
            }
            for raw in &values {
                native_slice_deletion::extent_generation(raw)?;
            }
            generations.extend(values);
        }
        Ok(generations)
    }

    pub(super) async fn read_extent_deltas_consistent(
        &self,
        request: ExtentQuery,
    ) -> Result<Vec<DataExtentDelta>, WorkspaceError> {
        if request.range_start > request.range_end {
            return Err(WorkspaceError::InvalidReadPlan(
                "extent query starts after its end".into(),
            ));
        }
        if request.range_start == request.range_end || request.layer_ids.is_empty() {
            return Ok(Vec::new());
        }
        // Own the bounded generation response and both retained snapshots
        // before allocating their keys. The extent scan keeps its row/byte cap.
        let _generation_owner = self
            .native_auxiliary_budget()
            .admit(&[(
                V3BudgetPool::Metadata,
                (64 << 10) + request.layer_ids.len() as u64 * 256,
            )])
            .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
        let keys = request
            .layer_ids
            .iter()
            .copied()
            .map(native_slice_deletion::layer_extent_generation_key)
            .collect::<Vec<_>>();
        for _ in 0..MAX_EXTENT_READ_ATTEMPTS {
            let before = self.extent_read_generations(&keys).await?;
            // A merge can rewrite an old sequence and delete a later mask.
            // Even a transient over-budget result needs its final fence: a
            // concurrent compaction may already have repaired that history.
            let result = self.scan_extent_deltas(&request).await;
            let after = self.extent_read_generations(&keys).await?;
            if before == after {
                return result;
            }
        }
        Err(WorkspaceError::Busy)
    }

    async fn scan_extent_deltas(
        &self,
        request: &ExtentQuery,
    ) -> Result<Vec<DataExtentDelta>, WorkspaceError> {
        let mut rows = Vec::new();
        let mut scanned_rows_total = 0usize;
        let mut scanned_bytes_total = 0usize;
        for &layer in &request.layer_ids {
            let prefix = extent_chunk_prefix(layer, request.ino, request.chunk_index);
            let mut after = None;
            let mut found = Vec::new();
            loop {
                let page = self
                    .backend
                    .scan_prefix_page_with_byte_limits(
                        &prefix,
                        after.as_deref(),
                        delta_scan_limits(),
                    )
                    .await?;
                if page.is_empty() {
                    break;
                }
                let next = page.last().map(|entry| entry.key.clone());
                scanned_rows_total = scanned_rows_total
                    .checked_add(page.len())
                    .ok_or(WorkspaceError::Fenced)?;
                scanned_bytes_total = scanned_bytes_total
                    .checked_add(validate_delta_scan_page(&page, &prefix, after.as_deref())?)
                    .ok_or(WorkspaceError::Fenced)?;
                if scanned_rows_total > DELTA_SCAN_MAX_ROWS
                    || scanned_bytes_total > DELTA_SCAN_MAX_BYTES
                {
                    return Err(WorkspaceError::InvalidReadPlan(
                        "extent delta scan exceeds bounded metadata budget".into(),
                    ));
                }
                for entry in page {
                    let row: DataExtentDelta = decode(&entry.value)?;
                    row.validate()?;
                    if row.layer_id != layer
                        || row.ino != request.ino
                        || row.chunk_index != request.chunk_index
                        || entry.key != extent_key(&row)
                    {
                        return Err(WorkspaceError::CorruptMetadata(
                            "extent key/record identity mismatch".into(),
                        ));
                    }
                    let row_end = row.logical_offset + row.length;
                    if row.logical_offset < request.range_end && row_end > request.range_start {
                        found.push(row);
                    }
                }
                after = next;
            }
            found.sort_by_key(|row| std::cmp::Reverse(row.sequence));
            rows.extend(found);
        }
        Ok(rows)
    }
}
