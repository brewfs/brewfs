//! Compact writable extent histories in the mutation's exact head CAS.
use super::*;
use crate::workspace::packed::snapshot::{SnapshotBudgetPool, SnapshotOwnedPermit};

// Recovery can inspect a legacy history larger than either reader admits.
// This is a separate, finite write-side bound, never a larger read budget.
pub(super) const RECOVERY_MAX_ROWS: usize = 8192;
const COMPACTION_PAGE_BYTES: u64 = 128 << 10;
// Covers decoded rows and interval/tree scratch. Recovery retains at most
// 8192 scanned rows for one chunk, plus at most 4096 incoming rows per batch.
// Derived writes and retired ownership are admitted separately through CAS.
const COMPACTION_ROW_BYTES: u64 = 1024;

fn budget_exceeded() -> WorkspaceError {
    WorkspaceError::InvalidReadPlan("extent compaction exceeds bounded recovery budget".into())
}

fn incoming_extent(
    write: &KvWrite,
    head: LayerId,
) -> Result<Option<DataExtentDelta>, WorkspaceError> {
    let KvWrite::Put { key, value } = write else {
        return Ok(None);
    };
    if !key.starts_with(b"delta/extent/") {
        return Ok(None);
    }
    let row: DataExtentDelta = decode_open_value(value, 512)?;
    row.validate()?;
    if row.layer_id != head || *key != extent_key(&row) {
        return Err(WorkspaceError::CorruptMetadata(
            "extent compaction identity mismatch".into(),
        ));
    }
    Ok(Some(row))
}

/// Drop only rows wholly masked by the union of newer coverage. Then join
/// touching holes at the OLDER sequence, provided lowering the newer hole
/// cannot expose an intervening Data row. Older masks are never promoted.
pub(super) fn coalesce_extent_rows(rows: &[DataExtentDelta]) -> Vec<DataExtentDelta> {
    let mut ordered = rows.iter().collect::<Vec<_>>();
    ordered.sort_by_key(|row| std::cmp::Reverse(row.sequence));
    let mut covered = BTreeMap::<u64, u64>::new();
    let mut data = Vec::new();
    let mut holes = Vec::new();
    for row in ordered {
        let mut start = row.logical_offset;
        let mut end = start + row.length;
        if let Some((&previous, &previous_end)) = covered.range(..=start).next_back() {
            if previous_end >= end {
                continue;
            }
            if previous_end >= start {
                start = previous;
                covered.remove(&previous);
            }
        }
        while let Some((&next, &next_end)) = covered.range(start..).next() {
            if next > end {
                break;
            }
            end = end.max(next_end);
            covered.remove(&next);
        }
        covered.insert(start, end);
        match row.kind {
            ExtentKind::Hole => holes.push(row.clone()),
            ExtentKind::Data { .. } => data.push(row.clone()),
        }
    }
    holes.sort_by_key(|row| (row.logical_offset, row.sequence));
    let mut merged: Vec<DataExtentDelta> = Vec::new();
    for hole in holes {
        if let Some(previous) = merged.last_mut()
            && hole.logical_offset <= previous.logical_offset + previous.length
        {
            let (older, newer) = if previous.sequence < hole.sequence {
                (&*previous, &hole)
            } else {
                (&hole, &*previous)
            };
            let crosses_data = data.iter().any(|row| {
                older.sequence < row.sequence
                    && row.sequence < newer.sequence
                    && row.logical_offset < newer.logical_offset + newer.length
                    && newer.logical_offset < row.logical_offset + row.length
            });
            if !crosses_data {
                previous.length = (previous.logical_offset + previous.length)
                    .max(hole.logical_offset + hole.length)
                    - previous.logical_offset;
                previous.sequence = previous.sequence.min(hole.sequence);
                continue;
            }
        }
        merged.push(hole);
    }
    data.extend(merged);
    data.sort_by_key(|row| row.sequence);
    data
}

struct CompactionScan {
    rows: usize,
    bytes: usize,
    admissions: Vec<SnapshotOwnedPermit>,
}

impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    async fn scan_compaction_chunk(
        &self,
        layer: LayerId,
        ino: i64,
        chunk: u64,
        scan: &mut CompactionScan,
    ) -> Result<Vec<DataExtentDelta>, WorkspaceError> {
        let prefix = extent_chunk_prefix(layer, ino, chunk);
        // Extent keys and values have fixed-size encodings. Redis can read a
        // larger page in one RPC; the backend clamps physical response bytes.
        // TiKV keeps its existing 32-data-request budget and page capacity.
        let mut limits = KvReadLimits {
            max_records: 128,
            max_key_bytes: 128,
            max_value_bytes: 128,
            max_total_bytes: 48 << 10,
            max_response_bytes: 64 << 10,
            max_data_requests: 32,
        };
        limits.max_records = self.backend.scan_page_record_limit(limits)?;
        let mut after = None;
        let mut rows = Vec::new();
        loop {
            let page = self
                .backend
                .scan_prefix_page_with_byte_limits(&prefix, after.as_deref(), limits)
                .await?;
            if page.is_empty() {
                break;
            }
            scan.rows = scan
                .rows
                .checked_add(page.len())
                .ok_or_else(budget_exceeded)?;
            scan.bytes = scan
                .bytes
                .checked_add(validate_delta_scan_page_with_limit(
                    &page,
                    &prefix,
                    after.as_deref(),
                    limits.max_records,
                )?)
                .ok_or_else(budget_exceeded)?;
            if scan.rows > RECOVERY_MAX_ROWS || scan.bytes > DELTA_SCAN_MAX_BYTES {
                return Err(budget_exceeded());
            }
            // Admit retained allocation before decoding or growing vectors.
            scan.admissions.push(
                self.native_auxiliary_budget()
                    .admit(&[(
                        SnapshotBudgetPool::Metadata,
                        page.len() as u64 * COMPACTION_ROW_BYTES,
                    )])
                    .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?,
            );
            after = page.last().map(|entry| entry.key.clone());
            for entry in page {
                let row: DataExtentDelta = decode_open_value(&entry.value, 512)?;
                row.validate()?;
                if row.layer_id != layer
                    || row.ino != ino
                    || row.chunk_index != chunk
                    || entry.key != extent_key(&row)
                {
                    return Err(WorkspaceError::CorruptMetadata(
                        "extent compaction key/record identity mismatch".into(),
                    ));
                }
                rows.push(row);
            }
        }
        Ok(rows)
    }

    /// Called after sequence allocation, before the extent generation fence.
    /// All replacements, deletions and slice ownership join the same head CAS.
    /// The caller retains admission through that CAS (including its retry).
    pub(super) async fn compact_extent_history(
        &self,
        head: &LayerRecord,
        packed_base: Option<&LayerRecord>,
        reader_limit: usize,
        writes: &mut Vec<KvWrite>,
    ) -> Result<Vec<SnapshotOwnedPermit>, WorkspaceError> {
        let mut count = 0usize;
        for write in writes.iter() {
            if incoming_extent(write, head.layer_id)?.is_some() {
                count += 1;
                if count > DELTA_SCAN_MAX_ROWS {
                    return Err(budget_exceeded());
                }
            }
        }
        if count == 0 {
            return Ok(Vec::new());
        }
        let admission = self
            .native_auxiliary_budget()
            .admit(&[(
                SnapshotBudgetPool::Metadata,
                COMPACTION_PAGE_BYTES + count as u64 * COMPACTION_ROW_BYTES,
            )])
            .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
        let mut admissions = vec![admission];
        let mut incoming: BTreeMap<(i64, u64), Vec<DataExtentDelta>> = BTreeMap::new();
        for write in writes.iter() {
            if let Some(row) = incoming_extent(write, head.layer_id)? {
                incoming
                    .entry((row.ino, row.chunk_index))
                    .or_default()
                    .push(row);
            }
        }
        let lower_layers = match (packed_base, head.parent_layer_id) {
            (Some(base), _) => vec![base.layer_id],
            (None, Some(parent)) => self
                .load_layer_chain(parent)
                .await?
                .into_iter()
                .map(|row| row.layer_id)
                .collect(),
            (None, None) => Vec::new(),
        };
        writes.retain(
            |write| !matches!(write, KvWrite::Put { key, .. } if key.starts_with(b"delta/extent/")),
        );
        let mut retired_slices = BTreeMap::<u64, u64>::new();
        for ((ino, chunk), incoming) in incoming {
            // Readers bound a single chunk across its layers. An unrelated
            // chunk in the same publication batch must not consume that bound
            // or retain the previous chunk's temporary scan allocation.
            let mut scan = CompactionScan {
                rows: 0,
                bytes: 0,
                admissions: Vec::new(),
            };
            let mut history = self
                .scan_compaction_chunk(head.layer_id, ino, chunk, &mut scan)
                .await?;
            let old_count = history.len();
            history.extend(incoming);
            let mut sequences = BTreeSet::new();
            if history.iter().any(|row| !sequences.insert(row.sequence)) {
                // A concurrent writer may have used our tentative sequence.
                return Err(WorkspaceError::Busy);
            }
            let compacted = coalesce_extent_rows(&history);
            let mut resulting_rows = compacted.len();
            // Readers count persisted rows before range/EOF filtering, across
            // the full chain (or both native layers of a packed mount).
            for &lower in &lower_layers {
                resulting_rows += self
                    .scan_compaction_chunk(lower, ino, chunk, &mut scan)
                    .await?
                    .len();
            }
            if resulting_rows > reader_limit {
                return Err(WorkspaceError::InvalidReadPlan(format!(
                    "compacted extent history exceeds reader row limit ({resulting_rows} > {reader_limit})"
                )));
            }
            let retained = compacted
                .iter()
                .map(|row| (row.sequence, row))
                .collect::<BTreeMap<_, _>>();
            let stored = history[..old_count]
                .iter()
                .map(|row| (row.sequence, row))
                .collect::<BTreeMap<_, _>>();
            let deletions = history[..old_count]
                .iter()
                .filter(|row| !retained.contains_key(&row.sequence))
                .count();
            let replacements = compacted
                .iter()
                .filter(|row| stored.get(&row.sequence).copied() != Some(*row))
                .count();
            let owners = history
                .iter()
                .filter(|row| {
                    !retained.contains_key(&row.sequence)
                        && matches!(row.kind, ExtentKind::Data { .. })
                })
                .count();
            // These allocations outlive the per-chunk scratch. Reserve before
            // growing writes or retired_slices; duplicate owners deliberately
            // overestimate the map and eventual ownership writes.
            admissions.push(
                self.native_auxiliary_budget()
                    .admit(&[(
                        SnapshotBudgetPool::Metadata,
                        (deletions + replacements + owners) as u64 * COMPACTION_ROW_BYTES,
                    )])
                    .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?,
            );
            for row in &history[..old_count] {
                if !retained.contains_key(&row.sequence) {
                    writes.push(KvWrite::Delete {
                        key: extent_key(row),
                    });
                }
            }
            for row in &history {
                if !retained.contains_key(&row.sequence)
                    && let ExtentKind::Data {
                        slice_id,
                        slice_offset,
                    } = row.kind
                {
                    let end = slice_offset + row.length; // validated before compaction
                    retired_slices
                        .entry(slice_id)
                        .and_modify(|old| *old = (*old).max(end))
                        .or_insert(end);
                }
            }
            for row in &compacted {
                if stored.get(&row.sequence).copied() != Some(row) {
                    writes.push(put(extent_key(row), row)?);
                }
            }
        }
        self.retain_extent_ownership(head.layer_id, retired_slices, writes)
            .await?;
        Ok(admissions)
    }
}
