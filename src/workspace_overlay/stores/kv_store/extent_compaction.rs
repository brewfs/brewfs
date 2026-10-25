//! Remove obsolete hole masks while publishing a writable head mutation.
use super::*;
use crate::workspace_overlay::packed_v3::wire005::{V3BudgetPool, V3OwnedPermit};

// Reserve a scan page and incoming groups first; admit deletion storage in
// batches before retaining it. Exact extent keys are <128 bytes, so 512 bytes
// per deletion covers the key, KvWrite capacity growth and permit tracking.
// Maximum retained admission: 128 KiB + 4096 * 512 + 4096 * 512 = 4.125 MiB.
const COMPACTION_PAGE_BYTES: u64 = 128 << 10;
const COMPACTION_ROW_BYTES: u64 = 512;

fn budget_exceeded() -> WorkspaceError {
    WorkspaceError::InvalidReadPlan("hole compaction exceeds bounded metadata budget".into())
}

fn incoming_hole(
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
            "hole compaction extent identity mismatch".into(),
        ));
    }
    Ok(matches!(row.kind, ExtentKind::Hole).then_some(row))
}

impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    /// Call only after allocating sequences under an exact writable-head CAS,
    /// and before preparing the native extent generation fence. Never use for
    /// sealed layers or a standalone cleanup transaction.
    pub(super) async fn compact_obsolete_holes(
        &self,
        head: LayerId,
        writes: &mut Vec<KvWrite>,
    ) -> Result<Vec<V3OwnedPermit>, WorkspaceError> {
        let mut count = 0usize;
        for write in writes.iter() {
            if incoming_hole(write, head)?.is_some() {
                count += 1;
                if count > DELTA_SCAN_MAX_ROWS {
                    return Err(budget_exceeded());
                }
            }
        }
        if count == 0 {
            return Ok(Vec::new());
        }
        let owner = self
            .native_auxiliary_budget()
            .admit(&[(
                V3BudgetPool::Metadata,
                COMPACTION_PAGE_BYTES + count as u64 * COMPACTION_ROW_BYTES,
            )])
            .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
        let mut owners = vec![owner];
        let mut holes: BTreeMap<(i64, u64), Vec<DataExtentDelta>> = BTreeMap::new();
        for write in writes.iter() {
            if let Some(row) = incoming_hole(write, head)? {
                holes
                    .entry((row.ino, row.chunk_index))
                    .or_default()
                    .push(row);
            }
        }
        let mut scanned_bytes = 0usize;
        let mut deleted = 0usize;
        for ((ino, chunk), incoming) in holes {
            let prefix = extent_chunk_prefix(head, ino, chunk);
            let mut after = None;
            let mut scanned_rows = 0usize;
            loop {
                let page = self
                    .backend
                    .scan_prefix_page_with_byte_limits(
                        &prefix,
                        after.as_deref(),
                        KvReadLimits {
                            max_value_bytes: 512,
                            ..delta_scan_limits()
                        },
                    )
                    .await?;
                if page.is_empty() {
                    break;
                }
                scanned_rows = scanned_rows
                    .checked_add(page.len())
                    .ok_or_else(budget_exceeded)?;
                scanned_bytes = scanned_bytes
                    .checked_add(validate_delta_scan_page(&page, &prefix, after.as_deref())?)
                    .ok_or_else(budget_exceeded)?;
                // Bound each chunk exactly as the reader does. The byte cap
                // covers the entire mutation, including all touched chunks.
                if scanned_rows > DELTA_SCAN_MAX_ROWS || scanned_bytes > DELTA_SCAN_MAX_BYTES {
                    return Err(budget_exceeded());
                }
                after = page.last().map(|entry| entry.key.clone());
                for entry in page {
                    let old: DataExtentDelta = decode_open_value(&entry.value, 512)?;
                    old.validate()?;
                    if old.layer_id != head
                        || old.ino != ino
                        || old.chunk_index != chunk
                        || entry.key != extent_key(&old)
                    {
                        return Err(WorkspaceError::CorruptMetadata(
                            "hole compaction key/record identity mismatch".into(),
                        ));
                    }
                    // Preserve every Data row: even masked slices belong to
                    // the layer and must remain visible to its GC census.
                    // Partial overlap also stays intact; promoting an old
                    // hole's surviving tail to a newer sequence could erase
                    // an intervening write. Full containment needs no split,
                    // resequencing, or change to the resolver's precedence.
                    if old.kind == ExtentKind::Hole
                        && incoming.iter().any(|new| {
                            new.sequence > old.sequence
                                && new.logical_offset <= old.logical_offset
                                && new.logical_offset + new.length
                                    >= old.logical_offset + old.length
                        })
                    {
                        deleted += 1;
                        if deleted > DELTA_SCAN_MAX_ROWS {
                            return Err(budget_exceeded());
                        }
                        if (deleted - 1).is_multiple_of(DELTA_SCAN_PAGE_RECORDS) {
                            owners.push(
                                self.native_auxiliary_budget()
                                    .admit(&[(
                                        V3BudgetPool::Metadata,
                                        DELTA_SCAN_PAGE_RECORDS as u64 * COMPACTION_ROW_BYTES,
                                    )])
                                    .map_err(|error| {
                                        WorkspaceError::InvalidReadPlan(error.to_string())
                                    })?,
                            );
                        }
                        writes.push(KvWrite::Delete { key: entry.key });
                    }
                }
            }
        }
        // The head check fences concurrent truncates/writes; the caller's
        // extent generation fence also covers these deletions for GC. A
        // failed/uncertain CAS is handled by the original mutation driver.
        Ok(owners)
    }
}
