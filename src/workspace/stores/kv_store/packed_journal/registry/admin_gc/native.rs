//! Bound the existing native collector and journal each physical block delete.
//! Unknown outcomes are retained; a later tick never resends that block pair.
use super::*;
use crate::chunk::cache::ChunksCacheConfig;
use crate::chunk::store::{BlockKey, BlockStore, BlockStoreConfig, ObjectBlockStore};
use crate::workspace::gc::WorkspaceGc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

// This adapter bounds a collector tick across the entity catalog. CONTROL is
// only a small format header; larger xattrs defer before any physical DELETE.
// Reuse TiKV's real 8/16/64/128 KiB decoders; no extra TLS/SDK client is needed.
const CONTROL_VALUE_BYTES: usize = 96 << 10;
const RESPONSE_BYTES: usize = 128 << 10;
const RESULT_BYTES: usize = 112 << 10;
const MUTATION_BYTES: usize = 256 << 10;
const TOTAL_BYTES: usize = 32 << 20;
const MAX_ROWS: usize = 4096;
const MAX_CALLS: usize = 4096;
const BLOCK_DELETE_QUOTA: usize = 32;

struct Bounded<B: WorkspaceKvBackend> {
    backend: Arc<B>,
    cancel: CancellationToken,
    budget: Arc<SnapshotMountBudget>,
    calls: AtomicUsize,
    rows: AtomicUsize,
    bytes: AtomicUsize,
}
impl<B: WorkspaceKvBackend> Bounded<B> {
    fn live(&self) -> Result<(), WorkspaceError> {
        if self.cancel.is_cancelled() || self.budget.state().closed {
            return Err(WorkspaceError::Busy);
        }
        if self.calls.fetch_add(1, Ordering::SeqCst) >= MAX_CALLS {
            return Err(WorkspaceError::Busy);
        }
        Ok(())
    }
    fn charge(&self, rows: usize, bytes: usize) -> Result<(), WorkspaceError> {
        let old_rows = self.rows.fetch_add(rows, Ordering::SeqCst);
        let old_bytes = self.bytes.fetch_add(bytes, Ordering::SeqCst);
        if rows > MAX_ROWS.saturating_sub(old_rows) || bytes > TOTAL_BYTES.saturating_sub(old_bytes)
        {
            return Err(WorkspaceError::Busy);
        }
        Ok(())
    }
    fn validate_control(raw: &[u8]) -> Result<(), WorkspaceError> {
        if raw.len() > OPEN_RECORD_MAX_BYTES {
            return Err(WorkspaceError::Busy);
        }
        let state: ControlHeader = decode_control(raw)?;
        let header = state.header.as_ref().ok_or(WorkspaceError::Fenced)?;
        if state.schema_version != WORKSPACE_SCHEMA_VERSION
            || state.catalog_format != CATALOG_FORMAT
            || header.schema_version != WORKSPACE_SCHEMA_VERSION
            || header.volume_format != VOLUME_FORMAT
            || header.volume_id.is_nil()
            || header.created_at_ns <= 0
        {
            return Err(WorkspaceError::Fenced);
        }
        Ok(())
    }
    fn value_bytes(key: &[u8]) -> usize {
        if key == CONTROL_KEY {
            OPEN_RECORD_MAX_BYTES
        } else if key.is_empty() || key.starts_with(b"delta/xattr/") {
            CONTROL_VALUE_BYTES
        } else if key.starts_with(b"packed/native-slice-delete/") {
            // Only the permanent SID reservation has the 48 KiB schema. The
            // existing 128 MiB tick/32 MiB reservation owners cover retained
            // values; point reads still cap total results at 112 KiB and wire
            // at 128 KiB, with no whole-catalog or native-family expansion.
            48 << 10
        } else if key.starts_with(b"packed/native-")
            || key == PACKED_ROOT_GENERATION_KEY
            || key == LAYER_INVENTORY_GENERATION_KEY
            || key.starts_with(HOT_ALLOCATOR_PREFIX)
        {
            4 << 10
        } else if key.starts_with(HOT_WORKSPACE_PREFIX)
            || key.starts_with(HOT_LAYER_PREFIX)
            || key.starts_with(HOT_LEASE_PREFIX)
            || key.starts_with(HOT_SNAPSHOT_PREFIX)
            || key.starts_with(HOT_JOURNAL_PREFIX)
            || key.starts_with(b"delta/")
            || key.starts_with(b"open/")
            || key == VOLUME_HEADER_KEY
        {
            12 << 10
        } else {
            48 << 10
        }
    }
    fn limits(records: usize, value_bytes: usize) -> KvReadLimits {
        KvReadLimits {
            max_records: records.clamp(1, 32),
            max_key_bytes: 1024,
            max_value_bytes: value_bytes,
            max_total_bytes: RESULT_BYTES,
            max_response_bytes: RESPONSE_BYTES,
            max_data_requests: 32,
        }
    }
    fn key_limits(keys: &[Vec<u8>]) -> KvReadLimits {
        Self::limits(
            keys.len(),
            keys.iter()
                .map(|key| Self::value_bytes(key))
                .max()
                .unwrap_or(4 << 10),
        )
    }
    fn clamp(limits: KvReadLimits, value_bytes: usize) -> KvReadLimits {
        let cap = Self::limits(limits.max_records, value_bytes);
        KvReadLimits {
            max_records: limits.max_records.min(cap.max_records),
            max_key_bytes: limits.max_key_bytes.min(cap.max_key_bytes),
            max_value_bytes: limits.max_value_bytes.min(cap.max_value_bytes),
            max_total_bytes: limits.max_total_bytes.min(cap.max_total_bytes),
            max_response_bytes: limits.max_response_bytes.min(cap.max_response_bytes),
            max_data_requests: limits.max_data_requests.min(cap.max_data_requests),
        }
    }
    fn page_limits(limits: KvReadLimits, prefix: &[u8]) -> KvReadLimits {
        let mut limits = Self::clamp(limits, Self::value_bytes(prefix));
        let wire: usize = match limits.max_value_bytes {
            0..=4096 => 8 << 10,
            4097..=12288 => 16 << 10,
            12289..=49152 => 64 << 10,
            _ => 128 << 10,
        };
        // A single Scan response holds the whole page. Plan for the largest
        // valid row, including key/framing, rather than requesting 32 large
        // values and discovering the decoder cannot hold even a valid page.
        let row_bytes = limits
            .max_key_bytes
            .saturating_add(limits.max_value_bytes)
            .saturating_add(64);
        let rows = wire.saturating_sub(1024) / row_bytes.max(1);
        limits.max_records = limits.max_records.min(rows.max(1));
        limits
    }
    fn validate_rows(&self, rows: &[KvEntry], limits: KvReadLimits) -> Result<(), WorkspaceError> {
        let bytes = rows.iter().try_fold(0usize, |bytes, row| {
            if row.key.len() > limits.max_key_bytes
                || row.value.len() > limits.max_value_bytes
                || row.value.len() > Self::value_bytes(&row.key)
            {
                return Err(WorkspaceError::Busy);
            }
            bytes
                .checked_add(row.key.len())
                .and_then(|bytes| bytes.checked_add(row.value.len()))
                .ok_or(WorkspaceError::Busy)
        })?;
        if rows.len() > limits.max_records || bytes > limits.max_total_bytes {
            return Err(WorkspaceError::Busy);
        }
        self.charge(
            rows.len(),
            rows.iter().map(|row| row.key.len() + row.value.len()).sum(),
        )?;
        for row in rows {
            if row.key == CONTROL_KEY {
                Self::validate_control(&row.value)?;
            }
        }
        Ok(())
    }
    fn mutation_plan(&self, checks: &[KvCheck], writes: &[KvWrite]) -> Result<(), WorkspaceError> {
        if checks.len() + writes.len() > 256 {
            return Err(WorkspaceError::Busy);
        }
        let mut bytes = 0usize;
        for check in checks {
            let size = check.expected.as_ref().map_or(0, Vec::len);
            if check.key.len() > 1024 || size > Self::value_bytes(&check.key) {
                return Err(WorkspaceError::Busy);
            }
            bytes = bytes
                .checked_add(check.key.len() + size)
                .ok_or(WorkspaceError::Busy)?;
        }
        for write in writes {
            let (key, size) = match write {
                KvWrite::Put { key, value } => (key, value.len()),
                KvWrite::Delete { key } => (key, 0),
            };
            if key.len() > 1024 || size > Self::value_bytes(key) {
                return Err(WorkspaceError::Busy);
            }
            bytes = bytes
                .checked_add(key.len() + size)
                .ok_or(WorkspaceError::Busy)?;
        }
        if bytes > MUTATION_BYTES {
            return Err(WorkspaceError::Busy);
        }
        self.charge(0, bytes)
    }
}

#[async_trait]
impl<B: WorkspaceKvBackend> WorkspaceKvBackend for Bounded<B> {
    fn native_gc_metadata_page_quota(&self) -> Option<usize> {
        Some(32)
    }
    fn name(&self) -> &'static str {
        "bounded-native-operator-gc"
    }
    fn supports_consistent_reads(&self) -> bool {
        self.backend.supports_consistent_reads()
    }
    async fn authenticate_gc_admin(&self) -> Result<(), WorkspaceError> {
        self.live()?;
        self.backend.authenticate_gc_admin().await
    }
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, WorkspaceError> {
        let (mut rows, _) = self
            .get_many_consistent_with_time_bounded(
                &[key.to_vec()],
                Self::limits(1, Self::value_bytes(key)),
            )
            .await?;
        if rows.len() != 1 {
            return Err(WorkspaceError::Fenced);
        }
        Ok(rows.remove(0))
    }
    async fn get_many_consistent(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>, WorkspaceError> {
        Ok(self
            .get_many_consistent_with_time_bounded(keys, Self::key_limits(keys))
            .await?
            .0)
    }
    async fn get_many_consistent_with_time(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        self.get_many_consistent_with_time_bounded(keys, Self::key_limits(keys))
            .await
    }
    async fn get_many_consistent_with_time_bounded(
        &self,
        keys: &[Vec<u8>],
        limits: KvReadLimits,
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        self.live()?;
        let limits = Self::clamp(limits, Self::key_limits(keys).max_value_bytes);
        limits.validate_keys(keys)?;
        let result = self
            .backend
            .get_many_consistent_with_time_bounded(keys, limits)
            .await?;
        if result.0.len() != keys.len() {
            return Err(WorkspaceError::Fenced);
        }
        let mut total = 0usize;
        for (key, value) in keys.iter().zip(&result.0) {
            let size = value.as_ref().map_or(0, Vec::len);
            if size > limits.max_value_bytes || size > Self::value_bytes(key) {
                return Err(WorkspaceError::Busy);
            }
            total = total
                .checked_add(key.len())
                .and_then(|total| total.checked_add(size))
                .ok_or(WorkspaceError::Busy)?;
        }
        if total > limits.max_total_bytes {
            return Err(WorkspaceError::Busy);
        }
        self.charge(
            keys.len(),
            keys.iter().map(Vec::len).sum::<usize>()
                + result
                    .0
                    .iter()
                    .map(|row| row.as_ref().map_or(0, Vec::len))
                    .sum::<usize>(),
        )?;
        for (key, value) in keys.iter().zip(&result.0) {
            if key.as_slice() == CONTROL_KEY {
                // A GC proof must retain the initialized format/volume header.
                // Entity root rows are authenticated separately in its packet.
                let raw = value.as_deref().ok_or(WorkspaceError::Fenced)?;
                Self::validate_control(raw)?;
            }
        }
        if self.cancel.is_cancelled() {
            return Err(WorkspaceError::Busy);
        }
        Ok(result)
    }
    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<KvEntry>, WorkspaceError> {
        let mut output = Vec::new();
        let mut after = None;
        loop {
            let page = self
                .scan_prefix_page_with_byte_limits(
                    prefix,
                    after.as_deref(),
                    Self::limits(32, Self::value_bytes(prefix)),
                )
                .await?;
            if page.is_empty() {
                return Ok(output);
            }
            after = page.last().map(|entry| entry.key.clone());
            output.extend(page);
        }
    }
    async fn scan_prefix_with_byte_limits(
        &self,
        prefix: &[u8],
        limits: KvReadLimits,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        self.live()?;
        let limits = Self::clamp(limits, Self::value_bytes(prefix));
        limits.validate()?;
        let rows = self
            .backend
            .scan_prefix_with_byte_limits(prefix, limits)
            .await?;
        if rows.iter().any(|row| !row.key.starts_with(prefix)) {
            return Err(WorkspaceError::Fenced);
        }
        self.validate_rows(&rows, limits)?;
        if self.cancel.is_cancelled() {
            return Err(WorkspaceError::Busy);
        }
        Ok(rows)
    }
    async fn scan_prefix_page_with_byte_limits(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limits: KvReadLimits,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        self.live()?;
        let limits = Self::page_limits(limits, prefix);
        limits.validate_scan_page(prefix, after)?;
        let rows = self
            .backend
            .scan_prefix_page_with_byte_limits(prefix, after, limits)
            .await?;
        let mut previous = after;
        for entry in &rows {
            if !entry.key.starts_with(prefix)
                || previous.is_some_and(|key| entry.key.as_slice() <= key)
            {
                return Err(WorkspaceError::Fenced);
            }
            previous = Some(&entry.key);
        }
        self.validate_rows(&rows, limits)?;
        if self.cancel.is_cancelled() {
            return Err(WorkspaceError::Busy);
        }
        Ok(rows)
    }
    async fn compare_and_swap(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
    ) -> Result<bool, WorkspaceError> {
        self.live()?;
        self.mutation_plan(checks, writes)?;
        self.backend.compare_and_swap(checks, writes).await
    }
    async fn compare_and_swap_before(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        before: i64,
    ) -> Result<bool, WorkspaceError> {
        self.live()?;
        self.mutation_plan(checks, writes)?;
        self.backend
            .compare_and_swap_before(checks, writes, before)
            .await
    }
    async fn compare_and_swap_in_time_window(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        lower: Option<i64>,
        upper: Option<i64>,
    ) -> Result<bool, WorkspaceError> {
        self.live()?;
        self.mutation_plan(checks, writes)?;
        self.backend
            .compare_and_swap_in_time_window(checks, writes, lower, upper)
            .await
    }
    async fn authenticate_checks_before_bounded(
        &self,
        checks: &[KvCheck],
        before: i64,
        limits: KvReadLimits,
    ) -> Result<bool, WorkspaceError> {
        self.live()?;
        self.mutation_plan(checks, &[])?;
        let value_bytes = checks
            .iter()
            .map(|check| check.expected.as_ref().map_or(0, Vec::len))
            .max()
            .unwrap_or(0);
        // Existing TiKV exact-check authentication has its own 48 KiB schema.
        // Do not request the larger read-only CONTROL envelope for small checks.
        if value_bytes > 48 << 10 {
            return Err(WorkspaceError::UnsupportedCapability(
                "native GC authentication value exceeds 48 KiB",
            ));
        }
        self.backend
            .authenticate_checks_before_bounded(
                checks,
                before,
                Self::clamp(limits, (48 << 10).min(limits.max_value_bytes)),
            )
            .await
    }
    async fn server_time_ns(&self) -> Result<i64, WorkspaceError> {
        self.live()?;
        self.backend.server_time_ns().await
    }
}

#[derive(Serialize, Deserialize, Eq, PartialEq)]
enum BlockDeleteState {
    Dispatched,
    Complete,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BlockDelete {
    slice: u64,
    block: u32,
    state: BlockDeleteState,
}
fn block_delete_key(key: BlockKey) -> Vec<u8> {
    format!("packed/native-block-delete/{:016x}/{:08x}", key.0, key.1).into_bytes()
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RangeProgress {
    slice: u64,
    first: u32,
    count: u64,
    next: u64,
}
fn range_progress_key(key: BlockKey, count: u64) -> Vec<u8> {
    format!(
        "packed/native-delete-progress/{:016x}/{:08x}/{:016x}",
        key.0, key.1, count
    )
    .into_bytes()
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SliceUpperBound {
    slice: u64,
    end: u64,
}
fn slice_upper_bound_key(slice: u64) -> Vec<u8> {
    format!("packed/native-slice-upper/{slice:016x}").into_bytes()
}

struct GuardedBlocks<B: WorkspaceKvBackend, O: ObjectBackend + Clone> {
    store: Arc<KvWorkspaceStore<Bounded<B>>>,
    inner: ObjectBlockStore<O>,
    cancel: CancellationToken,
    dispatched: AtomicUsize,
}
impl<B: WorkspaceKvBackend, O: ObjectBackend + Clone> GuardedBlocks<B, O> {
    async fn upper_bound_row(
        &self,
        slice: u64,
    ) -> anyhow::Result<(Vec<u8>, Option<Vec<u8>>, Option<u64>)> {
        if slice == 0 || self.cancel.is_cancelled() {
            anyhow::bail!("invalid/cancelled native GC slice bound");
        }
        let key = slice_upper_bound_key(slice);
        let (mut values, _) = self
            .store
            .backend
            .get_many_consistent_with_time_bounded(
                std::slice::from_ref(&key),
                KvReadLimits {
                    max_records: 1,
                    max_key_bytes: 128,
                    max_value_bytes: 256,
                    max_total_bytes: 4096,
                    max_response_bytes: 16 << 10,
                    max_data_requests: 1,
                },
            )
            .await?;
        if values.len() != 1 {
            anyhow::bail!("short native GC slice bound read");
        }
        let raw = values.remove(0);
        let end = if let Some(bytes) = &raw {
            let row: SliceUpperBound = decode_open_value(bytes, 256)?;
            if row.slice != slice || row.end == 0 {
                anyhow::bail!("native GC slice bound identity mismatch");
            }
            Some(row.end)
        } else {
            None
        };
        Ok((key, raw, end))
    }
}
#[async_trait]
impl<B: WorkspaceKvBackend, O: ObjectBackend + Clone + 'static> BlockStore for GuardedBlocks<B, O> {
    async fn write_fresh_range(&self, _: BlockKey, _: u64, _: &[u8]) -> anyhow::Result<u64> {
        anyhow::bail!("GC block client cannot write")
    }
    async fn read_range(&self, _: BlockKey, _: u64, _: &mut [u8]) -> anyhow::Result<()> {
        anyhow::bail!("GC block client cannot read data")
    }
    async fn retain_gc_slice_upper_bound(&self, slice: u64, end: u64) -> anyhow::Result<()> {
        if end == 0 {
            anyhow::bail!("native GC slice bound must be positive");
        }
        let (key, raw, old) = self.upper_bound_row(slice).await?;
        if old.is_some_and(|bound| bound >= end) {
            if !self
                .store
                .backend
                .compare_and_swap(&[KvCheck { key, expected: raw }], &[])
                .await?
            {
                anyhow::bail!("native GC retained slice bound changed");
            }
            return Ok(());
        }
        let bytes = encode(&SliceUpperBound { slice, end })?;
        // One metadata mutation only. An uncertain reply is conservatively
        // deferred; a later owner can authenticate the monotonic published row.
        if !self
            .store
            .backend
            .compare_and_swap(
                &[KvCheck {
                    key: key.clone(),
                    expected: raw,
                }],
                &[KvWrite::Put { key, value: bytes }],
            )
            .await?
        {
            anyhow::bail!("native GC slice bound publication changed");
        }
        Ok(())
    }
    async fn gc_slice_upper_bound(&self, slice: u64, observed: u64) -> anyhow::Result<u64> {
        let (key, raw, end) = self.upper_bound_row(slice).await?;
        let end = end
            .filter(|end| *end >= observed && observed > 0)
            .ok_or_else(|| anyhow::anyhow!("native GC slice bound was not durably retained"))?;
        if !self
            .store
            .backend
            .compare_and_swap(&[KvCheck { key, expected: raw }], &[])
            .await?
        {
            anyhow::bail!("native GC slice bound changed during authentication");
        }
        Ok(end)
    }
    async fn gc_slice_deletion_complete(
        &self,
        slice: u64,
        block_count: u64,
    ) -> anyhow::Result<bool> {
        if slice == 0 || block_count == 0 || self.cancel.is_cancelled() {
            return Ok(block_count == 0);
        }
        let progress_key = range_progress_key((slice, 0), block_count);
        let raw = self.store.backend.get(&progress_key).await?;
        let Some(raw) = raw else {
            return Ok(false);
        };
        let progress: RangeProgress = decode_open_value(&raw, 512)?;
        if progress.slice != slice || progress.first != 0 || progress.count != block_count {
            anyhow::bail!("native GC range progress identity mismatch");
        }
        if progress.next > block_count {
            anyhow::bail!("native GC range progress exceeds range");
        }
        Ok(progress.next == block_count)
    }
    async fn delete_range(&self, key: BlockKey, count: u64) -> anyhow::Result<()> {
        if count == 0 {
            return Ok(());
        }
        // ObjectBlockStore's exclusive range endpoint is a u32 as well.
        key.1
            .checked_add(u32::try_from(count)?)
            .ok_or_else(|| anyhow::anyhow!("native GC block span overflow"))?;
        let progress_key = range_progress_key(key, count);
        let mut expected_progress = self.store.backend.get(&progress_key).await?;
        let start = if let Some(raw) = &expected_progress {
            let progress: RangeProgress = decode_open_value(raw, 512)?;
            if progress.slice != key.0
                || progress.first != key.1
                || progress.count != count
                || progress.next > count
            {
                anyhow::bail!("native GC range progress identity mismatch");
            }
            progress.next
        } else {
            0
        };
        for offset in start..count {
            if self.cancel.is_cancelled() {
                anyhow::bail!("native GC cancelled before DELETE");
            }
            let block = key
                .1
                .checked_add(u32::try_from(offset)?)
                .ok_or_else(|| anyhow::anyhow!("native GC block overflow"))?;
            let target = (key.0, block);
            let row_key = block_delete_key(target);
            let complete = if let Some(raw) = self.store.backend.get(&row_key).await? {
                let row: BlockDelete = decode_open_value(&raw, 512)?;
                if row.slice != target.0 || row.block != target.1 {
                    anyhow::bail!("native GC DELETE identity mismatch");
                }
                if row.state != BlockDeleteState::Complete {
                    anyhow::bail!("native GC uncertain DELETE remains quarantined");
                }
                true
            } else {
                false
            };
            if !complete {
                if self.dispatched.fetch_add(1, Ordering::SeqCst) >= BLOCK_DELETE_QUOTA {
                    anyhow::bail!("native GC DELETE tick quota exhausted");
                }
                let row = encode(&BlockDelete {
                    slice: target.0,
                    block: target.1,
                    state: BlockDeleteState::Dispatched,
                })?;
                if !self
                    .store
                    .backend
                    .compare_and_swap(
                        &[KvCheck {
                            key: row_key.clone(),
                            expected: None,
                        }],
                        &[KvWrite::Put {
                            key: row_key.clone(),
                            value: row.clone(),
                        }],
                    )
                    .await?
                {
                    anyhow::bail!("native GC DELETE reservation changed");
                }
                // Reservation is conservative: cancellation/crash from here on
                // leaves Dispatched even if transport never started. No later
                // invocation can infer non-dispatch or replay the block pair.
                if self.cancel.is_cancelled() {
                    anyhow::bail!("native GC cancelled after reservation");
                }
                self.inner.delete_range(target, 1).await?;
                let completed = encode(&BlockDelete {
                    slice: target.0,
                    block: target.1,
                    state: BlockDeleteState::Complete,
                })?;
                if !self
                    .store
                    .backend
                    .compare_and_swap(
                        &[KvCheck {
                            key: row_key.clone(),
                            expected: Some(row),
                        }],
                        &[KvWrite::Put {
                            key: row_key,
                            value: completed,
                        }],
                    )
                    .await?
                {
                    anyhow::bail!("native GC DELETE completion changed");
                }
            }
            // Completion and progress are separate conservative CASes. A lost
            // progress reply can only revisit a confirmed Complete block; it
            // can never cause a second physical DELETE.
            let next_progress = encode(&RangeProgress {
                slice: key.0,
                first: key.1,
                count,
                next: offset + 1,
            })?;
            if !self
                .store
                .backend
                .compare_and_swap(
                    &[KvCheck {
                        key: progress_key.clone(),
                        expected: expected_progress.clone(),
                    }],
                    &[KvWrite::Put {
                        key: progress_key.clone(),
                        value: next_progress.clone(),
                    }],
                )
                .await?
            {
                anyhow::bail!("native GC range progress changed");
            }
            expected_progress = Some(next_progress);
        }
        Ok(())
    }
}

pub(super) async fn collect_one<B: WorkspaceKvBackend, O: ObjectBackend + Clone + 'static>(
    original: Arc<KvWorkspaceStore<B>>,
    client: ObjectClient<O>,
    budget: Arc<SnapshotMountBudget>,
    layout: ChunkLayout,
    policy: PackedGcPolicy,
    cancel: CancellationToken,
    target: LayerId,
) -> Result<u64, WorkspaceError> {
    let _owner = budget
        .admit(&[(SnapshotBudgetPool::Metadata, 128 << 20)])
        .map_err(journal_budget_error)?;
    if cancel.is_cancelled() || budget.state().closed {
        return Err(WorkspaceError::Busy);
    }
    let backend = Arc::new(Bounded {
        backend: original.backend.clone(),
        cancel: cancel.clone(),
        budget: budget.clone(),
        calls: AtomicUsize::new(0),
        rows: AtomicUsize::new(0),
        bytes: AtomicUsize::new(0),
    });
    let store = Arc::new(KvWorkspaceStore::from_arc(backend).with_packed_reader_pin_budget(budget));
    let scratch = tempfile::tempdir().map_err(journal_error)?;
    let blocks = ObjectBlockStore::new_with_configs_async(
        client,
        ChunksCacheConfig::with_budgets(0, 0, scratch.path().join("chunks")),
        BlockStoreConfig {
            block_size: layout.block_size as usize,
            page_cache_capacity: 0,
            range_background_prefetch: false,
            populate_write_cache_after_upload: false,
            persist_write_cache_after_upload: false,
            ..Default::default()
        },
    )
    .await
    .map_err(journal_error)?;
    let blocks = Arc::new(GuardedBlocks {
        store: store.clone(),
        inner: blocks,
        cancel: cancel.clone(),
        dispatched: AtomicUsize::new(0),
    });
    let now = store.backend.server_time_ns().await?;
    let collector = WorkspaceGc::new(
        store,
        blocks,
        layout,
        Duration::from_secs(policy.grace_seconds),
        Duration::from_secs(policy.grace_seconds),
    )
    .with_volume_format("workspace");
    // Discovery selects one route. The existing collector still authenticates
    // the complete root/shared-slice basis before touching that exact target.
    let result = collector.run_layer_at(now, target).await?;
    if cancel.is_cancelled() {
        return Err(WorkspaceError::Busy);
    }
    Ok(result.deleted_layers.len() as u64)
}

#[cfg(test)]
#[path = "native_small_catalog_tests.rs"]
mod small_catalog_tests;

#[cfg(test)]
mod tests {
    use super::super::tests::{Backend, Objects};
    use super::*;

    async fn blocks(
        backend: Arc<Backend>,
        objects: Objects,
    ) -> (GuardedBlocks<Backend, Objects>, tempfile::TempDir) {
        let budget = SnapshotMountBudget::defaults();
        let bounded = Arc::new(Bounded {
            backend,
            cancel: CancellationToken::new(),
            budget: budget.clone(),
            calls: AtomicUsize::new(0),
            rows: AtomicUsize::new(0),
            bytes: AtomicUsize::new(0),
        });
        let store =
            Arc::new(KvWorkspaceStore::from_arc(bounded).with_packed_reader_pin_budget(budget));
        let scratch = tempfile::tempdir().unwrap();
        let inner = ObjectBlockStore::new_with_configs_async(
            ObjectClient::new(objects),
            ChunksCacheConfig::with_budgets(0, 0, scratch.path().join("cache")),
            BlockStoreConfig {
                block_size: 1 << 20,
                page_cache_capacity: 0,
                range_background_prefetch: false,
                populate_write_cache_after_upload: false,
                persist_write_cache_after_upload: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        (
            GuardedBlocks {
                store,
                inner,
                cancel: CancellationToken::new(),
                dispatched: AtomicUsize::new(0),
            },
            scratch,
        )
    }

    #[tokio::test]
    async fn uncertain_native_delete_is_never_replayed_on_pickup() {
        let backend = Arc::new(Backend::default());
        let objects = Objects::default();
        objects.fail_delete.store(true, Ordering::SeqCst);
        let (first, _scratch) = blocks(backend.clone(), objects.clone()).await;
        assert!(first.delete_range((77, 0), 1).await.is_err());
        assert_eq!(objects.deletes.load(Ordering::SeqCst), 1);
        let row: BlockDelete =
            decode_open_value(&backend.rows.lock().await[&block_delete_key((77, 0))], 512).unwrap();
        assert!(row.state == BlockDeleteState::Dispatched);
        objects.fail_delete.store(false, Ordering::SeqCst);
        let (pickup, _scratch) = blocks(backend, objects.clone()).await;
        assert!(pickup.delete_range((77, 0), 1).await.is_err());
        assert_eq!(objects.deletes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn native_large_slice_progress_survives_fixed_dispatch_quota() {
        let backend = Arc::new(Backend::default());
        let objects = Objects::default();
        for (iteration, expected) in [(0, 64), (1, 128), (2, 130)] {
            let (scope, _scratch) = blocks(backend.clone(), objects.clone()).await;
            let result = scope.delete_range((91, 0), 65).await;
            if iteration < 2 {
                assert!(result.is_err());
            } else {
                result.unwrap();
            }
            // ObjectBlockStore deletes the versioned and legacy key once each.
            assert_eq!(objects.deletes.load(Ordering::SeqCst), expected);
        }
        let (pickup, _scratch) = blocks(backend.clone(), objects.clone()).await;
        pickup.delete_range((91, 0), 65).await.unwrap();
        assert_eq!(objects.deletes.load(Ordering::SeqCst), 130);
        let progress: RangeProgress = decode_open_value(
            &backend.rows.lock().await[&range_progress_key((91, 0), 65)],
            512,
        )
        .unwrap();
        assert_eq!(progress.next, 65);
    }

    #[tokio::test]
    async fn native_completed_record_advances_without_physical_delete() {
        let backend = Arc::new(Backend::default());
        let objects = Objects::default();
        backend.rows.lock().await.insert(
            block_delete_key((102, 0)),
            encode(&BlockDelete {
                slice: 102,
                block: 0,
                state: BlockDeleteState::Complete,
            })
            .unwrap(),
        );
        let (pickup, _scratch) = blocks(backend, objects.clone()).await;
        pickup.delete_range((102, 0), 1).await.unwrap();
        assert_eq!(objects.deletes.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn native_slice_range_upper_bound_survives_fresh_owner_and_never_shrinks() {
        let backend = Arc::new(Backend::default());
        let objects = Objects::default();
        let (first, _scratch) = blocks(backend.clone(), objects.clone()).await;
        first.retain_gc_slice_upper_bound(501, 8192).await.unwrap();
        drop(first);
        let (second, _scratch) = blocks(backend.clone(), objects.clone()).await;
        second.retain_gc_slice_upper_bound(501, 4096).await.unwrap();
        assert_eq!(second.gc_slice_upper_bound(501, 4096).await.unwrap(), 8192);
        second
            .retain_gc_slice_upper_bound(501, 16384)
            .await
            .unwrap();
        assert_eq!(second.gc_slice_upper_bound(501, 8192).await.unwrap(), 16384);
        assert_eq!(objects.deletes.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn native_slice_range_missing_wrong_identity_or_too_small_refuses_delete() {
        let backend = Arc::new(Backend::default());
        let objects = Objects::default();
        let (scope, _scratch) = blocks(backend.clone(), objects.clone()).await;
        assert!(scope.gc_slice_upper_bound(600, 4096).await.is_err());
        backend.rows.lock().await.insert(
            slice_upper_bound_key(600),
            encode(&SliceUpperBound {
                slice: 601,
                end: 8192,
            })
            .unwrap(),
        );
        assert!(scope.gc_slice_upper_bound(600, 4096).await.is_err());
        backend.rows.lock().await.insert(
            slice_upper_bound_key(600),
            encode(&SliceUpperBound {
                slice: 600,
                end: 2048,
            })
            .unwrap(),
        );
        assert!(scope.gc_slice_upper_bound(600, 4096).await.is_err());
        assert_eq!(objects.deletes.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn native_span_overflow_and_cancellation_never_reserve_or_delete() {
        let backend = Arc::new(Backend::default());
        let objects = Objects::default();
        let (scope, _scratch) = blocks(backend.clone(), objects.clone()).await;
        assert!(scope.delete_range((1, u32::MAX), 2).await.is_err());
        assert!(scope.delete_range((1, u32::MAX), 1).await.is_err());
        scope.cancel.cancel();
        assert!(scope.delete_range((1, 0), 1).await.is_err());
        assert!(backend.rows.lock().await.is_empty());
        assert_eq!(objects.deletes.load(Ordering::SeqCst), 0);
    }
}

#[cfg(test)]
#[path = "native_slice_resume_tests.rs"]
mod slice_resume_tests;

#[cfg(test)]
#[path = "native_slice_real_rustfs_tests.rs"]
mod slice_real_rustfs_tests;
