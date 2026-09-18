use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::chunk::compress::decompress_framed;
use crate::native_base::seal::{BlockBinding, NATIVE_LAYOUT_VERSIONED_FRAMED};
use crate::native_base::wire::bnct::ObjectRegistration;
use crate::native_base::wire::refs::ObjectId;
use crate::native_base::wire::uvarint::Reader;
use crate::native_base::write::commit::read_inode_view;
use crate::native_base::write::domain::decode_registration;
use crate::native_base::write::error::WriteError;
use crate::native_base::write::overlay::{
    DrainReport, MutationSpec, Ticket, WriteOverlay, ensure_workspace_head,
};
use crate::native_base::write::records::{ExtentKind, HeadPlacement};

#[async_trait]
pub trait BaseDataSource: Send + Sync {
    async fn size(&self, inode: u64) -> Result<u64, NativeIoError>;

    /// Fill the requested baseline range exactly. Missing or short data is an
    /// error; holes must be represented as explicit zero bytes by the source.
    async fn read_exact(
        &self,
        inode: u64,
        offset: u64,
        output: &mut [u8],
    ) -> Result<(), NativeIoError>;
}

#[derive(Default)]
pub struct ZeroBaseDataSource;

#[async_trait]
impl BaseDataSource for ZeroBaseDataSource {
    async fn size(&self, _inode: u64) -> Result<u64, NativeIoError> {
        Ok(0)
    }

    async fn read_exact(
        &self,
        _inode: u64,
        _offset: u64,
        output: &mut [u8],
    ) -> Result<(), NativeIoError> {
        output.fill(0);
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AcceptedWrite {
    pub ticket: u64,
    pub dirty_generation: u64,
    pub inode: u64,
    pub offset: u64,
    pub data: Arc<Vec<u8>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum PendingMutation {
    Write(AcceptedWrite),
    Truncate {
        ticket: u64,
        dirty_generation: u64,
        new_size: u64,
    },
    PunchHole {
        ticket: u64,
        dirty_generation: u64,
        offset: u64,
        len: u64,
        resulting_size: u64,
    },
}

impl PendingMutation {
    fn ticket(&self) -> u64 {
        match self {
            Self::Write(write) => write.ticket,
            Self::Truncate { ticket, .. } | Self::PunchHole { ticket, .. } => *ticket,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeWriteReceipt {
    pub admission_ticket: u64,
    pub dirty_generation: u64,
    pub accepted_len: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum NativeIoError {
    #[error("native I/O range overflows")]
    RangeOverflow,
    #[error("native I/O request is invalid: {0}")]
    Invalid(String),
    #[error("native base read failed: {0}")]
    Base(String),
    #[error("native object is missing: {0}")]
    MissingObject(String),
    #[error("native object integrity failure: {0}")]
    Integrity(String),
    #[error("native fsync failed: {0}")]
    Fsync(String),
    #[error(transparent)]
    Write(#[from] WriteError),
    #[error(transparent)]
    Store(#[from] crate::native_base::write::store::StoreError),
    #[error(transparent)]
    Wire(#[from] crate::native_base::wire::error::WireError),
}

#[derive(Default)]
struct RuntimeState {
    next_ticket: u64,
    next_dirty_generation: u64,
    pending: BTreeMap<u64, VecDeque<PendingMutation>>,
}

pub struct NativeDataRuntime {
    overlay: Arc<WriteOverlay>,
    base: Arc<dyn BaseDataSource>,
    state: Mutex<RuntimeState>,
    decoded_cache: Mutex<BTreeMap<ObjectId, Arc<Vec<u8>>>>,
}

impl NativeDataRuntime {
    pub fn new(overlay: Arc<WriteOverlay>, base: Arc<dyn BaseDataSource>) -> Self {
        Self {
            overlay,
            base,
            state: Mutex::new(RuntimeState::default()),
            decoded_cache: Mutex::new(BTreeMap::new()),
        }
    }

    pub async fn initialize(&self, owner_id: [u8; 16], epoch: u64) -> Result<(), NativeIoError> {
        self.overlay.ensure_domain(owner_id).await?;
        ensure_workspace_head(
            self.overlay.control_store(),
            self.overlay.keys(),
            self.overlay.params(),
            epoch,
        )
        .await?;
        Ok(())
    }

    async fn ensure_baseline_size(&self, inode: u64) -> Result<u64, NativeIoError> {
        let size = self.base.size(inode).await?;
        self.overlay.set_baseline_size(inode, size).await;
        Ok(size)
    }

    pub async fn write(
        &self,
        inode: u64,
        offset: u64,
        data: &[u8],
    ) -> Result<RuntimeWriteReceipt, NativeIoError> {
        if data.is_empty() {
            return Ok(RuntimeWriteReceipt {
                admission_ticket: 0,
                dirty_generation: 0,
                accepted_len: 0,
            });
        }
        offset
            .checked_add(data.len() as u64)
            .ok_or(NativeIoError::RangeOverflow)?;
        self.ensure_baseline_size(inode).await?;
        let mut state = self.state.lock().await;
        state.next_ticket = state
            .next_ticket
            .checked_add(1)
            .ok_or(NativeIoError::RangeOverflow)?;
        state.next_dirty_generation = state
            .next_dirty_generation
            .checked_add(1)
            .ok_or(NativeIoError::RangeOverflow)?;
        let accepted = AcceptedWrite {
            ticket: state.next_ticket,
            dirty_generation: state.next_dirty_generation,
            inode,
            offset,
            data: Arc::new(data.to_vec()),
        };
        let receipt = RuntimeWriteReceipt {
            admission_ticket: accepted.ticket,
            dirty_generation: accepted.dirty_generation,
            accepted_len: data.len(),
        };
        state
            .pending
            .entry(inode)
            .or_default()
            .push_back(PendingMutation::Write(accepted));
        Ok(receipt)
    }

    async fn accept_control(
        &self,
        inode: u64,
        mutation: impl FnOnce(u64, u64) -> PendingMutation,
    ) -> Result<RuntimeWriteReceipt, NativeIoError> {
        let mut state = self.state.lock().await;
        state.next_ticket = state
            .next_ticket
            .checked_add(1)
            .ok_or(NativeIoError::RangeOverflow)?;
        state.next_dirty_generation = state
            .next_dirty_generation
            .checked_add(1)
            .ok_or(NativeIoError::RangeOverflow)?;
        let ticket = state.next_ticket;
        let dirty_generation = state.next_dirty_generation;
        state
            .pending
            .entry(inode)
            .or_default()
            .push_back(mutation(ticket, dirty_generation));
        Ok(RuntimeWriteReceipt {
            admission_ticket: ticket,
            dirty_generation,
            accepted_len: 0,
        })
    }

    pub async fn truncate(
        &self,
        inode: u64,
        new_size: u64,
    ) -> Result<RuntimeWriteReceipt, NativeIoError> {
        self.ensure_baseline_size(inode).await?;
        self.accept_control(inode, |ticket, dirty_generation| {
            PendingMutation::Truncate {
                ticket,
                dirty_generation,
                new_size,
            }
        })
        .await
    }

    pub async fn discard(
        &self,
        inode: u64,
        offset: u64,
        len: u64,
        keep_size: bool,
    ) -> Result<RuntimeWriteReceipt, NativeIoError> {
        self.ensure_baseline_size(inode).await?;
        let resulting_size = offset
            .checked_add(len)
            .ok_or(NativeIoError::RangeOverflow)?;
        self.accept_control(inode, |ticket, dirty_generation| {
            PendingMutation::PunchHole {
                ticket,
                dirty_generation,
                offset,
                len,
                resulting_size: if keep_size { 0 } else { resulting_size },
            }
        })
        .await
    }

    pub async fn read(
        &self,
        inode: u64,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, NativeIoError> {
        let boundary = self.state.lock().await.next_ticket;
        self.read_at_boundary(inode, offset, len, boundary).await
    }

    pub async fn size(&self, inode: u64) -> Result<u64, NativeIoError> {
        let boundary = self.state.lock().await.next_ticket;
        self.effective_size_at(inode, boundary).await
    }

    async fn pending_through(&self, inode: u64, boundary: u64) -> Vec<PendingMutation> {
        self.state
            .lock()
            .await
            .pending
            .get(&inode)
            .into_iter()
            .flatten()
            .filter(|mutation| mutation.ticket() <= boundary)
            .cloned()
            .collect()
    }

    async fn effective_size_at(&self, inode: u64, boundary: u64) -> Result<u64, NativeIoError> {
        let base_size = self.base.size(inode).await?;
        let committed = read_inode_view(
            self.overlay.control_store(),
            self.overlay.keys(),
            &self.overlay.params().workspace_id,
            inode,
        )
        .await?;
        let mut size = if committed.had_record {
            committed.data.size
        } else {
            base_size
        };
        for mutation in self.pending_through(inode, boundary).await {
            match mutation {
                PendingMutation::Write(write) => {
                    size = size.max(
                        write
                            .offset
                            .checked_add(write.data.len() as u64)
                            .ok_or(NativeIoError::RangeOverflow)?,
                    );
                }
                PendingMutation::Truncate { new_size, .. } => size = new_size,
                PendingMutation::PunchHole { resulting_size, .. } => {
                    size = size.max(resulting_size)
                }
            }
        }
        Ok(size)
    }

    async fn read_at_boundary(
        &self,
        inode: u64,
        offset: u64,
        len: usize,
        boundary: u64,
    ) -> Result<Vec<u8>, NativeIoError> {
        let requested_end = offset
            .checked_add(len as u64)
            .ok_or(NativeIoError::RangeOverflow)?;
        let size = self.effective_size_at(inode, boundary).await?;
        if len == 0 || offset >= size {
            return Ok(Vec::new());
        }
        let end = requested_end.min(size);
        let mut output = vec![
            0;
            usize::try_from(end - offset).map_err(|_| {
                NativeIoError::Invalid("read result does not fit in address space".into())
            })?
        ];

        let base_size = self.base.size(inode).await?;
        let committed = read_inode_view(
            self.overlay.control_store(),
            self.overlay.keys(),
            &self.overlay.params().workspace_id,
            inode,
        )
        .await?;
        let baseline_visible_end = if committed.had_record {
            committed.data.size.min(base_size)
        } else {
            base_size
        };
        if offset < baseline_visible_end {
            let base_end = end.min(baseline_visible_end);
            let take =
                usize::try_from(base_end - offset).map_err(|_| NativeIoError::RangeOverflow)?;
            self.base
                .read_exact(inode, offset, &mut output[..take])
                .await?;
        }
        for (&extent_start, extent) in &committed.extents {
            let extent_end = extent_start
                .checked_add(extent.logical_len)
                .ok_or(NativeIoError::RangeOverflow)?;
            let start = extent_start.max(offset);
            let stop = extent_end.min(end);
            if start >= stop {
                continue;
            }
            let output_start = usize::try_from(start - offset).unwrap();
            let output_end = usize::try_from(stop - offset).unwrap();
            if extent.kind == ExtentKind::Hole {
                output[output_start..output_end].fill(0);
                continue;
            }
            self.read_data_extent(
                extent,
                extent_start,
                start,
                stop,
                &mut output[output_start..output_end],
            )
            .await?;
        }

        for mutation in self.pending_through(inode, boundary).await {
            match mutation {
                PendingMutation::Write(write) => {
                    let write_end = write
                        .offset
                        .checked_add(write.data.len() as u64)
                        .ok_or(NativeIoError::RangeOverflow)?;
                    let start = write.offset.max(offset);
                    let stop = write_end.min(end);
                    if start >= stop {
                        continue;
                    }
                    let source_start = usize::try_from(start - write.offset).unwrap();
                    let source_end = usize::try_from(stop - write.offset).unwrap();
                    let output_start = usize::try_from(start - offset).unwrap();
                    let output_end = usize::try_from(stop - offset).unwrap();
                    output[output_start..output_end]
                        .copy_from_slice(&write.data[source_start..source_end]);
                }
                PendingMutation::Truncate { new_size, .. } => {
                    if new_size < end {
                        let start = new_size.max(offset);
                        let output_start = usize::try_from(start - offset).unwrap();
                        output[output_start..].fill(0);
                    }
                }
                PendingMutation::PunchHole {
                    offset: hole_start,
                    len,
                    ..
                } => {
                    let hole_end = hole_start
                        .checked_add(len)
                        .ok_or(NativeIoError::RangeOverflow)?;
                    let start = hole_start.max(offset);
                    let stop = hole_end.min(end);
                    if start < stop {
                        let output_start = usize::try_from(start - offset).unwrap();
                        let output_end = usize::try_from(stop - offset).unwrap();
                        output[output_start..output_end].fill(0);
                    }
                }
            }
        }
        Ok(output)
    }

    async fn read_data_extent(
        &self,
        extent: &crate::native_base::write::records::NativeExtent,
        extent_start: u64,
        start: u64,
        end: u64,
        output: &mut [u8],
    ) -> Result<(), NativeIoError> {
        let block_size = self.overlay.params().block_size;
        let mut cursor = start;
        while cursor < end {
            let relative = cursor - extent_start;
            let block_in_extent = relative / block_size;
            let block_offset = relative % block_size;
            let block_index = extent
                .first_block
                .checked_add(block_in_extent)
                .ok_or(NativeIoError::RangeOverflow)?;
            let take = (end - cursor).min(block_size - block_offset);
            let decoded = self.load_loose_block(&extent.slice_id, block_index).await?;
            let source_start = usize::try_from(block_offset).unwrap();
            let source_end = source_start
                .checked_add(usize::try_from(take).unwrap())
                .ok_or(NativeIoError::RangeOverflow)?;
            if source_end > decoded.len() {
                return Err(NativeIoError::Integrity(format!(
                    "decoded block {block_index} is shorter than its logical range"
                )));
            }
            let target_start = usize::try_from(cursor - start).unwrap();
            let target_end = target_start + usize::try_from(take).unwrap();
            output[target_start..target_end].copy_from_slice(&decoded[source_start..source_end]);
            cursor += take;
        }
        Ok(())
    }

    async fn load_loose_block(
        &self,
        slice_id: &[u8; 16],
        block_index: u64,
    ) -> Result<Arc<Vec<u8>>, NativeIoError> {
        let keys = self.overlay.keys();
        let store = self.overlay.control_store();
        let binding_bytes = store
            .get(&keys.binding(slice_id, block_index))
            .await?
            .ok_or_else(|| NativeIoError::MissingObject("block binding".into()))?;
        let mut reader = Reader::new(&binding_bytes);
        let binding = BlockBinding::decode(&mut reader)?;
        if !reader.is_empty() {
            return Err(NativeIoError::Integrity(
                "block binding has trailing bytes".into(),
            ));
        }
        let placement_bytes = store
            .get(&keys.placement(slice_id, block_index))
            .await?
            .ok_or_else(|| NativeIoError::MissingObject("head placement".into()))?;
        let HeadPlacement::Loose {
            object_id,
            native_layout,
        } = HeadPlacement::decode(&placement_bytes)?;
        if native_layout != NATIVE_LAYOUT_VERSIONED_FRAMED {
            return Err(NativeIoError::Integrity(format!(
                "unsupported loose native layout {native_layout}"
            )));
        }
        if let Some(cached) = self.decoded_cache.lock().await.get(&object_id).cloned() {
            return Ok(cached);
        }
        let registration_bytes = store
            .get(&keys.object(&self.overlay.params().domain_id, &object_id))
            .await?
            .ok_or_else(|| NativeIoError::MissingObject("object registration".into()))?;
        let registration: ObjectRegistration = decode_registration(&registration_bytes)?;
        if registration.object_ref.object_id != object_id {
            return Err(NativeIoError::Integrity(
                "placement and registration object IDs differ".into(),
            ));
        }
        let encoded = self
            .overlay
            .object_sink()
            .get(&registration.object_ref)
            .await
            .map_err(|error| NativeIoError::MissingObject(error.to_string()))?;
        let decoded = decompress_framed(&encoded)
            .map_err(|error| NativeIoError::Integrity(error.to_string()))?
            .into_owned();
        if decoded.len() != binding.decoded_len as usize
            || <[u8; 32]>::from(Sha256::digest(&decoded)) != binding.content_hash
        {
            return Err(NativeIoError::Integrity(
                "decoded loose block does not match its binding".into(),
            ));
        }
        let decoded = Arc::new(decoded);
        self.decoded_cache
            .lock()
            .await
            .insert(object_id, decoded.clone());
        Ok(decoded)
    }

    pub async fn fsync(&self, inode: u64) -> Result<DrainReport, NativeIoError> {
        let boundary = self.state.lock().await.next_ticket;
        let pending = self.pending_through(inode, boundary).await;
        if pending.is_empty() {
            return Ok(DrainReport::default());
        }
        // A previous attempt may have left operations half-way (an upload
        // that failed after registration).  Re-drive them first: the retry
        // waits for the dependency chain instead of skipping it, and it
        // reuses the original tickets so per-inode order is preserved.
        self.overlay.retry_incomplete(inode).await?;
        let effective_size = self.effective_size_at(inode, boundary).await?;
        let block_size = self.overlay.params().block_size;

        // Collect write ranges for overlap detection.
        let mut write_ranges: Vec<(u64, u64)> = Vec::new();
        let mut write_blocks = BTreeSet::new();
        for mutation in &pending {
            if let PendingMutation::Write(write) = mutation {
                let end = write
                    .offset
                    .checked_add(write.data.len() as u64)
                    .ok_or(NativeIoError::RangeOverflow)?;
                write_ranges.push((write.offset, end));
                let first = write.offset / block_size;
                let last = (end - 1) / block_size;
                write_blocks.extend(first..=last);
            }
        }

        // Read the committed view to check whether the punch hole would
        // overlap any committed data extents.  The direct-hole path produces
        // native Hole extents via the commit planner, but punching into an
        // existing data extent would split it and leave sub-block-aligned
        // fragments that the read path cannot decode.  In that case we fall
        // back to block materialization.
        let committed_view = read_inode_view(
            self.overlay.control_store(),
            self.overlay.keys(),
            &self.overlay.params().workspace_id,
            inode,
        )
        .await?;
        let committed_data_ranges: Vec<(u64, u64)> = committed_view
            .extents
            .iter()
            .filter(|(_, ext)| ext.kind == ExtentKind::Data)
            .map(|(&off, ext)| (off, off + ext.logical_len))
            .collect();

        // Punch holes that don't overlap any pending write AND don't
        // overlap any committed data extent go directly to the commit
        // planner as MutationSpec::PunchHole, producing proper Hole extents
        // instead of zero-filled data blocks.  Holes that do overlap either
        // pending writes or committed data extents still go through block
        // materialization so last-writer-wins ordering is preserved and
        // sub-block-aligned extent fragments are avoided.
        let mut direct_holes: Vec<(u64, u64)> = Vec::new();
        for mutation in &pending {
            if let PendingMutation::PunchHole { offset, len, .. } = mutation {
                if *len == 0 {
                    continue;
                }
                let hole_end = offset
                    .checked_add(*len)
                    .ok_or(NativeIoError::RangeOverflow)?;
                let overlaps_pending = write_ranges
                    .iter()
                    .any(|&(w_start, w_end)| *offset < w_end && w_start < hole_end);
                let overlaps_committed = committed_data_ranges
                    .iter()
                    .any(|&(d_start, d_end)| *offset < d_end && d_start < hole_end);
                if overlaps_pending || overlaps_committed {
                    let first = *offset / block_size;
                    let last = (hole_end - 1) / block_size;
                    write_blocks.extend(first..=last);
                } else {
                    direct_holes.push((*offset, hole_end));
                }
            }
        }

        let mut report = DrainReport::default();

        // Commit non-overlapping punch holes directly as Hole extents.
        for (hole_offset, hole_end) in direct_holes {
            let ticket = self
                .overlay
                .accept(
                    inode,
                    MutationSpec::PunchHole {
                        offset: hole_offset,
                        len: hole_end - hole_offset,
                    },
                )
                .await?;
            self.commit_ticket(&ticket, &mut report).await?;
        }

        // Materialize blocks touched by writes (or holes overlapping writes).
        for block in write_blocks {
            let block_offset = block
                .checked_mul(block_size)
                .ok_or(NativeIoError::RangeOverflow)?;
            let mut data = self
                .read_at_boundary(inode, block_offset, block_size as usize, boundary)
                .await?;
            data.resize(block_size as usize, 0);
            let ticket = self
                .overlay
                .accept(
                    inode,
                    MutationSpec::Write {
                        offset: block_offset,
                        data: Arc::new(data),
                    },
                )
                .await?;
            self.commit_ticket(&ticket, &mut report).await?;
        }

        let committed = read_inode_view(
            self.overlay.control_store(),
            self.overlay.keys(),
            &self.overlay.params().workspace_id,
            inode,
        )
        .await?;
        if committed.data.size != effective_size {
            let ticket = self
                .overlay
                .accept(
                    inode,
                    MutationSpec::Truncate {
                        new_size: effective_size,
                    },
                )
                .await?;
            self.commit_ticket(&ticket, &mut report).await?;
        }

        // ORD-005: fsync may only consume its pending mutations once the
        // overlay has no unfinished work left.  If anything is still
        // accepted/uploaded the drain stopped early (for example behind an
        // incomplete predecessor), so report it instead of silently dropping
        // the write.
        let incomplete = self.overlay.incomplete_count(inode).await;
        if incomplete != 0 {
            return Err(NativeIoError::Fsync(format!(
                "{incomplete} native mutations remain uncommitted"
            )));
        }

        let mut state = self.state.lock().await;
        if let Some(writes) = state.pending.get_mut(&inode) {
            while writes
                .front()
                .is_some_and(|mutation| mutation.ticket() <= boundary)
            {
                writes.pop_front();
            }
            if writes.is_empty() {
                state.pending.remove(&inode);
            }
        }
        Ok(report)
    }

    async fn commit_ticket(
        &self,
        ticket: &Ticket,
        aggregate: &mut DrainReport,
    ) -> Result<(), NativeIoError> {
        self.overlay.dispatch(ticket).await?;
        self.overlay.complete_upload(ticket).await?;
        let mut report = self.overlay.drain().await?;
        if !report.failed.is_empty() || !report.blocked.is_empty() {
            return Err(NativeIoError::Fsync(format!(
                "{} failed and {} blocked native mutations",
                report.failed.len(),
                report.blocked.len()
            )));
        }
        aggregate.committed.append(&mut report.committed);
        aggregate.failed.append(&mut report.failed);
        aggregate.blocked.append(&mut report.blocked);
        Ok(())
    }

    pub async fn discard_dirty(&self, inode: u64) -> usize {
        self.state
            .lock()
            .await
            .pending
            .remove(&inode)
            .map_or(0, |writes| writes.len())
    }

    pub async fn pending_count(&self, inode: u64) -> usize {
        self.state
            .lock()
            .await
            .pending
            .get(&inode)
            .map_or(0, VecDeque::len)
    }

    pub async fn clear_cache(&self) {
        self.decoded_cache.lock().await.clear();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;
    use crate::native_base::write::memory::MemoryControlStore;
    use crate::native_base::write::overlay::OverlayParams;
    use crate::native_base::write::receipts::MemorySink;
    use crate::native_base::write::receipts::ObjectSink;

    /// A sink whose uploads can be failed on demand, to model an incomplete
    /// predecessor at an fsync boundary.
    #[derive(Default)]
    struct FlakySink {
        inner: MemorySink,
        fail_puts: std::sync::atomic::AtomicBool,
    }

    #[async_trait]
    impl ObjectSink for FlakySink {
        async fn put(
            &self,
            object: &crate::native_base::wire::refs::ObjectRef,
            bytes: &[u8],
        ) -> anyhow::Result<()> {
            if self.fail_puts.load(Ordering::Relaxed) {
                anyhow::bail!("injected upload failure");
            }
            self.inner.put(object, bytes).await
        }

        async fn get(
            &self,
            object: &crate::native_base::wire::refs::ObjectRef,
        ) -> anyhow::Result<Vec<u8>> {
            self.inner.get(object).await
        }
    }

    /// A sink that counts object traffic, so a metadata-only workload can be
    /// proven not to touch file data.
    #[derive(Default)]
    struct CountingSink {
        inner: MemorySink,
        puts: AtomicU64,
        gets: AtomicU64,
    }

    #[async_trait]
    impl ObjectSink for CountingSink {
        async fn put(
            &self,
            object: &crate::native_base::wire::refs::ObjectRef,
            bytes: &[u8],
        ) -> anyhow::Result<()> {
            self.puts.fetch_add(1, Ordering::Relaxed);
            self.inner.put(object, bytes).await
        }

        async fn get(
            &self,
            object: &crate::native_base::wire::refs::ObjectRef,
        ) -> anyhow::Result<Vec<u8>> {
            self.gets.fetch_add(1, Ordering::Relaxed);
            self.inner.get(object).await
        }
    }

    struct CountingBase {
        data: Vec<u8>,
        bytes_read: AtomicU64,
    }

    #[async_trait]
    impl BaseDataSource for CountingBase {
        async fn size(&self, _inode: u64) -> Result<u64, NativeIoError> {
            Ok(self.data.len() as u64)
        }

        async fn read_exact(
            &self,
            _inode: u64,
            offset: u64,
            output: &mut [u8],
        ) -> Result<(), NativeIoError> {
            let start = offset as usize;
            let end = start + output.len();
            let source = self
                .data
                .get(start..end)
                .ok_or_else(|| NativeIoError::Base("short baseline read".into()))?;
            output.copy_from_slice(source);
            self.bytes_read
                .fetch_add(output.len() as u64, Ordering::Relaxed);
            Ok(())
        }
    }

    async fn runtime(
        store: Arc<MemoryControlStore>,
        sink: Arc<dyn ObjectSink>,
        base: Arc<CountingBase>,
    ) -> NativeDataRuntime {
        let overlay = Arc::new(WriteOverlay::new(
            store,
            sink,
            OverlayParams {
                volume_id: [1; 16],
                workspace_id: [2; 16],
                domain_id: [3; 16],
                writer_generation: 1,
                block_size: 64,
            },
        ));
        let runtime = NativeDataRuntime::new(overlay, base);
        runtime.initialize([4; 16], 1).await.unwrap();
        runtime
    }

    #[tokio::test]
    async fn patch_is_dirty_visible_and_fsync_reads_only_the_touched_block() {
        let store = Arc::new(MemoryControlStore::new());
        let sink = Arc::new(MemorySink::default());
        let base = Arc::new(CountingBase {
            data: (0..=255).collect(),
            bytes_read: AtomicU64::new(0),
        });
        let runtime = runtime(store, sink, base.clone()).await;

        runtime.write(7, 70, b"PATCH").await.unwrap();
        assert_eq!(
            &runtime.read(7, 68, 10).await.unwrap(),
            &[68, 69, b'P', b'A', b'T', b'C', b'H', 75, 76, 77]
        );
        base.bytes_read.store(0, Ordering::Relaxed);
        runtime.fsync(7).await.unwrap();
        assert_eq!(base.bytes_read.load(Ordering::Relaxed), 64);
        assert_eq!(
            &runtime.read(7, 68, 10).await.unwrap(),
            &[68, 69, b'P', b'A', b'T', b'C', b'H', 75, 76, 77]
        );
    }

    /// OPT-004: a stat/readdir-only workload must not drag file data along.
    /// `size` is the metadata-only entry point behind getattr: it may read
    /// the control plane and the baseline size, but it must issue no object
    /// GET/PUT and copy no baseline bytes.
    #[tokio::test]
    async fn metadata_only_operations_never_fetch_file_data() {
        let store = Arc::new(MemoryControlStore::new());
        let sink = Arc::new(CountingSink::default());
        let base = Arc::new(CountingBase {
            data: vec![7u8; 4096 * 3],
            bytes_read: AtomicU64::new(0),
        });
        let runtime = runtime(store, sink.clone(), base.clone()).await;

        assert_eq!(runtime.size(41).await.unwrap(), 4096 * 3);
        assert_eq!(runtime.pending_count(41).await, 0);
        runtime.clear_cache().await;

        assert_eq!(sink.gets.load(Ordering::Relaxed), 0, "no object GET");
        assert_eq!(sink.puts.load(Ordering::Relaxed), 0, "no object PUT");
        assert_eq!(
            base.bytes_read.load(Ordering::Relaxed),
            0,
            "no baseline data copy"
        );
    }

    #[tokio::test]
    async fn cache_clear_and_restart_preserve_fsynced_patch() {
        let store = Arc::new(MemoryControlStore::new());
        let sink = Arc::new(MemorySink::default());
        let base = Arc::new(CountingBase {
            data: vec![b'x'; 160],
            bytes_read: AtomicU64::new(0),
        });
        let first = runtime(store.clone(), sink.clone(), base.clone()).await;
        first.write(9, 63, b"ABC").await.unwrap();
        first.fsync(9).await.unwrap();
        first.clear_cache().await;
        assert_eq!(&first.read(9, 61, 7).await.unwrap(), b"xxABCxx");

        let restarted = runtime(store, sink, base).await;
        assert_eq!(&restarted.read(9, 61, 7).await.unwrap(), b"xxABCxx");
    }

    #[tokio::test]
    async fn overlapping_dirty_writes_keep_acceptance_order_and_discard_is_private() {
        let store = Arc::new(MemoryControlStore::new());
        let sink = Arc::new(MemorySink::default());
        let base = Arc::new(CountingBase {
            data: vec![0; 128],
            bytes_read: AtomicU64::new(0),
        });
        let runtime = runtime(store, sink, base).await;
        let first = runtime.write(5, 10, b"AAAA").await.unwrap();
        let second = runtime.write(5, 10, b"BBBB").await.unwrap();
        assert!(first.admission_ticket < second.admission_ticket);
        assert_eq!(&runtime.read(5, 10, 4).await.unwrap(), b"BBBB");
        assert_eq!(runtime.discard_dirty(5).await, 2);
        assert_eq!(&runtime.read(5, 10, 4).await.unwrap(), &[0; 4]);
    }

    #[tokio::test]
    async fn mixed_baseline_loose_and_hole_read_matches_byte_oracle() {
        let store = Arc::new(MemoryControlStore::new());
        let sink = Arc::new(MemorySink::default());
        // The baseline represents the immutable packed snapshot. The
        // committed loose extent and hole below must override it only in
        // their exact ranges.
        let baseline: Vec<u8> = (0..=255).collect();
        let base = Arc::new(CountingBase {
            data: baseline.clone(),
            bytes_read: AtomicU64::new(0),
        });
        let runtime = runtime(store, sink, base).await;

        let patch: Vec<u8> = (0xa0..=0xdf).collect();
        runtime.write(11, 64, &patch).await.unwrap();
        runtime.fsync(11).await.unwrap();
        runtime.discard(11, 128, 64, true).await.unwrap();
        runtime.fsync(11).await.unwrap();
        let view = read_inode_view(
            runtime.overlay.control_store(),
            runtime.overlay.keys(),
            &runtime.overlay.params().workspace_id,
            11,
        )
        .await
        .unwrap();
        assert!(matches!(
            view.extents.get(&128).map(|extent| extent.kind),
            Some(ExtentKind::Hole)
        ));

        let mut expected = baseline;
        expected[64..128].copy_from_slice(&patch);
        expected[128..192].fill(0);
        let got = runtime.read(11, 0, expected.len()).await.unwrap();
        assert_eq!(got, expected);
    }

    #[tokio::test]
    async fn truncate_down_then_extend_does_not_resurrect_baseline_tail() {
        let store = Arc::new(MemoryControlStore::new());
        let sink = Arc::new(MemorySink::default());
        let baseline: Vec<u8> = (0..=255).collect();
        let base = Arc::new(CountingBase {
            data: baseline.clone(),
            bytes_read: AtomicU64::new(0),
        });
        let runtime = runtime(store, sink, base).await;

        runtime.truncate(12, 128).await.unwrap();
        runtime.fsync(12).await.unwrap();
        assert_eq!(runtime.read(12, 0, 256).await.unwrap(), baseline[..128]);

        runtime.truncate(12, 256).await.unwrap();
        runtime.fsync(12).await.unwrap();
        let mut expected = baseline[..128].to_vec();
        expected.resize(256, 0);
        assert_eq!(runtime.read(12, 0, 256).await.unwrap(), expected);
    }

    #[tokio::test]
    async fn punch_hole_on_fresh_inode_preserves_baseline_size_and_tail() {
        let store = Arc::new(MemoryControlStore::new());
        let sink = Arc::new(MemorySink::default());
        let baseline: Vec<u8> = (0..=255).collect();
        let base = Arc::new(CountingBase {
            data: baseline.clone(),
            bytes_read: AtomicU64::new(0),
        });
        let runtime = runtime(store, sink, base).await;

        runtime.discard(13, 64, 64, true).await.unwrap();
        runtime.fsync(13).await.unwrap();

        let mut expected = baseline;
        expected[64..128].fill(0);
        assert_eq!(runtime.read(13, 0, 256).await.unwrap(), expected);
        let view = read_inode_view(
            runtime.overlay.control_store(),
            runtime.overlay.keys(),
            &runtime.overlay.params().workspace_id,
            13,
        )
        .await
        .unwrap();
        assert_eq!(view.data.size, 256);
        assert!(matches!(
            view.extents.get(&64).map(|extent| extent.kind),
            Some(ExtentKind::Hole)
        ));
    }

    #[tokio::test]
    async fn truncate_then_far_write_keeps_the_truncated_gap_zeroed() {
        let store = Arc::new(MemoryControlStore::new());
        let sink = Arc::new(MemorySink::default());
        let baseline: Vec<u8> = (0..=255).collect();
        let base = Arc::new(CountingBase {
            data: baseline.clone(),
            bytes_read: AtomicU64::new(0),
        });
        let runtime = runtime(store, sink, base).await;

        runtime.truncate(14, 128).await.unwrap();
        runtime.fsync(14).await.unwrap();
        runtime.write(14, 192, &[0xa5; 64]).await.unwrap();
        runtime.fsync(14).await.unwrap();

        let mut expected = baseline[..128].to_vec();
        expected.extend_from_slice(&[0; 64]);
        expected.extend_from_slice(&[0xa5; 64]);
        assert_eq!(runtime.read(14, 0, 256).await.unwrap(), expected);
    }

    #[tokio::test]
    async fn cross_block_partial_write_roundtrips_byte_exact() {
        // WRITE-002: a write that straddles a native block boundary must
        // round-trip correctly.  With block_size=64, offset 60 + len 10
        // spans block 0 ([0..64]) and block 1 ([64..128]).
        let store = Arc::new(MemoryControlStore::new());
        let sink = Arc::new(MemorySink::default());
        let baseline: Vec<u8> = (0..=255).collect();
        let base = Arc::new(CountingBase {
            data: baseline.clone(),
            bytes_read: AtomicU64::new(0),
        });
        let runtime = runtime(store, sink, base).await;

        let patch: Vec<u8> = (0xa0..=0xa9).collect();
        runtime.write(21, 60, &patch).await.unwrap();
        // Dirty read must see the cross-block patch immediately.
        assert_eq!(
            &runtime.read(21, 58, 14).await.unwrap(),
            &[
                58, 59, 0xa0, 0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8, 0xa9, 70, 71
            ]
        );

        runtime.fsync(21).await.unwrap();
        // After fsync the committed view must match.
        assert_eq!(
            &runtime.read(21, 58, 14).await.unwrap(),
            &[
                58, 59, 0xa0, 0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8, 0xa9, 70, 71
            ]
        );

        // Full-file read matches oracle.
        let mut expected = baseline;
        expected[60..70].copy_from_slice(&patch);
        assert_eq!(runtime.read(21, 0, 256).await.unwrap(), expected);
    }

    #[tokio::test]
    async fn multi_extent_non_contiguous_writes_read_back_correctly() {
        // Two disjoint writes produce two extents; the gap between them
        // must still read from baseline.
        let store = Arc::new(MemoryControlStore::new());
        let sink = Arc::new(MemorySink::default());
        let baseline: Vec<u8> = (0..=255).collect();
        let base = Arc::new(CountingBase {
            data: baseline.clone(),
            bytes_read: AtomicU64::new(0),
        });
        let runtime = runtime(store, sink, base).await;

        runtime.write(22, 10, &[0xaa; 20]).await.unwrap();
        runtime.write(22, 200, &[0xbb; 30]).await.unwrap();
        runtime.fsync(22).await.unwrap();

        let mut expected = baseline;
        expected[10..30].fill(0xaa);
        expected[200..230].fill(0xbb);
        assert_eq!(runtime.read(22, 0, 256).await.unwrap(), expected);
    }

    #[tokio::test]
    async fn size_consistency_after_write_truncate_and_punch() {
        // CONS-001 (component-level): size() must stay consistent with
        // the committed extent set after every mutation.
        let store = Arc::new(MemoryControlStore::new());
        let sink = Arc::new(MemorySink::default());
        let baseline: Vec<u8> = (0..=255).collect();
        let base = Arc::new(CountingBase {
            data: baseline.clone(),
            bytes_read: AtomicU64::new(0),
        });
        let runtime = runtime(store, sink, base).await;

        // Fresh inode: size == baseline.
        assert_eq!(runtime.size(23).await.unwrap(), 256);

        // Write past end extends the logical size.
        runtime.write(23, 250, &[0xcc; 20]).await.unwrap();
        assert_eq!(runtime.size(23).await.unwrap(), 270);
        runtime.fsync(23).await.unwrap();
        assert_eq!(runtime.size(23).await.unwrap(), 270);

        // Truncate down shrinks it.
        runtime.truncate(23, 100).await.unwrap();
        assert_eq!(runtime.size(23).await.unwrap(), 100);
        runtime.fsync(23).await.unwrap();
        assert_eq!(runtime.size(23).await.unwrap(), 100);

        // Punch hole within size keeps size unchanged.
        runtime.discard(23, 40, 20, true).await.unwrap();
        assert_eq!(runtime.size(23).await.unwrap(), 100);
        runtime.fsync(23).await.unwrap();
        assert_eq!(runtime.size(23).await.unwrap(), 100);
    }

    #[tokio::test]
    async fn ord_005_incomplete_predecessor_fails_fsync_and_is_not_skipped() {
        // ORD-005 (INV-03/INV-06/INV-22): when an upload for a mutation at or
        // before the fsync boundary cannot complete, fsync must report the
        // error and keep every mutation pending.  It must never drop the
        // predecessor (or commit its successor ahead of it).
        let store = Arc::new(MemoryControlStore::new());
        let sink = Arc::new(FlakySink::default());
        let baseline: Vec<u8> = (0..=255).collect();
        let base = Arc::new(CountingBase {
            data: baseline.clone(),
            bytes_read: AtomicU64::new(0),
        });
        let runtime = runtime(store, sink.clone(), base).await;

        runtime.write(31, 70, b"PATCH").await.unwrap();
        runtime.write(31, 130, b"TAIL").await.unwrap();
        assert_eq!(runtime.pending_count(31).await, 2);

        sink.fail_puts.store(true, Ordering::Relaxed);
        assert!(
            runtime.fsync(31).await.is_err(),
            "an incomplete predecessor must surface an error"
        );
        sink.fail_puts.store(false, Ordering::Relaxed);

        // Nothing was committed and nothing was dropped: both mutations are
        // still pending, so a retry can complete the dependency chain.
        assert_eq!(
            runtime.pending_count(31).await,
            2,
            "the blocked mutations must stay pending, not be skipped"
        );
        // Dirty reads still see both pending mutations ...
        assert_eq!(
            &runtime.read(31, 68, 10).await.unwrap(),
            &[68, 69, b'P', b'A', b'T', b'C', b'H', 75, 76, 77]
        );
        assert_eq!(
            &runtime.read(31, 128, 6).await.unwrap(),
            &[128, 129, b'T', b'A', b'I', b'L']
        );
        // ... but nothing reached the committed view.
        let view = read_inode_view(
            runtime.overlay.control_store(),
            runtime.overlay.keys(),
            &runtime.overlay.params().workspace_id,
            31,
        )
        .await
        .unwrap();
        assert!(!view.had_record);

        // The retry commits both, in admission order, byte-exact.
        runtime.fsync(31).await.unwrap();
        assert_eq!(runtime.pending_count(31).await, 0);
        let mut expected = baseline;
        expected[70..75].copy_from_slice(b"PATCH");
        expected[130..134].copy_from_slice(b"TAIL");
        assert_eq!(runtime.read(31, 0, 256).await.unwrap(), expected);
    }

    #[tokio::test]
    async fn overlapping_write_hole_write_last_writer_wins() {
        // Write -> punch hole (overlap) -> write (overlap hole) pattern.
        // Final state: the last write wins over the hole range.
        let store = Arc::new(MemoryControlStore::new());
        let sink = Arc::new(MemorySink::default());
        let baseline: Vec<u8> = (0..=255).collect();
        let base = Arc::new(CountingBase {
            data: baseline.clone(),
            bytes_read: AtomicU64::new(0),
        });
        let runtime = runtime(store, sink, base).await;

        // First write fills [64..128] with 0xaa.
        runtime.write(24, 64, &[0xaa; 64]).await.unwrap();
        runtime.fsync(24).await.unwrap();

        // Punch hole [80..112] inside the written range.
        runtime.discard(24, 80, 32, true).await.unwrap();
        runtime.fsync(24).await.unwrap();

        // Second write [96..128] (overlaps tail of hole + post-hole area).
        runtime.write(24, 96, &[0xbb; 32]).await.unwrap();
        runtime.fsync(24).await.unwrap();

        let mut expected = baseline;
        expected[64..80].fill(0xaa); // first write, untouched
        expected[80..96].fill(0); // hole region (not overwritten by second write)
        expected[96..128].fill(0xbb); // second write wins
        assert_eq!(runtime.read(24, 0, 256).await.unwrap(), expected);
    }
}
