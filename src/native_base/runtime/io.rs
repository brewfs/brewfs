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
        let mut size = base_size.max(committed.data.size);
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
        if offset < base_size {
            let base_end = end.min(base_size);
            let take =
                usize::try_from(base_end - offset).map_err(|_| NativeIoError::RangeOverflow)?;
            self.base
                .read_exact(inode, offset, &mut output[..take])
                .await?;
        }

        let committed = read_inode_view(
            self.overlay.control_store(),
            self.overlay.keys(),
            &self.overlay.params().workspace_id,
            inode,
        )
        .await?;
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
        let effective_size = self.effective_size_at(inode, boundary).await?;
        let block_size = self.overlay.params().block_size;
        let mut blocks = BTreeSet::new();
        for mutation in &pending {
            if let PendingMutation::Write(write) = mutation {
                let end = write
                    .offset
                    .checked_add(write.data.len() as u64)
                    .ok_or(NativeIoError::RangeOverflow)?;
                let first = write.offset / block_size;
                let last = (end - 1) / block_size;
                blocks.extend(first..=last);
            }
            if let PendingMutation::PunchHole { offset, len, .. } = mutation {
                if *len != 0 {
                    let end = offset
                        .checked_add(*len)
                        .ok_or(NativeIoError::RangeOverflow)?;
                    blocks.extend((*offset / block_size)..=((end - 1) / block_size));
                }
            }
        }

        let mut report = DrainReport::default();
        for block in blocks {
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
        sink: Arc<MemorySink>,
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
}
