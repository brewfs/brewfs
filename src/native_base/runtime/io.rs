use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::chunk::compress::decompress_framed;
use crate::native_base::seal::{BlockBinding, NATIVE_LAYOUT_VERSIONED_FRAMED};
use crate::native_base::wire::bnct::ObjectRegistration;
use crate::native_base::wire::refs::ObjectId;
use crate::native_base::wire::uvarint::Reader;
use crate::native_base::write::commit::{InodeView, read_inode_view};
use crate::native_base::write::domain::decode_registration;
use crate::native_base::write::error::WriteError;
use crate::native_base::write::overlay::{
    DrainReport, MutationSpec, Ticket, WriteOverlay, ensure_workspace_head,
};
use crate::native_base::write::records::{ExtentKind, HeadPlacement};
use crate::native_base::write::store::ControlStore;

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
    /// The read metadata could not be pinned to one generation within the
    /// request's bounded budget (CONS-001/CONS-006).  The caller retries with
    /// a fresh request rather than serving a mixed or stale view.
    #[error("native read metadata is unstable: {0}")]
    MetadataUnstable(String),
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

/// How one read request bounds its metadata capture (CONS-006).
///
/// A capture re-reads the inode row before and after the extent scan and
/// retries while the two disagree, so a request never mixes an inode row from
/// one commit with extents from another.  The retry is bounded in attempts and
/// in wall-clock time per attempt: a metadata plane that never settles costs a
/// failed request, not a hung caller.
#[derive(Clone, Copy, Debug)]
pub struct MetadataCapturePolicy {
    /// Attempts per request, including the first.
    pub max_attempts: u32,
    /// Wall-clock bound on one attempt's metadata reads.
    pub attempt_timeout: Duration,
}

impl Default for MetadataCapturePolicy {
    fn default() -> Self {
        Self {
            max_attempts: 4,
            attempt_timeout: Duration::from_secs(2),
        }
    }
}

/// One consistent metadata generation for a single request (CONS-001,
/// CONS-002): everything the request serves comes from this snapshot.
struct MetadataSnapshot {
    pending: Vec<PendingMutation>,
    committed: InodeView,
}

pub struct NativeDataRuntime {
    overlay: Arc<WriteOverlay>,
    base: Arc<dyn BaseDataSource>,
    state: Mutex<RuntimeState>,
    decoded_cache: Mutex<BTreeMap<ObjectId, Arc<Vec<u8>>>>,
    capture_policy: MetadataCapturePolicy,
    /// Completed metadata captures: one per read/size/fsync request.
    captures: AtomicU64,
    /// Capture attempts, including the revalidated retries (CONS-006).
    capture_attempts: AtomicU64,
}

impl NativeDataRuntime {
    pub fn new(overlay: Arc<WriteOverlay>, base: Arc<dyn BaseDataSource>) -> Self {
        Self::with_capture_policy(overlay, base, MetadataCapturePolicy::default())
    }

    pub fn with_capture_policy(
        overlay: Arc<WriteOverlay>,
        base: Arc<dyn BaseDataSource>,
        capture_policy: MetadataCapturePolicy,
    ) -> Self {
        Self {
            overlay,
            base,
            state: Mutex::new(RuntimeState::default()),
            decoded_cache: Mutex::new(BTreeMap::new()),
            capture_policy,
            captures: AtomicU64::new(0),
            capture_attempts: AtomicU64::new(0),
        }
    }

    /// Completed metadata captures.  One read request -- however many blocks
    /// or extents it spans -- consumes exactly one capture (CONS-003).
    pub fn capture_count(&self) -> u64 {
        self.captures.load(Ordering::Relaxed)
    }

    /// Capture attempts, including revalidated retries.  A request whose
    /// metadata never settles stops after [`MetadataCapturePolicy::max_attempts`]
    /// (CONS-006).
    pub fn capture_attempt_count(&self) -> u64 {
        self.capture_attempts.load(Ordering::Relaxed)
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
        let boundary = self.capture_boundary().await;
        self.read_at_boundary(inode, offset, len, boundary).await
    }

    pub async fn size(&self, inode: u64) -> Result<u64, NativeIoError> {
        let boundary = self.capture_boundary().await;
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
        let snapshot = self.snapshot_metadata(inode, boundary).await?;
        effective_size(&snapshot, base_size)
    }

    /// Pin the request's generation: every mutation with a ticket up to this
    /// watermark belongs to the view the request serves, and everything after
    /// it is invisible to the request (CONS-001).
    async fn capture_boundary(&self) -> u64 {
        self.state.lock().await.next_ticket
    }

    /// Snapshot one metadata generation for `inode` (CONS-001/CONS-002).
    ///
    /// Order matters.  The pending set is taken **before** the committed view
    /// is read: a commit that lands after that point has already written its
    /// extents to the control plane, so the committed view read afterwards
    /// still serves its bytes.  The opposite order has a gap -- a commit
    /// landing between the two steps is neither pending (it was handed off)
    /// nor committed (the view was read before the transaction) -- and would
    /// serve stale bytes for a write that already returned to the client.
    ///
    /// The committed view itself is validated: the inode row is read before
    /// and after the extent scan and must be byte-identical, because every
    /// commit bumps it.  A difference means a commit landed inside the scan,
    /// so the pair would mix two generations; the request re-reads instead,
    /// bounded by [`MetadataCapturePolicy`].  The runtime's state lock is not
    /// held across these metadata reads, so a concurrent commit is never
    /// blocked behind a reader (CONS-006).
    async fn snapshot_metadata(
        &self,
        inode: u64,
        boundary: u64,
    ) -> Result<MetadataSnapshot, NativeIoError> {
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            self.capture_attempts.fetch_add(1, Ordering::Relaxed);
            let pending = self.pending_through(inode, boundary).await;
            match self.capture_stable_view(inode).await {
                Ok(Some(committed)) => {
                    self.captures.fetch_add(1, Ordering::Relaxed);
                    return Ok(MetadataSnapshot { pending, committed });
                }
                Ok(None) => {
                    if attempt >= self.capture_policy.max_attempts {
                        return Err(NativeIoError::MetadataUnstable(format!(
                            "inode {inode} changed while its view was captured, {attempt} attempts"
                        )));
                    }
                }
                Err(error) => {
                    // A timeout is retried; any other store failure is real and
                    // returned as-is.  Either way the retry budget is bounded.
                    let retryable = matches!(error, NativeIoError::MetadataUnstable(_));
                    if !retryable || attempt >= self.capture_policy.max_attempts {
                        return Err(error);
                    }
                }
            }
        }
    }

    /// Read `inode`'s committed view once, returning `None` when the inode row
    /// moved while the extents were scanned (a torn generation).
    async fn capture_stable_view(&self, inode: u64) -> Result<Option<InodeView>, NativeIoError> {
        let store = self.overlay.control_store();
        let keys = self.overlay.keys();
        let workspace = self.overlay.params().workspace_id;
        let ino_key = keys.inode(&workspace, inode);
        let before = self.timed_get(store, &ino_key, inode).await?;
        let view = self.timed_view(store, inode).await?;
        let after = self.timed_get(store, &ino_key, inode).await?;
        Ok((before == after).then_some(view))
    }

    async fn timed_get(
        &self,
        store: &dyn ControlStore,
        key: &[u8],
        inode: u64,
    ) -> Result<Option<Vec<u8>>, NativeIoError> {
        tokio::time::timeout(self.capture_policy.attempt_timeout, store.get(key))
            .await
            .map_err(|_| {
                NativeIoError::MetadataUnstable(format!("reading inode {inode} metadata timed out"))
            })?
            .map_err(NativeIoError::from)
    }

    async fn timed_view(
        &self,
        store: &dyn ControlStore,
        inode: u64,
    ) -> Result<InodeView, NativeIoError> {
        tokio::time::timeout(
            self.capture_policy.attempt_timeout,
            read_inode_view(
                store,
                self.overlay.keys(),
                &self.overlay.params().workspace_id,
                inode,
            ),
        )
        .await
        .map_err(|_| {
            NativeIoError::MetadataUnstable(format!("reading inode {inode} extents timed out"))
        })?
        .map_err(NativeIoError::from)
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
        let base_size = self.base.size(inode).await?;
        // One capture for the whole request: the size, the committed extents
        // and the pending overlay below all come from this generation
        // (CONS-001/CONS-003).
        let snapshot = self.snapshot_metadata(inode, boundary).await?;
        let committed = &snapshot.committed;
        let size = effective_size(&snapshot, base_size)?;
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

        for mutation in &snapshot.pending {
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
                    let new_size = *new_size;
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
                    let hole_start = *hole_start;
                    let len = *len;
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

/// The logical size one metadata snapshot implies: the committed size (or the
/// immutable baseline size when no native inode row exists yet) with every
/// pending mutation up to the request's boundary applied in acceptance order.
/// Size and extents therefore come from the same generation (CONS-001).
fn effective_size(snapshot: &MetadataSnapshot, base_size: u64) -> Result<u64, NativeIoError> {
    let committed = &snapshot.committed;
    let mut size = if committed.had_record {
        committed.data.size
    } else {
        base_size
    };
    for mutation in &snapshot.pending {
        match mutation {
            PendingMutation::Write(write) => {
                size = size.max(
                    write
                        .offset
                        .checked_add(write.data.len() as u64)
                        .ok_or(NativeIoError::RangeOverflow)?,
                );
            }
            PendingMutation::Truncate { new_size, .. } => size = *new_size,
            PendingMutation::PunchHole { resulting_size, .. } => size = size.max(*resulting_size),
        }
    }
    Ok(size)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use std::collections::BTreeMap;

    use sha2::{Digest, Sha256};

    use super::*;
    use crate::native_base::seal::builder::SealBuilder;
    use crate::native_base::seal::descriptor::FrameDescriptor;
    use crate::native_base::seal::placement::Span;
    use crate::native_base::seal::source::{ObjectSource, ObjectSourceError};
    use crate::native_base::seal::{SealReader, SealSnapshot};
    use crate::native_base::wire::container::ObjectKind;
    use crate::native_base::wire::datapack::{PackBuilder, PackFrame, ScrubbedPack};
    use crate::native_base::wire::refs::ObjectRef;
    use crate::native_base::write::memory::MemoryControlStore;
    use crate::native_base::write::overlay::OverlayParams;
    use crate::native_base::write::receipts::MemorySink;
    use crate::native_base::write::receipts::ObjectSink;
    use crate::native_base::write::store::{StoreError, Txn};

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
        store: Arc<dyn ControlStore>,
        sink: Arc<dyn ObjectSink>,
        base: Arc<dyn BaseDataSource>,
    ) -> NativeDataRuntime {
        runtime_with_policy(store, sink, base, MetadataCapturePolicy::default()).await
    }

    async fn runtime_with_policy(
        store: Arc<dyn ControlStore>,
        sink: Arc<dyn ObjectSink>,
        base: Arc<dyn BaseDataSource>,
        policy: MetadataCapturePolicy,
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
        let runtime = NativeDataRuntime::with_capture_policy(overlay, base, policy);
        runtime.initialize([4; 16], 1).await.unwrap();
        runtime
    }

    /// A store wrapper that observes and can interleave metadata traffic: it
    /// counts reads, can delay them beyond a capture attempt's timeout, can run
    /// a one-shot action *inside* an extent scan (so a test can land a
    /// concurrent commit between a capture's steps), and can keep an inode row
    /// moving to model a metadata plane that never settles.
    #[derive(Default)]
    struct ObservedStore {
        inner: MemoryControlStore,
        gets: AtomicU64,
        scans: AtomicU64,
        delay: tokio::sync::Mutex<Option<std::time::Duration>>,
        on_scan: tokio::sync::Mutex<Option<ScanHook>>,
        bump_inode_on_scan: tokio::sync::Mutex<Option<Vec<u8>>>,
    }

    struct ScanHook {
        reached: tokio::sync::mpsc::UnboundedSender<()>,
        proceed: tokio::sync::oneshot::Receiver<()>,
    }

    impl ObservedStore {
        fn new() -> Self {
            Self::default()
        }

        fn gets(&self) -> u64 {
            self.gets.load(Ordering::Relaxed)
        }

        fn scans(&self) -> u64 {
            self.scans.load(Ordering::Relaxed)
        }

        async fn set_delay(&self, delay: std::time::Duration) {
            *self.delay.lock().await = Some(delay);
        }

        async fn hook_next_scan(
            &self,
        ) -> (
            tokio::sync::mpsc::UnboundedReceiver<()>,
            tokio::sync::oneshot::Sender<()>,
        ) {
            let (reached_tx, reached_rx) = tokio::sync::mpsc::unbounded_channel();
            let (proceed_tx, proceed_rx) = tokio::sync::oneshot::channel();
            *self.on_scan.lock().await = Some(ScanHook {
                reached: reached_tx,
                proceed: proceed_rx,
            });
            (reached_rx, proceed_tx)
        }

        async fn bump_inode_row_on_scan(&self, key: Vec<u8>) {
            *self.bump_inode_on_scan.lock().await = Some(key);
        }
    }

    #[async_trait]
    impl ControlStore for ObservedStore {
        async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
            self.gets.fetch_add(1, Ordering::Relaxed);
            if let Some(delay) = *self.delay.lock().await {
                tokio::time::sleep(delay).await;
            }
            self.inner.get(key).await
        }

        async fn run(&self, txn: Txn) -> Result<(), StoreError> {
            self.inner.run(txn).await
        }

        async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StoreError> {
            self.scans.fetch_add(1, Ordering::Relaxed);
            let hook = self.on_scan.lock().await.take();
            if let Some(hook) = hook {
                let _ = hook.reached.send(());
                let _ = hook.proceed.await;
            }
            let rows = self.inner.scan(prefix).await?;
            let bump = self.bump_inode_on_scan.lock().await.clone();
            if let Some(key) = bump {
                if let Some(bytes) = self.inner.get(&key).await? {
                    let mut data = crate::native_base::write::records::InodeData::decode(&bytes)
                        .map_err(|error| StoreError::Backend(error.to_string()))?;
                    data.data_version += 1;
                    self.inner.run(Txn::new().put(key, data.encode())).await?;
                }
            }
            Ok(rows)
        }
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

    const PACKED_SLICE: u64 = 1;
    const PACKED_PACK_ID: [u8; 16] = [0x91; 16];

    /// An in-process object backend for the packed baseline fixture.
    #[derive(Default)]
    struct MapSource {
        objects: BTreeMap<[u8; 16], Vec<u8>>,
    }

    impl ObjectSource for MapSource {
        fn get_range(
            &self,
            object_id: &[u8; 16],
            start: u64,
            end: u64,
        ) -> Result<Vec<u8>, ObjectSourceError> {
            let bytes = self
                .objects
                .get(object_id)
                .ok_or(ObjectSourceError::NotFound)?;
            if start > end || end > bytes.len() as u64 {
                return Err(ObjectSourceError::ShortRead {
                    requested: end - start,
                    received: (bytes.len() as u64).saturating_sub(start),
                });
            }
            Ok(bytes[start as usize..end as usize].to_vec())
        }
    }

    /// A `BaseDataSource` whose baseline is a real packed `.brfds` seal read
    /// through the planned reader ? not a byte array.
    struct PackedSealBase {
        snapshot: SealSnapshot,
        source: MapSource,
        blocks: u32,
        block_size: u32,
    }

    #[async_trait]
    impl BaseDataSource for PackedSealBase {
        async fn size(&self, _inode: u64) -> Result<u64, NativeIoError> {
            Ok(u64::from(self.block_size) * u64::from(self.blocks))
        }

        async fn read_exact(
            &self,
            _inode: u64,
            offset: u64,
            output: &mut [u8],
        ) -> Result<(), NativeIoError> {
            let reader = SealReader::new(&self.snapshot, &self.source);
            let bytes = reader
                .read_range(PACKED_SLICE, offset, output.len() as u64, self.block_size)
                .map_err(|error| NativeIoError::Base(error.to_string()))?;
            output.copy_from_slice(&bytes);
            Ok(())
        }
    }

    /// Build one DataPack and a seal with one packed block per input block.
    fn packed_seal(blocks: &[Vec<u8>]) -> (Vec<u8>, Vec<u8>) {
        let block_size = blocks[0].len() as u32;
        let mut pack = PackBuilder::new();
        for block in blocks {
            pack.push(PackFrame::plain_bytes(block).unwrap());
        }
        let pack_bytes = pack.build().unwrap();
        let scrubbed = ScrubbedPack::scrub(&pack_bytes).unwrap();

        let mut seal = SealBuilder::new(block_size);
        let ordinal = seal.next_object_ordinal();
        seal.add_object(
            ordinal,
            ObjectRef {
                object_id: PACKED_PACK_ID,
                kind: ObjectKind::DataPack.as_u8(),
                object_len: pack_bytes.len() as u64,
                full_hash: Sha256::digest(&pack_bytes).into(),
                key: b"runtime/packed".to_vec(),
            },
        );
        let mut slots = Vec::new();
        for frame in &scrubbed.frames {
            let slot = seal.next_frame_slot();
            seal.add_frame(slot, FrameDescriptor::from_scrubbed(frame, ordinal));
            slots.push(slot);
        }
        for (index, block) in blocks.iter().enumerate() {
            seal.add_packed_block(
                PACKED_SLICE,
                index as u32,
                block,
                vec![Span {
                    block_offset: 0,
                    length: block.len() as u32,
                    frame_slot: slots[index],
                    frame_raw_offset: 0,
                }],
            )
            .unwrap();
        }
        seal.validate().unwrap();
        (seal.build().unwrap(), pack_bytes)
    }

    /// READ-001: one read that spans packed baseline blocks, a loose patch and
    /// a hole must match a byte-for-byte oracle.  The baseline here is a real
    /// `.brfds` pack read through the seal reader, not a byte array.
    #[tokio::test]
    async fn packed_baseline_loose_patch_and_hole_read_back_byte_exact() {
        let blocks: Vec<Vec<u8>> = (0..4u8).map(|seed| vec![0x10 + seed; 64]).collect();
        let (seal_bytes, pack_bytes) = packed_seal(&blocks);
        let mut source = MapSource::default();
        source.objects.insert(PACKED_PACK_ID, pack_bytes);
        let base = Arc::new(PackedSealBase {
            snapshot: SealSnapshot::open(seal_bytes).unwrap(),
            source,
            blocks: blocks.len() as u32,
            block_size: 64,
        });

        let store = Arc::new(MemoryControlStore::new());
        let sink = Arc::new(MemorySink::default());
        let runtime = runtime(store, sink, base).await;
        assert_eq!(runtime.size(9).await.unwrap(), 256);

        // A loose patch straddling the first block boundary, and a hole inside
        // the last block.
        runtime.write(9, 32, b"LOOSE").await.unwrap();
        runtime.discard(9, 200, 16, true).await.unwrap();
        runtime.fsync(9).await.unwrap();

        let mut oracle: Vec<u8> = blocks.concat();
        oracle[32..37].copy_from_slice(b"LOOSE");
        oracle[200..216].fill(0);

        assert_eq!(runtime.read(9, 0, 256).await.unwrap(), oracle);
        assert_eq!(
            runtime.read(9, 30, 8).await.unwrap(),
            oracle[30..38].to_vec()
        );
        assert_eq!(
            runtime.read(9, 198, 20).await.unwrap(),
            oracle[198..218].to_vec()
        );
    }

    // -----------------------------------------------------------------------
    // CONS-001/002/003/006: one request, one bounded and coherent capture.
    // -----------------------------------------------------------------------

    /// CONS-003: one read request reuses a single capture however many blocks
    /// or extents it spans, and the metadata cost does not grow with the chunk
    /// count.  A second syscall is a second capture: the runtime never claims
    /// more than one request's worth of atomicity.
    #[tokio::test]
    async fn one_read_request_reuses_a_single_bounded_capture() {
        let store = Arc::new(ObservedStore::new());
        let sink = Arc::new(MemorySink::default());
        let base = Arc::new(CountingBase {
            data: Vec::new(),
            bytes_read: AtomicU64::new(0),
        });
        let runtime = runtime(store.clone(), sink, base).await;

        // Three separate blocks committed as three data extents, so a
        // three-block read crosses chunk, block and extent boundaries.
        for block in 0..3u64 {
            let data = vec![0x40 + block as u8; 64];
            runtime.write(9, block * 64, &data).await.unwrap();
        }
        runtime.fsync(9).await.unwrap();
        assert_eq!(runtime.pending_count(9).await, 0);

        let captures = runtime.capture_count();
        let attempts = runtime.capture_attempt_count();
        let scans = store.scans();
        let gets = store.gets();
        let mut oracle = Vec::new();
        for block in 0..3u8 {
            oracle.extend(vec![0x40 + block; 64]);
        }
        assert_eq!(runtime.read(9, 0, 192).await.unwrap(), oracle);
        assert_eq!(
            runtime.capture_count(),
            captures + 1,
            "one capture serves the whole request"
        );
        assert_eq!(
            runtime.capture_attempt_count(),
            attempts + 1,
            "a settled metadata plane costs one attempt"
        );
        let request_scans = store.scans() - scans;
        // The capture's own metadata reads are a fixed three gets (inode row,
        // view, inode row again); the remaining gets are per-block object
        // reads, which the decoded cache amortizes across requests.
        assert_eq!(
            store.gets() - gets,
            3 + 3 * 3,
            "one block's object read each"
        );

        // The same metadata cost for a one-block read: the capture is bounded
        // by the request, not by the bytes the request spans.
        let scans = store.scans();
        runtime.read(9, 0, 64).await.unwrap();
        assert_eq!(store.scans() - scans, request_scans);
        assert_eq!(request_scans, 1, "{request_scans} extent scans per request");

        // Two requests are two captures: nothing here claims that two system
        // calls observe one generation.
        let captures = runtime.capture_count();
        runtime.read(9, 0, 64).await.unwrap();
        runtime.read(9, 0, 64).await.unwrap();
        assert_eq!(runtime.capture_count(), captures + 2);
    }

    /// CONS-001/CONS-002: a commit that lands *inside* a request's capture --
    /// while its extent scan is in flight -- is absorbed without a stale read
    /// and without a gap.  The commit also proves the reader holds no gate
    /// across metadata I/O: a lock held across the scan would block the commit
    /// and hang this test instead of failing it.
    #[tokio::test]
    async fn a_commit_inside_a_read_capture_is_never_served_stale() {
        let store = Arc::new(ObservedStore::new());
        let sink = Arc::new(MemorySink::default());
        let base = Arc::new(CountingBase {
            data: Vec::new(),
            bytes_read: AtomicU64::new(0),
        });
        let runtime = Arc::new(runtime(store.clone(), sink, base).await);

        let (mut reached, proceed) = store.hook_next_scan().await;
        let writer = runtime.clone();
        let commit = tokio::spawn(async move {
            // Wait until the reader is inside its extent scan, then commit the
            // accepted write: the store rows the reader is about to receive
            // already contain it.
            reached
                .recv()
                .await
                .expect("the reader reaches its extent scan");
            writer.fsync(9).await.unwrap();
            let _ = proceed.send(());
        });

        let attempts = runtime.capture_attempt_count();
        let captured = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            runtime.write(9, 0, &vec![0xaa; 64]).await.unwrap();
            runtime.read(9, 0, 64).await.unwrap()
        })
        .await
        .expect("the reader completes with the commit interleaved");
        commit.await.unwrap();

        assert_eq!(
            captured,
            vec![0xaa; 64],
            "the write's bytes are never hidden by the handoff"
        );
        assert!(
            runtime.capture_attempt_count() - attempts >= 2,
            "the capture revalidated the generation the commit moved"
        );
        assert_eq!(runtime.pending_count(9).await, 0);
        // A later request sees the same content.
        assert_eq!(runtime.read(9, 0, 64).await.unwrap(), vec![0xaa; 64]);
    }

    /// CONS-006: a metadata plane whose inode row keeps moving is retried only
    /// within the request's bounded budget and then fails, instead of serving
    /// a torn view or looping forever.
    #[tokio::test]
    async fn an_inode_row_that_never_settles_fails_within_the_bounded_budget() {
        let store = Arc::new(ObservedStore::new());
        let sink = Arc::new(MemorySink::default());
        let base = Arc::new(CountingBase {
            data: Vec::new(),
            bytes_read: AtomicU64::new(0),
        });
        let policy = MetadataCapturePolicy {
            max_attempts: 3,
            attempt_timeout: std::time::Duration::from_secs(2),
        };
        let runtime = runtime_with_policy(store.clone(), sink, base.clone(), policy).await;

        // Create the inode row, then make every extent scan move it.
        runtime.write(9, 0, &vec![1u8; 64]).await.unwrap();
        runtime.fsync(9).await.unwrap();
        store
            .bump_inode_row_on_scan(runtime64_key(&runtime, 9))
            .await;

        let attempts = runtime.capture_attempt_count();
        let captures = runtime.capture_count();
        let error = runtime.read(9, 0, 64).await.expect_err("unstable metadata");
        assert!(
            matches!(error, NativeIoError::MetadataUnstable(_)),
            "unexpected error: {error}"
        );
        assert_eq!(
            runtime.capture_attempt_count() - attempts,
            3,
            "the retry budget is attempts, not time"
        );
        assert_eq!(
            runtime.capture_count(),
            captures,
            "no capture completes without a stable generation"
        );
    }

    /// CONS-006: a metadata read that exceeds the attempt timeout is retried
    /// within the same bounded budget and then fails; a slow metadata plane
    /// never turns into an unbounded wait.
    #[tokio::test]
    async fn a_metadata_read_beyond_the_attempt_timeout_is_bounded() {
        let store = Arc::new(ObservedStore::new());
        let sink = Arc::new(MemorySink::default());
        let base = Arc::new(CountingBase {
            data: Vec::new(),
            bytes_read: AtomicU64::new(0),
        });
        let policy = MetadataCapturePolicy {
            max_attempts: 2,
            attempt_timeout: std::time::Duration::from_millis(20),
        };
        let runtime = runtime_with_policy(store.clone(), sink, base, policy).await;
        store.set_delay(std::time::Duration::from_millis(200)).await;

        let started = std::time::Instant::now();
        let error = runtime
            .read(9, 0, 64)
            .await
            .expect_err("a slow metadata plane fails the request");
        assert!(
            matches!(error, NativeIoError::MetadataUnstable(_)),
            "unexpected error: {error}"
        );
        assert!(
            started.elapsed() < std::time::Duration::from_secs(2),
            "the request is bounded by the attempt budget, not by the backend"
        );
    }

    fn runtime64_key(runtime: &NativeDataRuntime, inode: u64) -> Vec<u8> {
        runtime
            .overlay
            .keys()
            .inode(&runtime.overlay.params().workspace_id, inode)
    }

    /// A store that can apply a commit transaction and then report the reply as
    /// lost, which is the only failure mode a commit may legitimately retry.
    #[derive(Default)]
    struct LostReplyStore {
        inner: MemoryControlStore,
        lose_next_commit: std::sync::atomic::AtomicBool,
    }

    #[async_trait]
    impl ControlStore for LostReplyStore {
        async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
            self.inner.get(key).await
        }

        async fn run(&self, txn: Txn) -> Result<(), StoreError> {
            let is_commit = txn
                .writes
                .iter()
                .any(|(key, _)| key.windows(4).any(|w| w == b"mut/"));
            let outcome = self.inner.run(txn).await;
            if outcome.is_ok() && is_commit && self.lose_next_commit.swap(false, Ordering::SeqCst) {
                // The transaction applied; the caller just never saw the reply.
                return Err(StoreError::Conflict);
            }
            outcome
        }

        async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StoreError> {
            self.inner.scan(prefix).await
        }
    }

    /// CONS-002: the dirty->committed handoff is confirmed before it happens.
    /// When the commit transaction applies but its reply is lost, the retry
    /// answers from the recorded result (`AlreadyCommitted`), the pending
    /// overlay is released exactly then, and the bytes a reader sees never
    /// vanish in the window between the two.
    #[tokio::test]
    async fn a_lost_commit_reply_hands_off_without_a_gap() {
        let store = Arc::new(LostReplyStore::default());
        let sink = Arc::new(MemorySink::default());
        let base = Arc::new(CountingBase {
            data: Vec::new(),
            bytes_read: AtomicU64::new(0),
        });
        let runtime = runtime(store.clone(), sink, base).await;
        let written = vec![0xaa; 64];

        runtime.write(9, 0, &written).await.unwrap();
        assert_eq!(
            runtime.pending_count(9).await,
            1,
            "the write is still dirty"
        );
        store
            .lose_next_commit
            .store(true, std::sync::atomic::Ordering::SeqCst);

        let report = runtime
            .fsync(9)
            .await
            .expect("a lost reply is retried, not surfaced as a failure");
        assert_eq!(report.committed.len(), 1);
        assert_eq!(
            runtime.pending_count(9).await,
            0,
            "the handoff happens only once the commit is confirmed"
        );
        // The reader saw the dirty bytes before the handoff and the committed
        // bytes after it: the same content, with no window in between.
        assert_eq!(runtime.read(9, 0, 64).await.unwrap(), written);
        assert_eq!(runtime.size(9).await.unwrap(), 64);
    }
}
