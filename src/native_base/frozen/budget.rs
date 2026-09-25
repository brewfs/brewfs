use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MetadataBudgetSnapshot {
    pub limit: usize,
    pub used: usize,
    pub reclaimable_cache: usize,
    pub pinned_metadata: usize,
    pub compressed_inflight: usize,
    pub decompression_workspace: usize,
    pub handles_and_inodes: usize,
    pub output_pages: usize,
    pub control: usize,
}

#[derive(Debug)]
struct BudgetState {
    limit: usize,
    used: AtomicUsize,
    reclaimable_cache: AtomicUsize,
    pinned_metadata: AtomicUsize,
    compressed_inflight: AtomicUsize,
    decompression_workspace: AtomicUsize,
    handles_and_inodes: AtomicUsize,
    output_pages: AtomicUsize,
    control: AtomicUsize,
}

#[derive(Clone, Debug)]
pub struct MetadataBudget {
    state: Arc<BudgetState>,
}

#[derive(Debug)]
pub struct BudgetReservation {
    state: Option<Arc<BudgetState>>,
    class: BudgetClass,
    bytes: usize,
}

#[derive(Clone, Copy, Debug)]
enum BudgetClass {
    ReclaimableCache,
    PinnedMetadata,
    CompressedInflight,
    DecompressionWorkspace,
    HandlesAndInodes,
    OutputPages,
    Control,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BudgetError {
    Exhausted { requested: usize, available: usize },
    ZeroReservation,
}

impl fmt::Display for BudgetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Exhausted {
                requested,
                available,
            } => write!(
                f,
                "metadata budget exhausted: requested {requested}, available {available}"
            ),
            Self::ZeroReservation => f.write_str("metadata reservation must be non-zero"),
        }
    }
}

impl std::error::Error for BudgetError {}

impl MetadataBudget {
    pub fn new(limit: usize) -> Self {
        Self {
            state: Arc::new(BudgetState {
                limit,
                used: AtomicUsize::new(0),
                reclaimable_cache: AtomicUsize::new(0),
                pinned_metadata: AtomicUsize::new(0),
                compressed_inflight: AtomicUsize::new(0),
                decompression_workspace: AtomicUsize::new(0),
                handles_and_inodes: AtomicUsize::new(0),
                output_pages: AtomicUsize::new(0),
                control: AtomicUsize::new(0),
            }),
        }
    }

    pub fn limit(&self) -> usize {
        self.state.limit
    }

    pub fn snapshot(&self) -> MetadataBudgetSnapshot {
        let state = &self.state;
        MetadataBudgetSnapshot {
            limit: state.limit,
            used: state.used.load(Ordering::Acquire),
            reclaimable_cache: state.reclaimable_cache.load(Ordering::Acquire),
            pinned_metadata: state.pinned_metadata.load(Ordering::Acquire),
            compressed_inflight: state.compressed_inflight.load(Ordering::Acquire),
            decompression_workspace: state.decompression_workspace.load(Ordering::Acquire),
            handles_and_inodes: state.handles_and_inodes.load(Ordering::Acquire),
            output_pages: state.output_pages.load(Ordering::Acquire),
            control: state.control.load(Ordering::Acquire),
        }
    }

    pub fn reserve_reclaimable_cache(
        &self,
        bytes: usize,
    ) -> Result<BudgetReservation, BudgetError> {
        self.reserve(BudgetClass::ReclaimableCache, bytes)
    }

    pub fn reserve_pinned_metadata(&self, bytes: usize) -> Result<BudgetReservation, BudgetError> {
        self.reserve(BudgetClass::PinnedMetadata, bytes)
    }

    pub fn reserve_compressed_inflight(
        &self,
        bytes: usize,
    ) -> Result<BudgetReservation, BudgetError> {
        self.reserve(BudgetClass::CompressedInflight, bytes)
    }

    pub fn reserve_decompression_workspace(
        &self,
        bytes: usize,
    ) -> Result<BudgetReservation, BudgetError> {
        self.reserve(BudgetClass::DecompressionWorkspace, bytes)
    }

    pub fn reserve_handles_and_inodes(
        &self,
        bytes: usize,
    ) -> Result<BudgetReservation, BudgetError> {
        self.reserve(BudgetClass::HandlesAndInodes, bytes)
    }

    pub fn reserve_output_page(&self, bytes: usize) -> Result<BudgetReservation, BudgetError> {
        self.reserve(BudgetClass::OutputPages, bytes)
    }

    pub fn reserve_control(&self, bytes: usize) -> Result<BudgetReservation, BudgetError> {
        self.reserve(BudgetClass::Control, bytes)
    }

    fn reserve(&self, class: BudgetClass, bytes: usize) -> Result<BudgetReservation, BudgetError> {
        if bytes == 0 {
            return Err(BudgetError::ZeroReservation);
        }
        let mut used = self.state.used.load(Ordering::Acquire);
        loop {
            let available = self.state.limit.saturating_sub(used);
            if bytes > available {
                return Err(BudgetError::Exhausted {
                    requested: bytes,
                    available,
                });
            }
            match self.state.used.compare_exchange_weak(
                used,
                used + bytes,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    class_counter(&self.state, class).fetch_add(bytes, Ordering::AcqRel);
                    return Ok(BudgetReservation {
                        state: Some(Arc::clone(&self.state)),
                        class,
                        bytes,
                    });
                }
                Err(current) => used = current,
            }
        }
    }
}

impl BudgetReservation {
    pub fn bytes(&self) -> usize {
        self.bytes
    }
}

impl Drop for BudgetReservation {
    fn drop(&mut self) {
        let Some(state) = self.state.take() else {
            return;
        };
        class_counter(&state, self.class).fetch_sub(self.bytes, Ordering::AcqRel);
        state.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

fn class_counter(state: &BudgetState, class: BudgetClass) -> &AtomicUsize {
    match class {
        BudgetClass::ReclaimableCache => &state.reclaimable_cache,
        BudgetClass::PinnedMetadata => &state.pinned_metadata,
        BudgetClass::CompressedInflight => &state.compressed_inflight,
        BudgetClass::DecompressionWorkspace => &state.decompression_workspace,
        BudgetClass::HandlesAndInodes => &state.handles_and_inodes,
        BudgetClass::OutputPages => &state.output_pages,
        BudgetClass::Control => &state.control,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_working_set_classes_share_one_hard_limit() {
        let budget = MetadataBudget::new(100);
        let cache = budget.reserve_reclaimable_cache(40).unwrap();
        let page = budget.reserve_output_page(30).unwrap();
        assert_eq!(budget.snapshot().used, 70);
        assert_eq!(budget.snapshot().reclaimable_cache, 40);
        assert_eq!(budget.snapshot().output_pages, 30);
        assert!(matches!(
            budget.reserve_pinned_metadata(31),
            Err(BudgetError::Exhausted { available: 30, .. })
        ));
        drop(page);
        assert_eq!(budget.snapshot().used, 40);
        drop(cache);
        assert_eq!(budget.snapshot().used, 0);
    }
}
