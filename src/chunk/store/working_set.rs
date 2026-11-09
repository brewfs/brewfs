//! Bounded working-set estimator for the read path.
//!
//! A speculative whole-block background prefetch only pays off when the block
//! is read again before it leaves the block cache. That keeps happening while
//! the working set fits in the cache and stops happening once it does not: the
//! prefetched block is evicted before anybody asks for it again, so the
//! download transfers a whole block to serve one page.
//!
//! The estimator approximates the working set of a random-access workload by
//! counting the distinct blocks the store recently had to fetch. It is a
//! lower bound: the store only knows the blocks it has already touched, so it
//! never suppresses a prefetch that the configured limit would still allow.

use crate::chunk::store::BlockKey;
use parking_lot::Mutex;
use std::collections::{HashSet, VecDeque};

/// Smallest tracked window.
///
/// The window must stay well above the number of blocks that fit in the block
/// cache, otherwise the estimate saturates below the configured limit and the
/// guard never fires.
const MIN_WINDOW_BLOCKS: usize = 1024;

/// Largest tracked window, bounding the estimator's own memory.
const MAX_WINDOW_BLOCKS: usize = 65536;

/// Number of blocks the estimator tracks for a given cache budget.
///
/// The guard compares the estimated working set against
/// `cache_budget * factor`. The estimate can only reach that limit if the
/// window holds at least that many blocks, so the window has to scale with
/// the cache budget. A fixed window would saturate below the limit for a
/// large cache, and the guard would never fire there.
///
/// The window is sized for the limit the factor produces, with headroom for
/// the factor itself, and is clamped to bound the estimator's memory.
pub(crate) fn window_blocks_for(cache_budget_bytes: u64, block_size: usize, factor: u32) -> usize {
    if block_size == 0 || factor == 0 {
        return MIN_WINDOW_BLOCKS;
    }
    let limit = cache_budget_bytes.saturating_mul(factor as u64);
    let blocks = limit / block_size as u64;
    // Round up so the estimate can actually reach the limit, and give the
    // window some headroom so the guard trips slightly before the limit
    // rather than exactly at it.
    let blocks = blocks.saturating_add(blocks / 4).saturating_add(1);
    usize::try_from(blocks)
        .unwrap_or(MAX_WINDOW_BLOCKS)
        .clamp(MIN_WINDOW_BLOCKS, MAX_WINDOW_BLOCKS)
}

/// Distinct blocks tracked over a sliding window.
pub(crate) struct WorkingSetEstimator {
    inner: Mutex<Window>,
    window_blocks: usize,
}

struct Window {
    /// Touch order, oldest first. The oldest entry leaves the window when the
    /// window overflows.
    order: VecDeque<BlockKey>,
    keys: HashSet<BlockKey>,
}

impl WorkingSetEstimator {
    /// Creates an estimator tracking at most `window_blocks` distinct blocks.
    pub(crate) fn new(window_blocks: usize) -> Self {
        Self {
            inner: Mutex::new(Window {
                order: VecDeque::new(),
                keys: HashSet::new(),
            }),
            window_blocks: window_blocks.clamp(MIN_WINDOW_BLOCKS, MAX_WINDOW_BLOCKS),
        }
    }

    /// Records a cold block touch.
    ///
    /// A block already inside the window does not change the estimate: the
    /// working set counts distinct blocks, not touches.
    pub(crate) fn touch(&self, key: BlockKey) {
        let mut window = self.inner.lock();
        if !window.keys.insert(key) {
            return;
        }
        window.order.push_back(key);
        while window.order.len() > self.window_blocks {
            if let Some(evicted) = window.order.pop_front() {
                window.keys.remove(&evicted);
            }
        }
    }

    /// Number of distinct blocks currently inside the window.
    pub(crate) fn distinct_blocks(&self) -> u64 {
        self.inner.lock().keys.len() as u64
    }

    /// Estimated working set of the tracked blocks, in bytes.
    pub(crate) fn estimated_bytes(&self, block_size: usize) -> u64 {
        self.distinct_blocks().saturating_mul(block_size as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeated_touches_of_one_block_count_once() {
        let estimator = WorkingSetEstimator::new(MIN_WINDOW_BLOCKS);
        estimator.touch((1, 0));
        estimator.touch((1, 0));
        estimator.touch((1, 0));
        assert_eq!(estimator.distinct_blocks(), 1);
        assert_eq!(estimator.estimated_bytes(4096), 4096);
    }

    #[test]
    fn the_window_forgets_the_oldest_blocks() {
        let estimator = WorkingSetEstimator::new(MIN_WINDOW_BLOCKS);
        for index in 0..MIN_WINDOW_BLOCKS as u64 + 10 {
            estimator.touch((index, 0));
        }
        assert_eq!(
            estimator.distinct_blocks(),
            MIN_WINDOW_BLOCKS as u64,
            "the estimate must saturate at the window size"
        );
    }

    #[test]
    fn the_window_size_is_clamped() {
        assert_eq!(WorkingSetEstimator::new(0).window_blocks, MIN_WINDOW_BLOCKS);
        assert_eq!(
            WorkingSetEstimator::new(usize::MAX).window_blocks,
            MAX_WINDOW_BLOCKS
        );
    }

    #[test]
    fn the_window_can_reach_the_guarded_limit() {
        let block_size = 4 << 20;
        // A large cache must still produce a window that holds enough blocks
        // for the estimate to reach `cache_budget * factor`. A fixed window
        // saturates below that limit, and the guard never fires.
        for cache_bytes in [64 << 20, 512 << 20, 8 << 30] {
            let factor = 4u32;
            let window = window_blocks_for(cache_bytes, block_size, factor);
            let limit_blocks = (cache_bytes as u128 * factor as u128) / block_size as u128;
            assert!(
                window as u128 > limit_blocks,
                "window {window} must exceed the {limit_blocks} blocks the limit needs \
                 for a {cache_bytes}-byte cache"
            );
            assert!(window <= MAX_WINDOW_BLOCKS);
        }
    }

    #[test]
    fn a_disabled_factor_keeps_the_smallest_window() {
        // Factor 0 disables the estimate, so the window size is irrelevant.
        assert_eq!(window_blocks_for(8 << 30, 4 << 20, 0), MIN_WINDOW_BLOCKS);
    }
}
