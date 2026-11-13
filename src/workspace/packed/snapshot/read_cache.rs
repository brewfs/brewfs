//! Retained authenticated metadata with exact LRU ownership and view fencing.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use super::{SnapshotBudgetPool, SnapshotMountBudget, SnapshotOwnedPermit};
use crate::chunk::read_plan::ReadGeneration;
use crate::workspace::packed::wire::{PackedResult, PackedWireError};

// Covers the fixed digest key, BTree node, Arc and entry bookkeeping. The
// decoded value independently retains its original mount budget permit.
const ENTRY_BYTES: u64 = 512;
const MAX_FILLS: usize = 8;
// Covers even the first BTree leaf's spare key/value slots, plus the Fill,
// Arc, mutex and permit. The decoded value retains its own payload permit.
const FILL_BYTES: u64 = 1024;

struct Fill<T> {
    value: tokio::sync::Mutex<Option<Arc<T>>>,
    _permit: SnapshotOwnedPermit,
}

type InFlightFills<T> = BTreeMap<(u64, [u8; 32]), Arc<Fill<T>>>;

struct FillGuard<'a, T> {
    cache: &'a RetainedMetadata<T>,
    key: (u64, [u8; 32]),
    fill: Option<Arc<Fill<T>>>,
}

impl<T> Drop for FillGuard<'_, T> {
    fn drop(&mut self) {
        let mut fills = self.cache.fills.lock().unwrap();
        // The map and this guard are the final owners. Waiting followers also
        // own guards, so a non-retained result survives until they consume it.
        let fill = self.fill.take().unwrap();
        if Arc::strong_count(&fill) == 2 {
            fills.remove(&self.key);
            // Release the final allocation/permit before waking a contender
            // whose Control budget may have room for exactly one fill.
            drop(fill);
            self.cache.fill_changed.notify_waiters();
        }
    }
}

struct Entry<T> {
    value: Arc<T>,
    bytes: u64,
    accessed: u64,
    _permit: SnapshotOwnedPermit,
}

struct State<T> {
    generation: Option<ReadGeneration>,
    epoch: u64,
    clock: u64,
    bytes: u64,
    entries: BTreeMap<[u8; 32], Entry<T>>,
}

/// Eviction drops only the cache reference. A caller holding the value keeps
/// its original decoded allocation charged until the last consumer leaves.
pub(super) struct RetainedMetadata<T> {
    capacity: u64,
    budget: Arc<SnapshotMountBudget>,
    state: Mutex<State<T>>,
    fills: Mutex<InFlightFills<T>>,
    fill_changed: tokio::sync::Notify,
}

impl<T> RetainedMetadata<T> {
    pub(super) fn new(capacity: u64, budget: Arc<SnapshotMountBudget>) -> Self {
        Self {
            capacity,
            state: Mutex::new(State {
                generation: None,
                epoch: budget.read_cache_epoch(),
                clock: 0,
                bytes: 0,
                entries: BTreeMap::new(),
            }),
            budget,
            fills: Mutex::new(BTreeMap::new()),
            fill_changed: tokio::sync::Notify::new(),
        }
    }

    async fn fill(&self, epoch: u64, key: [u8; 32]) -> PackedResult<FillGuard<'_, T>> {
        loop {
            let changed = self.fill_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            self.check_epoch(epoch)?;
            {
                let mut fills = self.fills.lock().unwrap();
                let key = (epoch, key);
                if let Some(fill) = fills.get(&key) {
                    return Ok(FillGuard {
                        cache: self,
                        key,
                        fill: Some(fill.clone()),
                    });
                }
                if fills.len() < MAX_FILLS {
                    let permit = self
                        .budget
                        .admit(&[(SnapshotBudgetPool::Control, FILL_BYTES)])?;
                    let fill = Arc::new(Fill {
                        value: tokio::sync::Mutex::new(None),
                        _permit: permit,
                    });
                    fills.insert(key, fill.clone());
                    return Ok(FillGuard {
                        cache: self,
                        key,
                        fill: Some(fill),
                    });
                }
            }
            tokio::select! {
                () = changed => {},
                () = self.budget.wait_read_cache_epoch_changed(epoch) => {
                    return Err(PackedWireError::ReadViewChanged);
                }
            }
        }
    }

    pub(super) fn cached(
        &self,
        generation: ReadGeneration,
        key: &[u8; 32],
    ) -> PackedResult<Option<Arc<T>>> {
        let epoch = self.prepare(generation)?;
        let value = self.get(key, epoch);
        self.check_epoch(epoch)?;
        Ok(value)
    }

    fn prepare(&self, generation: ReadGeneration) -> PackedResult<u64> {
        if self.budget.state().closed {
            self.clear();
            return Err(PackedWireError::ReadViewChanged);
        }
        let mut state = self.state.lock().unwrap();
        if state.generation.is_some_and(|old| old != generation) {
            // A changed view fences all retained metadata sharing this mount,
            // including a fetch that is still awaiting the old backend body.
            self.budget.invalidate_read_caches();
        }
        let epoch = self.budget.read_cache_epoch();
        if state.generation != Some(generation) || state.epoch != epoch {
            state.entries.clear();
            state.bytes = 0;
            state.generation = Some(generation);
            state.epoch = epoch;
        }
        Ok(epoch)
    }

    fn get(&self, key: &[u8; 32], epoch: u64) -> Option<Arc<T>> {
        let mut state = self.state.lock().unwrap();
        let current_epoch = self.budget.read_cache_epoch();
        if state.epoch != current_epoch {
            state.entries.clear();
            state.bytes = 0;
        }
        if state.epoch != epoch || current_epoch != epoch {
            return None;
        }
        state.clock = state.clock.wrapping_add(1);
        let clock = state.clock;
        let entry = state.entries.get_mut(key)?;
        entry.accessed = clock;
        Some(entry.value.clone())
    }

    pub(super) async fn read(
        &self,
        generation: ReadGeneration,
        key: [u8; 32],
        weight: impl FnOnce(&T) -> u64,
        fetch: impl std::future::Future<Output = PackedResult<Arc<T>>>,
    ) -> PackedResult<Arc<T>> {
        let epoch = self.prepare(generation)?;
        if self.capacity == 0 {
            let value = fetch.await?;
            self.check_epoch(epoch)?;
            return Ok(value);
        }
        if let Some(value) = self.get(&key, epoch) {
            self.check_epoch(epoch)?;
            return Ok(value);
        }
        // Only identical epoch/digest pairs wait on each other. The map is
        // bounded, its owners are charged, and no registry lock spans I/O.
        let fill = self.fill(epoch, key).await?;
        let mut completed = tokio::select! {
            completed = fill.fill.as_ref().unwrap().value.lock() => completed,
            () = self.budget.wait_read_cache_epoch_changed(epoch) => {
                return Err(PackedWireError::ReadViewChanged);
            }
        };
        self.check_epoch(epoch)?;
        if let Some(value) = completed.as_ref() {
            return Ok(value.clone());
        }
        if let Some(value) = self.get(&key, epoch) {
            self.check_epoch(epoch)?;
            return Ok(value);
        }
        let value = tokio::select! {
            value = fetch => value?,
            () = self.budget.wait_read_cache_epoch_changed(epoch) => {
                return Err(PackedWireError::ReadViewChanged);
            }
        };
        let bytes = weight(&value).saturating_add(ENTRY_BYTES);
        let mut state = self.state.lock().unwrap();
        if state.epoch != epoch || self.budget.read_cache_epoch() != epoch {
            return Err(PackedWireError::ReadViewChanged);
        }
        if bytes <= self.capacity {
            while state.bytes.saturating_add(bytes) > self.capacity {
                let Some(oldest) = state
                    .entries
                    .iter()
                    .min_by_key(|(_, entry)| entry.accessed)
                    .map(|(key, _)| *key)
                else {
                    break;
                };
                let removed = state.entries.remove(&oldest).unwrap();
                state.bytes -= removed.bytes;
            }
            // Ordinary retention pressure must not fail a valid read. All
            // actual allocations remain covered by the decoded value owner.
            if let Ok(permit) = self
                .budget
                .admit(&[(SnapshotBudgetPool::Metadata, ENTRY_BYTES)])
            {
                state.clock = state.clock.wrapping_add(1);
                let accessed = state.clock;
                state.entries.insert(
                    key,
                    Entry {
                        value: value.clone(),
                        bytes,
                        accessed,
                        _permit: permit,
                    },
                );
                state.bytes += bytes;
            }
        }
        *completed = Some(value.clone());
        Ok(value)
    }

    fn check_epoch(&self, epoch: u64) -> PackedResult<()> {
        if self.budget.read_cache_epoch() != epoch || self.budget.state().closed {
            return Err(PackedWireError::ReadViewChanged);
        }
        Ok(())
    }

    pub(super) fn clear(&self) {
        let mut state = self.state.lock().unwrap();
        state.entries.clear();
        state.bytes = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn unrelated_digest_fills_do_not_wait_for_each_other() {
        let budget = SnapshotMountBudget::defaults();
        let cache = RetainedMetadata::new(8192, budget);
        let generation = ReadGeneration::readonly([1; 32]);
        let mut other_key = [0; 32];
        other_key[31] = 1; // Deliberately collide in the old striped lock scheme.
        let first = cache.read(generation, [0; 32], |_| 100, async {
            std::future::pending::<PackedResult<Arc<u64>>>().await
        });
        tokio::pin!(first);
        assert!(futures::poll!(first.as_mut()).is_pending());
        let second = cache.read(generation, other_key, |_| 100, async { Ok(Arc::new(2)) });
        tokio::pin!(second);
        assert!(
            matches!(futures::poll!(second.as_mut()), std::task::Poll::Ready(Ok(value)) if *value == 2)
        );
    }

    #[tokio::test]
    async fn same_key_followers_share_even_when_value_is_too_large_to_retain() {
        let budget = SnapshotMountBudget::defaults();
        let cache = RetainedMetadata::new(1, budget);
        let generation = ReadGeneration::readonly([1; 32]);
        let calls = AtomicUsize::new(0);
        let (release, wait) = tokio::sync::oneshot::channel();
        let first = cache.read(generation, [1; 32], |_| 100, async {
            calls.fetch_add(1, Ordering::SeqCst);
            wait.await.unwrap();
            Ok(Arc::new(1))
        });
        let second = cache.read(generation, [1; 32], |_| 100, async {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(Arc::new(2))
        });
        tokio::pin!(first, second);
        assert!(futures::poll!(first.as_mut()).is_pending());
        assert!(futures::poll!(second.as_mut()).is_pending());
        release.send(()).unwrap();
        let first = first.await.unwrap();
        let second = second.await.unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(cache.state.lock().unwrap().entries.is_empty());
    }

    #[tokio::test]
    async fn fill_limit_and_cancellation_release_registry_and_permits() {
        let budget = SnapshotMountBudget::defaults();
        let cache = RetainedMetadata::new(8192, budget.clone());
        let generation = ReadGeneration::readonly([1; 32]);
        let calls = AtomicUsize::new(0);
        let mut fills: Vec<_> = (0..=MAX_FILLS)
            .map(|key| {
                Box::pin(cache.read(generation, [key as u8; 32], |_| 100, async {
                    calls.fetch_add(1, Ordering::SeqCst);
                    std::future::pending::<PackedResult<Arc<u64>>>().await
                }))
            })
            .collect();
        for fill in &mut fills {
            assert!(futures::poll!(fill.as_mut()).is_pending());
        }
        assert_eq!(calls.load(Ordering::SeqCst), MAX_FILLS);
        assert_eq!(cache.fills.lock().unwrap().len(), MAX_FILLS);
        assert_eq!(
            budget.state().used[SnapshotBudgetPool::Control as usize],
            MAX_FILLS as u64 * FILL_BYTES
        );
        drop(fills.remove(0));
        assert!(futures::poll!(fills.last_mut().unwrap().as_mut()).is_pending());
        assert_eq!(calls.load(Ordering::SeqCst), MAX_FILLS + 1);
        drop(fills);
        assert!(cache.fills.lock().unwrap().is_empty());
        assert_eq!(budget.state().used[SnapshotBudgetPool::Control as usize], 0);
    }

    #[tokio::test]
    async fn cancelled_leader_and_failed_leader_allow_followers_to_retry() {
        for cancel in [true, false] {
            let budget = SnapshotMountBudget::defaults();
            let cache = RetainedMetadata::new(8192, budget.clone());
            let generation = ReadGeneration::readonly([1; 32]);
            let (release, wait) = tokio::sync::oneshot::channel();
            let mut first = Box::pin(cache.read(generation, [1; 32], |_| 100, async {
                let _ = wait.await;
                Err::<Arc<u64>, _>(PackedWireError::Invalid("failed fill".into()))
            }));
            let mut second =
                Box::pin(cache.read(generation, [1; 32], |_| 100, async { Ok(Arc::new(2)) }));
            assert!(futures::poll!(first.as_mut()).is_pending());
            assert!(futures::poll!(second.as_mut()).is_pending());
            if cancel {
                drop(first);
                drop(release);
            } else {
                release.send(()).unwrap();
                assert!(first.await.is_err());
            }
            assert_eq!(*second.await.unwrap(), 2);
            assert!(cache.fills.lock().unwrap().is_empty());
            assert_eq!(budget.state().used[SnapshotBudgetPool::Control as usize], 0);
        }
    }

    #[tokio::test]
    async fn close_or_generation_fence_wakes_leaders_and_all_queued_fill_waiters() {
        for close in [true, false] {
            let budget = SnapshotMountBudget::defaults();
            let cache = RetainedMetadata::new(8192, budget.clone());
            let generation = ReadGeneration::readonly([1; 32]);
            // Eight leaders, one new-key slot waiter, and one same-key waiter.
            let mut fills: Vec<_> = (0..=MAX_FILLS)
                .chain(std::iter::once(0))
                .map(|key| {
                    Box::pin(cache.read(generation, [key as u8; 32], |_| 100, async {
                        std::future::pending::<PackedResult<Arc<u64>>>().await
                    }))
                })
                .collect();
            for fill in &mut fills {
                assert!(futures::poll!(fill.as_mut()).is_pending());
            }
            if close {
                budget.close();
            } else {
                let mut next = generation;
                next.workspace_head_epoch += 1;
                assert!(cache.cached(next, &[0; 32]).unwrap().is_none());
            }
            for mut fill in fills {
                assert!(matches!(
                    futures::poll!(fill.as_mut()),
                    std::task::Poll::Ready(Err(PackedWireError::ReadViewChanged))
                ));
            }
            assert!(cache.fills.lock().unwrap().is_empty());
            assert_eq!(budget.state().used[SnapshotBudgetPool::Control as usize], 0);
            assert!(cache.state.lock().unwrap().entries.is_empty());
        }
    }

    #[tokio::test]
    async fn exhausted_control_budget_rejects_fill_before_backend_and_leaks_nothing() {
        let budget = SnapshotMountBudget::defaults();
        let held = budget
            .admit(&[(
                SnapshotBudgetPool::Control,
                budget.capacity(SnapshotBudgetPool::Control),
            )])
            .unwrap();
        let cache = RetainedMetadata::new(8192, budget.clone());
        let generation = ReadGeneration::readonly([1; 32]);
        let result = cache
            .read(generation, [1; 32], |_| 100, async {
                panic!("an unadmitted fill must not start backend I/O");
                #[allow(unreachable_code)]
                Ok(Arc::new(1u64))
            })
            .await;
        assert!(matches!(result, Err(PackedWireError::LimitExceeded(_))));
        assert!(cache.fills.lock().unwrap().is_empty());
        drop(held);
        assert_eq!(budget.state().used[SnapshotBudgetPool::Control as usize], 0);
        let calls = AtomicUsize::new(0);
        assert_eq!(*read(&cache, generation, 1, &calls).await, 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    async fn read(
        cache: &RetainedMetadata<u64>,
        generation: ReadGeneration,
        key: u8,
        calls: &AtomicUsize,
    ) -> Arc<u64> {
        cache
            .read(generation, [key; 32], |_| 100, async {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(Arc::new(key as u64))
            })
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn hits_and_lru_eviction_stay_within_capacity() {
        let budget = SnapshotMountBudget::defaults();
        let cache = RetainedMetadata::new(2 * (100 + ENTRY_BYTES), budget.clone());
        let calls = AtomicUsize::new(0);
        let generation = ReadGeneration::readonly([1; 32]);
        for key in [1, 2, 1, 3, 1] {
            assert_eq!(*read(&cache, generation, key, &calls).await, key as u64);
        }
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert_eq!(cache.state.lock().unwrap().entries.len(), 2);
        assert_eq!(cache.state.lock().unwrap().bytes, cache.capacity);
        read(&cache, generation, 2, &calls).await;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            4,
            "least recently used entry was evicted"
        );
        cache.clear();
        assert_eq!(
            budget.state().used[SnapshotBudgetPool::Metadata as usize],
            0
        );
    }

    #[tokio::test]
    async fn directory_fills_overlap_and_same_key_followers_share() {
        let budget = SnapshotMountBudget::defaults();
        let cache = RetainedMetadata::new(16384, budget.clone());
        let generation = ReadGeneration::readonly([1; 32]);
        let barrier = Arc::new(tokio::sync::Barrier::new(8));
        let fills = futures::future::join_all((0..8).map(|key| {
            let barrier = Arc::clone(&barrier);
            cache.read(generation, [key; 32], |_| 100, async move {
                barrier.wait().await;
                Ok(Arc::new(key))
            })
        }));
        let results = tokio::time::timeout(std::time::Duration::from_secs(5), fills)
            .await
            .expect("independent directory fills serialized");
        for (key, result) in results.into_iter().enumerate() {
            assert_eq!(*result.unwrap(), key as u8);
        }
        cache.clear();
        let calls = AtomicUsize::new(0);
        let reads = futures::future::join_all((0..8).map(|_| {
            cache.read(generation, [9; 32], |_| 100, async {
                calls.fetch_add(1, Ordering::SeqCst);
                tokio::task::yield_now().await;
                Ok(Arc::new(9u8))
            })
        }))
        .await;
        let values: Vec<_> = reads.into_iter().map(Result::unwrap).collect();
        assert!(values.iter().all(|value| Arc::ptr_eq(value, &values[0])));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        budget.invalidate_read_caches();
        assert!(cache.cached(generation, &[9; 32]).unwrap().is_none());
        budget.close();
        assert!(matches!(
            cache.cached(generation, &[9; 32]),
            Err(PackedWireError::ReadViewChanged)
        ));
    }

    #[tokio::test]
    async fn generation_change_invalidates_every_entry_and_sibling_cache() {
        let budget = SnapshotMountBudget::defaults();
        let cache = RetainedMetadata::new(8192, budget.clone());
        let sibling = RetainedMetadata::new(8192, budget.clone());
        let calls = AtomicUsize::new(0);
        let first = ReadGeneration::readonly([1; 32]);
        read(&cache, first, 1, &calls).await;
        read(&cache, first, 2, &calls).await;
        read(&sibling, first, 3, &calls).await;
        let mut next = first;
        next.workspace_head_epoch = 1;
        read(&cache, next, 1, &calls).await;
        assert_eq!(cache.state.lock().unwrap().entries.len(), 1);
        read(&sibling, first, 3, &calls).await;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            5,
            "sibling cannot reuse old epoch"
        );
        next.lower_snapshot = [2; 32];
        read(&cache, next, 1, &calls).await;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            6,
            "sealed revision is part of the identity"
        );
    }

    #[tokio::test]
    async fn lease_epoch_invalidates_all_entries_and_prevents_old_fill_publication() {
        let budget = SnapshotMountBudget::defaults();
        let cache = RetainedMetadata::new(8192, budget.clone());
        let calls = AtomicUsize::new(0);
        let generation = ReadGeneration::readonly([1; 32]);
        read(&cache, generation, 1, &calls).await;
        read(&cache, generation, 2, &calls).await;
        let started = tokio::sync::Notify::new();
        let release = tokio::sync::Notify::new();
        let fill = cache.read(generation, [3; 32], |_| 100, async {
            started.notify_one();
            release.notified().await;
            Ok(Arc::new(3))
        });
        let invalidate = async {
            started.notified().await;
            budget.invalidate_read_caches();
            release.notify_one();
        };
        let (result, ()) = tokio::join!(fill, invalidate);
        assert!(matches!(result, Err(PackedWireError::ReadViewChanged)));
        read(&cache, generation, 1, &calls).await;
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert_eq!(cache.state.lock().unwrap().entries.len(), 1);
        read(&cache, generation, 2, &calls).await;
        assert_eq!(calls.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn failed_authentication_is_never_retained_and_close_fences_hits() {
        let budget = SnapshotMountBudget::defaults();
        let cache = RetainedMetadata::new(8192, budget.clone());
        let generation = ReadGeneration::readonly([1; 32]);
        for _ in 0..2 {
            let result = cache
                .read(generation, [1; 32], |_| 100, async {
                    Err::<Arc<u64>, _>(PackedWireError::Invalid("digest mismatch".into()))
                })
                .await;
            assert!(result.is_err());
            assert!(cache.state.lock().unwrap().entries.is_empty());
        }
        let calls = AtomicUsize::new(0);
        read(&cache, generation, 1, &calls).await;
        budget.close();
        let result = cache
            .read(generation, [1; 32], |_| 100, async { Ok(Arc::new(1)) })
            .await;
        assert!(matches!(result, Err(PackedWireError::ReadViewChanged)));
        assert!(cache.state.lock().unwrap().entries.is_empty());
    }

    #[tokio::test]
    async fn eviction_preserves_live_decoded_owner_and_budget_charge() {
        let budget = SnapshotMountBudget::defaults();
        let cache = RetainedMetadata::new(4096, budget.clone());
        let retained = cache
            .read(
                ReadGeneration::readonly([1; 32]),
                [1; 32],
                |_| 1024,
                async {
                    let permit = budget.admit(&[(SnapshotBudgetPool::Metadata, 1024)])?;
                    Ok(Arc::new(super::super::SnapshotOwned::new(
                        vec![1u8; 1024],
                        permit,
                    )))
                },
            )
            .await
            .unwrap();
        assert_eq!(
            budget.state().used[SnapshotBudgetPool::Metadata as usize],
            1024 + ENTRY_BYTES
        );
        cache.clear();
        assert_eq!(
            budget.state().used[SnapshotBudgetPool::Metadata as usize],
            1024
        );
        assert_eq!(retained.len(), 1024);
        drop(retained);
        assert_eq!(
            budget.state().used[SnapshotBudgetPool::Metadata as usize],
            0
        );
    }
}
