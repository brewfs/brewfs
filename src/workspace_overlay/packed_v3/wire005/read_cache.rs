//! Retained authenticated metadata with exact LRU ownership and view fencing.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use super::{V3BudgetPool, V3MountBudget, V3OwnedPermit};
use crate::chunk::read_plan::ReadGeneration;
use crate::workspace_overlay::packed_v3::wire::{PackedResult, PackedWireError};

// Covers the fixed digest key, BTree node, Arc and entry bookkeeping. The
// decoded value independently retains its original mount budget permit.
const ENTRY_BYTES: u64 = 512;

struct Entry<T> {
    value: Arc<T>,
    bytes: u64,
    accessed: u64,
    _permit: V3OwnedPermit,
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
    budget: Arc<V3MountBudget>,
    state: Mutex<State<T>>,
    fetch: tokio::sync::Mutex<()>,
}

impl<T> RetainedMetadata<T> {
    pub(super) fn new(capacity: u64, budget: Arc<V3MountBudget>) -> Self {
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
            fetch: tokio::sync::Mutex::new(()),
        }
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
        // Metadata fills are tiny and rare. Serialize misses independently
        // of hits, sharing a successful fill without unbounded flight state.
        let _fill = self.fetch.lock().await;
        self.check_epoch(epoch)?;
        if let Some(value) = self.get(&key, epoch) {
            self.check_epoch(epoch)?;
            return Ok(value);
        }
        let value = fetch.await?;
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
            if let Ok(permit) = self.budget.admit(&[(V3BudgetPool::Metadata, ENTRY_BYTES)]) {
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
        let budget = V3MountBudget::defaults();
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
        assert_eq!(budget.state().used[V3BudgetPool::Metadata as usize], 0);
    }

    #[tokio::test]
    async fn generation_change_invalidates_every_entry_and_sibling_cache() {
        let budget = V3MountBudget::defaults();
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
        let budget = V3MountBudget::defaults();
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
        let budget = V3MountBudget::defaults();
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
        let budget = V3MountBudget::defaults();
        let cache = RetainedMetadata::new(4096, budget.clone());
        let retained = cache
            .read(
                ReadGeneration::readonly([1; 32]),
                [1; 32],
                |_| 1024,
                async {
                    let permit = budget.admit(&[(V3BudgetPool::Metadata, 1024)])?;
                    Ok(Arc::new(super::super::V3Owned::new(
                        vec![1u8; 1024],
                        permit,
                    )))
                },
            )
            .await
            .unwrap();
        assert_eq!(
            budget.state().used[V3BudgetPool::Metadata as usize],
            1024 + ENTRY_BYTES
        );
        cache.clear();
        assert_eq!(budget.state().used[V3BudgetPool::Metadata as usize], 1024);
        assert_eq!(retained.len(), 1024);
        drop(retained);
        assert_eq!(budget.state().used[V3BudgetPool::Metadata as usize], 0);
    }
}
