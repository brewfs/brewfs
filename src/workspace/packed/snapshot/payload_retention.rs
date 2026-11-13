//! Mount-local copies of authenticated payload frames, separate from transient
//! Raw and observer coverage ownership. Cache consumers never retain a frame
//! after dropping the lock, so eviction immediately releases its byte charge.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard};

use super::{SnapshotMountBudget, SnapshotObjectRef};
use crate::chunk::read_plan::ReadGeneration;
use crate::workspace::packed::PackedFrameDescriptor;
use crate::workspace::packed::wire::{PackedResult, PackedWireError};

// Conservative charge for the two bounded tree entries and allocation overhead.
const ENTRY_BYTES: u64 = 512;
const MAX_BYTES: u64 = 64 << 20;

type FrameKey = [u8; 32];

pub(super) fn frame_key(
    generation: ReadGeneration,
    container: &SnapshotObjectRef,
    frame: &PackedFrameDescriptor,
) -> FrameKey {
    let mut key = blake3::Hasher::new();
    key.update(b"brewfs/wire005/retained-payload/v1");
    key.update(&generation.lower_snapshot);
    key.update(&generation.workspace_head_epoch.to_le_bytes());
    key.update(&generation.workspace_mutation_sequence.to_le_bytes());
    key.update(&[container.kind as u8]);
    key.update(&container.digest);
    key.update(&container.object_len.to_le_bytes());
    key.update(&(container.key.len() as u64).to_le_bytes());
    key.update(container.key.as_bytes());
    key.update(&frame.frame_ordinal.to_le_bytes());
    key.update(&frame.object_offset.to_le_bytes());
    key.update(&frame.stored_len.to_le_bytes());
    key.update(&frame.raw_len.to_le_bytes());
    key.update(&[frame.size_class as u8, frame.codec]);
    key.update(&frame.frame_digest);
    key.update(&frame.first_file_slot.to_le_bytes());
    key.update(&frame.last_file_slot.to_le_bytes());
    key.finalize().into()
}

struct Entry {
    raw: Box<[u8]>,
    accessed: u64,
}

#[derive(Default)]
struct State {
    epoch: u64,
    closed: bool,
    clock: u64,
    bytes: u64,
    hits: u64,
    misses: u64,
    evictions: u64,
    entries: BTreeMap<FrameKey, Entry>,
    lru: BTreeSet<(u64, FrameKey)>,
}

impl State {
    fn clear(&mut self) {
        self.entries.clear();
        self.lru.clear();
        self.bytes = 0;
    }

    fn touch(&mut self, key: FrameKey) {
        let entry = self.entries.get_mut(&key).expect("retained entry exists");
        self.lru.remove(&(entry.accessed, key));
        self.clock = self.clock.saturating_add(1);
        entry.accessed = self.clock;
        self.lru.insert((self.clock, key));
    }
}

pub(super) struct PayloadRetention {
    capacity: u64,
    budget: Arc<SnapshotMountBudget>,
    state: Mutex<State>,
}

impl PayloadRetention {
    pub(super) fn new(capacity: u64, budget: Arc<SnapshotMountBudget>) -> PackedResult<Self> {
        if !(ENTRY_BYTES + 1..=MAX_BYTES).contains(&capacity) {
            return Err(PackedWireError::LimitExceeded(
                "wire005 retained payload capacity must be 513 bytes through 64 MiB".into(),
            ));
        }
        Ok(Self {
            capacity,
            state: Mutex::new(State {
                epoch: budget.read_cache_epoch(),
                ..State::default()
            }),
            budget,
        })
    }

    fn current(&self, expected_epoch: u64) -> PackedResult<MutexGuard<'_, State>> {
        let mut state = self.state.lock().unwrap();
        let current = self.budget.read_cache_epoch();
        if state.epoch != current {
            state.clear();
            state.epoch = current;
        }
        if state.closed || current != expected_epoch || self.budget.state().closed {
            return Err(PackedWireError::ReadViewChanged);
        }
        Ok(state)
    }

    pub(super) fn copy_range(
        &self,
        epoch: u64,
        key: &FrameKey,
        start: usize,
        output: &mut [u8],
    ) -> PackedResult<bool> {
        let mut state = self.current(epoch)?;
        if let Some(entry) = state.entries.get(key) {
            let end = start.checked_add(output.len()).ok_or_else(|| {
                PackedWireError::Invalid("retained frame output range overflows".into())
            })?;
            output.copy_from_slice(entry.raw.get(start..end).ok_or_else(|| {
                PackedWireError::Invalid("retained frame output range exceeds source".into())
            })?);
            if self.budget.read_cache_epoch() != epoch {
                return Err(PackedWireError::ReadViewChanged);
            }
            state.hits = state.hits.saturating_add(1);
            state.touch(*key);
            Ok(true)
        } else {
            state.misses = state.misses.saturating_add(1);
            Ok(false)
        }
    }

    /// Called only after the fetched stored frame was hashed and decoded
    /// successfully. In-flight observer accounting stays with the live frame.
    pub(super) fn remember(&self, epoch: u64, key: FrameKey, raw: &[u8]) -> PackedResult<()> {
        let mut state = self.current(epoch)?;
        let charge = raw.len() as u64 + ENTRY_BYTES;
        if raw.is_empty() || charge > self.capacity || state.entries.contains_key(&key) {
            return Ok(());
        }
        while state.bytes + charge > self.capacity {
            let (_, oldest) = state.lru.pop_first().expect("byte charge has an owner");
            let removed = state.entries.remove(&oldest).expect("LRU entry exists");
            state.bytes -= removed.raw.len() as u64 + ENTRY_BYTES;
            state.evictions = state.evictions.saturating_add(1);
        }
        state.bytes += charge;
        state.clock = state.clock.saturating_add(1);
        let accessed = state.clock;
        state.entries.insert(
            key,
            Entry {
                raw: raw.into(),
                accessed,
            },
        );
        state.lru.insert((accessed, key));
        Ok(())
    }

    pub(super) fn close(&self) {
        let mut state = self.state.lock().unwrap();
        state.closed = true;
        state.clear();
    }

    pub(super) fn render_into(&self, output: &mut dyn std::fmt::Write) {
        let state = self.state.lock().unwrap();
        for (name, value) in [
            ("capacity_bytes", self.capacity),
            ("resident_charge_bytes", state.bytes),
            ("entries", state.entries.len() as u64),
            ("hits_total", state.hits),
            ("misses_total", state.misses),
            ("evictions_total", state.evictions),
        ] {
            let _ = writeln!(output, "brewfs_packed_v3_payload_retention_{name} {value}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_byte_cap_eviction_and_epoch_do_not_serve_old_bytes() {
        let budget = SnapshotMountBudget::defaults();
        let cache = PayloadRetention::new(ENTRY_BYTES + 4, budget.clone()).unwrap();
        let epoch = budget.read_cache_epoch();
        let mut output = [0; 2];
        assert!(!cache.copy_range(epoch, &[1; 32], 1, &mut output).unwrap());
        cache.remember(epoch, [1; 32], b"data").unwrap();
        assert!(cache.copy_range(epoch, &[1; 32], 1, &mut output).unwrap());
        assert_eq!(&output, b"at");
        cache.remember(epoch, [2; 32], b"01234").unwrap();
        assert!(!cache.copy_range(epoch, &[2; 32], 1, &mut output).unwrap());
        cache.remember(epoch, [3; 32], b"more").unwrap();
        assert!(!cache.copy_range(epoch, &[1; 32], 1, &mut output).unwrap());
        assert_eq!(cache.state.lock().unwrap().bytes, ENTRY_BYTES + 4);
        budget.invalidate_read_caches();
        assert!(matches!(
            cache.copy_range(epoch, &[3; 32], 1, &mut output),
            Err(PackedWireError::ReadViewChanged)
        ));
        assert_eq!(cache.state.lock().unwrap().bytes, 0);
        cache.close();
        assert!(
            cache
                .remember(budget.read_cache_epoch(), [3; 32], b"late")
                .is_err()
        );
    }
}
