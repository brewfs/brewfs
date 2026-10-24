//! Atomic in-memory reference backend with a controllable backend clock.

use std::ops::Bound::{Excluded, Included, Unbounded};
use std::{collections::BTreeMap, sync::Mutex};

use async_trait::async_trait;

use super::{
    EntityError, Result,
    backend::{
        CommitOutcome, CreateAllocation, EntityBackend, EntityPlan, RowCheck, RowSnapshot,
        RowWrite, SliceAllocation, TransactionAuthority, add_bytes, next_counter, prepare_create,
        prepare_slices, validate_checked_read, validate_key, validate_plan, validate_scan,
        validate_scan_page,
    },
};

pub struct MemoryEntityBackend {
    state: Mutex<State>,
}

struct State {
    now_ns: i64,
    rows: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl MemoryEntityBackend {
    pub fn new(now_ns: i64) -> Self {
        Self {
            state: Mutex::new(State {
                now_ns,
                rows: BTreeMap::new(),
            }),
        }
    }

    pub fn set_time(&self, now_ns: i64) -> Result<()> {
        if now_ns < 0 {
            return Err(EntityError::Invalid("negative backend time".into()));
        }
        self.state.lock().map_err(poisoned)?.now_ns = now_ns;
        Ok(())
    }
}

fn poisoned<T>(_: std::sync::PoisonError<T>) -> EntityError {
    EntityError::Backend("memory entity backend lock poisoned".into())
}

impl State {
    /// Check stored bounds and authority before any allocation or write. Keep
    /// the byte count so generated keys participate in the same bound.
    fn preflight(
        &self,
        authority: &TransactionAuthority,
        plan: &EntityPlan,
        extra_keys: &[&[u8]],
    ) -> Result<Option<usize>> {
        let mut stored_bytes = 0;
        for key in authority
            .checks
            .iter()
            .chain(&plan.checks)
            .map(|check| check.key.as_slice())
            .chain(plan.writes.iter().map(RowWrite::key))
            .chain(extra_keys.iter().copied())
        {
            add_bytes(
                &mut stored_bytes,
                key,
                self.rows.get(key).map(Vec::as_slice),
            )?;
        }
        if !authority.allows_time(self.now_ns) {
            return Err(EntityError::Fenced);
        }
        Ok(authority
            .checks
            .iter()
            .chain(&plan.checks)
            .all(|check| self.rows.get(&check.key) == check.expected.as_ref())
            .then_some(stored_bytes))
    }

    fn apply(&mut self, writes: impl IntoIterator<Item = RowWrite>) {
        for write in writes {
            match write {
                RowWrite::Put { key, value } => {
                    self.rows.insert(key, value);
                }
                RowWrite::Delete { key } => {
                    self.rows.remove(&key);
                }
            }
        }
    }
}

#[async_trait]
impl EntityBackend for MemoryEntityBackend {
    async fn read(&self, keys: &[Vec<u8>]) -> Result<RowSnapshot> {
        self.read_checked(keys, &[])
            .await
            .map(|(snapshot, _)| snapshot)
    }

    async fn read_checked(
        &self,
        keys: &[Vec<u8>],
        checks: &[RowCheck],
    ) -> Result<(RowSnapshot, bool)> {
        validate_checked_read(keys, checks)?;
        let state = self.state.lock().map_err(poisoned)?;
        if state.now_ns < 0 {
            return Err(EntityError::Corrupt("negative backend time".into()));
        }
        let mut total = 0;
        for key in keys.iter().chain(checks.iter().map(|check| &check.key)) {
            add_bytes(&mut total, key, state.rows.get(key).map(Vec::as_slice))?;
        }
        let matched = checks
            .iter()
            .all(|check| state.rows.get(&check.key) == check.expected.as_ref());
        Ok((
            RowSnapshot {
                now_ns: state.now_ns,
                rows: keys
                    .iter()
                    .map(|key| state.rows.get(key).cloned())
                    .collect(),
            },
            matched,
        ))
    }

    async fn commit(
        &self,
        authority: &TransactionAuthority,
        plan: &EntityPlan,
    ) -> Result<CommitOutcome> {
        validate_plan(authority, plan)?;
        let mut state = self.state.lock().map_err(poisoned)?;
        if state.preflight(authority, plan, &[])?.is_none() {
            return Ok(CommitOutcome::Conflict);
        }
        state.apply(plan.writes.iter().cloned());
        Ok(CommitOutcome::Committed)
    }

    async fn commit_with_inode_allocation(
        &self,
        authority: &TransactionAuthority,
        plan: &EntityPlan,
        create: &CreateAllocation,
    ) -> Result<Option<i64>> {
        let prepared = prepare_create(authority, plan, create)?;
        let mut state = self.state.lock().map_err(poisoned)?;
        let Some(mut stored_bytes) = state.preflight(
            authority,
            plan,
            &[&prepared.counter_key, &prepared.dentry_key],
        )?
        else {
            return Ok(None);
        };
        let current = state
            .rows
            .get(&prepared.counter_key)
            .ok_or_else(|| EntityError::Corrupt("uninitialized entity counter".into()))?;
        let ino = next_counter(current)? as i64;
        if ino <= create.lower_max_ino {
            return Err(EntityError::Corrupt(
                "inode allocator overlaps lower".into(),
            ));
        }
        let generated = prepared.writes(ino);
        for write in &generated {
            if write.key() != prepared.dentry_key.as_slice() && state.rows.contains_key(write.key())
            {
                return Err(EntityError::Corrupt("inode allocator collision".into()));
            }
            if write.key() != prepared.dentry_key.as_slice() {
                add_bytes(&mut stored_bytes, write.key(), None)?;
            }
        }
        let mut combined = plan.clone();
        combined.writes.push(RowWrite::Put {
            key: prepared.counter_key,
            value: ino.to_string().into_bytes(),
        });
        combined.writes.extend(generated);
        validate_plan(authority, &combined)?;
        state.apply(combined.writes);
        Ok(Some(ino))
    }

    async fn reserve_slices(
        &self,
        authority: &TransactionAuthority,
        allocation: &SliceAllocation,
    ) -> Result<Option<Vec<u64>>> {
        let prepared = prepare_slices(authority, allocation)?;
        let mut state = self.state.lock().map_err(poisoned)?;
        let Some(mut stored_bytes) =
            state.preflight(authority, &EntityPlan::default(), &[&prepared.counter_key])?
        else {
            return Ok(None);
        };
        let mut counter = state
            .rows
            .get(&prepared.counter_key)
            .ok_or_else(|| EntityError::Corrupt("uninitialized entity counter".into()))?
            .clone();
        let mut ids = Vec::with_capacity(allocation.sizes.len());
        for _ in &allocation.sizes {
            let id = next_counter(&counter)?;
            counter = id.to_string().into_bytes();
            for key in [prepared.slice_key(id), prepared.descriptor_key(id)] {
                if state.rows.contains_key(&key) {
                    return Err(EntityError::Corrupt("slice allocator collision".into()));
                }
                add_bytes(&mut stored_bytes, &key, None)?;
            }
            ids.push(id);
        }
        let writes = prepared.writes(&ids);
        // The bounded canonical templates were validated before locking; every
        // generated identity/row has now been checked before the first mutation.
        state.rows.insert(prepared.counter_key, counter);
        state.apply(writes);
        Ok(Some(ids))
    }

    async fn scan_prefix(&self, prefix: &[u8], limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        validate_scan(prefix, limit)?;
        let state = self.state.lock().map_err(poisoned)?;
        let entries = state
            .rows
            .range(prefix.to_vec()..)
            .take_while(|(key, _)| key.starts_with(prefix));
        let mut result = Vec::new();
        let mut total = 0;
        for (key, value) in entries {
            if result.len() == limit {
                return Err(EntityError::Invalid("entity scan exceeds row bound".into()));
            }
            add_bytes(&mut total, key, Some(value))?;
            result.push((key.clone(), value.clone()));
        }
        Ok(result)
    }

    async fn scan_prefix_batch(
        &self,
        prefix: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        validate_scan(prefix, limit)?;
        let state = self.state.lock().map_err(poisoned)?;
        let entries = state
            .rows
            .range(prefix.to_vec()..)
            .take_while(|(key, _)| key.starts_with(prefix))
            .take(limit);
        let mut result = Vec::new();
        let mut total = 0;
        for (key, value) in entries {
            add_bytes(&mut total, key, Some(value))?;
            result.push((key.clone(), value.clone()));
        }
        Ok(result)
    }

    async fn scan_keys_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<Vec<u8>>> {
        validate_scan_page(prefix, after, limit)?;
        let lower = match after {
            Some(key) => Excluded(key.to_vec()),
            None => Included(prefix.to_vec()),
        };
        let state = self.state.lock().map_err(poisoned)?;
        state
            .rows
            .range((lower, Unbounded))
            .take_while(|(key, _)| key.starts_with(prefix))
            .take(limit)
            .map(|(key, _)| {
                validate_key(key)?;
                Ok(key.clone())
            })
            .collect()
    }
}
