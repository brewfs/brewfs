//! Bounded physical read planning and same-frame singleflight (PR10/PR11).

use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::sync::Arc;

use tokio::sync::{Mutex, Notify};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadUnit {
    pub namespace: [u8; 16],
    pub object_id: [u8; 16],
    pub full_hash: [u8; 32],
    pub authorized_domain: [u8; 16],
    pub start: u64,
    pub end: u64,
}

impl ReadUnit {
    pub fn len(&self) -> u64 {
        self.end.saturating_sub(self.start)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct MergePolicy {
    pub max_coalesced_get: u64,
    pub max_gap_bytes: u64,
    pub max_merge_amplification: f64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoalescedRange {
    pub namespace: [u8; 16],
    pub object_id: [u8; 16],
    pub full_hash: [u8; 32],
    pub authorized_domain: [u8; 16],
    pub start: u64,
    pub end: u64,
    pub units: Vec<ReadUnit>,
    pub useful_bytes: u64,
}

impl CoalescedRange {
    pub fn physical_bytes(&self) -> u64 {
        self.end.saturating_sub(self.start)
    }

    pub fn extra_bytes(&self) -> u64 {
        self.physical_bytes().saturating_sub(self.useful_bytes)
    }
}

fn same_identity(a: &ReadUnit, b: &ReadUnit) -> bool {
    a.namespace == b.namespace
        && a.object_id == b.object_id
        && a.full_hash == b.full_hash
        && a.authorized_domain == b.authorized_domain
}

/// Merge only already queued, contiguous candidates with the exact immutable
/// identity. Exact duplicate units are counted once, so amplification cannot
/// be hidden by repeating a waiter.
pub fn coalesce_ranges(mut units: Vec<ReadUnit>, policy: MergePolicy) -> Vec<CoalescedRange> {
    units.sort_by_key(|unit| (unit.namespace, unit.object_id, unit.start, unit.end));
    units.dedup_by(|a, b| {
        a.namespace == b.namespace
            && a.object_id == b.object_id
            && a.full_hash == b.full_hash
            && a.authorized_domain == b.authorized_domain
            && a.start == b.start
            && a.end == b.end
    });

    let mut output = Vec::new();
    for unit in units {
        let Some(current) = output.last_mut() else {
            output.push(CoalescedRange {
                namespace: unit.namespace,
                object_id: unit.object_id,
                full_hash: unit.full_hash,
                authorized_domain: unit.authorized_domain,
                start: unit.start,
                end: unit.end,
                useful_bytes: unit.len(),
                units: vec![unit],
            });
            continue;
        };
        let gap = unit.start.saturating_sub(current.end);
        let candidate_end = current.end.max(unit.end);
        let candidate_physical = candidate_end.saturating_sub(current.start);
        let candidate_useful = current.useful_bytes.saturating_add(unit.len());
        let candidate_ratio = if candidate_useful == 0 {
            f64::INFINITY
        } else {
            candidate_physical as f64 / candidate_useful as f64
        };
        if same_identity(&current.units[0], &unit)
            && gap <= policy.max_gap_bytes
            && candidate_physical <= policy.max_coalesced_get
            && candidate_ratio <= policy.max_merge_amplification
        {
            current.end = candidate_end;
            current.useful_bytes = candidate_useful;
            current.units.push(unit);
        } else {
            output.push(CoalescedRange {
                namespace: unit.namespace,
                object_id: unit.object_id,
                full_hash: unit.full_hash,
                authorized_domain: unit.authorized_domain,
                start: unit.start,
                end: unit.end,
                useful_bytes: unit.len(),
                units: vec![unit],
            });
        }
    }
    output
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadBudgetConfig {
    pub encoded_bytes: u64,
    pub decoded_bytes: u64,
    pub demand_reserve: u64,
}

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
pub enum BudgetError {
    #[error("read unit exceeds its budget")]
    UnitTooLarge,
    #[error("read budget is exhausted")]
    Exhausted,
}

#[derive(Debug)]
struct BudgetState {
    encoded: u64,
    decoded: u64,
    demand_encoded: u64,
}

#[derive(Debug)]
pub struct ReadBudget {
    config: ReadBudgetConfig,
    state: Mutex<BudgetState>,
}

#[derive(Debug)]
pub struct BudgetPermit {
    budget: Arc<ReadBudget>,
    encoded: u64,
    decoded: u64,
    demand: bool,
}

impl ReadBudget {
    pub fn new(config: ReadBudgetConfig) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(BudgetState {
                encoded: 0,
                decoded: 0,
                demand_encoded: 0,
            }),
            config,
        })
    }

    pub async fn try_reserve(
        self: &Arc<Self>,
        encoded: u64,
        decoded: u64,
        demand: bool,
    ) -> Result<BudgetPermit, BudgetError> {
        if encoded > self.config.encoded_bytes || decoded > self.config.decoded_bytes {
            return Err(BudgetError::UnitTooLarge);
        }
        let mut state = self.state.lock().await;
        if state.encoded.saturating_add(encoded) > self.config.encoded_bytes
            || state.decoded.saturating_add(decoded) > self.config.decoded_bytes
            || (!demand
                && state.encoded.saturating_add(encoded)
                    > self
                        .config
                        .encoded_bytes
                        .saturating_sub(self.config.demand_reserve))
        {
            return Err(BudgetError::Exhausted);
        }
        state.encoded += encoded;
        state.decoded += decoded;
        if demand {
            state.demand_encoded += encoded;
        }
        Ok(BudgetPermit {
            budget: self.clone(),
            encoded,
            decoded,
            demand,
        })
    }
}

impl Drop for BudgetPermit {
    fn drop(&mut self) {
        let budget = self.budget.clone();
        let encoded = self.encoded;
        let decoded = self.decoded;
        let demand = self.demand;
        tokio::spawn(async move {
            let mut state = budget.state.lock().await;
            state.encoded = state.encoded.saturating_sub(encoded);
            state.decoded = state.decoded.saturating_sub(decoded);
            if demand {
                state.demand_encoded = state.demand_encoded.saturating_sub(encoded);
            }
        });
    }
}

/// In-process singleflight for immutable frame identity. A cancelled waiter
/// simply drops its future; the shared loader remains alive for other waiters.
#[derive(Debug)]
pub struct FrameSingleflight<K, V> {
    entries: Mutex<HashMap<K, Arc<FlightEntry<V>>>>,
}

#[derive(Debug)]
struct FlightEntry<V> {
    result: Mutex<Option<Result<Arc<V>, String>>>,
    ready: Notify,
}

impl<K, V> Default for FrameSingleflight<K, V> {
    fn default() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
        }
    }
}

impl<K, V> FrameSingleflight<K, V>
where
    K: Eq + Hash + Clone + Send + 'static,
    V: Send + Sync + 'static,
{
    pub async fn get_or_load<F, Fut>(&self, key: K, load: F) -> Result<Arc<V>, String>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<V, String>> + Send + 'static,
    {
        let (entry, leader) = {
            let mut entries = self.entries.lock().await;
            match entries.get(&key) {
                Some(entry) => (entry.clone(), false),
                None => {
                    let entry = Arc::new(FlightEntry {
                        result: Mutex::new(None),
                        ready: Notify::new(),
                    });
                    entries.insert(key.clone(), entry.clone());
                    (entry, true)
                }
            }
        };
        if leader {
            let entry = entry.clone();
            tokio::spawn(async move {
                *entry.result.lock().await = Some(load().await.map(Arc::new));
                entry.ready.notify_waiters();
            });
        }
        let result = loop {
            let notified = entry.ready.notified();
            if let Some(result) = entry.result.lock().await.clone() {
                break result;
            }
            notified.await;
        };
        if result.is_err() {
            self.entries.lock().await.remove(&key);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    fn unit(start: u64, end: u64) -> ReadUnit {
        ReadUnit {
            namespace: [1; 16],
            object_id: [2; 16],
            full_hash: [3; 32],
            authorized_domain: [4; 16],
            start,
            end,
        }
    }

    #[test]
    fn coalesce_deduplicates_and_obeys_amplification() {
        let merged = coalesce_ranges(
            vec![unit(0, 100), unit(0, 100), unit(120, 180)],
            MergePolicy {
                max_coalesced_get: 256,
                max_gap_bytes: 32,
                max_merge_amplification: 1.25,
            },
        );
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].useful_bytes, 160);
        assert_eq!(merged[0].physical_bytes(), 180);

        let split = coalesce_ranges(
            vec![unit(0, 100), unit(120, 180)],
            MergePolicy {
                max_coalesced_get: 256,
                max_gap_bytes: 32,
                max_merge_amplification: 1.05,
            },
        );
        assert_eq!(split.len(), 2);
    }

    #[tokio::test]
    async fn singleflight_cancelled_waiter_does_not_cancel_loader() {
        let flights = Arc::new(FrameSingleflight::<u64, Vec<u8>>::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let first = {
            let flights = flights.clone();
            let calls = calls.clone();
            tokio::spawn(async move {
                flights
                    .get_or_load(1, || async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                        Ok(vec![7])
                    })
                    .await
            })
        };
        tokio::task::yield_now().await;
        first.abort();
        let result = flights
            .get_or_load(1, || async { Ok(vec![7]) })
            .await
            .unwrap();
        assert_eq!(&*result, &[7]);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn prefetch_cannot_consume_demand_reserve() {
        let budget = ReadBudget::new(ReadBudgetConfig {
            encoded_bytes: 100,
            decoded_bytes: 100,
            demand_reserve: 40,
        });
        let _prefetch = budget.try_reserve(60, 1, false).await.unwrap();
        assert_eq!(
            budget.try_reserve(1, 1, false).await.unwrap_err(),
            BudgetError::Exhausted
        );
        let demand = budget.try_reserve(40, 1, true).await.unwrap();
        drop(demand);
    }
}
