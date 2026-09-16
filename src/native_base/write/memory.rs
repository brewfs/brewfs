//! In-memory [`ControlStore`] used by the contract tests and as the
//! executable specification the Redis/TiKV backends must match.

use std::collections::BTreeMap;

use async_trait::async_trait;
use tokio::sync::Mutex;

use super::store::{ControlStore, StoreError, Txn, expect_matches};

/// A strict in-memory key/value store with atomic conditional transactions.
#[derive(Default)]
pub struct MemoryControlStore {
    map: Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl MemoryControlStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl ControlStore for MemoryControlStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        let map = self.map.lock().await;
        Ok(map.get(key).cloned())
    }

    async fn run(&self, txn: Txn) -> Result<(), StoreError> {
        let mut map = self.map.lock().await;
        for (key, expect) in &txn.checks {
            if !expect_matches(expect, map.get(key).map(|v| v.as_slice())) {
                return Err(StoreError::Conflict);
            }
        }
        for (key, value) in txn.writes {
            match value {
                Some(v) => {
                    map.insert(key, v);
                }
                None => {
                    map.remove(&key);
                }
            }
        }
        Ok(())
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StoreError> {
        let map = self.map.lock().await;
        Ok(map
            .range::<[u8], _>((
                std::ops::Bound::Included(prefix),
                std::ops::Bound::Unbounded,
            ))
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }
}
