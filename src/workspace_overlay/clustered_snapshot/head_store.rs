//! Workspace-head persistence for clustered snapshot v2.
//!
//! Immutable objects live in object storage, while the head is the one
//! mutable control record. This adapter uses the existing transactional
//! workspace KV substrate so Redis and TiKV share the same encoding and CAS
//! semantics.

use std::sync::Arc;

use async_trait::async_trait;

use crate::native_base::wire::error::{WireError, WireResult};
use crate::workspace_overlay::stores::kv_backend::{KvCheck, KvWrite, WorkspaceKvBackend};

use super::publication::{SnapshotRef, WorkspaceHead, WorkspaceHeadStore};

const HEAD_MAGIC: &[u8; 8] = b"BWSH002\0";
const HEAD_LEN: usize = 96;
const HEAD_KEY: &[u8] = b"clustered/v2/workspace-head";

/// Fixed-width workspace head backed by a transactional workspace KV store.
#[derive(Clone)]
pub struct KvWorkspaceHeadStore<B> {
    backend: Arc<B>,
    key: Arc<[u8]>,
}

impl<B> KvWorkspaceHeadStore<B>
where
    B: WorkspaceKvBackend,
{
    /// Create a store using the namespace-local default key.
    pub fn new(backend: B) -> Self {
        Self::with_key(backend, HEAD_KEY.to_vec())
    }

    /// Create a store with an explicit logical key. The backend applies its
    /// own namespace prefix, so callers pass only the logical suffix.
    pub fn with_key(backend: B, key: Vec<u8>) -> Self {
        Self {
            backend: Arc::new(backend),
            key: Arc::from(key),
        }
    }

    pub fn backend(&self) -> &Arc<B> {
        &self.backend
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }
}

#[async_trait]
impl<B> WorkspaceHeadStore for KvWorkspaceHeadStore<B>
where
    B: WorkspaceKvBackend,
{
    async fn load(&self) -> WireResult<Option<WorkspaceHead>> {
        let value = self
            .backend
            .get(&self.key)
            .await
            .map_err(map_backend_error)?;
        value.map(|bytes| decode_head(&bytes)).transpose()
    }

    async fn compare_and_swap(
        &self,
        expected: Option<WorkspaceHead>,
        desired: WorkspaceHead,
    ) -> WireResult<bool> {
        validate_transition(expected, desired)?;
        let desired_bytes = encode_head(desired);

        // A lost response after a successful CAS must be retryable. Checking
        // the already-published desired value makes publication idempotent
        // without weakening a conditional write for a new value.
        if self
            .backend
            .get(&self.key)
            .await
            .map_err(map_backend_error)?
            .as_deref()
            == Some(desired_bytes.as_slice())
        {
            return Ok(true);
        }

        let checks = [KvCheck {
            key: self.key.to_vec(),
            expected: expected.map(encode_head),
        }];
        let writes = [KvWrite::Put {
            key: self.key.to_vec(),
            value: desired_bytes,
        }];
        self.backend
            .compare_and_swap(&checks, &writes)
            .await
            .map_err(map_backend_error)
    }
}

fn validate_transition(expected: Option<WorkspaceHead>, desired: WorkspaceHead) -> WireResult<()> {
    match expected {
        Some(current) if desired.epoch <= current.epoch => Err(WireError::invalid(
            "workspace head",
            "new epoch is not greater than the expected epoch",
        )),
        None if desired.epoch != 1 => Err(WireError::invalid(
            "workspace head",
            "the first published epoch must be one",
        )),
        _ => Ok(()),
    }
}

fn encode_head(head: WorkspaceHead) -> Vec<u8> {
    let mut bytes = vec![0u8; HEAD_LEN];
    bytes[..8].copy_from_slice(HEAD_MAGIC);
    bytes[8..16].copy_from_slice(&head.epoch.to_le_bytes());
    bytes[16..48].copy_from_slice(&head.snapshot.manifest_hash);
    bytes[48..80].copy_from_slice(&head.snapshot.superblock_digest);
    bytes[80..88].copy_from_slice(&head.snapshot.object_len.to_le_bytes());
    bytes
}

fn decode_head(bytes: &[u8]) -> WireResult<WorkspaceHead> {
    if bytes.len() != HEAD_LEN {
        return Err(WireError::invalid(
            "workspace head",
            format!("expected {HEAD_LEN} bytes, got {}", bytes.len()),
        ));
    }
    if &bytes[..8] != HEAD_MAGIC {
        return Err(WireError::UnsupportedFormat(
            "workspace head magic/version is not supported".into(),
        ));
    }
    if bytes[88..].iter().any(|byte| *byte != 0) {
        return Err(WireError::invalid(
            "workspace head",
            "reserved bytes are non-zero",
        ));
    }
    let epoch = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
    if epoch == 0 {
        return Err(WireError::invalid(
            "workspace head",
            "epoch must be non-zero",
        ));
    }
    Ok(WorkspaceHead {
        epoch,
        snapshot: SnapshotRef {
            manifest_hash: bytes[16..48].try_into().unwrap(),
            superblock_digest: bytes[48..80].try_into().unwrap(),
            object_len: u64::from_le_bytes(bytes[80..88].try_into().unwrap()),
        },
    })
}

fn map_backend_error(error: crate::workspace_overlay::error::WorkspaceError) -> WireError {
    WireError::invalid("workspace head backend", error.to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    use super::*;
    use crate::workspace_overlay::stores::kv_backend::KvEntry;

    #[derive(Clone, Default)]
    struct MemoryBackend {
        values: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
    }

    #[async_trait]
    impl WorkspaceKvBackend for MemoryBackend {
        fn name(&self) -> &'static str {
            "memory-head"
        }

        async fn get(
            &self,
            key: &[u8],
        ) -> Result<Option<Vec<u8>>, crate::workspace_overlay::error::WorkspaceError> {
            Ok(self.values.lock().unwrap().get(key).cloned())
        }

        async fn scan_prefix(
            &self,
            _prefix: &[u8],
        ) -> Result<Vec<KvEntry>, crate::workspace_overlay::error::WorkspaceError> {
            Ok(Vec::new())
        }

        async fn compare_and_swap(
            &self,
            checks: &[KvCheck],
            writes: &[KvWrite],
        ) -> Result<bool, crate::workspace_overlay::error::WorkspaceError> {
            let mut values = self.values.lock().unwrap();
            for check in checks {
                if values.get(&check.key) != check.expected.as_ref() {
                    return Ok(false);
                }
            }
            for write in writes {
                match write {
                    KvWrite::Put { key, value } => {
                        values.insert(key.clone(), value.clone());
                    }
                    KvWrite::Delete { key } => {
                        values.remove(key);
                    }
                }
            }
            Ok(true)
        }

        async fn server_time_ns(
            &self,
        ) -> Result<i64, crate::workspace_overlay::error::WorkspaceError> {
            Ok(0)
        }
    }

    fn head(epoch: u64, fill: u8) -> WorkspaceHead {
        WorkspaceHead {
            epoch,
            snapshot: SnapshotRef {
                manifest_hash: [fill; 32],
                superblock_digest: [fill.wrapping_add(1); 32],
                object_len: 4096 + u64::from(fill),
            },
        }
    }

    #[tokio::test]
    async fn fixed_head_round_trip_and_idempotent_retry() {
        let store = KvWorkspaceHeadStore::new(MemoryBackend::default());
        let first = head(1, 7);
        assert_eq!(store.load().await.unwrap(), None);
        assert!(store.compare_and_swap(None, first).await.unwrap());
        assert_eq!(store.load().await.unwrap(), Some(first));
        assert!(store.compare_and_swap(None, first).await.unwrap());
        assert_eq!(encode_head(first).len(), HEAD_LEN);
    }

    #[tokio::test]
    async fn stale_cas_and_corrupt_value_fail_closed() {
        let backend = MemoryBackend::default();
        let store = KvWorkspaceHeadStore::new(backend.clone());
        let first = head(1, 1);
        let second = head(2, 2);
        assert!(store.compare_and_swap(None, first).await.unwrap());
        assert!(
            !store
                .compare_and_swap(Some(head(1, 9)), second)
                .await
                .unwrap()
        );

        backend
            .values
            .lock()
            .unwrap()
            .insert(store.key().to_vec(), vec![0; HEAD_LEN]);
        assert!(store.load().await.is_err());
    }
}
