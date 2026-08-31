//! Transactional key/value substrate shared by remote workspace catalogs.

use async_trait::async_trait;

use crate::workspace_overlay::error::WorkspaceError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KvCheck {
    pub key: Vec<u8>,
    pub expected: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KvWrite {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KvEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[async_trait]
pub trait WorkspaceKvBackend: Send + Sync + 'static {
    fn name(&self) -> &'static str;

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, WorkspaceError>;

    /// Return exact values in the same order as `keys`.
    ///
    /// Remote backends override this to collapse a fixed two-layer lookup into
    /// one network round trip. The default keeps lightweight test backends
    /// simple without changing their semantics.
    async fn get_many(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>, WorkspaceError> {
        let mut values = Vec::with_capacity(keys.len());
        for key in keys {
            values.push(self.get(key).await?);
        }
        Ok(values)
    }

    /// Return exact values and a backend-authoritative timestamp. Remote
    /// backends override this to share the transaction/pipeline used by the
    /// read instead of paying a second round trip for lease validation time.
    async fn get_many_with_time(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        let now = self.server_time_ns().await?;
        Ok((self.get_many(keys).await?, now))
    }

    /// Return logical key/value pairs whose keys start with `prefix`.
    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<KvEntry>, WorkspaceError>;

    /// Atomically verify every exact-value condition and apply all writes.
    /// Returns `false` when any condition changed and the caller should retry.
    async fn compare_and_swap(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
    ) -> Result<bool, WorkspaceError>;

    /// Backend-authoritative wall-clock time for lease expiry.
    async fn server_time_ns(&self) -> Result<i64, WorkspaceError>;
}
