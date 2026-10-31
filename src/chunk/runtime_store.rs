//! Object IO held by an operator mount. The underlying store is private and
//! cannot be recovered or widened into a deletion-capable interface.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use async_trait::async_trait;
use bytes::Bytes;

use super::store::{BlockKey, BlockStore, ObjectStoreMetrics};
use crate::cadapter::read_observer::TerminalGuard;

pub struct RuntimeBlockStore<S> {
    inner: S,
}

impl<S> RuntimeBlockStore<S> {
    pub fn new(inner: S) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl<S: BlockStore + Send + Sync> BlockStore for RuntimeBlockStore<S> {
    fn set_data_bucket(&self, bucket: &str) -> anyhow::Result<()> {
        self.inner.set_data_bucket(bucket)
    }

    async fn write_fresh_vectored(
        &self,
        key: BlockKey,
        offset: u64,
        chunks: Vec<Bytes>,
    ) -> anyhow::Result<u64> {
        self.inner.write_fresh_vectored(key, offset, chunks).await
    }

    async fn write_fresh_range(
        &self,
        key: BlockKey,
        offset: u64,
        data: &[u8],
    ) -> anyhow::Result<u64> {
        self.inner.write_fresh_range(key, offset, data).await
    }

    async fn read_range(&self, key: BlockKey, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
        self.inner.read_range(key, offset, buf).await
    }

    async fn delete_range(&self, _key: BlockKey, _block_count: u64) -> anyhow::Result<()> {
        anyhow::bail!("operator runtime does not hold object deletion authority")
    }

    async fn cache_block(&self, key: BlockKey, data: &[u8]) -> anyhow::Result<()> {
        self.inner.cache_block(key, data).await
    }

    fn begin_read_operation(&self, requested: u64) -> Option<TerminalGuard> {
        self.inner.begin_read_operation(requested)
    }

    fn cache_counters(&self) -> (Option<Arc<AtomicU64>>, Option<Arc<AtomicU64>>) {
        self.inner.cache_counters()
    }

    fn object_store_metrics(&self) -> Option<Arc<ObjectStoreMetrics>> {
        self.inner.object_store_metrics()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    struct Probe(Arc<AtomicU64>);
    #[async_trait]
    impl BlockStore for Probe {
        async fn write_fresh_vectored(
            &self,
            _: BlockKey,
            _: u64,
            chunks: Vec<Bytes>,
        ) -> anyhow::Result<u64> {
            Ok(chunks.iter().map(|chunk| chunk.len() as u64).sum())
        }
        async fn write_fresh_range(&self, _: BlockKey, _: u64, _: &[u8]) -> anyhow::Result<u64> {
            panic!("vectored runtime writes must not fall back to concatenation")
        }
        async fn read_range(&self, _: BlockKey, _: u64, _: &mut [u8]) -> anyhow::Result<()> {
            Ok(())
        }
        async fn delete_range(&self, _: BlockKey, _: u64) -> anyhow::Result<()> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn runtime_denies_deletion_without_dispatch_and_keeps_vectored_writes() {
        let deletes = Arc::new(AtomicU64::new(0));
        let runtime = RuntimeBlockStore::new(Probe(deletes.clone()));
        let key = (1, 0);
        assert!(runtime.delete_range(key, 1).await.is_err());
        assert_eq!(deletes.load(Ordering::SeqCst), 0);
        assert_eq!(
            runtime
                .write_fresh_vectored(
                    key,
                    0,
                    vec![Bytes::from_static(b"ab"), Bytes::from_static(b"c")]
                )
                .await
                .unwrap(),
            3
        );
    }
}
