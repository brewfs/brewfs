//! Keep both CLI mount modes in one generic dispatch without widening runtime IO.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use async_trait::async_trait;
use bytes::Bytes;

use crate::cadapter::read_observer::TerminalGuard;
use crate::chunk::runtime_store::RuntimeBlockStore;
use crate::chunk::store::{BlockKey, BlockStore, ObjectStoreMetrics};

// This module and its type are private to the CLI. The operator variant retains
// the original opaque runtime wrapper; there is no inner-store recovery path.
pub(super) enum MountStore<S> {
    Standalone(S),
    Operator(RuntimeBlockStore<S>),
}

#[async_trait]
impl<S: BlockStore + Send + Sync> BlockStore for MountStore<S> {
    fn set_data_bucket(&self, bucket: &str) -> anyhow::Result<()> {
        match self {
            Self::Standalone(store) => store.set_data_bucket(bucket),
            Self::Operator(store) => store.set_data_bucket(bucket),
        }
    }

    async fn write_fresh_vectored(
        &self,
        key: BlockKey,
        offset: u64,
        chunks: Vec<Bytes>,
    ) -> anyhow::Result<u64> {
        match self {
            Self::Standalone(store) => store.write_fresh_vectored(key, offset, chunks).await,
            Self::Operator(store) => store.write_fresh_vectored(key, offset, chunks).await,
        }
    }

    async fn write_fresh_range(
        &self,
        key: BlockKey,
        offset: u64,
        data: &[u8],
    ) -> anyhow::Result<u64> {
        match self {
            Self::Standalone(store) => store.write_fresh_range(key, offset, data).await,
            Self::Operator(store) => store.write_fresh_range(key, offset, data).await,
        }
    }

    async fn read_range(&self, key: BlockKey, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
        match self {
            Self::Standalone(store) => store.read_range(key, offset, buf).await,
            Self::Operator(store) => store.read_range(key, offset, buf).await,
        }
    }

    async fn delete_range(&self, key: BlockKey, block_count: u64) -> anyhow::Result<()> {
        match self {
            Self::Standalone(store) => store.delete_range(key, block_count).await,
            Self::Operator(store) => store.delete_range(key, block_count).await,
        }
    }

    async fn retain_gc_slice_upper_bound(
        &self,
        slice_id: u64,
        slice_end: u64,
    ) -> anyhow::Result<()> {
        match self {
            Self::Standalone(store) => store.retain_gc_slice_upper_bound(slice_id, slice_end).await,
            Self::Operator(store) => store.retain_gc_slice_upper_bound(slice_id, slice_end).await,
        }
    }

    async fn gc_slice_upper_bound(&self, slice_id: u64, observed_end: u64) -> anyhow::Result<u64> {
        match self {
            Self::Standalone(store) => store.gc_slice_upper_bound(slice_id, observed_end).await,
            Self::Operator(store) => store.gc_slice_upper_bound(slice_id, observed_end).await,
        }
    }

    async fn cache_block(&self, key: BlockKey, data: &[u8]) -> anyhow::Result<()> {
        match self {
            Self::Standalone(store) => store.cache_block(key, data).await,
            Self::Operator(store) => store.cache_block(key, data).await,
        }
    }

    fn begin_read_operation(&self, requested: u64) -> Option<TerminalGuard> {
        match self {
            Self::Standalone(store) => store.begin_read_operation(requested),
            Self::Operator(store) => store.begin_read_operation(requested),
        }
    }

    fn cache_counters(&self) -> (Option<Arc<AtomicU64>>, Option<Arc<AtomicU64>>) {
        match self {
            Self::Standalone(store) => store.cache_counters(),
            Self::Operator(store) => store.cache_counters(),
        }
    }

    fn object_store_metrics(&self) -> Option<Arc<ObjectStoreMetrics>> {
        match self {
            Self::Standalone(store) => store.object_store_metrics(),
            Self::Operator(store) => store.object_store_metrics(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[derive(Default)]
    struct Probe {
        bound: AtomicU64,
        deletes: Arc<AtomicU64>,
    }

    #[async_trait]
    impl BlockStore for Probe {
        async fn write_fresh_vectored(
            &self,
            _: BlockKey,
            _: u64,
            chunks: Vec<Bytes>,
        ) -> anyhow::Result<u64> {
            assert_eq!(chunks.len(), 2, "preserve the original vector of chunks");
            Ok(chunks.iter().map(|chunk| chunk.len() as u64).sum())
        }

        async fn write_fresh_range(&self, _: BlockKey, _: u64, _: &[u8]) -> anyhow::Result<u64> {
            panic!("vectored mount writes must not fall back to concatenation")
        }

        async fn read_range(&self, _: BlockKey, _: u64, _: &mut [u8]) -> anyhow::Result<()> {
            Ok(())
        }

        async fn delete_range(&self, _: BlockKey, _: u64) -> anyhow::Result<()> {
            self.deletes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn retain_gc_slice_upper_bound(&self, _: u64, end: u64) -> anyhow::Result<()> {
            self.bound.fetch_max(end, Ordering::SeqCst);
            Ok(())
        }

        async fn gc_slice_upper_bound(&self, _: u64, observed: u64) -> anyhow::Result<u64> {
            Ok(self.bound.load(Ordering::SeqCst).max(observed))
        }
    }

    #[tokio::test]
    async fn standalone_preserves_durable_gc_range_and_delete() {
        let probe = Probe::default();
        let deletes = probe.deletes.clone();
        let store = MountStore::Standalone(probe);
        store.retain_gc_slice_upper_bound(7, 8192).await.unwrap();
        store.retain_gc_slice_upper_bound(7, 4096).await.unwrap();
        assert_eq!(store.gc_slice_upper_bound(7, 1024).await.unwrap(), 8192);
        store.delete_range((7, 0), 2).await.unwrap();
        assert_eq!(deletes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn operator_preserves_runtime_denial_and_vectored_writes() {
        let probe = Probe::default();
        let deletes = probe.deletes.clone();
        let store = MountStore::Operator(RuntimeBlockStore::new(probe));
        assert!(store.delete_range((7, 0), 2).await.is_err());
        assert_eq!(deletes.load(Ordering::SeqCst), 0);
        assert!(store.retain_gc_slice_upper_bound(7, 8192).await.is_err());
        assert!(store.gc_slice_upper_bound(7, 1024).await.is_err());
        assert_eq!(
            store
                .write_fresh_vectored(
                    (7, 0),
                    0,
                    vec![Bytes::from_static(b"ab"), Bytes::from_static(b"c")],
                )
                .await
                .unwrap(),
            3
        );
    }
}
