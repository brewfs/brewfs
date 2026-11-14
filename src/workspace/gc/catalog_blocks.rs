use std::sync::Arc;

use crate::chunk::{BlockKey, BlockStore};
use crate::workspace::catalog::WorkspaceStore;
use async_trait::async_trait;

/// GC-only view: range bounds live with the catalog across mount restarts.
/// Physical deletion remains behind WorkspaceGc's durable slice reservation.
pub struct CatalogGcBlocks<W, B> {
    catalog: Arc<W>,
    blocks: Arc<B>,
}

impl<W, B> CatalogGcBlocks<W, B> {
    pub fn new(catalog: Arc<W>, blocks: Arc<B>) -> Self {
        Self { catalog, blocks }
    }
}

#[async_trait]
impl<W: WorkspaceStore, B: BlockStore + Send + Sync> BlockStore for CatalogGcBlocks<W, B> {
    async fn write_fresh_range(&self, _: BlockKey, _: u64, _: &[u8]) -> anyhow::Result<u64> {
        anyhow::bail!("GC block view cannot write file data")
    }

    async fn read_range(&self, _: BlockKey, _: u64, _: &mut [u8]) -> anyhow::Result<()> {
        anyhow::bail!("GC block view cannot read file data")
    }

    async fn delete_range(&self, key: BlockKey, count: u64) -> anyhow::Result<()> {
        self.blocks.delete_range(key, count).await
    }

    async fn retain_gc_slice_upper_bound(&self, slice: u64, end: u64) -> anyhow::Result<()> {
        Ok(self.catalog.retain_gc_slice_bound(slice, end).await?)
    }

    async fn gc_slice_upper_bound(&self, slice: u64, observed: u64) -> anyhow::Result<u64> {
        Ok(self.catalog.read_gc_slice_bound(slice, observed).await?)
    }
}
