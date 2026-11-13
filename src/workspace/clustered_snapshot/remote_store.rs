//! Read-only block-store adapter for a clustered v2 snapshot.
//!
//! The VFS still speaks in `(SliceId, block_index)` requests.  v2 stores the
//! slice bytes in authenticated DataPack frames instead of ordinary chunk
//! objects, so this adapter translates the block-relative offset into a
//! logical Data Seal slice read.

use std::sync::Arc;

use async_trait::async_trait;

use crate::cadapter::client::ObjectBackend;
use crate::chunk::store::{BlockKey, BlockStore};
use crate::workspace::clustered_snapshot::remote_union::RemoteSnapshot;

/// A read-only [`BlockStore`] backed by one immutable clustered snapshot.
#[derive(Clone)]
pub struct RemoteDataBlockStore<B: ObjectBackend + Clone> {
    snapshot: Arc<RemoteSnapshot<B>>,
    block_size: u64,
}

impl<B: ObjectBackend + Clone> RemoteDataBlockStore<B> {
    /// Create an adapter using the block size of the VFS `ChunkLayout`.
    pub fn new(snapshot: Arc<RemoteSnapshot<B>>, block_size: u32) -> Self {
        Self {
            snapshot,
            block_size: u64::from(block_size),
        }
    }

    pub fn snapshot(&self) -> &Arc<RemoteSnapshot<B>> {
        &self.snapshot
    }
}

#[async_trait]
impl<B: ObjectBackend + Clone + 'static> BlockStore for RemoteDataBlockStore<B> {
    async fn write_fresh_range(
        &self,
        _key: BlockKey,
        _offset: u64,
        _data: &[u8],
    ) -> anyhow::Result<u64> {
        anyhow::bail!("clustered v2 snapshot is read-only")
    }

    async fn read_range(&self, key: BlockKey, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
        if buf.is_empty() {
            return Ok(());
        }
        let block_offset = u64::from(key.1)
            .checked_mul(self.block_size)
            .and_then(|base| base.checked_add(offset))
            .ok_or_else(|| anyhow::anyhow!("packed block offset overflows u64"))?;
        self.snapshot
            .read_registered_data_slice(key.0, block_offset, buf)
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))
    }

    async fn delete_range(&self, _key: BlockKey, _block_count: u64) -> anyhow::Result<()> {
        anyhow::bail!("clustered v2 snapshot is read-only")
    }
}
