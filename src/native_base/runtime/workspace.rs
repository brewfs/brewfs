use std::sync::Arc;

use async_trait::async_trait;

use crate::chunk::read_plan::{WorkspaceReadPlanProvider, execute_into};
use crate::chunk::{BlockStore, ChunkLayout};
use crate::meta::MetaLayer;

use super::{BaseDataSource, NativeIoError};

/// Reads the immutable workspace-v1 view that a native volume was created
/// from. Native writes never mutate this source; they are resolved by the
/// runtime after the baseline has been filled.
pub struct WorkspaceBaseDataSource<S, M> {
    store: Arc<S>,
    meta: Arc<M>,
    layout: ChunkLayout,
}

impl<S, M> WorkspaceBaseDataSource<S, M> {
    pub fn new(store: Arc<S>, meta: Arc<M>, layout: ChunkLayout) -> Self {
        Self {
            store,
            meta,
            layout,
        }
    }
}

#[async_trait]
impl<S, M> BaseDataSource for WorkspaceBaseDataSource<S, M>
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaLayer + WorkspaceReadPlanProvider + Send + Sync + 'static,
{
    async fn size(&self, inode: u64) -> Result<u64, NativeIoError> {
        let inode = i64::try_from(inode)
            .map_err(|_| NativeIoError::Invalid("inode does not fit metadata domain".into()))?;
        let attr = self
            .meta
            .stat(inode)
            .await
            .map_err(|error| NativeIoError::Base(error.to_string()))?
            .ok_or_else(|| NativeIoError::Base(format!("inode {inode} is missing")))?;
        Ok(attr.size)
    }

    async fn read_exact(
        &self,
        inode: u64,
        offset: u64,
        output: &mut [u8],
    ) -> Result<(), NativeIoError> {
        if output.is_empty() {
            return Ok(());
        }
        let end = offset
            .checked_add(output.len() as u64)
            .ok_or(NativeIoError::RangeOverflow)?;
        let size = self.size(inode).await?;
        if end > size {
            return Err(NativeIoError::Base(format!(
                "baseline read [{offset}, {end}) exceeds inode size {size}"
            )));
        }
        let inode = i64::try_from(inode)
            .map_err(|_| NativeIoError::Invalid("inode does not fit metadata domain".into()))?;
        let mut cursor = offset;
        while cursor < end {
            let chunk_index = cursor / self.layout.chunk_size;
            let chunk_start = chunk_index * self.layout.chunk_size;
            let local_offset = cursor - chunk_start;
            let take = (end - cursor).min(self.layout.chunk_size - local_offset);
            let output_start =
                usize::try_from(cursor - offset).map_err(|_| NativeIoError::RangeOverflow)?;
            let output_end = output_start
                .checked_add(usize::try_from(take).map_err(|_| NativeIoError::RangeOverflow)?)
                .ok_or(NativeIoError::RangeOverflow)?;
            let plan = self
                .meta
                .read_plan(inode, chunk_index, local_offset, take)
                .await
                .map_err(|error| NativeIoError::Base(error.to_string()))?;
            execute_into(
                self.store.as_ref(),
                self.layout,
                local_offset,
                &plan,
                &mut output[output_start..output_end],
            )
            .await
            .map_err(|error| NativeIoError::Base(error.to_string()))?;
            cursor += take;
        }
        Ok(())
    }
}
