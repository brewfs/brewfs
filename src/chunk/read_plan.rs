//! Workspace-neutral resolved read plans and their block-store executor.

use async_trait::async_trait;
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;

use super::{BlockStore, ChunkLayout, SliceOffset, block_span_iter_slice};
use crate::meta::store::MetaError;
use crate::utils::NumCastExt;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReadPlanSegment {
    Data {
        logical_offset: u64,
        length: u64,
        slice_id: u64,
        slice_offset: u64,
    },
    Zero {
        logical_offset: u64,
        length: u64,
    },
}

impl ReadPlanSegment {
    fn logical_offset(&self) -> u64 {
        match self {
            Self::Data { logical_offset, .. } | Self::Zero { logical_offset, .. } => {
                *logical_offset
            }
        }
    }

    fn length(&self) -> u64 {
        match self {
            Self::Data { length, .. } | Self::Zero { length, .. } => *length,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ResolvedReadPlan {
    pub segments: Vec<ReadPlanSegment>,
}

#[async_trait]
pub trait WorkspaceReadPlanProvider: Send + Sync {
    async fn read_plan(
        &self,
        ino: i64,
        chunk_index: u64,
        offset: u64,
        len: u64,
    ) -> Result<ResolvedReadPlan, MetaError>;

    async fn range_has_data(&self, ino: i64, offset: u64, len: u64) -> Result<bool, MetaError>;

    /// Persist an uploaded slice that could not be attached to the workspace head.
    ///
    /// Providers used only by tests may keep the no-op default. Production workspace
    /// providers override this so GC can reclaim data uploaded before a fencing failure.
    async fn record_orphan_slice(&self, _slice_id: u64, _slice_end: u64) -> Result<(), MetaError> {
        Ok(())
    }

    /// Replace a file range with logical zeroes in the effective workspace view.
    /// `keep_size` implements `FALLOC_FL_KEEP_SIZE`; otherwise the range may extend EOF.
    async fn apply_hole_range(
        &self,
        _ino: i64,
        _offset: u64,
        _len: u64,
        _keep_size: bool,
    ) -> Result<u64, MetaError> {
        Err(MetaError::NotSupported(
            "workspace hole mutations are unavailable".into(),
        ))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReadPlanError {
    #[error("invalid read plan: {0}")]
    Invalid(String),
    #[error(transparent)]
    Backend(#[from] anyhow::Error),
}

struct SendBuf {
    ptr: *mut u8,
    len: usize,
}

// SAFETY: `execute_into` creates each SendBuf from a disjoint output range and
// waits for every future before using the output again.
unsafe impl Send for SendBuf {}

impl SendBuf {
    unsafe fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: upheld by the constructor site in `execute_into`.
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }
}

pub async fn execute_into<B: BlockStore + Sync>(
    store: &B,
    layout: ChunkLayout,
    requested_offset: u64,
    plan: &ResolvedReadPlan,
    output: &mut [u8],
) -> Result<(), ReadPlanError> {
    let requested_len = u64::try_from(output.len())
        .map_err(|_| ReadPlanError::Invalid("output length exceeds u64".into()))?;
    let requested_end = requested_offset
        .checked_add(requested_len)
        .ok_or_else(|| ReadPlanError::Invalid("requested range overflows".into()))?;

    let mut previous_end = requested_offset;
    for segment in &plan.segments {
        let start = segment.logical_offset();
        let length = segment.length();
        if length == 0 {
            return Err(ReadPlanError::Invalid("zero-length segment".into()));
        }
        let end = start
            .checked_add(length)
            .ok_or_else(|| ReadPlanError::Invalid("segment range overflows".into()))?;
        if start < requested_offset || end > requested_end {
            return Err(ReadPlanError::Invalid(
                "segment lies outside the requested range".into(),
            ));
        }
        if start < previous_end {
            return Err(ReadPlanError::Invalid(
                "segments overlap or are not sorted".into(),
            ));
        }
        previous_end = end;
        if let ReadPlanSegment::Data {
            slice_offset,
            length,
            ..
        } = segment
        {
            slice_offset
                .checked_add(*length)
                .ok_or_else(|| ReadPlanError::Invalid("slice range overflows".into()))?;
        }
    }

    output.fill(0);
    let mut futures = FuturesUnordered::new();
    for segment in &plan.segments {
        let ReadPlanSegment::Data {
            logical_offset,
            length,
            slice_id,
            slice_offset,
        } = *segment
        else {
            continue;
        };
        let output_start = usize::try_from(logical_offset - requested_offset)
            .map_err(|_| ReadPlanError::Invalid("output offset exceeds usize".into()))?;
        let output_len = usize::try_from(length)
            .map_err(|_| ReadPlanError::Invalid("segment length exceeds usize".into()))?;
        let output_end = output_start
            .checked_add(output_len)
            .ok_or_else(|| ReadPlanError::Invalid("output range overflows".into()))?;
        let segment_output = &mut output[output_start..output_end];
        let mut consumed = 0usize;
        for block in block_span_iter_slice(SliceOffset::from(slice_offset), length, layout) {
            let take = block.len.as_usize();
            let block_end = consumed
                .checked_add(take)
                .ok_or_else(|| ReadPlanError::Invalid("block span overflows".into()))?;
            let block_output = &mut segment_output[consumed..block_end];
            consumed = block_end;
            let mut send_buf = SendBuf {
                ptr: block_output.as_mut_ptr(),
                len: block_output.len(),
            };
            let key = (slice_id, block.index.as_u32());
            let block_offset = block.offset;
            futures.push(async move {
                // SAFETY: block spans and plan segments were validated as disjoint.
                store
                    .read_range(key, block_offset, unsafe { send_buf.as_mut_slice() })
                    .await
            });
        }
        if consumed != output_len {
            return Err(ReadPlanError::Invalid(
                "block spans do not cover the data segment".into(),
            ));
        }
    }
    while let Some(result) = futures.next().await {
        result?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use async_trait::async_trait;
    use bytes::Bytes;
    use tokio::sync::Mutex;

    use super::*;
    use crate::chunk::store::BlockKey;

    #[derive(Default)]
    struct TestStore {
        blocks: HashMap<BlockKey, Vec<u8>>,
        reads: Arc<Mutex<Vec<BlockKey>>>,
    }

    #[async_trait]
    impl BlockStore for TestStore {
        async fn write_fresh_range(
            &self,
            _key: BlockKey,
            _offset: u64,
            _data: &[u8],
        ) -> anyhow::Result<u64> {
            anyhow::bail!("unused")
        }

        async fn write_fresh_vectored(
            &self,
            _key: BlockKey,
            _offset: u64,
            _chunks: Vec<Bytes>,
        ) -> anyhow::Result<u64> {
            anyhow::bail!("unused")
        }

        async fn read_range(
            &self,
            key: BlockKey,
            offset: u64,
            buf: &mut [u8],
        ) -> anyhow::Result<()> {
            self.reads.lock().await.push(key);
            let block = self
                .blocks
                .get(&key)
                .ok_or_else(|| anyhow::anyhow!("missing"))?;
            let start = usize::try_from(offset)?;
            buf.copy_from_slice(&block[start..start + buf.len()]);
            Ok(())
        }

        async fn delete_range(&self, _key: BlockKey, _block_count: u64) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn executor_reads_only_data_segments_and_preserves_zeroes() {
        let mut store = TestStore::default();
        store.blocks.insert((7, 0), b"abcdefgh".to_vec());
        let plan = ResolvedReadPlan {
            segments: vec![
                ReadPlanSegment::Zero {
                    logical_offset: 2,
                    length: 2,
                },
                ReadPlanSegment::Data {
                    logical_offset: 4,
                    length: 3,
                    slice_id: 7,
                    slice_offset: 1,
                },
                ReadPlanSegment::Zero {
                    logical_offset: 7,
                    length: 1,
                },
            ],
        };
        let mut output = [9; 6];
        execute_into(
            &store,
            ChunkLayout {
                chunk_size: 16,
                block_size: 8,
            },
            2,
            &plan,
            &mut output,
        )
        .await
        .unwrap();
        assert_eq!(&output, b"\0\0bcd\0");
        assert_eq!(store.reads.lock().await.as_slice(), &[(7, 0)]);
    }

    #[tokio::test]
    async fn executor_rejects_overlap_and_out_of_range_before_io() {
        let store = TestStore::default();
        let cases = [
            ResolvedReadPlan {
                segments: vec![
                    ReadPlanSegment::Zero {
                        logical_offset: 0,
                        length: 3,
                    },
                    ReadPlanSegment::Zero {
                        logical_offset: 2,
                        length: 1,
                    },
                ],
            },
            ResolvedReadPlan {
                segments: vec![ReadPlanSegment::Data {
                    logical_offset: 3,
                    length: 2,
                    slice_id: 1,
                    slice_offset: 0,
                }],
            },
        ];
        for plan in cases {
            let mut output = [0; 4];
            assert!(
                execute_into(&store, ChunkLayout::default(), 0, &plan, &mut output)
                    .await
                    .is_err()
            );
        }
        assert!(store.reads.lock().await.is_empty());
    }
}
