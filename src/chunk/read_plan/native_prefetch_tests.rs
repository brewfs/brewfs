use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use async_trait::async_trait;

use super::{ReadPlanError, ReadPlanSegment, ResolvedReadPlan, warm_resolved};
use crate::chunk::store::BlockKey;
use crate::chunk::{BlockStore, ChunkLayout};

type WarmCall = (BlockKey, u64, usize);

#[derive(Default)]
struct RecordingStore {
    warm_calls: Mutex<Vec<WarmCall>>,
    read_calls: AtomicUsize,
    fail_warm: AtomicBool,
}

impl RecordingStore {
    fn warm_calls(&self) -> Vec<WarmCall> {
        self.warm_calls.lock().unwrap().clone()
    }
}

#[async_trait]
impl BlockStore for RecordingStore {
    async fn write_fresh_range(
        &self,
        _key: BlockKey,
        _offset: u64,
        data: &[u8],
    ) -> anyhow::Result<u64> {
        Ok(data.len() as u64)
    }

    async fn read_range(
        &self,
        _key: BlockKey,
        _offset: u64,
        output: &mut [u8],
    ) -> anyhow::Result<()> {
        self.read_calls.fetch_add(1, Ordering::SeqCst);
        output.fill(0);
        Ok(())
    }

    async fn warm_range(&self, key: BlockKey, offset: u64, len: usize) -> anyhow::Result<()> {
        self.warm_calls.lock().unwrap().push((key, offset, len));
        if self.fail_warm.load(Ordering::SeqCst) {
            anyhow::bail!("warm range failed");
        }
        Ok(())
    }

    async fn delete_range(&self, _key: BlockKey, _block_count: u64) -> anyhow::Result<()> {
        Ok(())
    }
}

fn layout() -> ChunkLayout {
    ChunkLayout {
        chunk_size: 64,
        block_size: 8,
    }
}

#[tokio::test]
async fn warm_resolved_skips_holes_and_maps_slice_offsets_to_block_ranges() {
    let store = RecordingStore::default();
    let plan = ResolvedReadPlan {
        segments: vec![
            ReadPlanSegment::Zero {
                logical_offset: 10,
                length: 4,
            },
            ReadPlanSegment::Data {
                logical_offset: 14,
                length: 16,
                slice_id: 7,
                slice_offset: 5,
            },
            ReadPlanSegment::Zero {
                logical_offset: 30,
                length: 4,
            },
            ReadPlanSegment::Data {
                logical_offset: 34,
                length: 16,
                slice_id: 9,
                slice_offset: 15,
            },
        ],
    };

    warm_resolved(&store, layout(), 10, 40, &plan)
        .await
        .unwrap();

    let mut actual = store.warm_calls();
    actual.sort_unstable();
    assert_eq!(
        actual,
        vec![
            ((7, 0), 5, 3),
            ((7, 1), 0, 8),
            ((7, 2), 0, 5),
            ((9, 1), 7, 1),
            ((9, 2), 0, 8),
            ((9, 3), 0, 7),
        ]
    );
    assert_eq!(store.read_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn warm_resolved_rejects_invalid_ranges_before_store_io() {
    let store = RecordingStore::default();
    let cases = [
        (
            0,
            16,
            ResolvedReadPlan {
                segments: vec![
                    ReadPlanSegment::Data {
                        logical_offset: 0,
                        length: 8,
                        slice_id: 1,
                        slice_offset: 0,
                    },
                    ReadPlanSegment::Zero {
                        logical_offset: 4,
                        length: 4,
                    },
                ],
            },
        ),
        (
            0,
            8,
            ResolvedReadPlan {
                segments: vec![ReadPlanSegment::Data {
                    logical_offset: 7,
                    length: 2,
                    slice_id: 1,
                    slice_offset: 0,
                }],
            },
        ),
        (u64::MAX - 1, 4, ResolvedReadPlan::default()),
        (
            0,
            u64::MAX,
            ResolvedReadPlan {
                segments: vec![ReadPlanSegment::Data {
                    logical_offset: u64::MAX - 1,
                    length: 2,
                    slice_id: 1,
                    slice_offset: 0,
                }],
            },
        ),
        (
            0,
            8,
            ResolvedReadPlan {
                segments: vec![ReadPlanSegment::Data {
                    logical_offset: 0,
                    length: 8,
                    slice_id: 1,
                    slice_offset: u64::MAX - 2,
                }],
            },
        ),
        (
            0,
            1,
            ResolvedReadPlan {
                segments: vec![ReadPlanSegment::Data {
                    logical_offset: 0,
                    length: 1,
                    slice_id: 1,
                    slice_offset: (u64::from(u32::MAX) + 1) * 8,
                }],
            },
        ),
    ];

    for (offset, len, plan) in cases {
        assert!(matches!(
            warm_resolved(&store, layout(), offset, len, &plan).await,
            Err(ReadPlanError::Invalid(_))
        ));
    }

    assert!(store.warm_calls().is_empty());
    assert_eq!(store.read_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn warm_resolved_propagates_backend_errors() {
    let store = RecordingStore::default();
    store.fail_warm.store(true, Ordering::SeqCst);
    let plan = ResolvedReadPlan {
        segments: vec![ReadPlanSegment::Data {
            logical_offset: 0,
            length: 1,
            slice_id: 3,
            slice_offset: 0,
        }],
    };

    let error = warm_resolved(&store, layout(), 0, 1, &plan)
        .await
        .unwrap_err();

    assert!(matches!(error, ReadPlanError::Backend(_)));
    assert!(error.to_string().contains("warm range failed"));
    assert_eq!(store.warm_calls().len(), 1);
    assert_eq!(store.read_calls.load(Ordering::SeqCst), 0);
}
