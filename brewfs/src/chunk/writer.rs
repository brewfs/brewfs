//! DataUploader: writes a slice payload into blocks without touching metadata.

use super::layout::ChunkLayout;
use super::slice::{SliceOffset, block_span_iter_slice};
use super::store::BlockStore;
use crate::meta::MetaLayer;
use crate::utils::NumCastExt;
use crate::vfs::backend::Backend;
use anyhow::Result;
use bytes::Bytes;
use std::sync::LazyLock;
use tokio::sync::Semaphore;

/// Foreground upload permits (flush/fsync) — higher priority, larger pool.
const FG_UPLOAD_PERMITS: usize = 192;
/// Background upload permits (compaction/warmup) — lower priority, smaller pool.
const BG_UPLOAD_PERMITS: usize = 64;
/// Commit-before-upload writeback permits — tiny pool to avoid starving reads.
const WRITEBACK_UPLOAD_PERMITS: usize = 1;

static FG_UPLOAD_SEM: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(FG_UPLOAD_PERMITS));
static BG_UPLOAD_SEM: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(BG_UPLOAD_PERMITS));
static WRITEBACK_UPLOAD_SEM: LazyLock<Semaphore> =
    LazyLock::new(|| Semaphore::new(writeback_upload_permits()));

fn writeback_upload_permits() -> usize {
    std::env::var("BREWFS_WRITEBACK_UPLOAD_CONCURRENCY")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|&value| value > 0)
        .unwrap_or(WRITEBACK_UPLOAD_PERMITS)
}

/// Acquire a foreground upload permit (flush/fsync path).
pub(crate) async fn fg_upload_permit() -> tokio::sync::SemaphorePermit<'static> {
    FG_UPLOAD_SEM
        .acquire()
        .await
        .expect("fg upload semaphore closed")
}

/// Acquire a background upload permit (compaction/GC).
pub(crate) async fn upload_permit() -> tokio::sync::SemaphorePermit<'static> {
    BG_UPLOAD_SEM
        .acquire()
        .await
        .expect("bg upload semaphore closed")
}

/// Acquire a writeback upload permit (commit-before-upload path).
pub(crate) async fn writeback_upload_permit() -> tokio::sync::SemaphorePermit<'static> {
    WRITEBACK_UPLOAD_SEM
        .acquire()
        .await
        .expect("writeback upload semaphore closed")
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum UploadPriority {
    Foreground,
    Writeback,
}

async fn upload_permit_for(priority: UploadPriority) -> tokio::sync::SemaphorePermit<'static> {
    match priority {
        UploadPriority::Foreground => fg_upload_permit().await,
        UploadPriority::Writeback => writeback_upload_permit().await,
    }
}

struct ChunkCursor<'a> {
    chunks: &'a [Bytes],
    idx: usize,
    off: usize,
}

impl<'a> ChunkCursor<'a> {
    fn new(chunks: &'a [Bytes]) -> Self {
        Self {
            chunks,
            idx: 0,
            off: 0,
        }
    }

    fn take(&mut self, mut need: usize) -> Vec<Bytes> {
        let mut out = Vec::new();

        while need > 0 {
            let chunk = &self.chunks[self.idx];
            let avail = chunk.len() - self.off;
            let take = need.min(avail);

            out.push(chunk.slice(self.off..self.off + take));
            self.off += take;
            need -= take;

            if self.off == chunk.len() {
                self.idx += 1;
                self.off = 0;
            }
        }
        out
    }
}

pub(crate) struct DataUploader<'a, B, M> {
    layout: ChunkLayout,
    backend: &'a Backend<B, M>,
}

impl<'a, B, M> DataUploader<'a, B, M>
where
    B: BlockStore + Sync,
    M: MetaLayer,
{
    pub(crate) fn new(layout: ChunkLayout, backend: &'a Backend<B, M>) -> Self {
        Self { layout, backend }
    }

    /// Write a slice from a set of byte segments without concatenating them.
    #[tracing::instrument(
        name = "DataUploader.write_at_vectored",
        level = "trace",
        skip(self, chunks),
        fields(slice_id, offset = offset.0,
        chunk_count = chunks.len(),
    ))]
    pub(crate) async fn write_at_vectored(
        &self,
        slice_id: u64,
        offset: SliceOffset,
        chunks: &[Bytes],
    ) -> Result<()> {
        self.write_at_vectored_with_priority(slice_id, offset, chunks, UploadPriority::Foreground)
            .await
    }

    pub(crate) async fn write_at_vectored_with_priority(
        &self,
        slice_id: u64,
        offset: SliceOffset,
        chunks: &[Bytes],
        priority: UploadPriority,
    ) -> Result<()> {
        let total_len = chunks.iter().map(|c| c.len()).sum::<usize>();

        let mut cursor = ChunkCursor::new(chunks);
        let mut futures = Vec::new();

        for span in block_span_iter_slice(offset, total_len as u64, self.layout) {
            let block_chunks = cursor.take(span.len.as_usize());

            let block_index = span.index.as_u32();
            let future = self.backend.store().write_fresh_vectored(
                (slice_id, block_index),
                span.offset,
                block_chunks,
            );
            futures.push(future);
        }

        // Bound total concurrent block uploads. Upload-before-commit flushes use
        // the foreground pool; commit-before-upload writeback uses a tiny pool so
        // foreground reads keep capacity. Compaction uses upload_permit() directly.
        let futures: Vec<_> = futures
            .into_iter()
            .map(|f| async move {
                let _p = upload_permit_for(priority).await;
                f.await
            })
            .collect();
        for res in futures_util::future::join_all(futures).await {
            res?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk::SliceDesc;
    use crate::chunk::layout::ChunkLayout;
    use crate::chunk::reader::DataFetcher;
    use crate::chunk::store::InMemoryBlockStore;
    use crate::meta::SLICE_ID_KEY;
    use crate::meta::factory::create_meta_store_from_url;
    use crate::vfs::backend::Backend;
    use bytes::Bytes;
    use std::sync::Arc;
    use std::sync::LazyLock as StdLazyLock;
    use tokio::sync::Mutex;
    use tokio::time::{Duration, timeout};

    static UPLOAD_PERMIT_TEST_LOCK: StdLazyLock<Mutex<()>> = StdLazyLock::new(|| Mutex::new(()));

    fn small_layout() -> ChunkLayout {
        ChunkLayout {
            chunk_size: 16 * 1024,
            block_size: 4 * 1024,
        }
    }

    fn patterned(len: usize, seed: u8) -> Vec<u8> {
        let mut buf = vec![0u8; len];
        for (i, b) in buf.iter_mut().enumerate() {
            *b = seed.wrapping_add(i as u8);
        }
        buf
    }

    #[tokio::test]
    async fn test_data_uploader_roundtrip() {
        let layout = small_layout();
        let store = Arc::new(InMemoryBlockStore::new());
        let meta = create_meta_store_from_url("sqlite::memory:")
            .await
            .unwrap()
            .layer();
        let backend = Arc::new(Backend::new(store.clone(), meta.clone()));

        let data = patterned(layout.block_size as usize + 512, 7);
        let offset = 512u64;
        let slice_id = meta.next_id(SLICE_ID_KEY).await.unwrap();

        let uploader = DataUploader::new(layout, backend.as_ref());
        uploader
            .write_at_vectored(
                slice_id as u64,
                0u64.into(),
                &[Bytes::copy_from_slice(&data)],
            )
            .await
            .unwrap();
        meta.append_slice(
            1,
            SliceDesc {
                slice_id: slice_id as u64,
                chunk_id: 1,
                offset,
                length: data.len() as u64,
            },
        )
        .await
        .unwrap();

        let mut fetcher = DataFetcher::new(layout, 1, backend.as_ref());
        fetcher.prepare_slices().await.unwrap();
        let out = fetcher.read_at(offset.into(), data.len()).await.unwrap();
        assert_eq!(out, data);
    }

    #[tokio::test]
    async fn test_data_uploader_vectored_roundtrip() {
        let layout = small_layout();
        let store = Arc::new(InMemoryBlockStore::new());
        let meta = create_meta_store_from_url("sqlite::memory:")
            .await
            .unwrap()
            .layer();
        let backend = Arc::new(Backend::new(store.clone(), meta.clone()));

        let offset = layout.block_size as u64 - 128;
        let part1 = patterned(300, 5);
        let part2 = patterned(700, 9);
        let part3 = patterned(500, 2);
        let mut data = Vec::new();
        data.extend_from_slice(&part1);
        data.extend_from_slice(&part2);
        data.extend_from_slice(&part3);

        let chunks = vec![Bytes::from(part1), Bytes::from(part2), Bytes::from(part3)];

        let slice_id = meta.next_id(SLICE_ID_KEY).await.unwrap();
        let uploader = DataUploader::new(layout, backend.as_ref());
        uploader
            .write_at_vectored(slice_id as u64, 0u64.into(), &chunks)
            .await
            .unwrap();
        meta.append_slice(
            8,
            SliceDesc {
                slice_id: slice_id as u64,
                chunk_id: 8,
                offset,
                length: data.len() as u64,
            },
        )
        .await
        .unwrap();

        let mut fetcher = DataFetcher::new(layout, 8, backend.as_ref());
        fetcher.prepare_slices().await.unwrap();
        let out = fetcher.read_at(offset.into(), data.len()).await.unwrap();
        assert_eq!(out, data);
    }

    #[tokio::test]
    async fn test_background_priority_waits_for_background_permits() {
        let _guard = UPLOAD_PERMIT_TEST_LOCK.lock().await;
        let mut background_permits = Vec::new();
        for _ in 0..BG_UPLOAD_PERMITS {
            background_permits.push(upload_permit().await);
        }

        let layout = small_layout();
        let store = Arc::new(InMemoryBlockStore::new());
        let meta = create_meta_store_from_url("sqlite::memory:")
            .await
            .unwrap()
            .layer();
        let backend = Arc::new(Backend::new(store.clone(), meta.clone()));
        let uploader = DataUploader::new(layout, backend.as_ref());
        let chunks = vec![Bytes::from(vec![1u8; layout.block_size as usize])];

        let background_result = timeout(Duration::from_millis(50), upload_permit()).await;
        assert!(
            background_result.is_err(),
            "background uploads should wait when background permits are exhausted"
        );

        timeout(
            Duration::from_secs(1),
            uploader.write_at_vectored(101, 0u64.into(), &chunks),
        )
        .await
        .expect("foreground upload should not wait on background permits")
        .expect("foreground upload should succeed");

        drop(background_permits);
        let resumed_permit = timeout(Duration::from_secs(1), upload_permit())
            .await
            .expect("background permit should resume after background permits are released");
        drop(resumed_permit);
    }

    #[tokio::test]
    async fn test_writeback_priority_waits_for_writeback_permits_only() {
        let _guard = UPLOAD_PERMIT_TEST_LOCK.lock().await;
        let mut writeback_permits = Vec::new();
        for _ in 0..WRITEBACK_UPLOAD_PERMITS {
            writeback_permits.push(writeback_upload_permit().await);
        }

        let layout = small_layout();
        let store = Arc::new(InMemoryBlockStore::new());
        let meta = create_meta_store_from_url("sqlite::memory:")
            .await
            .unwrap()
            .layer();
        let backend = Arc::new(Backend::new(store.clone(), meta.clone()));
        let uploader = DataUploader::new(layout, backend.as_ref());
        let chunks = vec![Bytes::from(vec![1u8; layout.block_size as usize])];

        let writeback_result = timeout(
            Duration::from_millis(50),
            uploader.write_at_vectored_with_priority(
                103,
                0u64.into(),
                &chunks,
                UploadPriority::Writeback,
            ),
        )
        .await;
        assert!(
            writeback_result.is_err(),
            "writeback uploads should wait when writeback permits are exhausted"
        );

        timeout(
            Duration::from_secs(1),
            uploader.write_at_vectored(104, 0u64.into(), &chunks),
        )
        .await
        .expect("foreground upload should not wait on writeback permits")
        .expect("foreground upload should succeed");

        drop(writeback_permits);
        timeout(
            Duration::from_secs(1),
            uploader.write_at_vectored_with_priority(
                105,
                0u64.into(),
                &chunks,
                UploadPriority::Writeback,
            ),
        )
        .await
        .expect("writeback upload should resume after writeback permits are released")
        .expect("writeback upload should succeed");
    }
}
