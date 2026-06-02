//! DataFetcher: fetch data from blocks according to offset/length, handling gaps with zeros.

use super::layout::ChunkLayout;
use super::slice::{ChunkOffset, SliceDesc, SliceOffset, block_span_iter_slice};
use super::store::BlockStore;
use crate::meta::MetaLayer;
use crate::utils::Intervals;
use crate::utils::NumCastExt;
use crate::vfs::backend::Backend;
use anyhow::{Result, ensure};
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use std::cmp::{max, min};
use tracing::Instrument;

/// A Send-able wrapper around a mutable buffer pointer.
///
/// SAFETY: The caller must guarantee that:
/// 1. The pointed-to memory is valid for the lifetime of any future using this.
/// 2. No two futures share overlapping regions (exclusive access).
struct SendBuf {
    ptr: *mut u8,
    len: usize,
}

unsafe impl Send for SendBuf {}

impl SendBuf {
    /// SAFETY: caller must ensure exclusive access and valid lifetime.
    unsafe fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }
}

pub(crate) struct DataFetcher<'a, B, M> {
    layout: ChunkLayout,
    id: u64,
    slices: Vec<SliceDesc>,
    prepared: bool,
    backend: &'a Backend<B, M>,
}

impl<'a, B, M> DataFetcher<'a, B, M>
where
    B: BlockStore,
    M: MetaLayer,
{
    pub(crate) fn new(layout: ChunkLayout, id: u64, backend: &'a Backend<B, M>) -> Self {
        Self {
            layout,
            id,
            backend,
            prepared: false,
            slices: Vec::new(),
        }
    }

    /// Create a DataFetcher with pre-fetched slice metadata, skipping
    /// the meta.get_slices() call entirely.  Used by FileReader's
    /// per-handle chunk→slice cache for repeated reads within the same chunk.
    pub(crate) fn with_slices(
        layout: ChunkLayout,
        id: u64,
        backend: &'a Backend<B, M>,
        slices: Vec<SliceDesc>,
    ) -> Self {
        Self {
            layout,
            id,
            backend,
            prepared: true,
            slices,
        }
    }

    /// Consume the fetcher and return the cached slice list so callers
    /// can reuse it for subsequent reads of the same chunk.
    pub(crate) fn into_slices(self) -> Vec<SliceDesc> {
        self.slices
    }

    pub(crate) async fn prepare_slices(&mut self) -> Result<()> {
        let chunk_id = self.id;
        let backend = self.backend;
        let slices = async {
            let slices = backend.meta().get_slices(chunk_id).await?;
            tracing::Span::current().record("slice_count", slices.len());
            Ok::<_, anyhow::Error>(slices)
        }
        .instrument(tracing::trace_span!(
            "fetch.prepare_slices",
            chunk_id,
            slice_count = tracing::field::Empty
        ))
        .await?;

        self.slices = slices;
        self.prepared = true;
        Ok(())
    }

    #[tracing::instrument(
        name = "DataFetcher.read_at",
        level = "trace",
        skip(self),
        fields(chunk_id = self.id, offset = offset.0, len, need_reads = tracing::field::Empty)
    )]
    pub(crate) async fn read_at(&mut self, offset: ChunkOffset, len: usize) -> Result<Vec<u8>> {
        let offset = offset.get();
        if len == 0 {
            return Ok(Vec::new());
        }
        ensure!(
            self.prepared,
            "DataFetcher::read_at requires prepare_slices() to run first"
        );

        let mut buf = vec![0; len];

        let need_read = tracing::trace_span!("fetch.read_at.build_need_read", offset, len)
            .in_scope(|| {
                let mut intervals = Intervals::new(offset, offset + len as u64);
                let mut need_read = Vec::new();

                for slice in self.slices.iter().copied().rev() {
                    for (l, r) in intervals.cut(slice.offset, slice.offset + slice.length) {
                        need_read.push((l, r, slice));
                    }
                }

                need_read.sort_by_key(|(l, _, _)| *l);
                need_read
            });
        tracing::Span::current().record("need_reads", need_read.len());

        let layout = self.layout;
        let backend = self.backend;

        {
            let mut cursor = 0;
            let mut tail = &mut buf[..];
            let mut futures = FuturesUnordered::new();

            for (l, r, slice) in need_read {
                let start = (l - offset).as_usize();
                let len = (r - l).as_usize();
                debug_assert!(start >= cursor);

                // Skip gaps
                let gap = start - cursor;
                let (_, rest) = tail.split_at_mut(gap);
                let (seg, rest) = rest.split_at_mut(len);
                tail = rest;
                cursor = start + len;

                // The blocks to fetch must be computed relative to the slice itself.
                let slice_offset = SliceOffset::from(l - slice.offset);
                let slice_len = r - l;
                let slice_id = slice.slice_id;

                // Flatten block reads into the FuturesUnordered for maximum
                // concurrency — blocks within the same slice are now fetched
                // in parallel rather than sequentially.
                let mut pos = 0_usize;
                for block in block_span_iter_slice(slice_offset, slice_len, layout) {
                    let take = block.len.as_usize();
                    let block_buf = &mut seg[pos..pos + take];
                    pos += take;

                    let block_key = (slice_id, block.index.as_u32());
                    let block_offset = block.offset;
                    let span = tracing::trace_span!(
                        "fetch.read_block",
                        slice_id,
                        block_idx = block.index.as_u32(),
                    );

                    // SAFETY: each block_buf is a non-overlapping sub-slice of
                    // `seg` (which itself is a non-overlapping sub-slice of `buf`).
                    // Only one future writes to each region.
                    let mut send_buf = SendBuf {
                        ptr: block_buf.as_mut_ptr(),
                        len: block_buf.len(),
                    };

                    futures.push(
                        async move {
                            // SAFETY: exclusive access guaranteed by non-overlapping split
                            let out = unsafe { send_buf.as_mut_slice() };
                            backend
                                .store()
                                .read_range(block_key, block_offset, out)
                                .await?;
                            Ok::<_, anyhow::Error>(())
                        }
                        .instrument(span),
                    );
                }
            }

            while let Some(res) = futures.next().await {
                res?;
            }
        }
        Ok(buf)
    }

    /// Zero-copy variant: fill `buf` directly from the block store without
    /// an intermediate allocation.  Used on the hot read path.
    #[tracing::instrument(
        name = "DataFetcher.read_at_into",
        level = "trace",
        skip(self, buf),
        fields(chunk_id = self.id, offset = offset.0, len = buf.len())
    )]
    pub(crate) async fn read_at_into(&mut self, offset: ChunkOffset, buf: &mut [u8]) -> Result<()> {
        let offset = offset.get();
        let len = buf.len();
        if len == 0 {
            return Ok(());
        }
        ensure!(
            self.prepared,
            "DataFetcher::read_at_into requires prepare_slices() to run first"
        );

        let need_read = {
            let mut intervals = Intervals::new(offset, offset + len as u64);
            let mut need_read = Vec::new();
            for slice in self.slices.iter().copied().rev() {
                for (l, r) in intervals.cut(slice.offset, slice.offset + slice.length) {
                    need_read.push((l, r, slice));
                }
            }
            need_read.sort_by_key(|(l, _, _)| *l);
            need_read
        };

        let layout = self.layout;
        let backend = self.backend;
        let mut cursor = 0;
        let mut tail = buf;
        let mut futures = FuturesUnordered::new();

        for (l, r, slice) in need_read {
            let start = (l - offset).as_usize();
            let len = (r - l).as_usize();
            let gap = start - cursor;
            let (gap_buf, rest) = tail.split_at_mut(gap);
            gap_buf.fill(0);
            let (seg, rest) = rest.split_at_mut(len);
            tail = rest;
            cursor = start + len;

            let slice_offset = SliceOffset::from(l - slice.offset);
            let slice_len = r - l;
            let slice_id = slice.slice_id;
            let mut pos = 0_usize;
            for block in block_span_iter_slice(slice_offset, slice_len, layout) {
                let take = block.len.as_usize();
                let block_buf = &mut seg[pos..pos + take];
                pos += take;
                let block_key = (slice_id, block.index.as_u32());
                let block_offset = block.offset;
                // SAFETY: each block_buf is a non-overlapping sub-slice of `seg`
                let mut send_buf = SendBuf {
                    ptr: block_buf.as_mut_ptr(),
                    len: block_buf.len(),
                };
                futures.push(async move {
                    backend
                        .store()
                        .read_range(block_key, block_offset, unsafe { send_buf.as_mut_slice() })
                        .await
                });
            }
        }

        while let Some(res) = futures.next().await {
            res?;
        }
        tail.fill(0);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk::store::InMemoryBlockStore;
    use crate::chunk::writer::DataUploader;
    use crate::meta::SLICE_ID_KEY;
    use crate::meta::factory::create_meta_store_from_url;
    use crate::vfs::backend::Backend;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_reader_zero_fills_holes() {
        let layout = ChunkLayout::default();
        let store = Arc::new(InMemoryBlockStore::new());
        let meta = create_meta_store_from_url("sqlite::memory:")
            .await
            .unwrap()
            .layer();
        let backend = Arc::new(Backend::new(store.clone(), meta.clone()));
        // Only write the first half of the second block
        {
            let buf = vec![1u8; (layout.block_size / 2) as usize];
            let slice_id = meta.next_id(SLICE_ID_KEY).await.unwrap();
            let uploader = DataUploader::new(layout, backend.as_ref());
            uploader
                .write_at_vectored(
                    slice_id as u64,
                    0u64.into(),
                    &[bytes::Bytes::copy_from_slice(&buf)],
                )
                .await
                .unwrap();
            meta.append_slice(
                7,
                SliceDesc {
                    slice_id: slice_id as u64,
                    chunk_id: 7,
                    offset: layout.block_size as u64,
                    length: buf.len() as u64,
                },
            )
            .await
            .unwrap();
        }
        let mut r = DataFetcher::new(layout, 7, backend.as_ref());
        r.prepare_slices().await.unwrap();
        // Read from the back half of block 0 to the front half of block 1 (one block total)
        let off = layout.block_size as u64 / 2;
        let res = r
            .read_at(off.into(), layout.block_size as usize)
            .await
            .unwrap();
        assert_eq!(res.len(), layout.block_size as usize);
        // The first half should be zero-filled and the second half should be ones
        assert!(
            res[..(layout.block_size / 2) as usize]
                .iter()
                .all(|&b| b == 0)
        );
        assert!(
            res[(layout.block_size / 2) as usize..]
                .iter()
                .all(|&b| b == 1)
        );
    }

    #[tokio::test]
    async fn test_reader_into_zero_fills_holes() {
        let layout = ChunkLayout::default();
        let store = Arc::new(InMemoryBlockStore::new());
        let meta = create_meta_store_from_url("sqlite::memory:")
            .await
            .unwrap()
            .layer();
        let backend = Arc::new(Backend::new(store.clone(), meta.clone()));

        let buf = vec![1u8; (layout.block_size / 2) as usize];
        let slice_id = meta.next_id(SLICE_ID_KEY).await.unwrap();
        let uploader = DataUploader::new(layout, backend.as_ref());
        uploader
            .write_at_vectored(
                slice_id as u64,
                0u64.into(),
                &[bytes::Bytes::copy_from_slice(&buf)],
            )
            .await
            .unwrap();
        meta.append_slice(
            7,
            SliceDesc {
                slice_id: slice_id as u64,
                chunk_id: 7,
                offset: layout.block_size as u64,
                length: buf.len() as u64,
            },
        )
        .await
        .unwrap();

        let off = layout.block_size as u64 / 2;
        let mut out = vec![0xff; layout.block_size as usize];
        let mut r = DataFetcher::new(layout, 7, backend.as_ref());
        r.prepare_slices().await.unwrap();
        r.read_at_into(off.into(), &mut out).await.unwrap();

        assert!(
            out[..(layout.block_size / 2) as usize]
                .iter()
                .all(|&b| b == 0)
        );
        assert!(
            out[(layout.block_size / 2) as usize..]
                .iter()
                .all(|&b| b == 1)
        );
    }

    #[tokio::test]
    async fn test_fetcher_cross_block_boundary() {
        let layout = ChunkLayout {
            chunk_size: 16 * 1024,
            block_size: 4 * 1024,
        };
        let store = Arc::new(InMemoryBlockStore::new());
        let meta = create_meta_store_from_url("sqlite::memory:")
            .await
            .unwrap()
            .layer();
        let backend = Arc::new(Backend::new(store.clone(), meta.clone()));

        let offset = layout.block_size as u64 - 512;
        let data = vec![7u8; 2048];
        let slice_id = meta.next_id(SLICE_ID_KEY).await.unwrap();
        let uploader = DataUploader::new(layout, backend.as_ref());
        uploader
            .write_at_vectored(
                slice_id as u64,
                0u64.into(),
                &[bytes::Bytes::copy_from_slice(&data)],
            )
            .await
            .unwrap();
        meta.append_slice(
            3,
            SliceDesc {
                slice_id: slice_id as u64,
                chunk_id: 3,
                offset,
                length: data.len() as u64,
            },
        )
        .await
        .unwrap();

        let mut fetcher = DataFetcher::new(layout, 3, backend.as_ref());
        fetcher.prepare_slices().await.unwrap();
        let out = fetcher.read_at(offset.into(), data.len()).await.unwrap();
        assert_eq!(out, data);
    }

    #[tokio::test]
    async fn test_fetcher_overlapping_slices_latest_wins() {
        let layout = ChunkLayout {
            chunk_size: 16 * 1024,
            block_size: 4 * 1024,
        };
        let store = Arc::new(InMemoryBlockStore::new());
        let meta = create_meta_store_from_url("sqlite::memory:")
            .await
            .unwrap()
            .layer();
        let backend = Arc::new(Backend::new(store.clone(), meta.clone()));

        let data1 = vec![1u8; 2048];
        let data2 = vec![2u8; 2048];

        let slice_id1 = meta.next_id(SLICE_ID_KEY).await.unwrap();
        let uploader = DataUploader::new(layout, backend.as_ref());
        uploader
            .write_at_vectored(
                slice_id1 as u64,
                0u64.into(),
                &[bytes::Bytes::copy_from_slice(&data1)],
            )
            .await
            .unwrap();
        meta.append_slice(
            9,
            SliceDesc {
                slice_id: slice_id1 as u64,
                chunk_id: 9,
                offset: 0,
                length: data1.len() as u64,
            },
        )
        .await
        .unwrap();

        let slice_id2 = meta.next_id(SLICE_ID_KEY).await.unwrap();
        uploader
            .write_at_vectored(
                slice_id2 as u64,
                0u64.into(),
                &[bytes::Bytes::copy_from_slice(&data2)],
            )
            .await
            .unwrap();
        meta.append_slice(
            9,
            SliceDesc {
                slice_id: slice_id2 as u64,
                chunk_id: 9,
                offset: 1024,
                length: data2.len() as u64,
            },
        )
        .await
        .unwrap();

        let mut fetcher = DataFetcher::new(layout, 9, backend.as_ref());
        fetcher.prepare_slices().await.unwrap();
        let out = fetcher.read_at(0u64.into(), 3072).await.unwrap();
        assert_eq!(&out[..1024], &data1[..1024]);
        assert_eq!(&out[1024..], &data2[..]);
    }
}
