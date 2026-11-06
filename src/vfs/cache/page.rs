use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::io::{Cursor, Read, Write};
use std::mem::take;
use std::sync::Arc;

use crate::chunk::{BlockTag, ChunkSpan, PageTag};
use crate::utils::NumCastExt;
use crate::utils::zero::make_zero_bytes;
use crate::vfs::config::WriteConfig;
use bytes::{Bytes, BytesMut};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WriteAction {
    Overlap,
    Append,
    GapAppend,
}

pub(crate) struct CacheSlice {
    config: Arc<WriteConfig>,
    len: u64,
    alloc_bytes: u64,
    // Only materialize the index for pages that contain data. Most random
    // write slices hold one short page, even with a 64 MiB chunk layout.
    pages: BTreeMap<usize, Page>,
    written_ranges: Vec<(u64, u64)>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct CacheSliceStats {
    pub len: u64,
    pub alloc_bytes: u64,
    pub pages_total: usize,
    pub pages_used: usize,
}

impl CacheSlice {
    pub(crate) fn new(config: Arc<WriteConfig>) -> Self {
        assert_eq!(
            config.layout.block_size % config.page_size,
            0,
            "The block size must be a multiple of the page size"
        );

        Self {
            config,
            len: 0,
            alloc_bytes: 0,
            pages: BTreeMap::new(),
            written_ranges: Vec::new(),
        }
    }

    pub(crate) fn can_write(&self, offset: u64, len: u64) -> Option<WriteAction> {
        self.can_write_with_gap(offset, len, false)
    }

    pub(crate) fn can_write_with_gap(
        &self,
        offset: u64,
        len: u64,
        allow_gap: bool,
    ) -> Option<WriteAction> {
        if self.can_append(offset) {
            return Some(WriteAction::Append);
        }

        if self.can_write_at(offset, len) {
            return Some(WriteAction::Overlap);
        }

        if allow_gap && self.can_gap_append(offset, len) {
            return Some(WriteAction::GapAppend);
        }

        None
    }

    pub(crate) fn write(
        &mut self,
        offset: u64,
        buf: &[u8],
        action: WriteAction,
    ) -> anyhow::Result<()> {
        match action {
            WriteAction::Append => self.append(buf),
            WriteAction::Overlap => self.write_at(offset, buf),
            WriteAction::GapAppend => self.gap_append(offset, buf),
        }
    }

    pub(crate) fn can_write_at(&self, offset: u64, len: u64) -> bool {
        let end = offset.saturating_add(len);
        end <= self.len
    }

    pub(crate) fn can_gap_append(&self, offset: u64, len: u64) -> bool {
        let end = offset.saturating_add(len);
        offset > self.len && end <= self.config.layout.chunk_size
    }

    pub(crate) fn has_written_overlap(&self, offset: u64, len: u64) -> bool {
        let end = offset.saturating_add(len);
        if offset >= end {
            return false;
        }
        self.written_ranges
            .iter()
            .any(|&(start, stop)| start < end && stop > offset)
    }

    #[cfg(feature = "workspace-overlay")]
    pub(crate) fn written_ranges(&self) -> &[(u64, u64)] {
        &self.written_ranges
    }

    pub(crate) fn copy_written_into(
        &self,
        offset: u64,
        buf: &mut [u8],
    ) -> anyhow::Result<Vec<(u64, u64)>> {
        if buf.is_empty() {
            return Ok(Vec::new());
        }

        let requested_end = offset.saturating_add(buf.len() as u64);
        let mut copied = Vec::new();
        for &(range_start, range_end) in &self.written_ranges {
            let read_start = offset.max(range_start);
            let read_end = requested_end.min(range_end);
            if read_start >= read_end {
                continue;
            }

            let dst_start = (read_start - offset).as_usize();
            let dst_end = (read_end - offset).as_usize();
            let copied_len = self.copy_into(read_start, &mut buf[dst_start..dst_end])?;
            if copied_len > 0 {
                copied.push((read_start, read_start + copied_len as u64));
            }
        }
        Ok(copied)
    }

    pub(crate) fn contiguous_written_len(&self) -> u64 {
        let mut cursor = 0;
        for &(start, end) in &self.written_ranges {
            if start > cursor {
                break;
            }
            cursor = cursor.max(end);
        }
        cursor.min(self.len)
    }

    /// Use buffer to overlap specific range.
    #[tracing::instrument(level = "trace", skip(self, buf), fields(len = buf.len()))]
    pub(crate) fn write_at(&mut self, offset: u64, buf: &[u8]) -> anyhow::Result<()> {
        let mut cursor = Cursor::new(buf);

        let (chunk_size, block_size, page_size, pages_per_block) = (
            self.config.layout.chunk_size,
            self.config.layout.block_size as u64,
            self.config.page_size as u64,
            self.pages_per_block(),
        );

        let span = ChunkSpan::new(0, offset, buf.len() as u64);

        for block_span in span.split_into::<BlockTag>(chunk_size, block_size, true) {
            let page_spans = block_span.split_into::<PageTag>(block_size, page_size, true);

            for page_span in page_spans {
                let (block_idx, page_idx) =
                    (block_span.index.as_usize(), page_span.index.as_usize());

                let (flat_idx, end) = (
                    self.flat_index(block_idx, page_idx, pages_per_block),
                    (page_span.offset + page_span.len).as_usize(),
                );

                let slice = {
                    let page = self.ensure_page_mut(flat_idx, end)?;
                    page.write_slice(page_span.offset.as_usize(), end)?
                };

                cursor.read_exact(slice)?;
            }
        }

        self.record_written(offset, offset + buf.len() as u64);
        Ok(())
    }

    /// Append a buffer behind the latest page. The buffer length must not exceed the remaining length.
    #[tracing::instrument(level = "trace", skip(self, buf), fields(len = buf.len()))]
    pub(crate) fn append(&mut self, buf: &[u8]) -> anyhow::Result<()> {
        let (max_len, next_len) = (self.config.layout.chunk_size, self.len + buf.len() as u64);

        anyhow::ensure!(next_len <= max_len, "append exceeds chunk size");

        let (page_size, mut position, mut cursor) =
            (self.config.page_size as usize, 0usize, Cursor::new(buf));

        while position < buf.len() {
            let (flat_idx, within_page) = self.next_write_slot_flat();
            let end = page_size.min(within_page + buf.len() - position);

            let read = {
                let page = self.ensure_page_mut(flat_idx, end)?;
                cursor.read(page.write_slice(within_page, end)?)?
            };

            if read == 0 {
                break;
            }

            position += read;
            self.len += read as u64;
        }

        self.record_written(next_len - buf.len() as u64, next_len);
        Ok(())
    }

    fn gap_append(&mut self, offset: u64, buf: &[u8]) -> anyhow::Result<()> {
        let end = offset.saturating_add(buf.len() as u64);
        anyhow::ensure!(
            offset > self.len && end <= self.config.layout.chunk_size,
            "gap append exceeds slice bounds"
        );

        self.write_at(offset, buf)?;
        self.len = end;
        Ok(())
    }

    pub(crate) fn len(&self) -> u64 {
        self.len
    }

    pub(crate) fn copy_into(&self, offset: u64, buf: &mut [u8]) -> anyhow::Result<usize> {
        if buf.is_empty() || offset >= self.len {
            return Ok(0);
        }

        let actual_len = buf.len().min((self.len - offset).as_usize());
        let mut cursor = Cursor::new(&mut buf[..actual_len]);

        let (chunk_size, block_size, page_size, pages_per_block) = (
            self.config.layout.chunk_size,
            self.config.layout.block_size as u64,
            self.config.page_size as u64,
            self.pages_per_block(),
        );

        let span = ChunkSpan::new(0, offset, actual_len as u64);

        for block_span in span.split_into::<BlockTag>(chunk_size, block_size, true) {
            let page_spans = block_span.split_into::<PageTag>(block_size, page_size, true);

            for page_span in page_spans {
                let (block_idx, page_idx) =
                    (block_span.index.as_usize(), page_span.index.as_usize());
                let flat_idx = self.flat_index(block_idx, page_idx, pages_per_block);
                let start = page_span.offset.as_usize();
                let end = (page_span.offset + page_span.len).as_usize();

                if let Some(page) = self.pages.get(&flat_idx) {
                    page.copy_slice(start, end, &mut cursor)?;
                } else {
                    cursor.write_all(&vec![0; end - start])?;
                }
            }
        }

        Ok(actual_len)
    }

    pub(crate) fn block_size(&self) -> u32 {
        self.config.layout.block_size
    }

    pub(crate) fn can_append(&self, offset: u64) -> bool {
        self.len == offset
    }

    pub(crate) fn stats(&self) -> CacheSliceStats {
        CacheSliceStats {
            len: self.len,
            alloc_bytes: self.alloc_bytes,
            pages_total: self.config.layout.blocks_per_chunk() as usize * self.pages_per_block(),
            pages_used: self.pages.len(),
        }
    }

    pub(crate) fn freeze(&mut self) {
        for page in self.pages.values_mut() {
            page.freeze();
        }
    }

    pub(crate) fn freeze_blocks(&mut self, start: usize, end: usize) {
        let pages_per_block = self.pages_per_block();

        let (start_idx, end_idx) = (start * pages_per_block, end * pages_per_block);

        for (_, page) in self.pages.range_mut(start_idx..end_idx) {
            page.freeze();
        }
    }

    pub fn release_block(&mut self, idx: Vec<usize>) -> u64 {
        let (pages_per_block, mut freed) = (self.pages_per_block(), 0);

        for block_idx in idx {
            let range = self.block_page_range(block_idx, pages_per_block);

            let keys = self
                .pages
                .range(range)
                .map(|(&key, _)| key)
                .collect::<Vec<_>>();
            for key in keys {
                freed += self.pages.remove(&key).unwrap().allocated_len() as u64;
            }
        }

        self.alloc_bytes = self.alloc_bytes.saturating_sub(freed);
        freed
    }

    pub(crate) fn release_all(&mut self) -> u64 {
        let freed = self.alloc_bytes;
        self.pages.clear();
        self.alloc_bytes = 0;
        freed
    }

    pub(crate) fn truncate(&mut self, len: u64) -> anyhow::Result<u64> {
        anyhow::ensure!(len <= self.len, "truncate exceeds slice length");

        if len == self.len {
            return Ok(0);
        }

        let page_size = self.config.page_size as u64;
        let first_released = len.div_ceil(page_size).as_usize();
        let mut freed = 0_u64;
        self.pages.retain(|&idx, page| {
            if idx >= first_released {
                freed += page.allocated_len() as u64;
                false
            } else {
                true
            }
        });

        self.alloc_bytes = self.alloc_bytes.saturating_sub(freed);
        self.len = len;
        self.truncate_written(len);
        Ok(freed)
    }

    pub(crate) fn collect_pages(
        &mut self,
        start: usize,
        end: usize,
    ) -> anyhow::Result<Vec<(usize, Vec<Bytes>)>> {
        let (page_size, block_size, pages_per_block) = (
            self.config.page_size as usize,
            self.config.layout.block_size as usize,
            self.pages_per_block(),
        );

        let (mut remaining, skip) = (self.len.as_usize(), start * block_size);
        remaining = remaining.saturating_sub(skip);

        if remaining == 0 {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();

        for block_idx in start..end {
            let mut pages = Vec::new();

            for page_idx in 0..pages_per_block {
                if remaining == 0 {
                    break;
                }

                let (take, flat_idx) = (
                    remaining.min(page_size),
                    self.flat_index(block_idx, page_idx, pages_per_block),
                );

                if let Some(page) = self.pages.get(&flat_idx) {
                    let bytes = page.bytes()?;
                    let allocated = take.min(bytes.len());
                    pages.push(bytes.slice(0..allocated));
                    // A sparse write can leave the rest of this logical page
                    // unallocated. Preserve its zero padding in the upload.
                    pages.extend(make_zero_bytes(take - allocated));
                } else {
                    pages.extend(make_zero_bytes(take));
                }

                remaining -= take;
            }

            out.push((block_idx, pages));
        }

        Ok(out)
    }

    fn next_write_slot_flat(&mut self) -> (usize, usize) {
        let (page_size, block_size, pages_per_block, total) = (
            self.config.page_size as usize,
            self.config.layout.block_size as usize,
            self.pages_per_block(),
            self.len.as_usize(),
        );

        let (block_index, within_block) = (total / block_size, total % block_size);

        let (page_index, within_page) = (within_block / page_size, within_block % page_size);

        let flat_idx = self.flat_index(block_index, page_index, pages_per_block);
        (flat_idx, within_page)
    }

    fn pages_per_block(&self) -> usize {
        (self.config.layout.block_size as usize).div_ceil(self.config.page_size as usize)
    }

    fn flat_index(&self, block_idx: usize, page_idx: usize, pages_per_block: usize) -> usize {
        debug_assert_eq!(pages_per_block, self.pages_per_block());
        block_idx * pages_per_block + page_idx
    }

    fn block_page_range(&self, block_idx: usize, pages_per_block: usize) -> std::ops::Range<usize> {
        let (start, end) = (
            block_idx * pages_per_block,
            (block_idx + 1) * pages_per_block,
        );

        start..end
    }

    fn ensure_page_mut(&mut self, flat_idx: usize, end: usize) -> anyhow::Result<&mut Page> {
        // Keep the configured logical page/block boundaries, but charge and
        // retain only the backing buffer needed so far. Geometric growth
        // bounds copying for small appends; full-page writes allocate once.
        let size = end
            .next_power_of_two()
            .max(4096)
            .min(self.config.page_size as usize);
        Ok(match self.pages.entry(flat_idx) {
            Entry::Vacant(entry) => {
                self.alloc_bytes += size as u64;
                entry.insert(Page::new(size))
            }
            Entry::Occupied(entry) => {
                let page = entry.into_mut();
                self.alloc_bytes += page.grow(size)?;
                page
            }
        })
    }

    pub(crate) fn alloc_bytes(&self) -> u64 {
        self.alloc_bytes
    }

    fn record_written(&mut self, start: u64, end: u64) {
        if start >= end {
            return;
        }

        let (mut merged_start, mut merged_end) = (start, end);
        let mut out = Vec::with_capacity(self.written_ranges.len() + 1);
        let mut inserted = false;

        for &(range_start, range_end) in &self.written_ranges {
            if range_end < merged_start {
                out.push((range_start, range_end));
            } else if merged_end < range_start {
                if !inserted {
                    out.push((merged_start, merged_end));
                    inserted = true;
                }
                out.push((range_start, range_end));
            } else {
                merged_start = merged_start.min(range_start);
                merged_end = merged_end.max(range_end);
            }
        }

        if !inserted {
            out.push((merged_start, merged_end));
        }

        self.written_ranges = out;
    }

    fn truncate_written(&mut self, len: u64) {
        self.written_ranges = self
            .written_ranges
            .iter()
            .filter_map(|&(start, end)| {
                if start >= len {
                    None
                } else {
                    Some((start, end.min(len)))
                }
            })
            .collect();
    }
}

#[derive(Clone)]
enum PageBuf {
    Mutable(BytesMut),
    Frozen(Bytes),
}

#[derive(Clone)]
pub(crate) struct Page {
    data: PageBuf,
}

impl Page {
    pub(crate) fn new(size: usize) -> Self {
        let buf = BytesMut::zeroed(size);

        Self {
            data: PageBuf::Mutable(buf),
        }
    }

    fn allocated_len(&self) -> usize {
        match &self.data {
            PageBuf::Mutable(buf) => buf.len(),
            PageBuf::Frozen(buf) => buf.len(),
        }
    }

    fn grow(&mut self, size: usize) -> anyhow::Result<u64> {
        let old_size = self.allocated_len();
        if size <= old_size {
            return Ok(0);
        }
        let PageBuf::Mutable(buf) = &mut self.data else {
            anyhow::bail!("attempt to grow frozen page");
        };
        // Allocate an exact-sized zeroed buffer rather than retaining spare
        // BytesMut capacity that would escape dirty-memory accounting.
        let mut grown = BytesMut::zeroed(size);
        grown[..old_size].copy_from_slice(buf);
        *buf = grown;
        Ok((size - old_size) as u64)
    }

    fn write_slice(&mut self, start: usize, end: usize) -> anyhow::Result<&mut [u8]> {
        match &mut self.data {
            PageBuf::Mutable(buf) => Ok(&mut buf[start..end]),
            // Return an error: a frozen page means the caller raced with a freeze.
            PageBuf::Frozen(_) => {
                anyhow::bail!("attempt to write to frozen page");
            }
        }
    }

    fn freeze(&mut self) {
        if let PageBuf::Mutable(buf) = &mut self.data {
            let frozen = take(buf).freeze();
            self.data = PageBuf::Frozen(frozen);
        }
    }

    fn bytes(&self) -> anyhow::Result<Bytes> {
        match &self.data {
            PageBuf::Frozen(buf) => Ok(buf.clone()),
            PageBuf::Mutable(_) => {
                // Return an error instead of panicking to surface races with freeze.
                anyhow::bail!(
                    "collect_pages called on mutable page (would read uninitialized bytes)"
                );
            }
        }
    }

    fn copy_slice(
        &self,
        start: usize,
        end: usize,
        cursor: &mut Cursor<&mut [u8]>,
    ) -> anyhow::Result<()> {
        let buf: &[u8] = match &self.data {
            PageBuf::Frozen(buf) => buf,
            PageBuf::Mutable(buf) => buf,
        };
        let allocated_end = end.min(buf.len());
        if start < allocated_end {
            cursor.write_all(&buf[start..allocated_end])?;
        }
        for zeros in make_zero_bytes(end - start.max(allocated_end)) {
            cursor.write_all(&zeros)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::chunk::ChunkLayout;
    use crate::vfs::cache::page::{CacheSlice, WriteAction};
    use crate::vfs::config::WriteConfig;
    use bytes::Bytes;

    fn config() -> Arc<WriteConfig> {
        Arc::new(
            WriteConfig::new(ChunkLayout {
                chunk_size: 16 * 1024,
                block_size: 4 * 1024,
            })
            .page_size(1024),
        )
    }

    fn patterned(len: usize, seed: u8) -> Vec<u8> {
        let mut buf = vec![0u8; len];
        for (i, b) in buf.iter_mut().enumerate() {
            *b = seed.wrapping_add(i as u8);
        }
        buf
    }

    fn flatten(parts: Vec<Bytes>) -> Vec<u8> {
        parts.into_iter().flat_map(|b| b.to_vec()).collect()
    }

    fn collect_all(slice: &mut CacheSlice) -> Vec<u8> {
        let (len, block_size) = (slice.len(), slice.block_size() as u64);

        let end = if len == 0 {
            0
        } else {
            len.div_ceil(block_size) as usize
        };
        let mut blocks = slice.collect_pages(0, end).unwrap();
        blocks.sort_by_key(|(idx, _)| *idx);
        let mut out = Vec::new();
        for (_, pages) in blocks {
            out.extend(flatten(pages));
        }
        out
    }

    #[test]
    fn test_demand_pages_empty_slice_has_no_index_entries() {
        let slice = CacheSlice::new(Arc::new(WriteConfig::new(ChunkLayout::default())));
        assert!(
            slice.pages.is_empty(),
            "an empty slice must not allocate page slots"
        );
        assert_eq!(slice.alloc_bytes(), 0);
    }

    #[test]
    fn test_demand_pages_small_write_keeps_only_a_small_buffer() {
        let mut slice = CacheSlice::new(Arc::new(WriteConfig::new(ChunkLayout::default())));
        let data = patterned(4096, 29);
        slice.append(&data).unwrap();
        assert_eq!(
            slice.alloc_bytes(),
            4096,
            "a 4 KiB write must not retain a 64 KiB page"
        );
        slice.freeze();
        assert_eq!(collect_all(&mut slice), data);
        assert_eq!(slice.release_all(), 4096);
        assert!(
            slice.pages.is_empty(),
            "release must also return the page index"
        );
    }

    fn demand_page_config() -> Arc<WriteConfig> {
        Arc::new(
            WriteConfig::new(ChunkLayout {
                chunk_size: 256 * 1024,
                block_size: 128 * 1024,
            })
            .page_size(64 * 1024),
        )
    }

    #[test]
    fn test_demand_pages_grow_across_page_boundaries_and_keep_overwrites() {
        let mut slice = CacheSlice::new(demand_page_config());
        let mut expected = patterned(4096, 7);
        slice.append(&expected).unwrap();
        let tail = patterned(70_000, 13);
        slice.append(&tail).unwrap();
        expected.extend(tail);
        let patch = patterned(10_000, 29);
        slice.write_at(60_000, &patch).unwrap();
        expected[60_000..70_000].copy_from_slice(&patch);
        let mut read = vec![0; expected.len()];
        assert_eq!(slice.copy_into(0, &mut read).unwrap(), expected.len());
        assert_eq!(read, expected);
        assert!(slice.alloc_bytes() >= expected.len() as u64);
        assert!(slice.alloc_bytes() <= expected.len() as u64 + 64 * 1024);
        slice.freeze();
        assert_eq!(collect_all(&mut slice), expected);
    }

    #[test]
    fn test_demand_pages_zero_unallocated_tails_and_release_actual_bytes() {
        let mut slice = CacheSlice::new(demand_page_config());
        let first = patterned(4096, 31);
        let tail = patterned(4096, 57);
        slice.append(&first).unwrap();
        slice
            .write(128 * 1024, &tail, WriteAction::GapAppend)
            .unwrap();
        assert_eq!(slice.alloc_bytes(), 8192);
        assert_eq!(slice.stats().pages_used, 2);
        assert_eq!(slice.stats().pages_total, 4);
        let mut expected = vec![0; 128 * 1024 + tail.len()];
        expected[..first.len()].copy_from_slice(&first);
        expected[128 * 1024..].copy_from_slice(&tail);
        let mut read = vec![0xAA; expected.len()];
        slice.copy_into(0, &mut read).unwrap();
        assert_eq!(read, expected);
        // Also start entirely beyond a short page's backing allocation.
        let mut hole = vec![0xAA; 8192];
        slice.copy_into(8192, &mut hole).unwrap();
        assert_eq!(hole, vec![0; 8192]);
        slice.freeze_blocks(0, 1);
        let first_block = slice.collect_pages(0, 1).unwrap().pop().unwrap().1;
        assert_eq!(flatten(first_block.clone()), expected[..128 * 1024]);
        assert_eq!(slice.release_block(vec![0, 0]), 4096);
        assert_eq!(slice.alloc_bytes(), 4096);
        slice.freeze_blocks(1, 2);
        let second_block = slice.collect_pages(1, 2).unwrap().pop().unwrap().1;
        assert_eq!(flatten(second_block.clone()), tail);
        assert_eq!(slice.release_all(), 4096);
        assert_eq!(slice.alloc_bytes(), 0);
        assert!(slice.pages.is_empty());
        // Upload readers that already own Bytes keep the frozen contents.
        assert_eq!(flatten(first_block), expected[..128 * 1024]);
        assert_eq!(flatten(second_block), tail);
    }

    #[test]
    fn test_demand_pages_truncate_sparse_tail_and_reject_frozen_growth() {
        let mut slice = CacheSlice::new(demand_page_config());
        let data = patterned(4096, 41);
        slice.append(&data).unwrap();
        slice
            .write(128 * 1024, &data, WriteAction::GapAppend)
            .unwrap();
        assert_eq!(slice.truncate(64 * 1024).unwrap(), 4096);
        assert_eq!(slice.alloc_bytes(), 4096);
        slice.freeze();
        let mut expected = data.clone();
        expected.resize(64 * 1024, 0);
        assert_eq!(collect_all(&mut slice), expected);
        assert!(slice.write_at(4096, &data).is_err());
        assert_eq!(slice.alloc_bytes(), 4096);
        assert_eq!(collect_all(&mut slice), expected);
    }

    #[test]
    fn test_append_single_page() {
        let (mut slice, data) = (CacheSlice::new(config()), patterned(512, 1));

        slice.append(&data).unwrap();
        slice.freeze();

        assert_eq!(slice.len, data.len() as u64);
        assert_eq!(slice.stats().pages_used, 1);
        assert_eq!(collect_all(&mut slice), data);
    }

    #[test]
    fn test_append_spans_pages() {
        let (mut slice, data) = (CacheSlice::new(config()), patterned(1024 + 10, 3));

        slice.append(&data).unwrap();
        slice.freeze();

        assert_eq!(slice.len, data.len() as u64);
        assert_eq!(slice.stats().pages_used, 2);
        assert_eq!(collect_all(&mut slice), data);
    }

    #[test]
    fn test_append_spans_blocks() {
        let (mut slice, data) = (CacheSlice::new(config()), patterned(4 * 1024 + 512, 7));

        slice.append(&data).unwrap();
        slice.freeze();

        let (block_size, page_size) = (
            slice.config.layout.block_size as usize,
            slice.config.page_size as usize,
        );

        let pages_per_block = block_size.div_ceil(page_size);
        assert_eq!(slice.stats().pages_used, pages_per_block + 1);
        assert_eq!(collect_all(&mut slice), data);
    }

    #[test]
    fn test_truncate_releases_tail_pages_and_keeps_prefix() {
        let (mut slice, data) = (CacheSlice::new(config()), patterned(4 * 1024 + 512, 13));

        slice.append(&data).unwrap();
        let before_alloc = slice.alloc_bytes();
        let freed = slice.truncate(4 * 1024).unwrap();
        slice.freeze();

        assert!(freed > 0);
        assert!(slice.alloc_bytes() < before_alloc);
        assert_eq!(slice.len(), 4 * 1024);
        assert_eq!(collect_all(&mut slice), data[..4 * 1024].to_vec());
    }

    #[test]
    fn test_append_multiple_calls() {
        let (mut slice, first, second) = (
            CacheSlice::new(config()),
            patterned(600, 11),
            patterned(900, 23),
        );

        slice.append(&first).unwrap();
        slice.append(&second).unwrap();
        slice.freeze();

        let mut expected = first.clone();
        expected.extend_from_slice(&second);

        assert_eq!(slice.len, expected.len() as u64);
        assert_eq!(collect_all(&mut slice), expected);
    }

    #[test]
    fn test_write_at() {
        let (mut slice, first, second) = (
            CacheSlice::new(config()),
            patterned(600, 11),
            patterned(600, 23),
        );

        slice.append(&first).unwrap();
        slice.write_at(0, &second).unwrap();
        slice.freeze();

        let expected = second.clone();

        assert_eq!(slice.len, expected.len() as u64);
        assert_eq!(collect_all(&mut slice), expected);
    }

    #[test]
    fn test_write_at_overwrite_middle() {
        let (mut slice, data, patch, offset) = (
            CacheSlice::new(config()),
            patterned(1500, 5),
            patterned(300, 200),
            700usize,
        );

        slice.append(&data).unwrap();
        slice.write_at(offset as u64, &patch).unwrap();
        slice.freeze();

        let mut expected = data.clone();

        let (start, end) = (offset, offset + patch.len());

        expected[start..end].copy_from_slice(&patch);

        assert_eq!(slice.len, data.len() as u64);
        assert_eq!(collect_all(&mut slice), expected);
    }

    #[test]
    fn test_write_at_crosses_block_boundary() {
        let (mut slice, data, patch, offset) = (
            CacheSlice::new(config()),
            patterned(4 * 1024 + 512, 9),
            patterned(512, 77),
            4 * 1024 - 256,
        );

        slice.append(&data).unwrap();
        slice.write_at(offset as u64, &patch).unwrap();
        slice.freeze();

        let mut expected = data.clone();

        let (start, end) = (offset, offset + patch.len());

        expected[start..end].copy_from_slice(&patch);

        assert_eq!(slice.len, data.len() as u64);
        assert_eq!(collect_all(&mut slice), expected);
    }

    #[test]
    fn test_can_write_at_disallows_append_or_extend() {
        let (mut slice, data) = (CacheSlice::new(config()), patterned(1024, 17));

        slice.append(&data).unwrap();

        assert!(slice.can_write_at(0, 512));
        assert!(slice.can_write_at(512, 512));
        assert!(!slice.can_write_at(1024, 1));
        assert!(!slice.can_write_at(900, 200));
    }

    #[test]
    fn test_gap_append_extends_logical_len_with_zero_hole() {
        let (mut slice, first, tail) = (
            CacheSlice::new(config()),
            patterned(512, 17),
            patterned(256, 91),
        );

        slice.append(&first).unwrap();

        assert_eq!(
            slice.can_write_with_gap(1024, tail.len() as u64, false),
            None
        );
        assert_eq!(
            slice.can_write_with_gap(1024, tail.len() as u64, true),
            Some(WriteAction::GapAppend)
        );

        slice.write(1024, &tail, WriteAction::GapAppend).unwrap();
        assert_eq!(slice.contiguous_written_len(), first.len() as u64);
        slice.freeze();

        let mut expected = first.clone();
        expected.extend(vec![0; 512]);
        expected.extend_from_slice(&tail);

        assert_eq!(slice.len(), expected.len() as u64);
        assert_eq!(collect_all(&mut slice), expected);
    }

    #[test]
    fn test_written_overlap_ignores_sparse_zero_hole_until_backfilled() {
        let (mut slice, first, tail, middle) = (
            CacheSlice::new(config()),
            patterned(512, 17),
            patterned(256, 91),
            patterned(512, 131),
        );

        slice.append(&first).unwrap();
        slice.write(1024, &tail, WriteAction::GapAppend).unwrap();

        assert!(slice.has_written_overlap(0, 16));
        assert!(!slice.has_written_overlap(512, 512));
        assert!(slice.has_written_overlap(1024, 16));
        assert_eq!(slice.contiguous_written_len(), first.len() as u64);

        slice.write_at(512, &middle).unwrap();
        assert_eq!(slice.contiguous_written_len(), 1280);
        slice.freeze();

        let mut expected = first.clone();
        expected.extend_from_slice(&middle);
        expected.extend_from_slice(&tail);

        assert!(slice.has_written_overlap(512, 512));
        assert_eq!(collect_all(&mut slice), expected);
    }

    #[test]
    fn test_copy_written_into_skips_sparse_zero_hole() {
        let (mut slice, first, tail) =
            (CacheSlice::new(config()), vec![0x11; 512], vec![0x22; 256]);

        slice.append(&first).unwrap();
        slice.write(1024, &tail, WriteAction::GapAppend).unwrap();

        let mut out = vec![0xAA; 1280];
        let copied = slice.copy_written_into(0, &mut out).unwrap();

        assert_eq!(copied, vec![(0, 512), (1024, 1280)]);
        assert_eq!(&out[..512], first.as_slice());
        assert_eq!(&out[512..1024], vec![0xAA; 512].as_slice());
        assert_eq!(&out[1024..1280], tail.as_slice());
    }

    #[test]
    fn test_append_then_write_at_overwrite_full() {
        let total = 16 * 1024;

        let (mut slice, first, second) = (
            CacheSlice::new(config()),
            patterned(6000, 1),
            patterned(total - 6000, 2),
        );

        slice.append(&first).unwrap();
        slice.append(&second).unwrap();

        let overwrite = patterned(total, 9);
        slice.write_at(0, &overwrite).unwrap();
        slice.freeze();

        assert_eq!(slice.len, total as u64);
        assert_eq!(collect_all(&mut slice), overwrite);
    }
}
