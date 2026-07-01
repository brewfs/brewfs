use bytes::{Bytes, BytesMut};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) struct CachedBlockAssembler {
    block_size: u64,
    page_size: u64,
    pages: BTreeMap<u64, CachedPage>,
}

struct CachedPage {
    unique: u64,
    data: Bytes,
}

pub(crate) struct AssembledExtent {
    pub(crate) offset: u64,
    pub(crate) data: Bytes,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum AssemblerWriteError {
    OlderOverlap,
}

impl CachedBlockAssembler {
    pub(crate) fn new(block_size: u64, page_size: u64) -> Self {
        assert!(block_size > 0, "block size must be non-zero");
        assert!(page_size > 0, "page size must be non-zero");
        assert_eq!(block_size % page_size, 0, "block size must be page-aligned");

        Self {
            block_size,
            page_size,
            pages: BTreeMap::new(),
        }
    }

    pub(crate) fn write(&mut self, offset: u64, data: Vec<u8>, unique: u64) {
        self.try_write(offset, data, unique)
            .expect("cached block assembler should accept non-stale page write");
    }

    pub(crate) fn try_write(
        &mut self,
        offset: u64,
        data: Vec<u8>,
        unique: u64,
    ) -> Result<(), AssemblerWriteError> {
        self.validate_page_write(offset, data.len());

        if let Some(existing) = self.pages.get(&offset)
            && unique < existing.unique
        {
            return Err(AssemblerWriteError::OlderOverlap);
        }

        self.pages.insert(
            offset,
            CachedPage {
                unique,
                data: Bytes::from(data),
            },
        );
        Ok(())
    }

    pub(crate) fn drain_ready_full_blocks(&mut self) -> Vec<AssembledExtent> {
        let ready_blocks = self.ready_full_blocks();
        let mut extents = Vec::with_capacity(ready_blocks.len());

        for block_start in ready_blocks {
            let mut data = BytesMut::with_capacity(self.block_size as usize);
            let page_offsets: Vec<u64> = self.block_page_offsets(block_start).collect();
            for page_start in page_offsets {
                let page = self
                    .pages
                    .remove(&page_start)
                    .expect("ready full block must contain all pages");
                data.extend_from_slice(&page.data);
            }
            extents.push(AssembledExtent {
                offset: block_start,
                data: data.freeze(),
            });
        }

        extents
    }

    pub(crate) fn drain_all(&mut self) -> Vec<AssembledExtent> {
        let pages = std::mem::take(&mut self.pages);
        let mut extents: Vec<AssembledExtent> = Vec::new();

        for (offset, page) in pages {
            if page.data.is_empty() {
                continue;
            }
            match extents.last_mut() {
                Some(current) if current.offset + current.data.len() as u64 == offset => {
                    let mut merged = BytesMut::with_capacity(current.data.len() + page.data.len());
                    merged.extend_from_slice(&current.data);
                    merged.extend_from_slice(&page.data);
                    current.data = merged.freeze();
                }
                _ => extents.push(AssembledExtent {
                    offset,
                    data: page.data,
                }),
            }
        }

        extents
    }

    pub(crate) fn truncate(&mut self, len: u64) {
        let offsets: Vec<u64> = self.pages.keys().copied().collect();
        for offset in offsets {
            if offset >= len {
                self.pages.remove(&offset);
                continue;
            }

            let Some(page) = self.pages.get_mut(&offset) else {
                continue;
            };
            let page_end = offset.saturating_add(page.data.len() as u64);
            if page_end > len {
                let keep = (len - offset) as usize;
                page.data = page.data.slice(..keep);
            }
        }
    }

    fn validate_page_write(&self, offset: u64, len: usize) {
        assert_eq!(offset % self.page_size, 0, "write must be page-aligned");
        assert!(len <= self.page_size as usize, "write must fit in one page");
    }

    fn ready_full_blocks(&self) -> BTreeSet<u64> {
        let mut candidates = BTreeSet::new();
        for offset in self.pages.keys() {
            candidates.insert(self.block_start(*offset));
        }

        candidates
            .into_iter()
            .filter(|block_start| self.is_full_block_ready(*block_start))
            .collect()
    }

    fn is_full_block_ready(&self, block_start: u64) -> bool {
        self.block_page_offsets(block_start).all(|page_start| {
            self.pages
                .get(&page_start)
                .is_some_and(|page| page.data.len() as u64 == self.page_size)
        })
    }

    fn block_page_offsets(&self, block_start: u64) -> impl Iterator<Item = u64> + '_ {
        let pages_per_block = self.block_size / self.page_size;
        (0..pages_per_block).map(move |page_idx| block_start + page_idx * self.page_size)
    }

    fn block_start(&self, offset: u64) -> u64 {
        (offset / self.block_size) * self.block_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assembler_emits_full_block_after_page_writes() {
        let mut a = CachedBlockAssembler::new(4096, 1024);
        for i in 0..4 {
            a.write(i * 1024, vec![i as u8; 1024], i as u64 + 1);
        }

        let ready = a.drain_ready_full_blocks();

        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].offset, 0);
        assert_eq!(ready[0].data.len(), 4096);
        assert!(a.drain_all().is_empty());
    }

    #[test]
    fn assembler_keeps_last_writer_for_overlap() {
        let mut a = CachedBlockAssembler::new(4096, 1024);
        a.write(0, vec![1; 1024], 10);
        a.write(0, vec![2; 1024], 20);

        let pending = a.drain_all();

        assert_eq!(pending.len(), 1);
        assert_eq!(&pending[0].data[..1024], vec![2; 1024].as_slice());
    }

    #[test]
    fn assembler_rejects_older_overlap_after_newer_unique() {
        let mut a = CachedBlockAssembler::new(4096, 1024);
        a.write(0, vec![2; 1024], 20);

        assert_eq!(
            a.try_write(0, vec![1; 1024], 10),
            Err(AssemblerWriteError::OlderOverlap)
        );
    }

    #[test]
    fn assembler_truncate_drops_tail_pages() {
        let mut a = CachedBlockAssembler::new(4096, 1024);
        a.write(0, vec![1; 1024], 1);
        a.write(4096, vec![2; 1024], 2);

        a.truncate(1024);
        let pending = a.drain_all();

        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].offset, 0);
        assert_eq!(pending[0].data.len(), 1024);
    }
}
