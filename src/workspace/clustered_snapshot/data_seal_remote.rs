//! Range-oriented reader for the fixed-table portion of `BRFDS002`.
//!
//! The current v2 seal stores authenticated slice, frame, and object tables
//! as independent sections.  This reader keeps the fixed header resident and
//! fetches only the authenticated index directory and selected table pages on
//! demand. Legacy continuous sections remain readable for compatibility.

use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::OnceCell;

use crate::cadapter::client::{ObjectBackend, ObjectClient};
use crate::native_base::wire::error::{WireError, WireResult};

use super::data_seal::{
    DATA_SEAL_FOOTER_LEN, DATA_SEAL_HEADER_LEN, DATA_SEAL_MAGIC, DataObjectDescriptor,
    DataSealSnapshot, FRAME_INDEX_MAGIC, FrameDescriptor, MAX_TABLE_BYTES, OBJECT_INDEX_MAGIC,
    SECTION_COUNT, SLICE_INDEX_HEADER_LEN, SLICE_INDEX_MAGIC, SectionRef, SliceDescriptor,
    SliceIndexHeader, SlicePageRef, TABLE_INDEX_HEADER_LEN, TablePageRef, decode_frame_records,
    decode_frames, decode_header, decode_object_records, decode_objects,
    decode_slice_index_directory, decode_slice_index_header, decode_slice_page, decode_slices,
    decode_table_index_prefix, verify_digest,
};
use super::snapshot_manifest::ManifestObjectRef;

/// A remote seal section is bounded by the current table limit.
pub const MAX_DATA_SEAL_RANGE_BYTES: usize = MAX_TABLE_BYTES;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SliceIndexInfo {
    pub record_count: u64,
    pub page_count: u32,
    pub page_record_limit: u32,
    pub paged: bool,
}

#[derive(Clone, Debug)]
enum RemoteSliceIndex {
    Paged {
        header: SliceIndexHeader,
        pages: Arc<[SlicePageRef]>,
    },
    Legacy(Arc<[SliceDescriptor]>),
}

impl RemoteSliceIndex {
    fn info(&self) -> SliceIndexInfo {
        match self {
            Self::Paged { header, .. } => SliceIndexInfo {
                record_count: header.record_count,
                page_count: header.page_count,
                page_record_limit: header.page_record_limit,
                paged: true,
            },
            Self::Legacy(slices) => SliceIndexInfo {
                record_count: slices.len() as u64,
                page_count: u32::from(!slices.is_empty()),
                page_record_limit: slices.len().min(u32::MAX as usize) as u32,
                paged: false,
            },
        }
    }
}

#[derive(Clone, Debug)]
enum RemoteFrameIndex {
    Paged { pages: Arc<[TablePageRef]> },
    Legacy(Arc<[FrameDescriptor]>),
}

#[derive(Clone, Debug)]
enum RemoteObjectIndex {
    Paged { pages: Arc<[TablePageRef]> },
    Legacy(Arc<[DataObjectDescriptor]>),
}

pub struct RemoteDataSeal<B: ObjectBackend + Clone> {
    client: ObjectClient<B>,
    object_key: String,
    object_len: u64,
    object_ref: ManifestObjectRef,
    cluster_id: [u8; 16],
    volume_id: [u8; 16],
    semantic_hash: [u8; 32],
    counts: [u64; SECTION_COUNT],
    sections: [SectionRef; SECTION_COUNT],
    slice_index: OnceCell<Arc<RemoteSliceIndex>>,
    slices: OnceCell<Arc<[SliceDescriptor]>>,
    frame_index: OnceCell<Arc<RemoteFrameIndex>>,
    frames: OnceCell<Arc<[FrameDescriptor]>>,
    frame_pages: DashMap<u64, Arc<[FrameDescriptor]>>,
    object_index: OnceCell<Arc<RemoteObjectIndex>>,
    objects: OnceCell<Arc<[DataObjectDescriptor]>>,
    object_pages: DashMap<u64, Arc<[DataObjectDescriptor]>>,
}

impl<B: ObjectBackend + Clone> RemoteDataSeal<B> {
    /// Open with one exact range read for the fixed 4 KiB header.  No table
    /// bytes are fetched until one of the table accessors is called.
    pub async fn open(client: &ObjectClient<B>, object_ref: ManifestObjectRef) -> WireResult<Self> {
        if object_ref.kind == 0 || object_ref.key.is_empty() || object_ref.key.len() > 1024 {
            return Err(WireError::invalid(
                "remote data seal",
                "object reference kind or key is invalid",
            ));
        }
        if object_ref.key.contains(&0) {
            return Err(WireError::invalid(
                "remote data seal",
                "object key contains NUL",
            ));
        }
        let object_key = std::str::from_utf8(&object_ref.key)
            .map_err(|_| WireError::invalid("remote data seal", "object key is not UTF-8"))?
            .to_owned();
        if object_ref.object_len < (DATA_SEAL_HEADER_LEN + DATA_SEAL_FOOTER_LEN) as u64 {
            return Err(WireError::Truncated {
                what: "remote data seal",
                need: DATA_SEAL_HEADER_LEN + DATA_SEAL_FOOTER_LEN,
                have: object_ref.object_len as usize,
            });
        }

        let reader = Self {
            client: client.clone(),
            object_key,
            object_len: object_ref.object_len,
            object_ref,
            cluster_id: [0; 16],
            volume_id: [0; 16],
            semantic_hash: [0; 32],
            counts: [0; SECTION_COUNT],
            sections: empty_sections(),
            slice_index: OnceCell::new(),
            slices: OnceCell::new(),
            frame_index: OnceCell::new(),
            frames: OnceCell::new(),
            frame_pages: DashMap::new(),
            object_index: OnceCell::new(),
            objects: OnceCell::new(),
            object_pages: DashMap::new(),
        };
        let header = reader.fetch_range(0, DATA_SEAL_HEADER_LEN as u32).await?;
        if &header[..8] != DATA_SEAL_MAGIC {
            return Err(WireError::UnsupportedFormat(
                "not a BRFDS002 data seal".into(),
            ));
        }
        let (cluster_id, volume_id, header_len, counts, semantic_hash, sections) =
            decode_header(&header)?;
        if header_len != reader.object_len {
            return Err(WireError::invalid(
                "remote data seal",
                "header object length does not match object reference",
            ));
        }
        reader.validate_sections(&sections)?;
        Ok(Self {
            cluster_id,
            volume_id,
            counts,
            semantic_hash,
            sections,
            ..reader
        })
    }

    pub fn object_ref(&self) -> &ManifestObjectRef {
        &self.object_ref
    }

    pub fn object_len(&self) -> u64 {
        self.object_len
    }

    pub fn cluster_id(&self) -> [u8; 16] {
        self.cluster_id
    }

    pub fn volume_id(&self) -> [u8; 16] {
        self.volume_id
    }

    pub fn semantic_hash(&self) -> [u8; 32] {
        self.semantic_hash
    }

    pub fn counts(&self) -> [u64; SECTION_COUNT] {
        self.counts
    }

    /// Fetch, authenticate, and decode the complete slice table on first use.
    pub async fn read_slices(&self) -> WireResult<Arc<[SliceDescriptor]>> {
        if let Some(index) = self.slice_index.get()
            && let RemoteSliceIndex::Legacy(slices) = index.as_ref()
        {
            return Ok(slices.clone());
        }
        self.slices
            .get_or_try_init(|| async {
                let payload = self.read_section(0).await?;
                Ok(Arc::from(decode_slices(&payload, self.sections[0].count)?))
            })
            .await
            .cloned()
    }

    /// Load only the authenticated SliceId directory.  A legacy seal without
    /// the directory is supported by materializing its old table once.
    pub async fn read_slice_index(&self) -> WireResult<SliceIndexInfo> {
        Ok(self.load_slice_index().await?.info())
    }

    /// Look up one SliceId without downloading the complete slice table.
    pub async fn read_slice(&self, slice_id: u64) -> WireResult<Option<SliceDescriptor>> {
        if slice_id == 0 {
            return Ok(None);
        }
        let index = self.load_slice_index().await?;
        match index.as_ref() {
            RemoteSliceIndex::Legacy(slices) => Ok(slices
                .binary_search_by_key(&slice_id, |slice| slice.slice_id)
                .ok()
                .map(|position| slices[position].clone())),
            RemoteSliceIndex::Paged { pages, .. } => {
                let Some(page) = pages
                    .iter()
                    .find(|page| page.first_slice_id <= slice_id && slice_id <= page.last_slice_id)
                else {
                    return Ok(None);
                };
                let section = &self.sections[0];
                let offset = section.offset.checked_add(page.offset).ok_or_else(|| {
                    WireError::LimitExceeded("data seal slice page offset overflows".into())
                })?;
                let page_bytes = self.fetch_range(offset, page.len).await?;
                verify_digest("remote data seal slice page", &page_bytes, &page.digest)?;
                let slices = decode_slice_page(
                    &page_bytes,
                    page.count,
                    page.first_slice_id,
                    page.last_slice_id,
                )?;
                Ok(slices
                    .binary_search_by_key(&slice_id, |slice| slice.slice_id)
                    .ok()
                    .map(|position| slices[position].clone()))
            }
        }
    }

    /// Alias used by read-plan callers that think in terms of lookup rather
    /// than a table read.
    pub async fn lookup_slice(&self, slice_id: u64) -> WireResult<Option<SliceDescriptor>> {
        self.read_slice(slice_id).await
    }

    /// Fetch, authenticate, and decode the complete frame descriptor table.
    pub async fn read_frames(&self) -> WireResult<Arc<[FrameDescriptor]>> {
        self.frames
            .get_or_try_init(|| async {
                let index = self.load_frame_index().await?;
                match index.as_ref() {
                    RemoteFrameIndex::Legacy(frames) => Ok(frames.clone()),
                    RemoteFrameIndex::Paged { pages } => {
                        let mut frames = Vec::with_capacity(self.sections[1].count as usize);
                        for page in pages.iter() {
                            frames.extend(self.read_frame_page(page).await?.iter().cloned());
                        }
                        Ok(Arc::from(frames))
                    }
                }
            })
            .await
            .cloned()
    }

    /// Look up one frame descriptor without downloading unrelated pages.
    pub async fn read_frame(&self, frame_ordinal: u32) -> WireResult<Option<FrameDescriptor>> {
        let index = self.load_frame_index().await?;
        match index.as_ref() {
            RemoteFrameIndex::Legacy(frames) => Ok(frames
                .binary_search_by_key(&frame_ordinal, |frame| frame.frame_ordinal)
                .ok()
                .map(|position| frames[position].clone())),
            RemoteFrameIndex::Paged { pages } => {
                let Some(page) = pages.iter().find(|page| {
                    page.first_ordinal <= frame_ordinal && frame_ordinal <= page.last_ordinal
                }) else {
                    return Ok(None);
                };
                let frames = self.read_frame_page(page).await?;
                Ok(frames
                    .binary_search_by_key(&frame_ordinal, |frame| frame.frame_ordinal)
                    .ok()
                    .map(|position| frames[position].clone()))
            }
        }
    }

    /// Fetch, authenticate, and decode the complete object descriptor table.
    pub async fn read_objects(&self) -> WireResult<Arc<[DataObjectDescriptor]>> {
        self.objects
            .get_or_try_init(|| async {
                let index = self.load_object_index().await?;
                match index.as_ref() {
                    RemoteObjectIndex::Legacy(objects) => Ok(objects.clone()),
                    RemoteObjectIndex::Paged { pages } => {
                        let mut objects = Vec::with_capacity(self.sections[2].count as usize);
                        for page in pages.iter() {
                            objects.extend(self.read_object_page(page).await?.iter().cloned());
                        }
                        Ok(Arc::from(objects))
                    }
                }
            })
            .await
            .cloned()
    }

    /// Look up one object descriptor without downloading unrelated pages.
    pub async fn read_object(
        &self,
        object_ordinal: u32,
    ) -> WireResult<Option<DataObjectDescriptor>> {
        let index = self.load_object_index().await?;
        match index.as_ref() {
            RemoteObjectIndex::Legacy(objects) => Ok(objects
                .binary_search_by_key(&object_ordinal, |object| object.object_ordinal)
                .ok()
                .map(|position| objects[position].clone())),
            RemoteObjectIndex::Paged { pages } => {
                let Some(page) = pages.iter().find(|page| {
                    page.first_ordinal <= object_ordinal && object_ordinal <= page.last_ordinal
                }) else {
                    return Ok(None);
                };
                let objects = self.read_object_page(page).await?;
                Ok(objects
                    .binary_search_by_key(&object_ordinal, |object| object.object_ordinal)
                    .ok()
                    .map(|position| objects[position].clone()))
            }
        }
    }

    async fn load_frame_index(&self) -> WireResult<Arc<RemoteFrameIndex>> {
        self.frame_index
            .get_or_try_init(|| async {
                let section = &self.sections[1];
                if section.len <= 64 * 1024
                    && section.count <= super::data_seal::FRAME_PAGE_RECORD_LIMIT as u64
                {
                    let payload = self.read_section(1).await?;
                    return Ok(Arc::new(RemoteFrameIndex::Legacy(Arc::from(
                        decode_frames(&payload, section.count)?,
                    ))));
                }
                let prefix_len = usize::try_from(section.len)
                    .unwrap_or(MAX_TABLE_BYTES)
                    .min(TABLE_INDEX_HEADER_LEN);
                let mut prefix = self
                    .fetch_range(
                        section.offset,
                        u32::try_from(prefix_len).map_err(|_| {
                            WireError::LimitExceeded("data seal frame index exceeds u32".into())
                        })?,
                    )
                    .await?;
                if prefix.len() < FRAME_INDEX_MAGIC.len()
                    || &prefix[..FRAME_INDEX_MAGIC.len()] != FRAME_INDEX_MAGIC
                {
                    let payload = self.read_section(1).await?;
                    return Ok(Arc::new(RemoteFrameIndex::Legacy(Arc::from(
                        decode_frames(&payload, section.count)?,
                    ))));
                }
                if prefix.len() < TABLE_INDEX_HEADER_LEN {
                    return Err(WireError::Truncated {
                        what: "remote data seal frame index",
                        need: TABLE_INDEX_HEADER_LEN,
                        have: prefix.len(),
                    });
                }
                let directory_len = u32::from_le_bytes(prefix[24..28].try_into().unwrap()) as usize;
                let prefix_len = TABLE_INDEX_HEADER_LEN
                    .checked_add(directory_len)
                    .ok_or_else(|| {
                        WireError::LimitExceeded("data seal frame directory overflows".into())
                    })?;
                if prefix_len > MAX_DATA_SEAL_RANGE_BYTES {
                    return Err(WireError::LimitExceeded(
                        "data seal frame directory exceeds configured bound".into(),
                    ));
                }
                if prefix.len() < prefix_len {
                    prefix = self
                        .fetch_range(
                            section.offset,
                            u32::try_from(prefix_len).map_err(|_| {
                                WireError::LimitExceeded(
                                    "data seal frame directory exceeds u32".into(),
                                )
                            })?,
                        )
                        .await?;
                }
                let (_, pages) = decode_table_index_prefix(
                    &prefix[..prefix_len],
                    FRAME_INDEX_MAGIC,
                    section.count,
                    section.len,
                )?;
                Ok(Arc::new(RemoteFrameIndex::Paged {
                    pages: Arc::from(pages),
                }))
            })
            .await
            .cloned()
    }

    async fn load_object_index(&self) -> WireResult<Arc<RemoteObjectIndex>> {
        self.object_index
            .get_or_try_init(|| async {
                let section = &self.sections[2];
                if section.len <= 64 * 1024
                    && section.count <= super::data_seal::OBJECT_PAGE_RECORD_LIMIT as u64
                {
                    let payload = self.read_section(2).await?;
                    return Ok(Arc::new(RemoteObjectIndex::Legacy(Arc::from(
                        decode_objects(&payload, section.count)?,
                    ))));
                }
                let prefix_len = usize::try_from(section.len)
                    .unwrap_or(MAX_TABLE_BYTES)
                    .min(TABLE_INDEX_HEADER_LEN);
                let mut prefix = self
                    .fetch_range(
                        section.offset,
                        u32::try_from(prefix_len).map_err(|_| {
                            WireError::LimitExceeded("data seal object index exceeds u32".into())
                        })?,
                    )
                    .await?;
                if prefix.len() < OBJECT_INDEX_MAGIC.len()
                    || &prefix[..OBJECT_INDEX_MAGIC.len()] != OBJECT_INDEX_MAGIC
                {
                    let payload = self.read_section(2).await?;
                    return Ok(Arc::new(RemoteObjectIndex::Legacy(Arc::from(
                        decode_objects(&payload, section.count)?,
                    ))));
                }
                if prefix.len() < TABLE_INDEX_HEADER_LEN {
                    return Err(WireError::Truncated {
                        what: "remote data seal object index",
                        need: TABLE_INDEX_HEADER_LEN,
                        have: prefix.len(),
                    });
                }
                let directory_len = u32::from_le_bytes(prefix[24..28].try_into().unwrap()) as usize;
                let prefix_len = TABLE_INDEX_HEADER_LEN
                    .checked_add(directory_len)
                    .ok_or_else(|| {
                        WireError::LimitExceeded("data seal object directory overflows".into())
                    })?;
                if prefix_len > MAX_DATA_SEAL_RANGE_BYTES {
                    return Err(WireError::LimitExceeded(
                        "data seal object directory exceeds configured bound".into(),
                    ));
                }
                if prefix.len() < prefix_len {
                    prefix = self
                        .fetch_range(
                            section.offset,
                            u32::try_from(prefix_len).map_err(|_| {
                                WireError::LimitExceeded(
                                    "data seal object directory exceeds u32".into(),
                                )
                            })?,
                        )
                        .await?;
                }
                let (_, pages) = decode_table_index_prefix(
                    &prefix[..prefix_len],
                    OBJECT_INDEX_MAGIC,
                    section.count,
                    section.len,
                )?;
                Ok(Arc::new(RemoteObjectIndex::Paged {
                    pages: Arc::from(pages),
                }))
            })
            .await
            .cloned()
    }

    async fn read_frame_page(&self, page: &TablePageRef) -> WireResult<Arc<[FrameDescriptor]>> {
        if let Some(cached) = self.frame_pages.get(&page.offset) {
            return Ok(cached.clone());
        }
        let section = &self.sections[1];
        let offset = section.offset.checked_add(page.offset).ok_or_else(|| {
            WireError::LimitExceeded("data seal frame page offset overflows".into())
        })?;
        let bytes = self.fetch_range(offset, page.len).await?;
        verify_digest("remote data seal frame page", &bytes, &page.digest)?;
        let frames = decode_frame_records(&bytes, u64::from(page.count))?;
        if frames.first().map(|frame| frame.frame_ordinal) != Some(page.first_ordinal)
            || frames.last().map(|frame| frame.frame_ordinal) != Some(page.last_ordinal)
        {
            return Err(WireError::invalid(
                "remote data seal frame page",
                "page ordinal coverage is not canonical",
            ));
        }
        let frames: Arc<[FrameDescriptor]> = Arc::from(frames);
        self.frame_pages.insert(page.offset, frames.clone());
        Ok(frames)
    }

    async fn read_object_page(
        &self,
        page: &TablePageRef,
    ) -> WireResult<Arc<[DataObjectDescriptor]>> {
        if let Some(cached) = self.object_pages.get(&page.offset) {
            return Ok(cached.clone());
        }
        let section = &self.sections[2];
        let offset = section.offset.checked_add(page.offset).ok_or_else(|| {
            WireError::LimitExceeded("data seal object page offset overflows".into())
        })?;
        let bytes = self.fetch_range(offset, page.len).await?;
        verify_digest("remote data seal object page", &bytes, &page.digest)?;
        let objects = decode_object_records(&bytes, u64::from(page.count))?;
        if objects.first().map(|object| object.object_ordinal) != Some(page.first_ordinal)
            || objects.last().map(|object| object.object_ordinal) != Some(page.last_ordinal)
        {
            return Err(WireError::invalid(
                "remote data seal object page",
                "page ordinal coverage is not canonical",
            ));
        }
        let objects: Arc<[DataObjectDescriptor]> = Arc::from(objects);
        self.object_pages.insert(page.offset, objects.clone());
        Ok(objects)
    }

    /// Load all three tables and run the same closed-graph validation used by
    /// [`DataSealSnapshot::open`].  This is an explicit full validation path,
    /// not part of remote open.
    pub async fn validate_complete(&self) -> WireResult<()> {
        let bytes = self.read_all_sections().await?;
        let mut complete = Vec::with_capacity(
            DATA_SEAL_HEADER_LEN + bytes.iter().map(Vec::len).sum::<usize>() + DATA_SEAL_FOOTER_LEN,
        );
        let header = self.fetch_range(0, DATA_SEAL_HEADER_LEN as u32).await?;
        complete.extend_from_slice(&header);
        for payload in &bytes {
            complete.extend_from_slice(payload);
        }
        let footer = self
            .fetch_range(
                self.object_len - DATA_SEAL_FOOTER_LEN as u64,
                DATA_SEAL_FOOTER_LEN as u32,
            )
            .await?;
        complete.extend_from_slice(&footer);
        DataSealSnapshot::open(complete).map(|_| ())
    }

    async fn read_all_sections(&self) -> WireResult<[Vec<u8>; SECTION_COUNT]> {
        Ok([
            self.read_section(0).await?,
            self.read_section(1).await?,
            self.read_section(2).await?,
        ])
    }

    async fn load_slice_index(&self) -> WireResult<Arc<RemoteSliceIndex>> {
        self.slice_index
            .get_or_try_init(|| async {
                let section = &self.sections[0];
                let prefix_len = usize::try_from(section.len)
                    .ok()
                    .filter(|length| *length >= SLICE_INDEX_HEADER_LEN)
                    .map_or(section.len as usize, |_| SLICE_INDEX_HEADER_LEN);
                let prefix = self
                    .fetch_range(
                        section.offset,
                        u32::try_from(prefix_len).map_err(|_| {
                            WireError::LimitExceeded("data seal slice index exceeds u32".into())
                        })?,
                    )
                    .await?;
                if prefix.len() < SLICE_INDEX_MAGIC.len()
                    || &prefix[..SLICE_INDEX_MAGIC.len()] != SLICE_INDEX_MAGIC
                {
                    let payload = self.read_section(0).await?;
                    return Ok(Arc::new(RemoteSliceIndex::Legacy(Arc::from(
                        decode_slices(&payload, section.count)?,
                    ))));
                }
                if prefix.len() < SLICE_INDEX_HEADER_LEN {
                    return Err(WireError::Truncated {
                        what: "remote data seal slice index",
                        need: SLICE_INDEX_HEADER_LEN,
                        have: prefix.len(),
                    });
                }
                let header =
                    decode_slice_index_header(&prefix[..SLICE_INDEX_HEADER_LEN], section.count)?;
                let directory = self
                    .fetch_range(
                        section
                            .offset
                            .checked_add(SLICE_INDEX_HEADER_LEN as u64)
                            .ok_or_else(|| {
                                WireError::LimitExceeded(
                                    "data seal slice directory offset overflows".into(),
                                )
                            })?,
                        header.directory_len,
                    )
                    .await?;
                let pages = decode_slice_index_directory(&header, &directory, section.len)?;
                Ok(Arc::new(RemoteSliceIndex::Paged {
                    header,
                    pages: Arc::from(pages),
                }))
            })
            .await
            .cloned()
    }

    async fn read_section(&self, index: usize) -> WireResult<Vec<u8>> {
        let section = &self.sections[index];
        let len = usize::try_from(section.len)
            .map_err(|_| WireError::LimitExceeded("data seal section exceeds usize".into()))?;
        if len > MAX_DATA_SEAL_RANGE_BYTES {
            return Err(WireError::LimitExceeded(format!(
                "data seal section length {len} exceeds {MAX_DATA_SEAL_RANGE_BYTES}"
            )));
        }
        let bytes = self
            .fetch_range(
                section.offset,
                u32::try_from(len).map_err(|_| {
                    WireError::LimitExceeded("data seal section exceeds u32 range".into())
                })?,
            )
            .await?;
        verify_digest("remote data seal section", &bytes, &section.digest)?;
        Ok(bytes)
    }

    fn validate_sections(&self, sections: &[SectionRef; SECTION_COUNT]) -> WireResult<()> {
        let footer_offset = self.object_len - DATA_SEAL_FOOTER_LEN as u64;
        let mut expected_offset = DATA_SEAL_HEADER_LEN as u64;
        for (index, section) in sections.iter().enumerate() {
            if section.kind != (index as u8) + 1 || section.offset != expected_offset {
                return Err(WireError::invalid(
                    "remote data seal",
                    "section order or kind is invalid",
                ));
            }
            let section_len = usize::try_from(section.len)
                .map_err(|_| WireError::LimitExceeded("data seal section exceeds usize".into()))?;
            if section_len > MAX_TABLE_BYTES {
                return Err(WireError::LimitExceeded(
                    "data seal section exceeds configured bound".into(),
                ));
            }
            let end = section.offset.checked_add(section.len).ok_or_else(|| {
                WireError::LimitExceeded("data seal section end overflows u64".into())
            })?;
            if end > footer_offset {
                return Err(WireError::Truncated {
                    what: "remote data seal section",
                    need: section.len as usize,
                    have: footer_offset.saturating_sub(section.offset) as usize,
                });
            }
            expected_offset = end;
        }
        if expected_offset != footer_offset {
            return Err(WireError::invalid(
                "remote data seal",
                "unreferenced bytes before footer",
            ));
        }
        Ok(())
    }

    async fn fetch_range(&self, offset: u64, len: u32) -> WireResult<Vec<u8>> {
        let len_usize = usize::try_from(len)
            .map_err(|_| WireError::LimitExceeded("data seal range exceeds usize".into()))?;
        if len_usize > MAX_DATA_SEAL_RANGE_BYTES {
            return Err(WireError::LimitExceeded(format!(
                "data seal range length {len_usize} exceeds {MAX_DATA_SEAL_RANGE_BYTES}"
            )));
        }
        let end = offset
            .checked_add(u64::from(len))
            .ok_or_else(|| WireError::LimitExceeded("data seal range overflows u64".into()))?;
        if end > self.object_len {
            return Err(WireError::Truncated {
                what: "remote data seal range",
                need: len_usize,
                have: self.object_len.saturating_sub(offset) as usize,
            });
        }
        if len_usize == 0 {
            return Ok(Vec::new());
        }
        let mut bytes = vec![0u8; len_usize];
        let actual = self
            .client
            .get_object_range(&self.object_key, offset, &mut bytes)
            .await
            .map_err(|error| WireError::invalid("remote data seal range", error.to_string()))?;
        if actual != len_usize {
            return Err(WireError::Truncated {
                what: "remote data seal range",
                need: len_usize,
                have: actual,
            });
        }
        Ok(bytes)
    }
}

fn empty_sections() -> [SectionRef; SECTION_COUNT] {
    [SectionRef {
        kind: 0,
        offset: 0,
        len: 0,
        count: 0,
        digest: [0; 32],
    }; SECTION_COUNT]
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;

    use super::*;
    use crate::cadapter::client::ObjectBackend;
    use crate::native_base::wire::container::Codec;
    use crate::native_base::wire::frame::PayloadFormat;
    use crate::workspace::clustered_snapshot::data_seal::{
        DataObjectDescriptor, DataSealBuilder, DataSpan,
    };

    #[derive(Clone, Default)]
    struct CountingBackend {
        state: Arc<Mutex<State>>,
    }

    #[derive(Default)]
    struct State {
        objects: HashMap<String, Vec<u8>>,
        ranges: Vec<(u64, usize)>,
    }

    impl CountingBackend {
        fn insert(&self, key: &str, bytes: Vec<u8>) {
            self.state
                .lock()
                .expect("backend mutex")
                .objects
                .insert(key.to_string(), bytes);
        }

        fn ranges(&self) -> Vec<(u64, usize)> {
            self.state.lock().expect("backend mutex").ranges.clone()
        }

        fn tamper(&self, key: &str, offset: usize) {
            self.state
                .lock()
                .expect("backend mutex")
                .objects
                .get_mut(key)
                .expect("object exists")[offset] ^= 1;
        }
    }

    #[async_trait]
    impl ObjectBackend for CountingBackend {
        async fn put_object(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
            self.insert(key, data.to_vec());
            Ok(())
        }

        async fn get_object(&self, _key: &str) -> anyhow::Result<Option<Vec<u8>>> {
            Ok(None)
        }

        async fn get_object_range(
            &self,
            key: &str,
            offset: u64,
            buf: &mut [u8],
        ) -> anyhow::Result<usize> {
            let mut state = self.state.lock().expect("backend mutex");
            let Some(object) = state.objects.get(key) else {
                return Ok(0);
            };
            let start = usize::try_from(offset)?;
            if start >= object.len() {
                return Ok(0);
            }
            let count = buf.len().min(object.len() - start);
            buf[..count].copy_from_slice(&object[start..start + count]);
            state.ranges.push((offset, count));
            Ok(count)
        }

        async fn get_etag(&self, _key: &str) -> anyhow::Result<String> {
            Ok(String::new())
        }

        async fn delete_object(&self, _key: &str) -> anyhow::Result<()> {
            Ok(())
        }
    }

    fn object_ref(object_len: usize) -> ManifestObjectRef {
        ManifestObjectRef {
            object_id: [1; 16],
            kind: 2,
            object_len: object_len as u64,
            full_hash: [2; 32],
            key: b"seals/test.brfds".to_vec(),
        }
    }

    fn paged_seal() -> Vec<u8> {
        let mut builder = DataSealBuilder::new([11; 16], [12; 16]);
        builder
            .add_object(DataObjectDescriptor {
                object_ordinal: 0,
                object_key: b"packs/paged.brfdp".to_vec(),
                object_len: 4096,
                object_checksum: [13; 32],
                etag: Vec::new(),
            })
            .unwrap();
        builder
            .add_frame(FrameDescriptor {
                frame_ordinal: 0,
                object_ordinal: 0,
                object_offset: 64,
                stored_len: 300,
                raw_len: 300,
                payload_format: PayloadFormat::PlainBytes.as_u8(),
                codec: Codec::None.as_u8(),
                frame_checksum: [14; 16],
            })
            .unwrap();
        for index in 0..300u32 {
            builder
                .add_slice(SliceDescriptor {
                    slice_id: u64::from(index + 1),
                    logical_len: 1,
                    spans: vec![DataSpan {
                        frame_ordinal: 0,
                        raw_offset_in_frame: index,
                        raw_len: 1,
                    }],
                })
                .unwrap();
        }
        builder.build().unwrap()
    }

    #[tokio::test]
    async fn opens_with_header_only_and_loads_tables_on_demand() {
        let mut builder = DataSealBuilder::new([3; 16], [4; 16]);
        builder
            .add_object(DataObjectDescriptor {
                object_ordinal: 0,
                object_key: b"packs/0.brfdp".to_vec(),
                object_len: 1,
                object_checksum: [5; 32],
                etag: Vec::new(),
            })
            .unwrap();
        let bytes = builder.build().unwrap();
        let backend = CountingBackend::default();
        backend.insert("seals/test.brfds", bytes.clone());
        let client = ObjectClient::new(backend.clone());
        let remote = RemoteDataSeal::open(&client, object_ref(bytes.len()))
            .await
            .unwrap();
        assert_eq!(backend.ranges().len(), 1);
        assert_eq!(remote.read_objects().await.unwrap().len(), 1);
        // A one-record object table stays in the compact legacy layout; the
        // remote reader keeps that representation compatible as well.
        assert_eq!(backend.ranges().len(), 2);
        remote.read_objects().await.unwrap();
        assert_eq!(backend.ranges().len(), 2);
    }

    #[tokio::test]
    async fn rejects_tampered_table_digest() {
        let mut builder = DataSealBuilder::new([6; 16], [7; 16]);
        builder
            .add_object(DataObjectDescriptor {
                object_ordinal: 0,
                object_key: b"packs/0.brfdp".to_vec(),
                object_len: 1,
                object_checksum: [8; 32],
                etag: Vec::new(),
            })
            .unwrap();
        let bytes = builder.build().unwrap();
        let object_len = bytes.len();
        let header = &bytes[..DATA_SEAL_HEADER_LEN];
        let (_, _, _, _, _, sections) = decode_header(header).unwrap();
        let backend = CountingBackend::default();
        backend.insert("seals/test.brfds", bytes);
        let client = ObjectClient::new(backend.clone());
        let remote = RemoteDataSeal::open(&client, object_ref(object_len))
            .await
            .unwrap();
        backend.tamper("seals/test.brfds", sections[2].offset as usize);
        assert!(matches!(
            remote.read_objects().await,
            Err(WireError::HashMismatch { .. })
        ));
    }

    #[tokio::test]
    async fn slice_lookup_reads_directory_and_one_page_only() {
        let bytes = paged_seal();
        let backend = CountingBackend::default();
        backend.insert("seals/test.brfds", bytes.clone());
        let client = ObjectClient::new(backend.clone());
        let remote = RemoteDataSeal::open(&client, object_ref(bytes.len()))
            .await
            .unwrap();
        assert_eq!(backend.ranges().len(), 1);

        let info = remote.read_slice_index().await.unwrap();
        assert_eq!(info.record_count, 300);
        assert_eq!(info.page_count, 2);
        assert!(info.paged);
        assert_eq!(backend.ranges().len(), 3);

        let slice = remote.lookup_slice(257).await.unwrap().unwrap();
        assert_eq!(slice.slice_id, 257);
        assert_eq!(slice.logical_len, 1);
        assert_eq!(backend.ranges().len(), 4);
        assert!(remote.lookup_slice(1000).await.unwrap().is_none());
        assert_eq!(backend.ranges().len(), 4);

        let (_, _, _, _, _, sections) = decode_header(&bytes[..DATA_SEAL_HEADER_LEN]).unwrap();
        assert!(
            !backend
                .ranges()
                .iter()
                .any(|(offset, len)| *offset == sections[0].offset
                    && *len == sections[0].len as usize)
        );
    }

    fn descriptor_paged_seal() -> Vec<u8> {
        let mut builder = DataSealBuilder::new([21; 16], [22; 16]);
        for ordinal in 0..300u32 {
            builder
                .add_object(DataObjectDescriptor {
                    object_ordinal: ordinal,
                    object_key: format!("packs/descriptor-pages-{ordinal}.brfdp").into_bytes(),
                    object_len: 1 << 20,
                    object_checksum: [23; 32],
                    etag: Vec::new(),
                })
                .unwrap();
        }
        for ordinal in 0..2048u32 {
            builder
                .add_frame(FrameDescriptor {
                    frame_ordinal: ordinal,
                    object_ordinal: 0,
                    object_offset: 64 + u64::from(ordinal) * 64,
                    stored_len: 1,
                    raw_len: 1,
                    payload_format: PayloadFormat::PlainBytes.as_u8(),
                    codec: Codec::None.as_u8(),
                    frame_checksum: [24; 16],
                })
                .unwrap();
        }
        builder
            .add_slice(SliceDescriptor {
                slice_id: 1,
                logical_len: 1,
                spans: vec![DataSpan {
                    frame_ordinal: 1500,
                    raw_offset_in_frame: 0,
                    raw_len: 1,
                }],
            })
            .unwrap();
        builder.build().unwrap()
    }

    #[tokio::test]
    async fn descriptor_lookup_reads_only_selected_frame_and_object_pages() {
        let bytes = descriptor_paged_seal();
        let backend = CountingBackend::default();
        backend.insert("seals/test.brfds", bytes.clone());
        let client = ObjectClient::new(backend.clone());
        let remote = RemoteDataSeal::open(&client, object_ref(bytes.len()))
            .await
            .unwrap();
        assert_eq!(backend.ranges().len(), 1);

        let frame = remote.read_frame(1500).await.unwrap().unwrap();
        assert_eq!(frame.frame_ordinal, 1500);
        let after_frame = backend.ranges().len();
        assert!(after_frame >= 3);
        assert!(after_frame <= 4);

        let object = remote.read_object(257).await.unwrap().unwrap();
        assert_eq!(object.object_ordinal, 257);
        let after_object = backend.ranges().len();
        assert!(after_object >= after_frame + 2);

        // Repeated lookups use the bounded page caches.
        remote.read_frame(1500).await.unwrap();
        remote.read_object(257).await.unwrap();
        assert_eq!(backend.ranges().len(), after_object);

        let (_, _, _, _, _, sections) = decode_header(&bytes[..DATA_SEAL_HEADER_LEN]).unwrap();
        assert!(
            !backend
                .ranges()
                .iter()
                .any(|(offset, len)| (*offset == sections[1].offset
                    && *len == sections[1].len as usize)
                    || (*offset == sections[2].offset && *len == sections[2].len as usize))
        );
    }
}
