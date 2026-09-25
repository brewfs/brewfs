//! Typed, page-oriented merge-route payload for `BRFSM002`.
//!
//! A route record is keyed by the merged `DirKey`.  Its windows are sorted by
//! raw component bytes and carry only bounded authenticated source locators;
//! no route record requires loading an entire contributor cluster.

use crate::native_base::wire::error::{WireError, WireResult};
use crate::native_base::wire::uvarint::{Reader, Writer};

use super::directory::{MAX_RANGE_SOURCES, MAX_WINDOW_ENTRIES, NodeRef, WindowSourceKind};
use super::identity::DirKey;
use super::merge::PlannedDirectory;
use super::name::NameBytes;

pub const MERGE_ROUTE_MAGIC: &[u8; 8] = b"BRFRTI02";
pub const MERGE_ROUTE_HEADER_LEN: usize = 64;
pub const MERGE_ROUTE_PAGE_REF_LEN: usize = 80;
const MERGE_ROUTE_VERSION: u16 = 2;
const PAGE_RECORD_LIMIT: usize = 128;
const PAGE_TARGET_BYTES: usize = 64 * 1024;
const MAX_ROUTE_RECORDS: usize = 1 << 29;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectoryViewRecord {
    pub dir_key: DirKey,
    pub canonical_node: NodeRef,
    pub visible_entry_count: u64,
    pub entry_index_root: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MergeRouteSource {
    pub source_id: u32,
    pub kind: WindowSourceKind,
    /// Physical contributor for this source.  `source_id` is a planner-local
    /// grouping key and is not sufficient to address a remote cluster.
    pub contributor: NodeRef,
    pub first_name: NameBytes,
    pub last_name: NameBytes,
    pub entry_count: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MergeRouteWindow {
    pub lower: Option<NameBytes>,
    pub upper: Option<NameBytes>,
    pub visible_entry_count: u32,
    pub sources: Vec<MergeRouteSource>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MergeRouteRecord {
    pub view: DirectoryViewRecord,
    pub windows: Vec<MergeRouteWindow>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MergeRoutePageRef {
    pub first_dir_key: DirKey,
    pub last_dir_key: DirKey,
    pub offset: u64,
    pub len: u32,
    pub count: u32,
    pub digest: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MergeRouteIndex {
    pub directory_count: u64,
    pub page_record_limit: u32,
    pub pages: Vec<MergeRoutePageRef>,
}

impl MergeRouteRecord {
    /// Convert the bounded planner output into the authenticated route record
    /// persisted in the manifest.  Every source carries its physical
    /// contributor; no runtime code needs to infer a local node from a
    /// planner-local source id.
    pub fn from_planned_directory(
        plan: &PlannedDirectory,
        entry_index_root: [u8; 32],
    ) -> WireResult<Self> {
        let mut windows = Vec::with_capacity(plan.windows.len());
        for window in &plan.windows {
            let mut sources = Vec::with_capacity(window.sources.len());
            for source in &window.sources {
                let planned = plan
                    .sources
                    .iter()
                    .find(|candidate| {
                        candidate.source_id == source.source_id && candidate.kind == source.kind
                    })
                    .ok_or_else(|| {
                        WireError::invalid(
                            "merge route",
                            format!("window references missing source {}", source.source_id),
                        )
                    })?;
                sources.push(MergeRouteSource {
                    source_id: source.source_id,
                    kind: source.kind,
                    contributor: planned.contributor,
                    first_name: source.first_name.clone(),
                    last_name: source.last_name.clone(),
                    entry_count: source.entry_count,
                });
            }
            windows.push(MergeRouteWindow {
                lower: window.lower.clone(),
                upper: window.upper.clone(),
                visible_entry_count: window.visible_entry_count,
                sources,
            });
        }
        let record = Self {
            view: DirectoryViewRecord {
                dir_key: DirKey::new(plan.identity.dir_key),
                canonical_node: plan.canonical_node,
                visible_entry_count: plan.visible_entry_count,
                entry_index_root,
            },
            windows,
        };
        validate_records(std::slice::from_ref(&record))?;
        Ok(record)
    }
}

impl MergeRouteIndex {
    pub fn encode(records: &[MergeRouteRecord]) -> WireResult<Vec<u8>> {
        validate_records(records)?;
        let (index, directory, pages) = build_pages(records)?;
        let mut header = [0u8; MERGE_ROUTE_HEADER_LEN];
        header[..8].copy_from_slice(MERGE_ROUTE_MAGIC);
        header[8..10].copy_from_slice(&MERGE_ROUTE_VERSION.to_le_bytes());
        header[12..16].copy_from_slice(&(index.pages.len() as u32).to_le_bytes());
        header[16..24].copy_from_slice(&(records.len() as u64).to_le_bytes());
        header[24..28].copy_from_slice(&(directory.len() as u32).to_le_bytes());
        header[28..32].copy_from_slice(&index.page_record_limit.to_le_bytes());
        header[32..64].copy_from_slice(blake3::hash(&directory).as_bytes());

        let mut payload = Vec::with_capacity(
            MERGE_ROUTE_HEADER_LEN
                .checked_add(directory.len())
                .and_then(|length| {
                    pages
                        .iter()
                        .map(Vec::len)
                        .try_fold(length, |total, page_len| total.checked_add(page_len))
                })
                .ok_or_else(|| WireError::LimitExceeded("merge route payload overflows".into()))?,
        );
        payload.extend_from_slice(&header);
        payload.extend_from_slice(&directory);
        for page in pages {
            payload.extend_from_slice(&page);
        }
        if payload.len() > 64 * 1024 * 1024 {
            return Err(WireError::LimitExceeded(
                "merge route payload exceeds 64 MiB".into(),
            ));
        }
        Ok(payload)
    }

    pub fn decode(bytes: &[u8]) -> WireResult<Vec<MergeRouteRecord>> {
        if bytes.len() > 64 * 1024 * 1024 {
            return Err(WireError::LimitExceeded(
                "merge route payload exceeds 64 MiB".into(),
            ));
        }
        let index = decode_index(bytes)?;
        let mut records = Vec::with_capacity(index.directory_count as usize);
        for page in &index.pages {
            let page_bytes = page_bytes(bytes, page)?;
            verify_page(page_bytes, &page.digest)?;
            records.extend(decode_page(
                page_bytes,
                page.count,
                page.first_dir_key,
                page.last_dir_key,
            )?);
        }
        validate_records(&records)?;
        Ok(records)
    }

    pub fn index(bytes: &[u8]) -> WireResult<MergeRouteIndex> {
        decode_index(bytes)
    }

    /// Decode only the fixed header and page directory. Page bodies may be
    /// fetched later using the returned absolute-within-payload offsets.
    pub fn index_from_directory(bytes: &[u8], payload_len: usize) -> WireResult<MergeRouteIndex> {
        if payload_len > 64 * 1024 * 1024 || bytes.len() > payload_len {
            return Err(WireError::LimitExceeded(
                "merge route payload exceeds 64 MiB".into(),
            ));
        }
        decode_index_with_payload_len(bytes, payload_len)
    }

    pub fn decode_page_at(
        bytes: &[u8],
        page: &MergeRoutePageRef,
    ) -> WireResult<Vec<MergeRouteRecord>> {
        let page_bytes = page_bytes(bytes, page)?;
        verify_page(page_bytes, &page.digest)?;
        decode_page(
            page_bytes,
            page.count,
            page.first_dir_key,
            page.last_dir_key,
        )
    }

    /// Verify and decode a page fetched as a standalone range.
    pub fn decode_page_bytes(
        bytes: &[u8],
        page: &MergeRoutePageRef,
    ) -> WireResult<Vec<MergeRouteRecord>> {
        if bytes.len() != page.len as usize {
            return Err(WireError::Truncated {
                what: "merge route page",
                need: page.len as usize,
                have: bytes.len(),
            });
        }
        verify_page(bytes, &page.digest)?;
        decode_page(bytes, page.count, page.first_dir_key, page.last_dir_key)
    }
}

fn validate_records(records: &[MergeRouteRecord]) -> WireResult<()> {
    if records.len() > MAX_ROUTE_RECORDS {
        return Err(WireError::LimitExceeded(
            "merge route directory count exceeds limit".into(),
        ));
    }
    for pair in records.windows(2) {
        if pair[0].view.dir_key >= pair[1].view.dir_key {
            return Err(WireError::invalid(
                "merge route",
                "directory keys are not sorted and unique",
            ));
        }
    }
    for record in records {
        let node = record.view.canonical_node;
        if node.cluster_slot >= (1 << 29) || node.local_node_id == 0 {
            return Err(WireError::invalid(
                "merge route",
                "canonical node is outside the committed inode range",
            ));
        }
        let mut visible = 0u64;
        for (index, window) in record.windows.iter().enumerate() {
            validate_window(window)?;
            if index == 0 && window.lower.is_some() {
                return Err(WireError::invalid(
                    "merge route",
                    "first range window must start at negative infinity",
                ));
            }
            if index > 0 {
                let previous = &record.windows[index - 1];
                if previous.upper != window.lower {
                    return Err(WireError::invalid(
                        "merge route",
                        "range windows have a gap or overlap",
                    ));
                }
            }
            visible = visible
                .checked_add(u64::from(window.visible_entry_count))
                .ok_or_else(|| WireError::LimitExceeded("merge route count overflows".into()))?;
        }
        if record
            .windows
            .last()
            .is_some_and(|window| window.upper.is_some())
        {
            return Err(WireError::invalid(
                "merge route",
                "last range window must end at positive infinity",
            ));
        }
        if visible != record.view.visible_entry_count {
            return Err(WireError::invalid(
                "merge route",
                "window visible counts do not match DirectoryView",
            ));
        }
    }
    Ok(())
}

fn validate_window(window: &MergeRouteWindow) -> WireResult<()> {
    if let (Some(lower), Some(upper)) = (&window.lower, &window.upper)
        && lower >= upper
    {
        return Err(WireError::invalid(
            "merge route window",
            "window bounds are not ordered",
        ));
    }
    if window.sources.len() > MAX_RANGE_SOURCES
        || window.visible_entry_count as usize > MAX_WINDOW_ENTRIES
    {
        return Err(WireError::invalid(
            "merge route window",
            "source or visible-entry bound exceeded",
        ));
    }
    let mut source_count = 0u64;
    let mut source_ids = Vec::with_capacity(window.sources.len());
    for source in &window.sources {
        if source.source_id == 0
            || source.contributor.cluster_slot >= (1 << 29)
            || source.contributor.local_node_id == 0
            || source.first_name > source.last_name
            || source.entry_count == 0
            || window
                .lower
                .as_ref()
                .is_some_and(|lower| source.first_name < *lower)
            || window
                .upper
                .as_ref()
                .is_some_and(|upper| source.last_name >= *upper)
        {
            return Err(WireError::invalid(
                "merge route source",
                "source bounds or count are invalid",
            ));
        }
        source_ids.push(source.source_id);
        source_count = source_count
            .checked_add(u64::from(source.entry_count))
            .ok_or_else(|| WireError::LimitExceeded("merge route source count overflows".into()))?;
    }
    source_ids.sort_unstable();
    if source_ids.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(WireError::invalid(
            "merge route window",
            "source ids are not unique",
        ));
    }
    if source_count < u64::from(window.visible_entry_count) {
        return Err(WireError::invalid(
            "merge route window",
            "visible count exceeds source counts",
        ));
    }
    Ok(())
}

fn encode_bound(writer: &mut Writer, bound: &Option<NameBytes>) {
    writer.option_tag(bound.is_some());
    if let Some(bound) = bound {
        writer.bytes(bound.as_bytes());
    }
}

fn decode_bound(reader: &mut Reader<'_>) -> WireResult<Option<NameBytes>> {
    if !reader.option_tag("merge route bound")? {
        return Ok(None);
    }
    Ok(Some(
        NameBytes::new(reader.bytes("merge route bound")?.to_vec())
            .map_err(|error| WireError::invalid("merge route bound", error.to_string()))?,
    ))
}

fn encode_record(record: &MergeRouteRecord) -> WireResult<Vec<u8>> {
    let mut writer = Writer::new();
    writer.put(record.view.dir_key.as_ref());
    writer.u32(record.view.canonical_node.cluster_slot);
    writer.u32(record.view.canonical_node.local_node_id);
    writer.u64(record.view.visible_entry_count);
    writer.put(&record.view.entry_index_root);
    writer.uvarint(record.windows.len() as u64);
    for window in &record.windows {
        encode_bound(&mut writer, &window.lower);
        encode_bound(&mut writer, &window.upper);
        writer.u32(window.visible_entry_count);
        writer.uvarint(window.sources.len() as u64);
        for source in &window.sources {
            writer.u32(source.source_id);
            writer.u8(match source.kind {
                WindowSourceKind::FileEntries => 0,
                WindowSourceKind::CanonicalDirectoryEntries => 1,
            });
            writer.u8(0);
            writer.u16(0);
            writer.u32(source.contributor.cluster_slot);
            writer.u32(source.contributor.local_node_id);
            writer.bytes(source.first_name.as_bytes());
            writer.bytes(source.last_name.as_bytes());
            writer.u32(source.entry_count);
        }
    }
    Ok(writer.into_bytes())
}

fn decode_record(reader: &mut Reader<'_>) -> WireResult<MergeRouteRecord> {
    let dir_key = DirKey::new(
        reader
            .take(16, "merge route directory key")?
            .try_into()
            .unwrap(),
    );
    let canonical_node = NodeRef {
        cluster_slot: reader.u32("merge route node")?,
        local_node_id: reader.u32("merge route node")?,
    };
    let visible_entry_count = reader.u64("merge route view")?;
    let entry_index_root = reader.take(32, "merge route view")?.try_into().unwrap();
    let window_count = usize::try_from(reader.uvarint("merge route windows")?)
        .map_err(|_| WireError::LimitExceeded("merge route window count exceeds usize".into()))?;
    if window_count > MAX_ROUTE_RECORDS {
        return Err(WireError::LimitExceeded(
            "merge route window count exceeds limit".into(),
        ));
    }
    let mut windows = Vec::with_capacity(window_count);
    for _ in 0..window_count {
        let lower = decode_bound(reader)?;
        let upper = decode_bound(reader)?;
        let visible_entry_count = reader.u32("merge route window")?;
        let source_count =
            usize::try_from(reader.uvarint("merge route sources")?).map_err(|_| {
                WireError::LimitExceeded("merge route source count exceeds usize".into())
            })?;
        if source_count > MAX_RANGE_SOURCES {
            return Err(WireError::invalid(
                "merge route window",
                "source count exceeds bound",
            ));
        }
        let mut sources = Vec::with_capacity(source_count);
        for _ in 0..source_count {
            let source_id = reader.u32("merge route source")?;
            let kind = match reader.u8("merge route source")? {
                0 => WindowSourceKind::FileEntries,
                1 => WindowSourceKind::CanonicalDirectoryEntries,
                _ => {
                    return Err(WireError::invalid(
                        "merge route source",
                        "unknown source kind",
                    ));
                }
            };
            if reader.u8("merge route source")? != 0 || reader.u16("merge route source")? != 0 {
                return Err(WireError::invalid(
                    "merge route source",
                    "reserved bytes are non-zero",
                ));
            }
            let contributor = NodeRef {
                cluster_slot: reader.u32("merge route source contributor")?,
                local_node_id: reader.u32("merge route source contributor")?,
            };
            let first_name = NameBytes::new(reader.bytes("merge route source name")?.to_vec())
                .map_err(|error| {
                    WireError::invalid("merge route source name", error.to_string())
                })?;
            let last_name = NameBytes::new(reader.bytes("merge route source name")?.to_vec())
                .map_err(|error| {
                    WireError::invalid("merge route source name", error.to_string())
                })?;
            sources.push(MergeRouteSource {
                source_id,
                kind,
                contributor,
                first_name,
                last_name,
                entry_count: reader.u32("merge route source")?,
            });
        }
        windows.push(MergeRouteWindow {
            lower,
            upper,
            visible_entry_count,
            sources,
        });
    }
    Ok(MergeRouteRecord {
        view: DirectoryViewRecord {
            dir_key,
            canonical_node,
            visible_entry_count,
            entry_index_root,
        },
        windows,
    })
}

fn build_pages(
    records: &[MergeRouteRecord],
) -> WireResult<(MergeRouteIndex, Vec<u8>, Vec<Vec<u8>>)> {
    let mut pages = Vec::new();
    let mut refs = Vec::new();
    let mut current = Vec::new();
    let mut first = DirKey::new([0; 16]);
    let mut last = DirKey::new([0; 16]);
    let mut count = 0usize;
    for record in records {
        let encoded = encode_record(record)?;
        if !current.is_empty()
            && (count >= PAGE_RECORD_LIMIT
                || current
                    .len()
                    .checked_add(encoded.len())
                    .is_none_or(|length| length > PAGE_TARGET_BYTES))
        {
            pages.push(std::mem::take(&mut current));
            refs.push((first, last, count as u32));
            count = 0;
        }
        if current.is_empty() {
            first = record.view.dir_key;
        }
        last = record.view.dir_key;
        count += 1;
        current.extend_from_slice(&encoded);
    }
    if !current.is_empty() {
        pages.push(current);
        refs.push((first, last, count as u32));
    }
    let directory_len = pages
        .len()
        .checked_mul(MERGE_ROUTE_PAGE_REF_LEN)
        .ok_or_else(|| WireError::LimitExceeded("merge route directory overflows".into()))?;
    let mut directory = Vec::with_capacity(directory_len);
    let mut offset = u64::try_from(MERGE_ROUTE_HEADER_LEN + directory_len)
        .map_err(|_| WireError::LimitExceeded("merge route page offset overflows".into()))?;
    let mut page_refs = Vec::with_capacity(pages.len());
    for (index, page) in pages.iter().enumerate() {
        let len = u32::try_from(page.len())
            .map_err(|_| WireError::LimitExceeded("merge route page exceeds u32".into()))?;
        let (first_dir_key, last_dir_key, count) = refs[index];
        let digest = *blake3::hash(page).as_bytes();
        directory.extend_from_slice(first_dir_key.as_ref());
        directory.extend_from_slice(last_dir_key.as_ref());
        directory.extend_from_slice(&offset.to_le_bytes());
        directory.extend_from_slice(&len.to_le_bytes());
        directory.extend_from_slice(&count.to_le_bytes());
        directory.extend_from_slice(&digest);
        page_refs.push(MergeRoutePageRef {
            first_dir_key,
            last_dir_key,
            offset,
            len,
            count,
            digest,
        });
        offset = offset
            .checked_add(u64::from(len))
            .ok_or_else(|| WireError::LimitExceeded("merge route page offset overflows".into()))?;
    }
    Ok((
        MergeRouteIndex {
            directory_count: records.len() as u64,
            page_record_limit: PAGE_RECORD_LIMIT as u32,
            pages: page_refs,
        },
        directory,
        pages,
    ))
}

fn decode_index(bytes: &[u8]) -> WireResult<MergeRouteIndex> {
    decode_index_with_payload_len(bytes, bytes.len())
}

fn decode_index_with_payload_len(bytes: &[u8], payload_len: usize) -> WireResult<MergeRouteIndex> {
    if bytes.len() < MERGE_ROUTE_HEADER_LEN || &bytes[..8] != MERGE_ROUTE_MAGIC {
        return Err(WireError::UnsupportedFormat(
            "not a BRFRTI02 merge route payload".into(),
        ));
    }
    if u16::from_le_bytes(bytes[8..10].try_into().unwrap()) != MERGE_ROUTE_VERSION
        || bytes[10..12].iter().any(|byte| *byte != 0)
    {
        return Err(WireError::UnsupportedFormat(
            "unsupported merge route payload version".into(),
        ));
    }
    let page_count = u32::from_le_bytes(bytes[12..16].try_into().unwrap());
    let directory_count = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
    if directory_count > MAX_ROUTE_RECORDS as u64 {
        return Err(WireError::LimitExceeded(
            "merge route directory count exceeds limit".into(),
        ));
    }
    let directory_len = u32::from_le_bytes(bytes[24..28].try_into().unwrap()) as usize;
    let expected_directory_len = (page_count as usize)
        .checked_mul(MERGE_ROUTE_PAGE_REF_LEN)
        .ok_or_else(|| WireError::LimitExceeded("merge route directory overflows".into()))?;
    if directory_len != expected_directory_len {
        return Err(WireError::invalid(
            "merge route index",
            "directory length is not canonical",
        ));
    }
    let page_record_limit = u32::from_le_bytes(bytes[28..32].try_into().unwrap());
    if page_record_limit == 0 || page_record_limit as usize > PAGE_RECORD_LIMIT {
        return Err(WireError::invalid(
            "merge route index",
            "page record limit is outside bounds",
        ));
    }
    let directory_end = MERGE_ROUTE_HEADER_LEN
        .checked_add(directory_len)
        .ok_or_else(|| WireError::LimitExceeded("merge route directory overflows".into()))?;
    if directory_end > bytes.len() {
        return Err(WireError::Truncated {
            what: "merge route directory",
            need: directory_end,
            have: bytes.len(),
        });
    }
    let computed = blake3::hash(&bytes[MERGE_ROUTE_HEADER_LEN..directory_end]);
    if computed.as_bytes() != &bytes[32..64] {
        return Err(WireError::HashMismatch {
            what: "merge route directory",
            stored: hex::encode(&bytes[32..64]),
            computed: hex::encode(computed.as_bytes()),
        });
    }
    let mut pages = Vec::with_capacity(page_count as usize);
    let mut expected_offset = directory_end as u64;
    let mut total_count = 0u64;
    let mut previous_last = None;
    for index in 0..page_count as usize {
        let start = MERGE_ROUTE_HEADER_LEN + index * MERGE_ROUTE_PAGE_REF_LEN;
        let entry = &bytes[start..start + MERGE_ROUTE_PAGE_REF_LEN];
        let first_dir_key = DirKey::new(entry[..16].try_into().unwrap());
        let last_dir_key = DirKey::new(entry[16..32].try_into().unwrap());
        let offset = u64::from_le_bytes(entry[32..40].try_into().unwrap());
        let len = u32::from_le_bytes(entry[40..44].try_into().unwrap());
        let count = u32::from_le_bytes(entry[44..48].try_into().unwrap());
        let digest = entry[48..80].try_into().unwrap();
        let end = offset
            .checked_add(u64::from(len))
            .ok_or_else(|| WireError::LimitExceeded("merge route page end overflows".into()))?;
        if first_dir_key > last_dir_key
            || previous_last.is_some_and(|previous| first_dir_key <= previous)
            || offset != expected_offset
            || len == 0
            || count == 0
            || count > page_record_limit
            || end > payload_len as u64
        {
            return Err(WireError::invalid(
                "merge route index",
                "page ordering or bounds are invalid",
            ));
        }
        total_count = total_count
            .checked_add(u64::from(count))
            .ok_or_else(|| WireError::LimitExceeded("merge route count overflows".into()))?;
        previous_last = Some(last_dir_key);
        expected_offset = end;
        pages.push(MergeRoutePageRef {
            first_dir_key,
            last_dir_key,
            offset,
            len,
            count,
            digest,
        });
    }
    if total_count != directory_count || expected_offset != payload_len as u64 {
        return Err(WireError::invalid(
            "merge route index",
            "page counts do not cover the complete payload",
        ));
    }
    Ok(MergeRouteIndex {
        directory_count,
        page_record_limit,
        pages,
    })
}

fn page_bytes<'a>(bytes: &'a [u8], page: &MergeRoutePageRef) -> WireResult<&'a [u8]> {
    let start = usize::try_from(page.offset)
        .map_err(|_| WireError::LimitExceeded("merge route page offset exceeds usize".into()))?;
    let end = start
        .checked_add(page.len as usize)
        .ok_or_else(|| WireError::LimitExceeded("merge route page end overflows".into()))?;
    bytes.get(start..end).ok_or(WireError::Truncated {
        what: "merge route page",
        need: page.len as usize,
        have: bytes.len().saturating_sub(start),
    })
}

fn verify_page(bytes: &[u8], expected: &[u8; 32]) -> WireResult<()> {
    let actual = blake3::hash(bytes);
    if expected != actual.as_bytes() {
        return Err(WireError::HashMismatch {
            what: "merge route page",
            stored: hex::encode(expected),
            computed: hex::encode(actual.as_bytes()),
        });
    }
    Ok(())
}

fn decode_page(
    bytes: &[u8],
    count: u32,
    first_dir_key: DirKey,
    last_dir_key: DirKey,
) -> WireResult<Vec<MergeRouteRecord>> {
    let mut reader = Reader::new(bytes);
    let mut records = Vec::with_capacity(count as usize);
    let mut previous = None;
    for _ in 0..count {
        let record = decode_record(&mut reader)?;
        if previous.is_some_and(|previous| record.view.dir_key <= previous) {
            return Err(WireError::invalid(
                "merge route page",
                "directory keys are not sorted",
            ));
        }
        previous = Some(record.view.dir_key);
        records.push(record);
    }
    if !reader.is_empty()
        || records.first().map(|record| record.view.dir_key) != Some(first_dir_key)
        || records.last().map(|record| record.view.dir_key) != Some(last_dir_key)
    {
        return Err(WireError::invalid(
            "merge route page",
            "page record coverage is not canonical",
        ));
    }
    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workspace_overlay::clustered_snapshot::directory::DirectoryIdentity;
    use crate::workspace_overlay::clustered_snapshot::merge::{
        ContributionEntry, DirectoryContribution, build_directory_plan,
    };

    fn record(seed: u8, visible_entry_count: u32) -> MergeRouteRecord {
        MergeRouteRecord {
            view: DirectoryViewRecord {
                dir_key: DirKey::new([seed; 16]),
                canonical_node: NodeRef {
                    cluster_slot: u32::from(seed),
                    local_node_id: 1,
                },
                visible_entry_count: u64::from(visible_entry_count),
                entry_index_root: [seed.wrapping_add(1); 32],
            },
            windows: vec![MergeRouteWindow {
                lower: None,
                upper: None,
                visible_entry_count,
                sources: vec![MergeRouteSource {
                    source_id: u32::from(seed) + 1,
                    kind: WindowSourceKind::FileEntries,
                    contributor: NodeRef {
                        cluster_slot: u32::from(seed),
                        local_node_id: 1,
                    },
                    first_name: NameBytes::new(b"a".to_vec()).unwrap(),
                    last_name: NameBytes::new(b"z".to_vec()).unwrap(),
                    entry_count: visible_entry_count.max(1),
                }],
            }],
        }
    }

    #[test]
    fn merge_route_round_trips_and_is_deterministic() {
        let records = vec![record(1, 1), record(2, 2)];
        let first = MergeRouteIndex::encode(&records).unwrap();
        assert_eq!(first, MergeRouteIndex::encode(&records).unwrap());
        assert_eq!(MergeRouteIndex::decode(&first).unwrap(), records);
        assert_eq!(MergeRouteIndex::index(&first).unwrap().pages.len(), 1);
    }

    #[test]
    fn merge_route_rejects_gap_and_page_tampering() {
        let mut invalid = record(1, 1);
        invalid.windows[0].lower = Some(NameBytes::new(b"a".to_vec()).unwrap());
        assert!(MergeRouteIndex::encode(&[invalid]).is_err());

        let records = vec![record(1, 1)];
        let mut bytes = MergeRouteIndex::encode(&records).unwrap();
        let page = MergeRouteIndex::index(&bytes).unwrap().pages[0];
        bytes[page.offset as usize] ^= 1;
        assert!(matches!(
            MergeRouteIndex::decode(&bytes),
            Err(WireError::HashMismatch { .. })
        ));
    }

    #[test]
    fn planner_route_persists_physical_contributor() {
        let dir_key = DirKey::new([7; 16]);
        let plan = build_directory_plan(
            DirectoryIdentity::from_dir_key([8; 32], dir_key),
            vec![DirectoryContribution {
                dir_key,
                node: NodeRef {
                    cluster_slot: 11,
                    local_node_id: 23,
                },
                attributes_digest: [9; 32],
                entries: vec![ContributionEntry {
                    name: NameBytes::new(b"sample".to_vec()).unwrap(),
                    inode: 42,
                    kind: 1,
                    child_dir_key: None,
                    attributes_digest: [0; 32],
                }],
            }],
        )
        .unwrap();
        let record = MergeRouteRecord::from_planned_directory(&plan, [10; 32]).unwrap();
        assert_eq!(
            record.windows[0].sources[0].contributor,
            NodeRef {
                cluster_slot: 11,
                local_node_id: 23,
            }
        );
        let bytes = MergeRouteIndex::encode(&[record.clone()]).unwrap();
        assert_eq!(MergeRouteIndex::decode(&bytes).unwrap(), vec![record]);
    }
}
