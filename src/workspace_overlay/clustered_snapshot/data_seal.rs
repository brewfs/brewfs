//! v2 Data Seal (`BRFDS002`).
//!
//! The seal is the publication boundary between logical SliceIds in a
//! metadata cluster and immutable DataPack bytes.  It stores three
//! deterministic, independently authenticated tables: slice spans, frame
//! descriptors, and object descriptors.  The table payloads are sorted and
//! length-bounded, so a later remote reader can page them without changing
//! the semantic identity of a seal.

use std::collections::BTreeMap;

use sha2::{Digest, Sha256};

use crate::native_base::wire::container::{Codec, crc32c};
use crate::native_base::wire::datapack::ScrubbedFrame;
use crate::native_base::wire::error::{WireError, WireResult};
use crate::native_base::wire::frame::{FRAME_HEADER_LEN, PayloadFormat};
use crate::native_base::wire::uvarint::{Reader, Writer};

use super::data_pack::DataPackSnapshot;

pub const DATA_SEAL_MAGIC: &[u8; 8] = b"BRFDS002";
pub const DATA_SEAL_HEADER_LEN: usize = 4096;
pub const DATA_SEAL_FOOTER_LEN: usize = 64;
const DATA_SEAL_FOOTER_MAGIC: &[u8; 8] = b"BRFDSEND";
pub(crate) const SECTION_COUNT: usize = 3;
const SECTION_REF_LEN: usize = 64;
pub(crate) const MAX_TABLE_BYTES: usize = 256 * 1024 * 1024;
const MAX_KEY_LEN: usize = 1024;

// Slice descriptors are variable-width because a logical slice can span more
// than one frame. Keep a small authenticated directory in front of the
// records so a remote reader can select one bounded page by SliceId without
// downloading the complete section.
pub(crate) const SLICE_INDEX_MAGIC: &[u8; 8] = b"BRFDSIX2";
const SLICE_INDEX_VERSION: u16 = 1;
pub(crate) const SLICE_INDEX_HEADER_LEN: usize = 64;
pub(crate) const SLICE_PAGE_REF_LEN: usize = 64;
pub(crate) const SLICE_PAGE_RECORD_LIMIT: usize = 256;
const SLICE_PAGE_TARGET_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SliceIndexHeader {
    pub(crate) page_count: u32,
    pub(crate) record_count: u64,
    pub(crate) directory_len: u32,
    pub(crate) page_record_limit: u32,
    pub(crate) directory_digest: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SlicePageRef {
    pub(crate) first_slice_id: u64,
    pub(crate) last_slice_id: u64,
    /// Offset relative to the beginning of the slice section payload.
    pub(crate) offset: u64,
    pub(crate) len: u32,
    pub(crate) count: u32,
    pub(crate) digest: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataSpan {
    pub frame_ordinal: u32,
    pub raw_offset_in_frame: u32,
    pub raw_len: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SliceDescriptor {
    pub slice_id: u64,
    pub logical_len: u64,
    pub spans: Vec<DataSpan>,
}

/// The compact frame-table value used by BRFDS002. `frame_checksum` is the
/// first 16 bytes of SHA-256(FrameHeader || stored payload), matching the v2
/// wire spec while keeping the hot seal table compact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FrameDescriptor {
    pub frame_ordinal: u32,
    pub object_ordinal: u32,
    pub object_offset: u64,
    pub stored_len: u32,
    pub raw_len: u32,
    pub payload_format: u8,
    pub codec: u8,
    pub frame_checksum: [u8; 16],
}

impl FrameDescriptor {
    pub const ENCODED_LEN: usize = 48;

    pub fn from_pack_frame(object_ordinal: u32, frame: &ScrubbedFrame) -> WireResult<Self> {
        let stored_digest = frame.header.stored_digest(&frame.stored);
        Ok(Self {
            frame_ordinal: u32::try_from(frame.header.ordinal)
                .map_err(|_| WireError::LimitExceeded("frame ordinal exceeds u32".into()))?,
            object_ordinal,
            object_offset: frame.object_offset,
            stored_len: frame.header.stored_len,
            raw_len: frame.header.raw_len,
            payload_format: frame.header.payload_format.as_u8(),
            codec: frame.header.codec.as_u8(),
            frame_checksum: stored_digest[..16].try_into().unwrap(),
        })
    }

    fn encode_into(&self, writer: &mut Writer) {
        writer.u32(self.frame_ordinal);
        writer.u32(self.object_ordinal);
        writer.u64(self.object_offset);
        writer.u32(self.stored_len);
        writer.u32(self.raw_len);
        writer.u8(self.payload_format);
        writer.u8(self.codec);
        writer.u16(0);
        writer.put(&self.frame_checksum);
        writer.u32(0);
    }

    fn decode(reader: &mut Reader<'_>) -> WireResult<Self> {
        let frame_ordinal = reader.u32("data seal frame descriptor")?;
        let object_ordinal = reader.u32("data seal frame descriptor")?;
        let object_offset = reader.u64("data seal frame descriptor")?;
        let stored_len = reader.u32("data seal frame descriptor")?;
        let raw_len = reader.u32("data seal frame descriptor")?;
        let payload_format = reader.u8("data seal frame descriptor")?;
        let codec = reader.u8("data seal frame descriptor")?;
        if reader.u16("data seal frame descriptor")? != 0 {
            return Err(WireError::invalid(
                "data seal frame descriptor",
                "reserved bytes are non-zero",
            ));
        }
        let frame_checksum = reader
            .take(16, "data seal frame descriptor")?
            .try_into()
            .unwrap();
        if reader.u32("data seal frame descriptor")? != 0 {
            return Err(WireError::invalid(
                "data seal frame descriptor",
                "reserved bytes are non-zero",
            ));
        }
        validate_frame_codec(payload_format, codec)?;
        Ok(Self {
            frame_ordinal,
            object_ordinal,
            object_offset,
            stored_len,
            raw_len,
            payload_format,
            codec,
            frame_checksum,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataObjectDescriptor {
    pub object_ordinal: u32,
    pub object_key: Vec<u8>,
    pub object_len: u64,
    pub object_checksum: [u8; 32],
    pub etag: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SectionRef {
    pub(crate) kind: u8,
    pub(crate) offset: u64,
    pub(crate) len: u64,
    pub(crate) count: u64,
    pub(crate) digest: [u8; 32],
}

impl SectionRef {
    fn encode_into(self, out: &mut [u8]) {
        out.fill(0);
        out[0] = self.kind;
        out[8..16].copy_from_slice(&self.offset.to_le_bytes());
        out[16..24].copy_from_slice(&self.len.to_le_bytes());
        out[24..32].copy_from_slice(&self.count.to_le_bytes());
        out[32..64].copy_from_slice(&self.digest);
    }

    fn decode(bytes: &[u8]) -> WireResult<Self> {
        if bytes.len() != SECTION_REF_LEN || bytes[1..8].iter().any(|byte| *byte != 0) {
            return Err(WireError::invalid(
                "data seal section reference",
                "invalid length or reserved bytes",
            ));
        }
        Ok(Self {
            kind: bytes[0],
            offset: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
            len: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
            count: u64::from_le_bytes(bytes[24..32].try_into().unwrap()),
            digest: bytes[32..64].try_into().unwrap(),
        })
    }
}

/// Deterministic Data Seal producer. A seal is valid only when all three
/// tables form a closed SliceId -> frame -> object graph.
#[derive(Clone, Debug)]
pub struct DataSealBuilder {
    cluster_id: [u8; 16],
    volume_id: [u8; 16],
    slices: BTreeMap<u64, SliceDescriptor>,
    frames: BTreeMap<u32, FrameDescriptor>,
    objects: BTreeMap<u32, DataObjectDescriptor>,
}

impl DataSealBuilder {
    pub fn new(cluster_id: [u8; 16], volume_id: [u8; 16]) -> Self {
        Self {
            cluster_id,
            volume_id,
            slices: BTreeMap::new(),
            frames: BTreeMap::new(),
            objects: BTreeMap::new(),
        }
    }

    pub fn add_slice(&mut self, descriptor: SliceDescriptor) -> WireResult<()> {
        if descriptor.slice_id == 0 {
            return Err(WireError::invalid(
                "data seal slice",
                "slice_id must be non-zero",
            ));
        }
        if self.slices.contains_key(&descriptor.slice_id) {
            return Err(WireError::invalid("data seal slice", "duplicate slice_id"));
        }
        self.slices.insert(descriptor.slice_id, descriptor);
        Ok(())
    }

    pub fn add_frame(&mut self, descriptor: FrameDescriptor) -> WireResult<()> {
        validate_frame_descriptor(&descriptor)?;
        if self.frames.contains_key(&descriptor.frame_ordinal) {
            return Err(WireError::invalid(
                "data seal frame",
                "duplicate frame ordinal",
            ));
        }
        self.frames.insert(descriptor.frame_ordinal, descriptor);
        Ok(())
    }

    pub fn add_frame_from_pack(
        &mut self,
        object_ordinal: u32,
        frame: &ScrubbedFrame,
    ) -> WireResult<()> {
        self.add_frame(FrameDescriptor::from_pack_frame(object_ordinal, frame)?)
    }

    pub fn add_object(&mut self, descriptor: DataObjectDescriptor) -> WireResult<()> {
        validate_object_descriptor(&descriptor)?;
        if self.objects.contains_key(&descriptor.object_ordinal) {
            return Err(WireError::invalid(
                "data seal object",
                "duplicate object ordinal",
            ));
        }
        self.objects.insert(descriptor.object_ordinal, descriptor);
        Ok(())
    }

    pub fn validate(&self) -> WireResult<()> {
        validate_tables(&self.slices, &self.frames, &self.objects)
    }

    pub fn build(&self) -> WireResult<Vec<u8>> {
        self.validate()?;
        let slices = encode_slices(&self.slices)?;
        let frames = encode_frames(&self.frames)?;
        let objects = encode_objects(&self.objects)?;
        let payloads = [slices, frames, objects];
        let semantic_hash = semantic_hash(&payloads);
        let sections = build_section_refs(
            &payloads,
            [
                self.slices.len() as u64,
                self.frames.len() as u64,
                self.objects.len() as u64,
            ],
        )?;
        let footer_offset = sections
            .iter()
            .try_fold(DATA_SEAL_HEADER_LEN as u64, |end, section| {
                end.checked_add(section.len)
            })
            .ok_or_else(|| WireError::LimitExceeded("data seal length overflows u64".into()))?;
        let object_len = footer_offset
            .checked_add(DATA_SEAL_FOOTER_LEN as u64)
            .ok_or_else(|| WireError::LimitExceeded("data seal length overflows u64".into()))?;

        let mut header = vec![0u8; DATA_SEAL_HEADER_LEN];
        encode_header(
            &mut header,
            self.cluster_id,
            self.volume_id,
            object_len,
            self.slices.len() as u64,
            self.frames.len() as u64,
            self.objects.len() as u64,
            semantic_hash,
            &sections,
        )?;
        let mut bytes = header;
        for payload in &payloads {
            bytes.extend_from_slice(payload);
        }
        let content_hash: [u8; 32] = Sha256::digest(&bytes).into();
        bytes.extend_from_slice(&encode_footer(object_len, content_hash));
        debug_assert_eq!(bytes.len() as u64, object_len);
        Ok(bytes)
    }
}

/// Parsed, fully authenticated Data Seal. Tables are retained in sorted form
/// and lookup uses binary search; the raw bytes remain available for a future
/// remote page reader.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataSealSnapshot {
    bytes: Vec<u8>,
    cluster_id: [u8; 16],
    volume_id: [u8; 16],
    semantic_hash: [u8; 32],
    slices: Vec<SliceDescriptor>,
    frames: Vec<FrameDescriptor>,
    objects: Vec<DataObjectDescriptor>,
}

impl DataSealSnapshot {
    pub fn open(bytes: Vec<u8>) -> WireResult<Self> {
        if bytes.len() < DATA_SEAL_HEADER_LEN + DATA_SEAL_FOOTER_LEN
            || &bytes[..8] != DATA_SEAL_MAGIC
        {
            return Err(WireError::UnsupportedFormat(
                "not a BRFDS002 data seal".into(),
            ));
        }
        let (cluster_id, volume_id, object_len, counts, semantic, sections) =
            decode_header(&bytes[..DATA_SEAL_HEADER_LEN])?;
        if object_len as usize != bytes.len() {
            return Err(WireError::invalid(
                "data seal",
                "header object_len does not match bytes",
            ));
        }
        let footer_offset = bytes.len() - DATA_SEAL_FOOTER_LEN;
        decode_footer(&bytes[footer_offset..], &bytes[..footer_offset], object_len)?;

        let mut payloads = Vec::with_capacity(SECTION_COUNT);
        let mut expected_offset = DATA_SEAL_HEADER_LEN as u64;
        for (index, section) in sections.iter().enumerate() {
            if section.kind != (index as u8) + 1 || section.offset != expected_offset {
                return Err(WireError::invalid(
                    "data seal section",
                    "section order or offset is invalid",
                ));
            }
            let end = section.offset.checked_add(section.len).ok_or_else(|| {
                WireError::LimitExceeded("data seal section overflows u64".into())
            })?;
            let section_len = usize::try_from(section.len)
                .map_err(|_| WireError::LimitExceeded("data seal section exceeds usize".into()))?;
            if end > footer_offset as u64 || section_len > MAX_TABLE_BYTES {
                return Err(WireError::Truncated {
                    what: "data seal section",
                    need: section_len,
                    have: footer_offset
                        .saturating_sub(usize::try_from(section.offset).unwrap_or(footer_offset)),
                });
            }
            let section_offset = usize::try_from(section.offset).map_err(|_| {
                WireError::LimitExceeded("data seal section offset exceeds usize".into())
            })?;
            let section_end = usize::try_from(end).map_err(|_| {
                WireError::LimitExceeded("data seal section end exceeds usize".into())
            })?;
            let payload = bytes[section_offset..section_end].to_vec();
            verify_digest("data seal section", &payload, &section.digest)?;
            payloads.push(payload);
            expected_offset = end;
        }
        if expected_offset != footer_offset as u64 {
            return Err(WireError::invalid(
                "data seal",
                "unreferenced bytes before footer",
            ));
        }
        let payloads: [Vec<u8>; SECTION_COUNT] = payloads
            .try_into()
            .map_err(|_| WireError::invalid("data seal", "invalid section count"))?;
        let slices = decode_slices(&payloads[0], sections[0].count)?;
        let frames = decode_frames(&payloads[1], sections[1].count)?;
        let objects = decode_objects(&payloads[2], sections[2].count)?;
        if counts
            != [
                slices.len() as u64,
                frames.len() as u64,
                objects.len() as u64,
            ]
        {
            return Err(WireError::invalid(
                "data seal",
                "table counts do not match decoded records",
            ));
        }
        let slices_map = slices
            .iter()
            .cloned()
            .map(|slice| (slice.slice_id, slice))
            .collect::<BTreeMap<_, _>>();
        let frames_map = frames
            .iter()
            .cloned()
            .map(|frame| (frame.frame_ordinal, frame))
            .collect::<BTreeMap<_, _>>();
        let objects_map = objects
            .iter()
            .cloned()
            .map(|object| (object.object_ordinal, object))
            .collect::<BTreeMap<_, _>>();
        validate_tables(&slices_map, &frames_map, &objects_map)?;
        if semantic_hash(&payloads) != semantic {
            return Err(WireError::HashMismatch {
                what: "data seal semantic hash",
                stored: hex::encode(semantic),
                computed: hex::encode(semantic_hash(&payloads)),
            });
        }
        Ok(Self {
            bytes,
            cluster_id,
            volume_id,
            semantic_hash: semantic,
            slices,
            frames,
            objects,
        })
    }

    pub fn object_bytes(&self) -> &[u8] {
        &self.bytes
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

    pub fn slices(&self) -> &[SliceDescriptor] {
        &self.slices
    }

    pub fn frames(&self) -> &[FrameDescriptor] {
        &self.frames
    }

    pub fn objects(&self) -> &[DataObjectDescriptor] {
        &self.objects
    }

    pub fn lookup_slice(&self, slice_id: u64) -> Option<&SliceDescriptor> {
        self.slices
            .binary_search_by_key(&slice_id, |slice| slice.slice_id)
            .ok()
            .map(|index| &self.slices[index])
    }
}

fn validate_tables(
    slices: &BTreeMap<u64, SliceDescriptor>,
    frames: &BTreeMap<u32, FrameDescriptor>,
    objects: &BTreeMap<u32, DataObjectDescriptor>,
) -> WireResult<()> {
    for object in objects.values() {
        validate_object_descriptor(object)?;
    }
    for frame in frames.values() {
        validate_frame_descriptor(frame)?;
        let object = objects.get(&frame.object_ordinal).ok_or_else(|| {
            WireError::invalid(
                "data seal frame",
                format!("frame {} references missing object", frame.frame_ordinal),
            )
        })?;
        let end = frame
            .object_offset
            .checked_add(FRAME_HEADER_LEN as u64)
            .and_then(|end| end.checked_add(u64::from(frame.stored_len)))
            .ok_or_else(|| WireError::LimitExceeded("frame range overflows u64".into()))?;
        if end > object.object_len {
            return Err(WireError::invalid(
                "data seal frame",
                format!("frame {} exceeds object bounds", frame.frame_ordinal),
            ));
        }
    }
    for slice in slices.values() {
        if slice.slice_id == 0 || slice.logical_len == 0 || slice.spans.is_empty() {
            return Err(WireError::invalid(
                "data seal slice",
                "slice id, logical length, and spans must be non-zero",
            ));
        }
        let mut logical = 0u64;
        let mut previous: Option<(u32, u32)> = None;
        for span in &slice.spans {
            if span.raw_len == 0 {
                return Err(WireError::invalid(
                    "data seal span",
                    "raw_len must be non-zero",
                ));
            }
            if let Some((previous_frame, previous_end)) = previous {
                if span.frame_ordinal < previous_frame
                    || (span.frame_ordinal == previous_frame
                        && span.raw_offset_in_frame < previous_end)
                {
                    return Err(WireError::invalid(
                        "data seal span",
                        "spans are not ordered",
                    ));
                }
            }
            let frame = frames.get(&span.frame_ordinal).ok_or_else(|| {
                WireError::invalid(
                    "data seal span",
                    format!("slice {} references missing frame", slice.slice_id),
                )
            })?;
            let raw_end = u64::from(span.raw_offset_in_frame)
                .checked_add(u64::from(span.raw_len))
                .ok_or_else(|| WireError::LimitExceeded("span range overflows u64".into()))?;
            if raw_end > u64::from(frame.raw_len) {
                return Err(WireError::invalid(
                    "data seal span",
                    format!("slice {} span exceeds frame raw length", slice.slice_id),
                ));
            }
            logical = logical
                .checked_add(u64::from(span.raw_len))
                .ok_or_else(|| {
                    WireError::LimitExceeded("slice logical length overflows u64".into())
                })?;
            previous = Some((span.frame_ordinal, raw_end as u32));
        }
        if logical != slice.logical_len {
            return Err(WireError::invalid(
                "data seal slice",
                format!(
                    "slice {} spans cover {logical}, logical length is {}",
                    slice.slice_id, slice.logical_len
                ),
            ));
        }
    }
    Ok(())
}

fn validate_frame_descriptor(frame: &FrameDescriptor) -> WireResult<()> {
    validate_frame_codec(frame.payload_format, frame.codec)?;
    if frame.raw_len == 0 || frame.stored_len == 0 || frame.frame_checksum == [0; 16] {
        return Err(WireError::invalid(
            "data seal frame descriptor",
            "lengths and checksum must be non-zero",
        ));
    }
    Ok(())
}

fn validate_frame_codec(payload_format: u8, codec: u8) -> WireResult<()> {
    PayloadFormat::from_u8(payload_format)?;
    Codec::from_u8(codec)?;
    if payload_format == PayloadFormat::NativeBlockV1.as_u8() && codec != Codec::None.as_u8() {
        return Err(WireError::invalid(
            "data seal frame descriptor",
            "NativeBlockV1 frame cannot use an outer codec",
        ));
    }
    Ok(())
}

fn validate_object_descriptor(object: &DataObjectDescriptor) -> WireResult<()> {
    if object.object_key.is_empty()
        || object.object_key.len() > MAX_KEY_LEN
        || object.object_key.contains(&0)
        || std::str::from_utf8(&object.object_key).is_err()
    {
        return Err(WireError::invalid(
            "data seal object descriptor",
            "object key must be non-empty UTF-8 without NUL",
        ));
    }
    if object.object_len == 0 || object.object_checksum == [0; 32] {
        return Err(WireError::invalid(
            "data seal object descriptor",
            "object length and checksum must be non-zero",
        ));
    }
    if object.etag.len() > MAX_KEY_LEN {
        return Err(WireError::LimitExceeded(
            "data seal diagnostic etag exceeds 1024 bytes".into(),
        ));
    }
    Ok(())
}

fn encode_slices(slices: &BTreeMap<u64, SliceDescriptor>) -> WireResult<Vec<u8>> {
    let mut pages = Vec::<Vec<u8>>::new();
    let mut first_ids = Vec::<u64>::new();
    let mut last_ids = Vec::<u64>::new();
    let mut page_counts = Vec::<u32>::new();
    let mut current = Vec::new();
    let mut current_first = 0u64;
    let mut current_last = 0u64;
    let mut current_count = 0usize;

    for slice in slices.values() {
        let record = encode_slice_record(slice)?;
        if record.len() > MAX_TABLE_BYTES {
            return Err(WireError::LimitExceeded(
                "single data seal slice record exceeds table limit".into(),
            ));
        }
        let should_flush = !current.is_empty()
            && (current_count >= SLICE_PAGE_RECORD_LIMIT
                || current
                    .len()
                    .checked_add(record.len())
                    .is_none_or(|length| length > SLICE_PAGE_TARGET_BYTES));
        if should_flush {
            pages.push(std::mem::take(&mut current));
            first_ids.push(current_first);
            last_ids.push(current_last);
            page_counts.push(u32::try_from(current_count).map_err(|_| {
                WireError::LimitExceeded("data seal slice page count exceeds u32".into())
            })?);
            current_count = 0;
        }
        if current.is_empty() {
            current_first = slice.slice_id;
        }
        current_last = slice.slice_id;
        current_count += 1;
        current.extend_from_slice(&record);
    }
    if !current.is_empty() {
        pages.push(current);
        first_ids.push(current_first);
        last_ids.push(current_last);
        page_counts.push(u32::try_from(current_count).map_err(|_| {
            WireError::LimitExceeded("data seal slice page count exceeds u32".into())
        })?);
    }

    let page_count = u32::try_from(pages.len())
        .map_err(|_| WireError::LimitExceeded("data seal slice page count exceeds u32".into()))?;
    let directory_len = pages
        .len()
        .checked_mul(SLICE_PAGE_REF_LEN)
        .ok_or_else(|| WireError::LimitExceeded("data seal slice directory overflows".into()))?;
    let pages_offset = SLICE_INDEX_HEADER_LEN
        .checked_add(directory_len)
        .ok_or_else(|| WireError::LimitExceeded("data seal slice index overflows".into()))?;
    let mut directory = Vec::with_capacity(directory_len);
    let mut page_offset = u64::try_from(pages_offset)
        .map_err(|_| WireError::LimitExceeded("data seal slice page offset overflows".into()))?;
    for (index, page) in pages.iter().enumerate() {
        let page_len = u32::try_from(page.len())
            .map_err(|_| WireError::LimitExceeded("data seal slice page exceeds u32".into()))?;
        directory.extend_from_slice(&first_ids[index].to_le_bytes());
        directory.extend_from_slice(&last_ids[index].to_le_bytes());
        directory.extend_from_slice(&page_offset.to_le_bytes());
        directory.extend_from_slice(&page_len.to_le_bytes());
        directory.extend_from_slice(&page_counts[index].to_le_bytes());
        directory.extend_from_slice(blake3::hash(page).as_bytes());
        page_offset = page_offset
            .checked_add(u64::from(page_len))
            .ok_or_else(|| {
                WireError::LimitExceeded("data seal slice page offset overflows".into())
            })?;
    }
    debug_assert_eq!(directory.len(), directory_len);

    let directory_digest = blake3::hash(&directory);
    let mut header = [0u8; SLICE_INDEX_HEADER_LEN];
    header[..8].copy_from_slice(SLICE_INDEX_MAGIC);
    header[8..10].copy_from_slice(&SLICE_INDEX_VERSION.to_le_bytes());
    header[12..16].copy_from_slice(&page_count.to_le_bytes());
    header[16..24].copy_from_slice(&(slices.len() as u64).to_le_bytes());
    header[24..28].copy_from_slice(
        &u32::try_from(directory_len)
            .map_err(|_| WireError::LimitExceeded("data seal slice directory exceeds u32".into()))?
            .to_le_bytes(),
    );
    header[28..32].copy_from_slice(&(SLICE_PAGE_RECORD_LIMIT as u32).to_le_bytes());
    header[32..64].copy_from_slice(directory_digest.as_bytes());

    let mut payload = Vec::with_capacity(
        pages_offset
            .checked_add(pages.iter().map(Vec::len).sum::<usize>())
            .ok_or_else(|| WireError::LimitExceeded("data seal slice table overflows".into()))?,
    );
    payload.extend_from_slice(&header);
    payload.extend_from_slice(&directory);
    for page in pages {
        payload.extend_from_slice(&page);
    }
    bounded_table(payload)
}

fn encode_frames(frames: &BTreeMap<u32, FrameDescriptor>) -> WireResult<Vec<u8>> {
    let mut writer = Writer::new();
    for frame in frames.values() {
        frame.encode_into(&mut writer);
    }
    bounded_table(writer.into_bytes())
}

fn encode_objects(objects: &BTreeMap<u32, DataObjectDescriptor>) -> WireResult<Vec<u8>> {
    let mut writer = Writer::new();
    for object in objects.values() {
        writer.u32(object.object_ordinal);
        writer.u64(object.object_len);
        writer.put(&object.object_checksum);
        writer.bytes(&object.object_key);
        writer.bytes(&object.etag);
    }
    bounded_table(writer.into_bytes())
}

pub(crate) fn decode_slices(bytes: &[u8], count: u64) -> WireResult<Vec<SliceDescriptor>> {
    if bytes.len() >= SLICE_INDEX_MAGIC.len() && &bytes[..8] == SLICE_INDEX_MAGIC {
        return decode_paged_slices(bytes, count);
    }
    decode_legacy_slices(bytes, count)
}

fn encode_slice_record(slice: &SliceDescriptor) -> WireResult<Vec<u8>> {
    let mut writer = Writer::new();
    writer.u64(slice.slice_id);
    writer.u64(slice.logical_len);
    writer.u32(
        u32::try_from(slice.spans.len())
            .map_err(|_| WireError::LimitExceeded("slice span count exceeds u32".into()))?,
    );
    writer.u32(0);
    for span in &slice.spans {
        writer.u32(span.frame_ordinal);
        writer.u32(span.raw_offset_in_frame);
        writer.u32(span.raw_len);
    }
    Ok(writer.into_bytes())
}

fn decode_slice_record(reader: &mut Reader<'_>) -> WireResult<SliceDescriptor> {
    let slice_id = reader.u64("data seal slice")?;
    let logical_len = reader.u64("data seal slice")?;
    let span_count = reader.u32("data seal slice")? as usize;
    if reader.u32("data seal slice")? != 0 || slice_id == 0 || logical_len == 0 || span_count == 0 {
        return Err(WireError::invalid(
            "data seal slice",
            "invalid slice id, logical length, span count, or reserved bytes",
        ));
    }
    const MAX_SPANS_PER_SLICE: usize = 1 << 20;
    if span_count > MAX_SPANS_PER_SLICE
        || span_count
            .checked_mul(12)
            .is_none_or(|bytes| bytes > reader.remaining())
    {
        return Err(WireError::LimitExceeded(
            "data seal slice span count exceeds bounded input".into(),
        ));
    }
    let mut spans = Vec::with_capacity(span_count);
    for _ in 0..span_count {
        spans.push(DataSpan {
            frame_ordinal: reader.u32("data seal span")?,
            raw_offset_in_frame: reader.u32("data seal span")?,
            raw_len: reader.u32("data seal span")?,
        });
    }
    Ok(SliceDescriptor {
        slice_id,
        logical_len,
        spans,
    })
}

fn decode_legacy_slices(bytes: &[u8], count: u64) -> WireResult<Vec<SliceDescriptor>> {
    let mut reader = Reader::new(bytes);
    let mut slices = Vec::with_capacity(
        usize::try_from(count)
            .map_err(|_| WireError::LimitExceeded("data seal slice count exceeds usize".into()))?,
    );
    let mut previous = 0u64;
    for index in 0..count {
        let slice = decode_slice_record(&mut reader)?;
        let slice_id = slice.slice_id;
        if index > 0 && slice_id <= previous {
            return Err(WireError::invalid(
                "data seal slice",
                "slice ids are not sorted",
            ));
        }
        previous = slice_id;
        slices.push(slice);
    }
    if !reader.is_empty() {
        return Err(WireError::invalid("data seal slices", "trailing bytes"));
    }
    Ok(slices)
}

fn decode_paged_slices(bytes: &[u8], count: u64) -> WireResult<Vec<SliceDescriptor>> {
    if bytes.len() < SLICE_INDEX_HEADER_LEN {
        return Err(WireError::Truncated {
            what: "data seal slice index",
            need: SLICE_INDEX_HEADER_LEN,
            have: bytes.len(),
        });
    }
    let header = decode_slice_index_header(&bytes[..SLICE_INDEX_HEADER_LEN], count)?;
    let directory_start = SLICE_INDEX_HEADER_LEN;
    let directory_end = directory_start
        .checked_add(header.directory_len as usize)
        .ok_or_else(|| WireError::LimitExceeded("data seal slice directory overflows".into()))?;
    if directory_end > bytes.len() {
        return Err(WireError::Truncated {
            what: "data seal slice directory",
            need: directory_end,
            have: bytes.len(),
        });
    }
    let pages = decode_slice_index_directory(
        &header,
        &bytes[directory_start..directory_end],
        bytes.len() as u64,
    )?;
    let mut slices = Vec::with_capacity(
        usize::try_from(count)
            .map_err(|_| WireError::LimitExceeded("data seal slice count exceeds usize".into()))?,
    );
    for page in pages {
        let start = usize::try_from(page.offset).map_err(|_| {
            WireError::LimitExceeded("data seal slice page offset exceeds usize".into())
        })?;
        let end = start
            .checked_add(page.len as usize)
            .ok_or_else(|| WireError::LimitExceeded("data seal slice page end overflows".into()))?;
        let page_bytes = bytes.get(start..end).ok_or(WireError::Truncated {
            what: "data seal slice page",
            need: page.len as usize,
            have: bytes.len().saturating_sub(start),
        })?;
        verify_digest("data seal slice page", page_bytes, &page.digest)?;
        slices.extend(decode_slice_page(
            page_bytes,
            page.count,
            page.first_slice_id,
            page.last_slice_id,
        )?);
    }
    if slices.len() as u64 != count {
        return Err(WireError::invalid(
            "data seal slice index",
            "page record counts do not match section count",
        ));
    }
    Ok(slices)
}

pub(crate) fn decode_slice_index_header(
    bytes: &[u8],
    expected_count: u64,
) -> WireResult<SliceIndexHeader> {
    if bytes.len() != SLICE_INDEX_HEADER_LEN || &bytes[..8] != SLICE_INDEX_MAGIC {
        return Err(WireError::UnsupportedFormat(
            "not a paged BRFDS002 slice index".into(),
        ));
    }
    if u16::from_le_bytes(bytes[8..10].try_into().unwrap()) != SLICE_INDEX_VERSION
        || bytes[10..12].iter().any(|byte| *byte != 0)
    {
        return Err(WireError::UnsupportedFormat(
            "unsupported BRFDS002 slice index version".into(),
        ));
    }
    let page_count = u32::from_le_bytes(bytes[12..16].try_into().unwrap());
    let record_count = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
    if record_count != expected_count {
        return Err(WireError::invalid(
            "data seal slice index",
            "record count does not match section count",
        ));
    }
    let directory_len = u32::from_le_bytes(bytes[24..28].try_into().unwrap());
    let expected_directory_len = usize::try_from(page_count)
        .ok()
        .and_then(|count| count.checked_mul(SLICE_PAGE_REF_LEN))
        .ok_or_else(|| WireError::LimitExceeded("data seal slice directory overflows".into()))?;
    if directory_len as usize != expected_directory_len || expected_directory_len > MAX_TABLE_BYTES
    {
        return Err(WireError::invalid(
            "data seal slice index",
            "directory length is not canonical",
        ));
    }
    let page_record_limit = u32::from_le_bytes(bytes[28..32].try_into().unwrap());
    if page_record_limit == 0 || page_record_limit as usize > 4096 {
        return Err(WireError::invalid(
            "data seal slice index",
            "page record limit is outside bounds",
        ));
    }
    if (record_count == 0) != (page_count == 0) {
        return Err(WireError::invalid(
            "data seal slice index",
            "empty record/page counts are inconsistent",
        ));
    }
    Ok(SliceIndexHeader {
        page_count,
        record_count,
        directory_len,
        page_record_limit,
        directory_digest: bytes[32..64].try_into().unwrap(),
    })
}

pub(crate) fn decode_slice_index_directory(
    header: &SliceIndexHeader,
    bytes: &[u8],
    section_len: u64,
) -> WireResult<Vec<SlicePageRef>> {
    if bytes.len() != header.directory_len as usize {
        return Err(WireError::Truncated {
            what: "data seal slice directory",
            need: header.directory_len as usize,
            have: bytes.len(),
        });
    }
    verify_digest("data seal slice directory", bytes, &header.directory_digest)?;
    let mut pages = Vec::with_capacity(header.page_count as usize);
    let mut expected_offset = u64::try_from(SLICE_INDEX_HEADER_LEN)
        .ok()
        .and_then(|offset| offset.checked_add(u64::from(header.directory_len)))
        .ok_or_else(|| WireError::LimitExceeded("data seal slice page offset overflows".into()))?;
    let mut previous_last = None;
    let mut total_count = 0u64;
    for index in 0..header.page_count as usize {
        let start = index.checked_mul(SLICE_PAGE_REF_LEN).ok_or_else(|| {
            WireError::LimitExceeded("data seal slice directory overflows".into())
        })?;
        let entry = &bytes[start..start + SLICE_PAGE_REF_LEN];
        let first_slice_id = u64::from_le_bytes(entry[..8].try_into().unwrap());
        let last_slice_id = u64::from_le_bytes(entry[8..16].try_into().unwrap());
        let offset = u64::from_le_bytes(entry[16..24].try_into().unwrap());
        let len = u32::from_le_bytes(entry[24..28].try_into().unwrap());
        let count = u32::from_le_bytes(entry[28..32].try_into().unwrap());
        let digest = entry[32..64].try_into().unwrap();
        if first_slice_id == 0
            || first_slice_id > last_slice_id
            || previous_last.is_some_and(|previous| first_slice_id <= previous)
            || offset != expected_offset
            || len == 0
            || count == 0
            || count > header.page_record_limit
        {
            return Err(WireError::invalid(
                "data seal slice directory",
                "page ordering, bounds, or count is invalid",
            ));
        }
        let end = offset
            .checked_add(u64::from(len))
            .ok_or_else(|| WireError::LimitExceeded("data seal slice page end overflows".into()))?;
        if end > section_len {
            return Err(WireError::Truncated {
                what: "data seal slice page",
                need: len as usize,
                have: section_len.saturating_sub(offset) as usize,
            });
        }
        total_count = total_count
            .checked_add(u64::from(count))
            .ok_or_else(|| WireError::LimitExceeded("data seal slice count overflows".into()))?;
        previous_last = Some(last_slice_id);
        expected_offset = end;
        pages.push(SlicePageRef {
            first_slice_id,
            last_slice_id,
            offset,
            len,
            count,
            digest,
        });
    }
    if total_count != header.record_count || expected_offset != section_len {
        return Err(WireError::invalid(
            "data seal slice directory",
            "page counts or section coverage are not canonical",
        ));
    }
    Ok(pages)
}

pub(crate) fn decode_slice_page(
    bytes: &[u8],
    count: u32,
    first_slice_id: u64,
    last_slice_id: u64,
) -> WireResult<Vec<SliceDescriptor>> {
    let mut reader = Reader::new(bytes);
    let mut slices = Vec::with_capacity(count as usize);
    let mut previous = None;
    for _ in 0..count {
        let slice = decode_slice_record(&mut reader)?;
        if previous.is_some_and(|previous| slice.slice_id <= previous) {
            return Err(WireError::invalid(
                "data seal slice page",
                "slice ids are not sorted",
            ));
        }
        previous = Some(slice.slice_id);
        slices.push(slice);
    }
    if !reader.is_empty()
        || slices.first().map(|slice| slice.slice_id) != Some(first_slice_id)
        || slices.last().map(|slice| slice.slice_id) != Some(last_slice_id)
    {
        return Err(WireError::invalid(
            "data seal slice page",
            "page record coverage is not canonical",
        ));
    }
    Ok(slices)
}

pub(crate) fn decode_frames(bytes: &[u8], count: u64) -> WireResult<Vec<FrameDescriptor>> {
    let count = usize::try_from(count)
        .map_err(|_| WireError::LimitExceeded("data seal frame count exceeds usize".into()))?;
    if bytes.len() != count.saturating_mul(FrameDescriptor::ENCODED_LEN) {
        return Err(WireError::invalid(
            "data seal frames",
            "table length is not canonical",
        ));
    }
    let mut reader = Reader::new(bytes);
    let mut frames = Vec::with_capacity(count);
    let mut previous = None;
    for _ in 0..count {
        let frame = FrameDescriptor::decode(&mut reader)?;
        if previous.is_some_and(|ordinal| frame.frame_ordinal <= ordinal) {
            return Err(WireError::invalid(
                "data seal frames",
                "ordinals are not sorted",
            ));
        }
        previous = Some(frame.frame_ordinal);
        frames.push(frame);
    }
    Ok(frames)
}

pub(crate) fn decode_objects(bytes: &[u8], count: u64) -> WireResult<Vec<DataObjectDescriptor>> {
    let count = usize::try_from(count)
        .map_err(|_| WireError::LimitExceeded("data seal object count exceeds usize".into()))?;
    let mut reader = Reader::new(bytes);
    let mut objects = Vec::with_capacity(count);
    let mut previous = None;
    for _ in 0..count {
        let object_ordinal = reader.u32("data seal object")?;
        if previous.is_some_and(|ordinal| object_ordinal <= ordinal) {
            return Err(WireError::invalid(
                "data seal objects",
                "ordinals are not sorted",
            ));
        }
        let object_len = reader.u64("data seal object")?;
        let object_checksum = reader.take(32, "data seal object")?.try_into().unwrap();
        let object_key = reader.bytes("data seal object key")?.to_vec();
        let etag = reader.bytes("data seal object etag")?.to_vec();
        let object = DataObjectDescriptor {
            object_ordinal,
            object_key,
            object_len,
            object_checksum,
            etag,
        };
        validate_object_descriptor(&object)?;
        previous = Some(object_ordinal);
        objects.push(object);
    }
    if !reader.is_empty() {
        return Err(WireError::invalid("data seal objects", "trailing bytes"));
    }
    Ok(objects)
}

fn bounded_table(bytes: Vec<u8>) -> WireResult<Vec<u8>> {
    if bytes.len() > MAX_TABLE_BYTES {
        return Err(WireError::LimitExceeded(format!(
            "data seal table exceeds {MAX_TABLE_BYTES} bytes"
        )));
    }
    Ok(bytes)
}

pub(crate) fn verify_digest(
    what: &'static str,
    bytes: &[u8],
    expected: &[u8; 32],
) -> WireResult<()> {
    let computed = blake3::hash(bytes);
    if expected != computed.as_bytes() {
        return Err(WireError::HashMismatch {
            what,
            stored: hex::encode(expected),
            computed: hex::encode(computed.as_bytes()),
        });
    }
    Ok(())
}

fn build_section_refs(
    payloads: &[Vec<u8>; SECTION_COUNT],
    counts: [u64; SECTION_COUNT],
) -> WireResult<[SectionRef; SECTION_COUNT]> {
    let mut offset = DATA_SEAL_HEADER_LEN as u64;
    let mut refs = [SectionRef {
        kind: 0,
        offset: 0,
        len: 0,
        count: 0,
        digest: [0; 32],
    }; SECTION_COUNT];
    for (index, payload) in payloads.iter().enumerate() {
        refs[index] = SectionRef {
            kind: (index as u8) + 1,
            offset,
            len: payload.len() as u64,
            count: counts[index],
            digest: *blake3::hash(payload).as_bytes(),
        };
        offset = offset
            .checked_add(payload.len() as u64)
            .ok_or_else(|| WireError::LimitExceeded("data seal section offset overflows".into()))?;
    }
    Ok(refs)
}

fn semantic_hash(payloads: &[Vec<u8>; SECTION_COUNT]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"BrewFS.BRFDS002.semantic.v2");
    for payload in payloads {
        hasher.update(payload);
    }
    *hasher.finalize().as_bytes()
}

fn encode_header(
    out: &mut [u8],
    cluster_id: [u8; 16],
    volume_id: [u8; 16],
    object_len: u64,
    slice_count: u64,
    frame_count: u64,
    object_count: u64,
    semantic_hash: [u8; 32],
    sections: &[SectionRef; SECTION_COUNT],
) -> WireResult<()> {
    if out.len() != DATA_SEAL_HEADER_LEN {
        return Err(WireError::invalid(
            "data seal header",
            "invalid output length",
        ));
    }
    out[..8].copy_from_slice(DATA_SEAL_MAGIC);
    out[8..10].copy_from_slice(&2u16.to_le_bytes());
    out[10..12].copy_from_slice(&0u16.to_le_bytes());
    out[16..32].copy_from_slice(&cluster_id);
    out[32..48].copy_from_slice(&volume_id);
    out[48..56].copy_from_slice(&object_len.to_le_bytes());
    out[56..64].copy_from_slice(&slice_count.to_le_bytes());
    out[64..72].copy_from_slice(&frame_count.to_le_bytes());
    out[72..80].copy_from_slice(&object_count.to_le_bytes());
    out[80..112].copy_from_slice(&semantic_hash);
    for (index, section) in sections.iter().copied().enumerate() {
        section.encode_into(&mut out[112 + index * SECTION_REF_LEN..][..SECTION_REF_LEN]);
    }
    let crc = crc32c(&out[..DATA_SEAL_HEADER_LEN - 4]);
    out[DATA_SEAL_HEADER_LEN - 4..].copy_from_slice(&crc.to_le_bytes());
    Ok(())
}

pub(crate) fn decode_header(
    bytes: &[u8],
) -> WireResult<(
    [u8; 16],
    [u8; 16],
    u64,
    [u64; 3],
    [u8; 32],
    [SectionRef; SECTION_COUNT],
)> {
    if bytes.len() != DATA_SEAL_HEADER_LEN || &bytes[..8] != DATA_SEAL_MAGIC {
        return Err(WireError::UnsupportedFormat("not a BRFDS002 header".into()));
    }
    if u16::from_le_bytes(bytes[8..10].try_into().unwrap()) != 2
        || u16::from_le_bytes(bytes[10..12].try_into().unwrap()) != 0
        || bytes[12..16].iter().any(|byte| *byte != 0)
    {
        return Err(WireError::UnsupportedFormat(
            "unsupported BRFDS002 version".into(),
        ));
    }
    let stored = u32::from_le_bytes(bytes[DATA_SEAL_HEADER_LEN - 4..].try_into().unwrap());
    let computed = crc32c(&bytes[..DATA_SEAL_HEADER_LEN - 4]);
    if stored != computed {
        return Err(WireError::CrcMismatch {
            what: "data seal header",
            stored,
            computed,
        });
    }
    let mut sections = [SectionRef {
        kind: 0,
        offset: 0,
        len: 0,
        count: 0,
        digest: [0; 32],
    }; SECTION_COUNT];
    for (index, section) in sections.iter_mut().enumerate() {
        *section = SectionRef::decode(&bytes[112 + index * SECTION_REF_LEN..][..SECTION_REF_LEN])?;
    }
    Ok((
        bytes[16..32].try_into().unwrap(),
        bytes[32..48].try_into().unwrap(),
        u64::from_le_bytes(bytes[48..56].try_into().unwrap()),
        [
            u64::from_le_bytes(bytes[56..64].try_into().unwrap()),
            u64::from_le_bytes(bytes[64..72].try_into().unwrap()),
            u64::from_le_bytes(bytes[72..80].try_into().unwrap()),
        ],
        bytes[80..112].try_into().unwrap(),
        sections,
    ))
}

fn encode_footer(object_len: u64, content_hash: [u8; 32]) -> [u8; DATA_SEAL_FOOTER_LEN] {
    let mut out = [0u8; DATA_SEAL_FOOTER_LEN];
    out[..8].copy_from_slice(DATA_SEAL_FOOTER_MAGIC);
    out[8..16].copy_from_slice(&object_len.to_le_bytes());
    out[16..48].copy_from_slice(&content_hash);
    let crc = crc32c(&out[..60]);
    out[60..].copy_from_slice(&crc.to_le_bytes());
    out
}

fn decode_footer(bytes: &[u8], content: &[u8], object_len: u64) -> WireResult<()> {
    if bytes.len() != DATA_SEAL_FOOTER_LEN || &bytes[..8] != DATA_SEAL_FOOTER_MAGIC {
        return Err(WireError::invalid(
            "data seal footer",
            "magic or length mismatch",
        ));
    }
    let stored_crc = u32::from_le_bytes(bytes[60..].try_into().unwrap());
    let computed_crc = crc32c(&bytes[..60]);
    if stored_crc != computed_crc {
        return Err(WireError::CrcMismatch {
            what: "data seal footer",
            stored: stored_crc,
            computed: computed_crc,
        });
    }
    if u64::from_le_bytes(bytes[8..16].try_into().unwrap()) != object_len {
        return Err(WireError::invalid(
            "data seal footer",
            "object length mismatch",
        ));
    }
    let expected: [u8; 32] = bytes[16..48].try_into().unwrap();
    let actual: [u8; 32] = Sha256::digest(content).into();
    if expected != actual {
        return Err(WireError::HashMismatch {
            what: "data seal content",
            stored: hex::encode(expected),
            computed: hex::encode(actual),
        });
    }
    Ok(())
}

/// Build frame descriptors for all frames in a v2 DataPack. The caller still
/// supplies the object-table checksum/key because those are learned by the
/// upload coordinator after the immutable object is verified.
pub fn frame_descriptors_from_pack(
    object_ordinal: u32,
    pack: &DataPackSnapshot,
) -> WireResult<Vec<FrameDescriptor>> {
    pack.frames()
        .iter()
        .map(|frame| FrameDescriptor::from_pack_frame(object_ordinal, frame))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native_base::wire::datapack::PackFrame;
    use crate::workspace_overlay::clustered_snapshot::data_pack::DataPackBuilder;

    fn object(ordinal: u32, bytes: &[u8]) -> DataObjectDescriptor {
        DataObjectDescriptor {
            object_ordinal: ordinal,
            object_key: format!("packs/{ordinal}.brfdp").into_bytes(),
            object_len: bytes.len() as u64,
            object_checksum: Sha256::digest(bytes).into(),
            etag: b"diagnostic-etag".to_vec(),
        }
    }

    #[test]
    fn seal_roundtrip_closes_slice_frame_object_graph() {
        let mut pack_builder = DataPackBuilder::new();
        pack_builder.push(PackFrame::plain_bytes(b"payload").unwrap());
        let pack = DataPackSnapshot::open(pack_builder.build().unwrap()).unwrap();
        let mut builder = DataSealBuilder::new([1; 16], [2; 16]);
        builder.add_object(object(0, pack.object_bytes())).unwrap();
        for frame in frame_descriptors_from_pack(0, &pack).unwrap() {
            builder.add_frame(frame).unwrap();
        }
        builder
            .add_slice(SliceDescriptor {
                slice_id: 7,
                logical_len: 7,
                spans: vec![DataSpan {
                    frame_ordinal: 0,
                    raw_offset_in_frame: 0,
                    raw_len: 7,
                }],
            })
            .unwrap();
        let bytes = builder.build().unwrap();
        assert_eq!(&bytes[..8], DATA_SEAL_MAGIC);
        let snapshot = DataSealSnapshot::open(bytes.clone()).unwrap();
        assert_eq!(snapshot.lookup_slice(7).unwrap().logical_len, 7);
        assert_eq!(snapshot.frames().len(), 1);
        assert_eq!(snapshot.objects().len(), 1);
        assert_eq!(snapshot.object_bytes(), bytes.as_slice());
    }

    #[test]
    fn seal_rejects_missing_frame_and_bad_span_coverage() {
        let mut builder = DataSealBuilder::new([3; 16], [4; 16]);
        builder
            .add_object(DataObjectDescriptor {
                object_ordinal: 0,
                object_key: b"packs/0.brfdp".to_vec(),
                object_len: 256,
                object_checksum: [5; 32],
                etag: Vec::new(),
            })
            .unwrap();
        builder
            .add_slice(SliceDescriptor {
                slice_id: 1,
                logical_len: 4,
                spans: vec![DataSpan {
                    frame_ordinal: 9,
                    raw_offset_in_frame: 0,
                    raw_len: 4,
                }],
            })
            .unwrap();
        assert!(builder.validate().is_err());
    }

    #[test]
    fn seal_is_deterministic_and_tamper_evident() {
        let mut builder = DataSealBuilder::new([8; 16], [9; 16]);
        builder
            .add_object(DataObjectDescriptor {
                object_ordinal: 0,
                object_key: b"packs/0.brfdp".to_vec(),
                object_len: 256,
                object_checksum: [1; 32],
                etag: Vec::new(),
            })
            .unwrap();
        builder
            .add_frame(FrameDescriptor {
                frame_ordinal: 0,
                object_ordinal: 0,
                object_offset: 64,
                stored_len: 8,
                raw_len: 8,
                payload_format: PayloadFormat::PlainBytes.as_u8(),
                codec: Codec::None.as_u8(),
                frame_checksum: [2; 16],
            })
            .unwrap();
        builder
            .add_slice(SliceDescriptor {
                slice_id: 3,
                logical_len: 8,
                spans: vec![DataSpan {
                    frame_ordinal: 0,
                    raw_offset_in_frame: 0,
                    raw_len: 8,
                }],
            })
            .unwrap();
        let first = builder.build().unwrap();
        assert_eq!(first, builder.build().unwrap());
        let mut tampered = first;
        tampered[DATA_SEAL_HEADER_LEN + 2] ^= 1;
        assert!(matches!(
            DataSealSnapshot::open(tampered),
            Err(WireError::HashMismatch { .. })
        ));
    }
}
