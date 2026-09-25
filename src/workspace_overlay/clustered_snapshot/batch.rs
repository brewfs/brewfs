//! Independently authenticated v2 metadata batches.
//!
//! A batch is the smallest object-store transfer and cache unit.  The wire
//! header deliberately carries physical lengths and a digest of the stored
//! bytes, while the payload codec is reset at every batch boundary.  This
//! makes a targeted range read independent from the sequential stream
//! frontier and prevents a corrupt predecessor from turning into a false
//! negative lookup.

use std::fmt;
use std::io::Read;

use blake3::Hasher;

use crate::native_base::wire::error::{WireError, WireResult};
use crate::native_base::wire::uvarint::{Reader, Writer};

use super::identity::DirKey;
use super::name::{NameBytes, NameError};

pub const BATCH_MAGIC: &[u8; 8] = b"BRFCBT02";
pub const BATCH_HEADER_LEN: usize = 128;
pub const BATCH_VERSION: u16 = 2;
pub const MAX_BATCH_RAW: usize = 4 * 1024 * 1024;
pub const MAX_BATCH_STORED: usize = 16 * 1024 * 1024;
pub const NAME_RESTART_INTERVAL: usize = 16;

/// Batch kind values are format identifiers, not Rust enum ordinals.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u16)]
pub enum BatchKind {
    Namespace = 1,
    Extent = 2,
    Attribute = 3,
    DirectoryProjection = 4,
}

impl BatchKind {
    pub fn from_u16(value: u16) -> WireResult<Self> {
        match value {
            1 => Ok(Self::Namespace),
            2 => Ok(Self::Extent),
            3 => Ok(Self::Attribute),
            4 => Ok(Self::DirectoryProjection),
            other => Err(WireError::UnsupportedFormat(format!(
                "unknown metadata batch kind {other}"
            ))),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum BatchCodec {
    None = 0,
    Zstd = 1,
}

impl BatchCodec {
    fn from_u8(value: u8) -> WireResult<Self> {
        match value {
            0 => Ok(Self::None),
            1 => Ok(Self::Zstd),
            other => Err(WireError::UnsupportedFormat(format!(
                "unknown metadata batch codec {other}"
            ))),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BatchHeader {
    pub kind: BatchKind,
    pub flags: u32,
    pub cluster_id: [u8; 16],
    pub batch_id: u32,
    pub stream_ordinal: u32,
    pub predecessor_ordinal: u32,
    pub record_count: u32,
    pub raw_payload_len: u64,
    pub stored_payload_len: u64,
    pub first_new_node_id: u32,
    pub new_node_count: u32,
    pub codec: BatchCodec,
    pub stored_payload_crc32c: u32,
    pub first_key_fingerprint: u64,
    pub digest: [u8; 32],
}

impl BatchHeader {
    pub const NO_PREDECESSOR: u32 = u32::MAX;

    pub fn encode(&self) -> [u8; BATCH_HEADER_LEN] {
        let mut out = [0u8; BATCH_HEADER_LEN];
        out[0..8].copy_from_slice(BATCH_MAGIC);
        out[8..10].copy_from_slice(&(self.kind as u16).to_le_bytes());
        out[10..12].copy_from_slice(&BATCH_VERSION.to_le_bytes());
        out[12..16].copy_from_slice(&self.flags.to_le_bytes());
        out[16..32].copy_from_slice(&self.cluster_id);
        out[32..36].copy_from_slice(&self.batch_id.to_le_bytes());
        out[36..40].copy_from_slice(&self.stream_ordinal.to_le_bytes());
        out[40..44].copy_from_slice(&self.predecessor_ordinal.to_le_bytes());
        out[44..48].copy_from_slice(&self.record_count.to_le_bytes());
        out[48..56].copy_from_slice(&self.raw_payload_len.to_le_bytes());
        out[56..64].copy_from_slice(&self.stored_payload_len.to_le_bytes());
        out[64..68].copy_from_slice(&self.first_new_node_id.to_le_bytes());
        out[68..72].copy_from_slice(&self.new_node_count.to_le_bytes());
        out[72] = self.codec as u8;
        // [73..80] is reserved and remains zero.
        out[80..84].copy_from_slice(&self.stored_payload_crc32c.to_le_bytes());
        let header_crc = crc32c::crc32c(&out[..84]);
        out[84..88].copy_from_slice(&header_crc.to_le_bytes());
        out[88..96].copy_from_slice(&self.first_key_fingerprint.to_le_bytes());
        out[96..128].copy_from_slice(&self.digest);
        out
    }

    pub fn decode(bytes: &[u8]) -> WireResult<Self> {
        if bytes.len() < BATCH_HEADER_LEN {
            return Err(WireError::Truncated {
                what: "metadata batch header",
                need: BATCH_HEADER_LEN,
                have: bytes.len(),
            });
        }
        let bytes = &bytes[..BATCH_HEADER_LEN];
        if &bytes[..8] != BATCH_MAGIC {
            return Err(WireError::UnsupportedFormat(format!(
                "metadata batch magic {:02x?} is not {BATCH_MAGIC:?}",
                &bytes[..8]
            )));
        }
        let version = u16::from_le_bytes([bytes[10], bytes[11]]);
        if version != BATCH_VERSION {
            return Err(WireError::UnsupportedFormat(format!(
                "metadata batch version {version}"
            )));
        }
        if bytes[73..80].iter().any(|byte| *byte != 0) {
            return Err(WireError::invalid(
                "metadata batch header",
                "reserved bytes are non-zero",
            ));
        }
        let stored_header_crc = u32::from_le_bytes(bytes[84..88].try_into().unwrap());
        let computed_header_crc = crc32c::crc32c(&bytes[..84]);
        if stored_header_crc != computed_header_crc {
            return Err(WireError::CrcMismatch {
                what: "metadata batch header",
                stored: stored_header_crc,
                computed: computed_header_crc,
            });
        }
        let raw_payload_len = u64::from_le_bytes(bytes[48..56].try_into().unwrap());
        let stored_payload_len = u64::from_le_bytes(bytes[56..64].try_into().unwrap());
        let raw_len = usize::try_from(raw_payload_len).map_err(|_| {
            WireError::LimitExceeded("metadata batch raw length does not fit usize".into())
        })?;
        let stored_len = usize::try_from(stored_payload_len).map_err(|_| {
            WireError::LimitExceeded("metadata batch stored length does not fit usize".into())
        })?;
        if raw_len > MAX_BATCH_RAW {
            return Err(WireError::LimitExceeded(format!(
                "metadata batch raw payload {raw_len} exceeds {MAX_BATCH_RAW}"
            )));
        }
        if stored_len > MAX_BATCH_STORED {
            return Err(WireError::LimitExceeded(format!(
                "metadata batch stored payload {stored_len} exceeds {MAX_BATCH_STORED}"
            )));
        }
        if raw_len == 0 && stored_len != 0 {
            return Err(WireError::invalid(
                "metadata batch header",
                "empty raw payload has stored bytes",
            ));
        }
        Ok(Self {
            kind: BatchKind::from_u16(u16::from_le_bytes([bytes[8], bytes[9]]))?,
            flags: u32::from_le_bytes(bytes[12..16].try_into().unwrap()),
            cluster_id: bytes[16..32].try_into().unwrap(),
            batch_id: u32::from_le_bytes(bytes[32..36].try_into().unwrap()),
            stream_ordinal: u32::from_le_bytes(bytes[36..40].try_into().unwrap()),
            predecessor_ordinal: u32::from_le_bytes(bytes[40..44].try_into().unwrap()),
            record_count: u32::from_le_bytes(bytes[44..48].try_into().unwrap()),
            raw_payload_len,
            stored_payload_len,
            first_new_node_id: u32::from_le_bytes(bytes[64..68].try_into().unwrap()),
            new_node_count: u32::from_le_bytes(bytes[68..72].try_into().unwrap()),
            codec: BatchCodec::from_u8(bytes[72])?,
            stored_payload_crc32c: u32::from_le_bytes(bytes[80..84].try_into().unwrap()),
            first_key_fingerprint: u64::from_le_bytes(bytes[88..96].try_into().unwrap()),
            digest: bytes[96..128].try_into().unwrap(),
        })
    }

    fn validate_payload(&self, stored: &[u8]) -> WireResult<()> {
        if stored.len()
            != usize::try_from(self.stored_payload_len).map_err(|_| {
                WireError::LimitExceeded("stored payload length does not fit usize".into())
            })?
        {
            return Err(WireError::Truncated {
                what: "metadata batch payload",
                need: usize::try_from(self.stored_payload_len).unwrap_or(usize::MAX),
                have: stored.len(),
            });
        }
        let crc = crc32c::crc32c(stored);
        if crc != self.stored_payload_crc32c {
            return Err(WireError::CrcMismatch {
                what: "metadata batch payload",
                stored: self.stored_payload_crc32c,
                computed: crc,
            });
        }
        let mut hasher = Hasher::new();
        let encoded = self.encode_without_digest();
        hasher.update(&encoded[..96]);
        hasher.update(stored);
        let computed = *hasher.finalize().as_bytes();
        if computed != self.digest {
            return Err(WireError::HashMismatch {
                what: "metadata batch",
                stored: hex::encode(self.digest),
                computed: hex::encode(computed),
            });
        }
        Ok(())
    }

    fn encode_without_digest(&self) -> [u8; BATCH_HEADER_LEN] {
        let mut header = self.clone();
        header.digest = [0; 32];
        header.encode()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EncodedBatch {
    pub header: BatchHeader,
    pub bytes: Vec<u8>,
    pub raw_payload: Vec<u8>,
}

impl EncodedBatch {
    pub fn decode(bytes: &[u8]) -> WireResult<Self> {
        let header = BatchHeader::decode(bytes)?;
        let stored_len = usize::try_from(header.stored_payload_len).map_err(|_| {
            WireError::LimitExceeded("stored payload length does not fit usize".into())
        })?;
        let total = BATCH_HEADER_LEN
            .checked_add(stored_len)
            .ok_or_else(|| WireError::invalid("metadata batch", "object length overflow"))?;
        if bytes.len() != total {
            return Err(WireError::Truncated {
                what: "metadata batch object",
                need: total,
                have: bytes.len(),
            });
        }
        let stored = &bytes[BATCH_HEADER_LEN..];
        header.validate_payload(stored)?;
        let raw_len = usize::try_from(header.raw_payload_len).map_err(|_| {
            WireError::LimitExceeded("raw payload length does not fit usize".into())
        })?;
        let raw = match header.codec {
            BatchCodec::None => stored.to_vec(),
            BatchCodec::Zstd => {
                let mut decoder = zstd::stream::read::Decoder::new(stored)
                    .map_err(|error| WireError::Codec(error.to_string()))?;
                let mut raw = Vec::with_capacity(raw_len);
                let mut limited = (&mut decoder).take((MAX_BATCH_RAW as u64).saturating_add(1));
                limited
                    .read_to_end(&mut raw)
                    .map_err(|error| WireError::Codec(error.to_string()))?;
                raw
            }
        };
        if raw.len() != raw_len {
            return Err(WireError::invalid(
                "metadata batch payload",
                "decoded length does not match header",
            ));
        }
        Ok(Self {
            header,
            bytes: bytes.to_vec(),
            raw_payload: raw,
        })
    }
}

pub fn encode_batch(
    kind: BatchKind,
    flags: u32,
    cluster_id: [u8; 16],
    batch_id: u32,
    stream_ordinal: u32,
    predecessor_ordinal: u32,
    record_count: u32,
    first_new_node_id: u32,
    new_node_count: u32,
    first_key: &[u8],
    raw_payload: &[u8],
) -> WireResult<EncodedBatch> {
    if raw_payload.len() > MAX_BATCH_RAW {
        return Err(WireError::LimitExceeded(format!(
            "metadata batch raw payload {} exceeds {MAX_BATCH_RAW}",
            raw_payload.len()
        )));
    }
    let compressed = zstd::stream::encode_all(raw_payload, 3)
        .map_err(|error| WireError::Codec(error.to_string()))?;
    let (codec, stored) = if compressed.len() < raw_payload.len() {
        (BatchCodec::Zstd, compressed)
    } else {
        (BatchCodec::None, raw_payload.to_vec())
    };
    if stored.len() > MAX_BATCH_STORED {
        return Err(WireError::LimitExceeded(format!(
            "metadata batch stored payload {} exceeds {MAX_BATCH_STORED}",
            stored.len()
        )));
    }
    let first_key_fingerprint = {
        let digest = blake3::hash(first_key);
        u64::from_le_bytes(digest.as_bytes()[..8].try_into().unwrap())
    };
    let mut header = BatchHeader {
        kind,
        flags,
        cluster_id,
        batch_id,
        stream_ordinal,
        predecessor_ordinal,
        record_count,
        raw_payload_len: raw_payload.len() as u64,
        stored_payload_len: stored.len() as u64,
        first_new_node_id,
        new_node_count,
        codec,
        stored_payload_crc32c: crc32c::crc32c(&stored),
        first_key_fingerprint,
        digest: [0; 32],
    };
    let header_without_digest = header.encode_without_digest();
    let mut hasher = Hasher::new();
    hasher.update(&header_without_digest[..96]);
    hasher.update(&stored);
    header.digest = *hasher.finalize().as_bytes();
    let header_bytes = header.encode();
    let mut bytes = Vec::with_capacity(BATCH_HEADER_LEN + stored.len());
    bytes.extend_from_slice(&header_bytes);
    bytes.extend_from_slice(&stored);
    Ok(EncodedBatch {
        header,
        bytes,
        raw_payload: raw_payload.to_vec(),
    })
}

/// A self-contained namespace directory segment.  A segment can be decoded
/// without its predecessor and is bounded by its enclosing batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamespaceSegment {
    pub parent_local_node_id: u32,
    pub parent_dir_key: DirKey,
    pub flags: u8,
    pub total_entry_count: Option<u64>,
    pub entries: Vec<NamespaceEntry>,
}

pub const SEGMENT_START: u8 = 1 << 0;
pub const SEGMENT_END: u8 = 1 << 1;
pub const SEGMENT_CONTINUATION: u8 = 1 << 2;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeRecord {
    pub local_node_id: u32,
    pub kind: u8,
    pub mode: u32,
    pub size: u64,
    pub dir_key: Option<DirKey>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NamespaceEntry {
    NewNode { name: NameBytes, node: NodeRecord },
    ExistingNode { name: NameBytes, local_node_id: u32 },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamespaceBatch {
    pub cluster_id: [u8; 16],
    pub batch_id: u32,
    pub stream_ordinal: u32,
    pub predecessor_ordinal: u32,
    pub segments: Vec<NamespaceSegment>,
}

impl NamespaceBatch {
    pub fn encode(&self) -> WireResult<EncodedBatch> {
        if self.segments.is_empty() {
            return Err(WireError::invalid("namespace batch", "no segments"));
        }
        let mut payload = Writer::new();
        let mut record_count = 0u32;
        let mut first_new_node_id = 0u32;
        let mut new_node_count = 0u32;
        for segment in &self.segments {
            validate_segment(segment)?;
            payload.uvarint(segment.parent_local_node_id as u64);
            payload.put(segment.parent_dir_key.as_ref());
            payload.u8(segment.flags);
            if segment.flags & SEGMENT_START != 0 {
                payload.uvarint(segment.total_entry_count.unwrap_or(0));
            } else if segment.total_entry_count.is_some() {
                return Err(WireError::invalid(
                    "namespace segment",
                    "non-start segment carries total entry count",
                ));
            }
            let first_name = segment
                .entries
                .first()
                .map_or(&[][..], |entry| namespace_entry_name(entry).as_bytes());
            payload.bytes(first_name);
            payload.uvarint(segment.entries.len() as u64);
            let mut previous = Vec::new();
            for (index, entry) in segment.entries.iter().enumerate() {
                let name = namespace_entry_name(entry).as_bytes();
                let shared = if index.is_multiple_of(NAME_RESTART_INTERVAL) {
                    0
                } else {
                    common_prefix_len(&previous, name)
                };
                payload.uvarint(shared as u64);
                payload.bytes(&name[shared..]);
                match entry {
                    NamespaceEntry::NewNode { node, .. } => {
                        validate_node(node)?;
                        payload.u8(0);
                        encode_node(node, &mut payload);
                        if first_new_node_id == 0 || node.local_node_id < first_new_node_id {
                            first_new_node_id = node.local_node_id;
                        }
                        new_node_count = new_node_count.saturating_add(1);
                    }
                    NamespaceEntry::ExistingNode { local_node_id, .. } => {
                        if *local_node_id == 0 {
                            return Err(WireError::invalid(
                                "namespace entry",
                                "existing local node id must be non-zero",
                            ));
                        }
                        payload.u8(1);
                        payload.uvarint(u64::from(*local_node_id));
                    }
                }
                previous.clear();
                previous.extend_from_slice(name);
                record_count = record_count.checked_add(1).ok_or_else(|| {
                    WireError::LimitExceeded("namespace record count overflow".into())
                })?;
            }
        }
        encode_batch(
            BatchKind::Namespace,
            0,
            self.cluster_id,
            self.batch_id,
            self.stream_ordinal,
            self.predecessor_ordinal,
            record_count,
            first_new_node_id,
            new_node_count,
            self.segments
                .first()
                .and_then(|segment| segment.entries.first())
                .map_or(&[][..], |entry| namespace_entry_name(entry).as_bytes()),
            payload.as_slice(),
        )
    }

    pub fn decode(encoded: &EncodedBatch) -> WireResult<Self> {
        if encoded.header.kind != BatchKind::Namespace {
            return Err(WireError::invalid(
                "namespace batch",
                "batch kind is not namespace",
            ));
        }
        let mut reader = Reader::new(&encoded.raw_payload);
        let mut segments = Vec::new();
        let mut records = 0u32;
        while !reader.is_empty() {
            let parent = reader.uvarint("namespace segment")?;
            let parent = u32::try_from(parent)
                .map_err(|_| WireError::LimitExceeded("parent local node id exceeds u32".into()))?;
            let parent_dir_key =
                DirKey::new(reader.take(16, "namespace segment")?.try_into().unwrap());
            let flags = reader.u8("namespace segment")?;
            if flags & !(SEGMENT_START | SEGMENT_END | SEGMENT_CONTINUATION) != 0 {
                return Err(WireError::invalid("namespace segment", "unknown flags"));
            }
            let total_entry_count = if flags & SEGMENT_START != 0 {
                Some(reader.uvarint("namespace segment")?)
            } else {
                None
            };
            let first_name = reader.bytes("namespace segment")?.to_vec();
            let entry_count =
                usize::try_from(reader.uvarint("namespace segment")?).map_err(|_| {
                    WireError::LimitExceeded("segment entry count exceeds usize".into())
                })?;
            if entry_count == 0 || entry_count > 4096 {
                return Err(WireError::LimitExceeded(
                    "namespace segment entry count must be in 1..=4096".into(),
                ));
            }
            let mut entries = Vec::with_capacity(entry_count);
            let mut previous = Vec::new();
            for index in 0..entry_count {
                let shared = usize::try_from(reader.uvarint("namespace entry")?)
                    .map_err(|_| WireError::LimitExceeded("name prefix exceeds usize".into()))?;
                if index.is_multiple_of(NAME_RESTART_INTERVAL) && shared != 0 {
                    return Err(WireError::invalid(
                        "namespace entry",
                        "restart entry has a non-zero shared prefix",
                    ));
                }
                if shared > previous.len() {
                    return Err(WireError::invalid(
                        "namespace entry",
                        "shared prefix exceeds previous name",
                    ));
                }
                let suffix = reader.bytes("namespace entry")?;
                let mut name = previous[..shared].to_vec();
                name.extend_from_slice(suffix);
                let name = NameBytes::new(name).map_err(name_error)?;
                let tag = reader.u8("namespace entry")?;
                let entry = match tag {
                    0 => {
                        let node = decode_node(&mut reader)?;
                        validate_node(&node)?;
                        NamespaceEntry::NewNode { name, node }
                    }
                    1 => NamespaceEntry::ExistingNode {
                        name,
                        local_node_id: u32::try_from(reader.uvarint("namespace entry")?).map_err(
                            |_| WireError::LimitExceeded("local node id exceeds u32".into()),
                        )?,
                    },
                    other => {
                        return Err(WireError::invalid(
                            "namespace entry",
                            format!("unknown entry tag {other}"),
                        ));
                    }
                };
                if entries.last().is_some_and(|previous_entry| {
                    namespace_entry_name(previous_entry) >= namespace_entry_name(&entry)
                }) {
                    return Err(WireError::invalid(
                        "namespace segment",
                        "names are not strictly sorted",
                    ));
                }
                previous = namespace_entry_name(&entry).as_bytes().to_vec();
                entries.push(entry);
                records = records.checked_add(1).ok_or_else(|| {
                    WireError::LimitExceeded("namespace record count overflow".into())
                })?;
            }
            if entries
                .first()
                .map(|entry| namespace_entry_name(entry).as_bytes())
                != Some(first_name.as_slice())
            {
                return Err(WireError::invalid(
                    "namespace segment",
                    "first name does not match first entry",
                ));
            }
            segments.push(NamespaceSegment {
                parent_local_node_id: parent,
                parent_dir_key,
                flags,
                total_entry_count,
                entries,
            });
        }
        if records != encoded.header.record_count {
            return Err(WireError::invalid(
                "namespace batch",
                "record count mismatch",
            ));
        }
        let decoded_first_new_node_id = segments
            .iter()
            .flat_map(|segment| segment.entries.iter())
            .filter_map(|entry| match entry {
                NamespaceEntry::NewNode { node, .. } => Some(node.local_node_id),
                NamespaceEntry::ExistingNode { .. } => None,
            })
            .min()
            .unwrap_or(0);
        let decoded_new_node_count = segments
            .iter()
            .flat_map(|segment| segment.entries.iter())
            .filter(|entry| matches!(entry, NamespaceEntry::NewNode { .. }))
            .count() as u32;
        if decoded_first_new_node_id != encoded.header.first_new_node_id
            || decoded_new_node_count != encoded.header.new_node_count
        {
            return Err(WireError::invalid(
                "namespace batch",
                "new-node range does not match header",
            ));
        }
        if let Some(first) = segments
            .iter()
            .flat_map(|segment| segment.entries.iter())
            .next()
        {
            let digest = blake3::hash(namespace_entry_name(first).as_bytes());
            let fingerprint = u64::from_le_bytes(digest.as_bytes()[..8].try_into().unwrap());
            if fingerprint != encoded.header.first_key_fingerprint {
                return Err(WireError::invalid(
                    "namespace batch",
                    "first-key fingerprint does not match payload",
                ));
            }
        }
        Ok(Self {
            cluster_id: encoded.header.cluster_id,
            batch_id: encoded.header.batch_id,
            stream_ordinal: encoded.header.stream_ordinal,
            predecessor_ordinal: encoded.header.predecessor_ordinal,
            segments,
        })
    }
}

fn encode_node(node: &NodeRecord, writer: &mut Writer) {
    writer.uvarint(u64::from(node.local_node_id));
    writer.u8(node.kind);
    writer.u32(node.mode);
    writer.u64(node.size);
    writer.option_tag(node.dir_key.is_some());
    if let Some(dir_key) = node.dir_key {
        writer.put(dir_key.as_ref());
    }
}

fn decode_node(reader: &mut Reader<'_>) -> WireResult<NodeRecord> {
    let local_node_id = u32::try_from(reader.uvarint("node record")?)
        .map_err(|_| WireError::LimitExceeded("local node id exceeds u32".into()))?;
    let kind = reader.u8("node record")?;
    let mode = reader.u32("node record")?;
    let size = reader.u64("node record")?;
    let dir_key = if reader.option_tag("node record")? {
        Some(DirKey::new(
            reader.take(16, "node record")?.try_into().unwrap(),
        ))
    } else {
        None
    };
    Ok(NodeRecord {
        local_node_id,
        kind,
        mode,
        size,
        dir_key,
    })
}

fn validate_segment(segment: &NamespaceSegment) -> WireResult<()> {
    if segment.entries.is_empty() || segment.entries.len() > 4096 {
        return Err(WireError::LimitExceeded(
            "namespace segment must contain 1..=4096 entries".into(),
        ));
    }
    if segment.flags & !(SEGMENT_START | SEGMENT_END | SEGMENT_CONTINUATION) != 0 {
        return Err(WireError::invalid("namespace segment", "unknown flags"));
    }
    if segment.flags & SEGMENT_START != 0 && segment.total_entry_count.is_none() {
        return Err(WireError::invalid(
            "namespace segment",
            "start segment is missing total entry count",
        ));
    }
    if segment.flags & SEGMENT_START == 0 && segment.total_entry_count.is_some() {
        return Err(WireError::invalid(
            "namespace segment",
            "non-start segment carries total entry count",
        ));
    }
    for pair in segment.entries.windows(2) {
        if namespace_entry_name(&pair[0]) >= namespace_entry_name(&pair[1]) {
            return Err(WireError::invalid(
                "namespace segment",
                "names are not strictly sorted",
            ));
        }
    }
    for entry in &segment.entries {
        match entry {
            NamespaceEntry::NewNode { node, .. } => validate_node(node)?,
            NamespaceEntry::ExistingNode { local_node_id, .. } if *local_node_id == 0 => {
                return Err(WireError::invalid(
                    "namespace entry",
                    "existing local node id must be non-zero",
                ));
            }
            NamespaceEntry::ExistingNode { .. } => {}
        }
    }
    Ok(())
}

fn validate_node(node: &NodeRecord) -> WireResult<()> {
    if node.local_node_id == 0 {
        return Err(WireError::invalid(
            "node record",
            "local node id must be non-zero",
        ));
    }
    if !(1..=7).contains(&node.kind) {
        return Err(WireError::invalid("node record", "unknown inode kind"));
    }
    if (node.kind == 2) != node.dir_key.is_some() {
        return Err(WireError::invalid(
            "node record",
            "directory key presence does not match inode kind",
        ));
    }
    Ok(())
}

fn namespace_entry_name(entry: &NamespaceEntry) -> &NameBytes {
    match entry {
        NamespaceEntry::NewNode { name, .. } | NamespaceEntry::ExistingNode { name, .. } => name,
    }
}

fn common_prefix_len(left: &[u8], right: &[u8]) -> usize {
    left.iter()
        .zip(right)
        .take_while(|(left, right)| left == right)
        .count()
}

fn name_error(error: NameError) -> WireError {
    WireError::invalid("namespace name", error.to_string())
}

impl fmt::Display for BatchKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Namespace => "namespace",
            Self::Extent => "extent",
            Self::Attribute => "attribute",
            Self::DirectoryProjection => "directory-projection",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn segment() -> NamespaceSegment {
        NamespaceSegment {
            parent_local_node_id: 1,
            parent_dir_key: DirKey::new([7; 16]),
            flags: SEGMENT_START | SEGMENT_END,
            total_entry_count: Some(2),
            entries: vec![
                NamespaceEntry::NewNode {
                    name: NameBytes::new(b"alpha".to_vec()).unwrap(),
                    node: NodeRecord {
                        local_node_id: 2,
                        kind: 1,
                        mode: 0o100644,
                        size: 4,
                        dir_key: None,
                    },
                },
                NamespaceEntry::ExistingNode {
                    name: NameBytes::new(b"alphabet".to_vec()).unwrap(),
                    local_node_id: 2,
                },
            ],
        }
    }

    #[test]
    fn namespace_batch_is_independently_decodable_and_front_coded() {
        let batch = NamespaceBatch {
            cluster_id: [3; 16],
            batch_id: 4,
            stream_ordinal: 4,
            predecessor_ordinal: BatchHeader::NO_PREDECESSOR,
            segments: vec![segment()],
        };
        let encoded = batch.encode().unwrap();
        assert_eq!(encoded.header.kind, BatchKind::Namespace);
        assert!(encoded.raw_payload.len() < 256);
        assert_eq!(
            NamespaceBatch::decode(&EncodedBatch::decode(&encoded.bytes).unwrap()).unwrap(),
            batch
        );
    }

    #[test]
    fn batch_rejects_truncation_crc_and_digest_tampering() {
        let batch = NamespaceBatch {
            cluster_id: [3; 16],
            batch_id: 1,
            stream_ordinal: 1,
            predecessor_ordinal: BatchHeader::NO_PREDECESSOR,
            segments: vec![segment()],
        };
        let encoded = batch.encode().unwrap();
        assert!(EncodedBatch::decode(&encoded.bytes[..encoded.bytes.len() - 1]).is_err());
        let mut crc_bad = encoded.bytes.clone();
        *crc_bad.last_mut().unwrap() ^= 1;
        assert!(EncodedBatch::decode(&crc_bad).is_err());
        let mut digest_bad = encoded.bytes.clone();
        digest_bad[96] ^= 1;
        assert!(EncodedBatch::decode(&digest_bad).is_err());
    }

    #[test]
    fn namespace_decoder_does_not_accept_unsorted_entries() {
        let mut value = segment();
        value.entries.reverse();
        let batch = NamespaceBatch {
            cluster_id: [3; 16],
            batch_id: 1,
            stream_ordinal: 1,
            predecessor_ordinal: BatchHeader::NO_PREDECESSOR,
            segments: vec![value],
        };
        assert!(batch.encode().is_err());
    }
    #[test]
    fn batch_rejects_reserved_header_bytes_nonzero() {
        let batch = NamespaceBatch {
            cluster_id: [3; 16],
            batch_id: 1,
            stream_ordinal: 1,
            predecessor_ordinal: BatchHeader::NO_PREDECESSOR,
            segments: vec![segment()],
        };
        let encoded = batch.encode().unwrap();
        let mut bad = encoded.bytes.clone();
        bad[73] = 1;
        assert!(matches!(
            EncodedBatch::decode(&bad),
            Err(WireError::Invalid { .. })
        ));
    }

    #[test]
    fn batch_rejects_unknown_codec() {
        let batch = NamespaceBatch {
            cluster_id: [3; 16],
            batch_id: 1,
            stream_ordinal: 1,
            predecessor_ordinal: BatchHeader::NO_PREDECESSOR,
            segments: vec![segment()],
        };
        let encoded = batch.encode().unwrap();
        let mut bad = encoded.bytes.clone();
        bad[72] = 0xFF;
        // Header CRC covers bytes[..84] so we must recompute.
        let header_crc = crc32c::crc32c(&bad[..84]);
        bad[84..88].copy_from_slice(&header_crc.to_le_bytes());
        // Digest covers header[..96] + stored payload.
        let mut hasher = blake3::Hasher::new();
        hasher.update(&bad[..96]);
        hasher.update(&encoded.bytes[BATCH_HEADER_LEN..]);
        bad[96..128].copy_from_slice(hasher.finalize().as_bytes());
        assert!(matches!(
            EncodedBatch::decode(&bad),
            Err(WireError::UnsupportedFormat(_))
        ));
    }

    #[test]
    fn batch_rejects_unknown_kind() {
        let batch = NamespaceBatch {
            cluster_id: [3; 16],
            batch_id: 1,
            stream_ordinal: 1,
            predecessor_ordinal: BatchHeader::NO_PREDECESSOR,
            segments: vec![segment()],
        };
        let encoded = batch.encode().unwrap();
        let mut bad = encoded.bytes.clone();
        bad[8..10].copy_from_slice(&0xFF00u16.to_le_bytes());
        let header_crc = crc32c::crc32c(&bad[..84]);
        bad[84..88].copy_from_slice(&header_crc.to_le_bytes());
        let mut hasher = blake3::Hasher::new();
        hasher.update(&bad[..96]);
        hasher.update(&encoded.bytes[BATCH_HEADER_LEN..]);
        bad[96..128].copy_from_slice(hasher.finalize().as_bytes());
        // kind is validated at BatchHeader::decode inside EncodedBatch::decode.
        assert!(matches!(
            EncodedBatch::decode(&bad),
            Err(WireError::UnsupportedFormat(_))
        ));
    }

    #[test]
    fn batch_rejects_excessive_raw_payload_length() {
        let batch = NamespaceBatch {
            cluster_id: [3; 16],
            batch_id: 1,
            stream_ordinal: 1,
            predecessor_ordinal: BatchHeader::NO_PREDECESSOR,
            segments: vec![segment()],
        };
        let encoded = batch.encode().unwrap();
        let mut bad = encoded.bytes.clone();
        // Set raw_payload_len (bytes 48..56) to MAX_BATCH_RAW + 1
        let too_big = (MAX_BATCH_RAW as u64 + 1).to_le_bytes();
        bad[48..56].copy_from_slice(&too_big);
        let header_crc = crc32c::crc32c(&bad[..84]);
        bad[84..88].copy_from_slice(&header_crc.to_le_bytes());
        let mut hasher = blake3::Hasher::new();
        hasher.update(&bad[..96]);
        hasher.update(&encoded.bytes[BATCH_HEADER_LEN..]);
        bad[96..128].copy_from_slice(hasher.finalize().as_bytes());
        assert!(matches!(
            EncodedBatch::decode(&bad),
            Err(WireError::LimitExceeded(_))
        ));
    }

    #[test]
    fn namespace_batch_rejects_zero_new_node_id() {
        let mut seg = segment();
        if let NamespaceEntry::NewNode { ref mut node, .. } = seg.entries[0] {
            node.local_node_id = 0;
        }
        let batch = NamespaceBatch {
            cluster_id: [3; 16],
            batch_id: 1,
            stream_ordinal: 1,
            predecessor_ordinal: BatchHeader::NO_PREDECESSOR,
            segments: vec![seg],
        };
        assert!(batch.encode().is_err());
    }

    #[test]
    fn namespace_batch_rejects_zero_existing_node_id() {
        let mut seg = segment();
        if let NamespaceEntry::ExistingNode {
            ref mut local_node_id,
            ..
        } = seg.entries[1]
        {
            *local_node_id = 0;
        }
        let batch = NamespaceBatch {
            cluster_id: [3; 16],
            batch_id: 1,
            stream_ordinal: 1,
            predecessor_ordinal: BatchHeader::NO_PREDECESSOR,
            segments: vec![seg],
        };
        assert!(batch.encode().is_err());
    }

    #[test]
    fn namespace_batch_rejects_empty_segments() {
        let batch = NamespaceBatch {
            cluster_id: [3; 16],
            batch_id: 1,
            stream_ordinal: 1,
            predecessor_ordinal: BatchHeader::NO_PREDECESSOR,
            segments: vec![],
        };
        assert!(batch.encode().is_err());
    }

    #[test]
    fn namespace_batch_rejects_first_key_fingerprint_mismatch() {
        let batch = NamespaceBatch {
            cluster_id: [3; 16],
            batch_id: 1,
            stream_ordinal: 1,
            predecessor_ordinal: BatchHeader::NO_PREDECESSOR,
            segments: vec![segment()],
        };
        let encoded = batch.encode().unwrap();
        let mut bad = encoded.bytes.clone();
        // first_key_fingerprint is at bytes 88..96 (outside header CRC range).
        bad[88] ^= 0xFF;
        // Recompute digest (covers header[..96] + stored payload).
        let mut hasher = blake3::Hasher::new();
        hasher.update(&bad[..96]);
        hasher.update(&encoded.bytes[BATCH_HEADER_LEN..]);
        bad[96..128].copy_from_slice(hasher.finalize().as_bytes());
        let decoded = EncodedBatch::decode(&bad).unwrap();
        assert!(matches!(
            NamespaceBatch::decode(&decoded),
            Err(WireError::Invalid { .. })
        ));
    }

    #[test]
    fn namespace_batch_rejects_new_node_range_mismatch() {
        let batch = NamespaceBatch {
            cluster_id: [3; 16],
            batch_id: 1,
            stream_ordinal: 1,
            predecessor_ordinal: BatchHeader::NO_PREDECESSOR,
            segments: vec![segment()],
        };
        let encoded = batch.encode().unwrap();
        let mut bad = encoded.bytes.clone();
        // first_new_node_id at bytes 64..68, new_node_count at 68..72.
        // Both are inside header CRC range [..84].
        bad[64..68].copy_from_slice(&9999u32.to_le_bytes());
        bad[68..72].copy_from_slice(&9999u32.to_le_bytes());
        let header_crc = crc32c::crc32c(&bad[..84]);
        bad[84..88].copy_from_slice(&header_crc.to_le_bytes());
        let mut hasher = blake3::Hasher::new();
        hasher.update(&bad[..96]);
        hasher.update(&encoded.bytes[BATCH_HEADER_LEN..]);
        bad[96..128].copy_from_slice(hasher.finalize().as_bytes());
        let decoded = EncodedBatch::decode(&bad).unwrap();
        assert!(matches!(
            NamespaceBatch::decode(&decoded),
            Err(WireError::Invalid { .. })
        ));
    }

    #[test]
    fn namespace_batch_rejects_record_count_mismatch() {
        let batch = NamespaceBatch {
            cluster_id: [3; 16],
            batch_id: 1,
            stream_ordinal: 1,
            predecessor_ordinal: BatchHeader::NO_PREDECESSOR,
            segments: vec![segment()],
        };
        let encoded = batch.encode().unwrap();
        let mut bad = encoded.bytes.clone();
        // record_count at bytes 44..48 (inside header CRC range).
        bad[44..48].copy_from_slice(&9999u32.to_le_bytes());
        let header_crc = crc32c::crc32c(&bad[..84]);
        bad[84..88].copy_from_slice(&header_crc.to_le_bytes());
        let mut hasher = blake3::Hasher::new();
        hasher.update(&bad[..96]);
        hasher.update(&encoded.bytes[BATCH_HEADER_LEN..]);
        bad[96..128].copy_from_slice(hasher.finalize().as_bytes());
        let decoded = EncodedBatch::decode(&bad).unwrap();
        assert!(matches!(
            NamespaceBatch::decode(&decoded),
            Err(WireError::Invalid { .. })
        ));
    }

    #[test]
    fn namespace_batch_rejects_unknown_segment_flags() {
        let mut seg = segment();
        seg.flags = 0xF0;
        let batch = NamespaceBatch {
            cluster_id: [3; 16],
            batch_id: 1,
            stream_ordinal: 1,
            predecessor_ordinal: BatchHeader::NO_PREDECESSOR,
            segments: vec![seg],
        };
        assert!(batch.encode().is_err());
    }

    #[test]
    fn namespace_batch_rejects_unknown_entry_tag() {
        // Manually craft a payload with an unknown entry tag.
        use crate::native_base::wire::uvarint::Writer;
        let mut payload = Writer::new();
        payload.uvarint(1); // parent_local_node_id
        payload.put(&[7u8; 16]); // parent_dir_key
        payload.u8(SEGMENT_START | SEGMENT_END); // flags
        payload.uvarint(1); // total_entry_count
        payload.bytes(b"test"); // first_name
        payload.uvarint(1); // entry_count
        payload.uvarint(0); // shared prefix (restart)
        payload.bytes(b"test"); // name suffix
        payload.u8(0xFE); // unknown entry tag
        let encoded = encode_batch(
            BatchKind::Namespace,
            0,
            [3; 16],
            1,
            1,
            BatchHeader::NO_PREDECESSOR,
            1,
            0,
            0,
            b"test",
            payload.as_slice(),
        )
        .unwrap();
        assert!(NamespaceBatch::decode(&encoded).is_err());
    }

    #[test]
    fn namespace_batch_rejects_restart_entry_with_shared_prefix() {
        use crate::native_base::wire::uvarint::Writer;
        let mut payload = Writer::new();
        payload.uvarint(1);
        payload.put(&[7u8; 16]);
        payload.u8(SEGMENT_START | SEGMENT_END);
        payload.uvarint(1);
        payload.bytes(b"alpha");
        payload.uvarint(1);
        // Entry 0 is a restart entry but we give it a non-zero shared prefix.
        payload.uvarint(3);
        payload.bytes(b"pha");
        payload.u8(1); // existing node
        payload.uvarint(5);
        let encoded = encode_batch(
            BatchKind::Namespace,
            0,
            [3; 16],
            1,
            1,
            BatchHeader::NO_PREDECESSOR,
            1,
            0,
            0,
            b"alpha",
            payload.as_slice(),
        )
        .unwrap();
        assert!(NamespaceBatch::decode(&encoded).is_err());
    }

    #[test]
    fn zstd_batch_round_trips_and_bounded_decode_works() {
        // Build a batch with enough entries to benefit from zstd,
        // then verify the bounded decoder rejects oversized claims.
        let mut entries = Vec::new();
        for i in 0..64 {
            let name = NameBytes::new(format!("file_{i:04}.dat").into_bytes()).unwrap();
            entries.push(NamespaceEntry::NewNode {
                name,
                node: NodeRecord {
                    local_node_id: 100 + i,
                    kind: 1,
                    mode: 0o100644,
                    size: 1024,
                    dir_key: None,
                },
            });
        }
        let seg = NamespaceSegment {
            parent_local_node_id: 1,
            parent_dir_key: DirKey::new([7; 16]),
            flags: SEGMENT_START | SEGMENT_END,
            total_entry_count: Some(entries.len() as u64),
            entries,
        };
        let batch = NamespaceBatch {
            cluster_id: [3; 16],
            batch_id: 1,
            stream_ordinal: 1,
            predecessor_ordinal: BatchHeader::NO_PREDECESSOR,
            segments: vec![seg],
        };
        let encoded = batch.encode().unwrap();
        // The encoder auto-picks zstd when it saves space; round-trip must work either way.
        let decoded =
            NamespaceBatch::decode(&EncodedBatch::decode(&encoded.bytes).unwrap()).unwrap();
        assert_eq!(decoded.segments[0].entries.len(), 64);
        // Also verify an oversized raw_payload_len claim is rejected even before decompression.
        let mut bad = encoded.bytes.clone();
        let too_big = (MAX_BATCH_RAW as u64 + 4096).to_le_bytes();
        bad[48..56].copy_from_slice(&too_big);
        let header_crc = crc32c::crc32c(&bad[..84]);
        bad[84..88].copy_from_slice(&header_crc.to_le_bytes());
        let mut hasher = blake3::Hasher::new();
        hasher.update(&bad[..96]);
        hasher.update(&encoded.bytes[BATCH_HEADER_LEN..]);
        bad[96..128].copy_from_slice(hasher.finalize().as_bytes());
        assert!(matches!(
            EncodedBatch::decode(&bad),
            Err(WireError::LimitExceeded(_))
        ));
    }
}
