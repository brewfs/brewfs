//! Independently decodable extent batches for clustered metadata v2.
//!
//! Extents are grouped by local inode and bounded into segments.  The codec
//! carries no predecessor state: integer/gap state resets for every segment
//! and every batch, so an indexed range can be fetched without replaying an
//! earlier namespace stream.

use crate::native_base::wire::error::{WireError, WireResult};
use crate::native_base::wire::uvarint::{Reader, Writer};

use super::batch::{BatchKind, EncodedBatch, encode_batch};

const MAX_SEGMENTS: usize = 1 << 20;
const MAX_EXTENTS_PER_SEGMENT: usize = 1 << 20;

/// Canonical key used by the extent index: `(LocalNodeId, file offset)`.
pub fn extent_index_key(local_node_id: u32, file_offset: u64) -> [u8; 12] {
    let mut key = [0u8; 12];
    key[..4].copy_from_slice(&local_node_id.to_be_bytes());
    key[4..].copy_from_slice(&file_offset.to_be_bytes());
    key
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExtentRecord {
    pub gap_from_previous_end: u64,
    pub logical_length: u64,
    pub slice_id: u64,
    pub slice_offset: u64,
}

/// A resolved, bounded portion of one logical file extent.
///
/// The on-disk record stores a gap from the previous extent so that the
/// payload stays compact.  Readers expose absolute offsets instead.  The
/// range reader clips each returned span to the requested file interval, so a
/// caller never has to retain or scan an entire inode extent list.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExtentSpan {
    pub local_node_id: u32,
    pub file_offset: u64,
    pub logical_length: u64,
    pub slice_id: u64,
    pub slice_offset: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExtentSegment {
    pub local_node_id: u32,
    /// The logical offset anchor before the first gap in this segment.
    pub first_file_offset: u64,
    pub extents: Vec<ExtentRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExtentBatch {
    pub cluster_id: [u8; 16],
    pub batch_id: u32,
    pub stream_ordinal: u32,
    pub predecessor_ordinal: u32,
    pub segments: Vec<ExtentSegment>,
}

impl ExtentBatch {
    /// Resolve only the portions of one inode that intersect `[start, end)`.
    ///
    /// Holes are intentionally absent from the result.  A caller can fill
    /// the gaps with zeroes while using the returned data spans to resolve
    /// SliceIds through the Data Seal.  The returned spans are clipped to the
    /// requested range and remain sorted by logical file offset.
    pub fn spans_in_range(
        &self,
        local_node_id: u32,
        start: u64,
        end: u64,
    ) -> WireResult<Vec<ExtentSpan>> {
        if local_node_id == 0 {
            return Err(WireError::invalid(
                "extent range",
                "local node id must be non-zero",
            ));
        }
        if start >= end {
            return Err(WireError::invalid(
                "extent range",
                "range must be non-empty and ordered",
            ));
        }

        let mut spans = Vec::new();
        for segment in &self.segments {
            if segment.local_node_id != local_node_id {
                continue;
            }
            let mut cursor = segment.first_file_offset;
            for record in &segment.extents {
                let extent_start = cursor
                    .checked_add(record.gap_from_previous_end)
                    .ok_or_else(|| {
                        WireError::LimitExceeded("extent file offset overflows u64".into())
                    })?;
                let extent_end =
                    extent_start
                        .checked_add(record.logical_length)
                        .ok_or_else(|| {
                            WireError::LimitExceeded("extent file offset overflows u64".into())
                        })?;
                cursor = extent_end;

                let clipped_start = extent_start.max(start);
                let clipped_end = extent_end.min(end);
                if clipped_start >= clipped_end {
                    continue;
                }
                let slice_delta = clipped_start - extent_start;
                let slice_offset =
                    record
                        .slice_offset
                        .checked_add(slice_delta)
                        .ok_or_else(|| {
                            WireError::LimitExceeded("extent slice offset overflows u64".into())
                        })?;
                spans.push(ExtentSpan {
                    local_node_id,
                    file_offset: clipped_start,
                    logical_length: clipped_end - clipped_start,
                    slice_id: record.slice_id,
                    slice_offset,
                });
            }
        }
        Ok(spans)
    }

    pub fn encode(&self) -> WireResult<EncodedBatch> {
        validate_segments(&self.segments)?;
        let mut payload = Writer::new();
        let mut record_count = 0u32;
        for segment in &self.segments {
            payload.uvarint(u64::from(segment.local_node_id));
            payload.u64(segment.first_file_offset);
            payload.uvarint(segment.extents.len() as u64);
            for extent in &segment.extents {
                payload.uvarint(extent.gap_from_previous_end);
                payload.uvarint(extent.logical_length);
                payload.u64(extent.slice_id);
                payload.u64(extent.slice_offset);
                record_count = record_count.checked_add(1).ok_or_else(|| {
                    WireError::LimitExceeded("extent record count overflows u32".into())
                })?;
            }
        }
        let first_key = self
            .segments
            .first()
            .map(|segment| extent_index_key(segment.local_node_id, segment.first_file_offset))
            .unwrap_or([0; 12]);
        encode_batch(
            BatchKind::Extent,
            0,
            self.cluster_id,
            self.batch_id,
            self.stream_ordinal,
            self.predecessor_ordinal,
            record_count,
            0,
            0,
            &first_key,
            payload.as_slice(),
        )
    }

    pub fn decode(encoded: &EncodedBatch) -> WireResult<Self> {
        if encoded.header.kind != BatchKind::Extent {
            return Err(WireError::invalid(
                "extent batch",
                "batch kind is not extent",
            ));
        }
        if encoded.header.first_new_node_id != 0 || encoded.header.new_node_count != 0 {
            return Err(WireError::invalid(
                "extent batch",
                "extent batch cannot introduce namespace nodes",
            ));
        }
        let mut reader = Reader::new(&encoded.raw_payload);
        let mut segments = Vec::new();
        let mut records = 0u32;
        while !reader.is_empty() {
            if segments.len() >= MAX_SEGMENTS {
                return Err(WireError::LimitExceeded(
                    "extent segment count exceeds bound".into(),
                ));
            }
            let local_node_id = u32::try_from(reader.uvarint("extent segment")?)
                .map_err(|_| WireError::LimitExceeded("extent local node id exceeds u32".into()))?;
            let first_file_offset = reader.u64("extent segment")?;
            let extent_count = usize::try_from(reader.uvarint("extent segment")?)
                .map_err(|_| WireError::LimitExceeded("extent count exceeds usize".into()))?;
            if extent_count == 0 || extent_count > MAX_EXTENTS_PER_SEGMENT {
                return Err(WireError::LimitExceeded(
                    "extent segment count is outside bounds".into(),
                ));
            }
            let mut extents = Vec::with_capacity(extent_count);
            for _ in 0..extent_count {
                let extent = ExtentRecord {
                    gap_from_previous_end: reader.uvarint("extent record")?,
                    logical_length: reader.uvarint("extent record")?,
                    slice_id: reader.u64("extent record")?,
                    slice_offset: reader.u64("extent record")?,
                };
                extents.push(extent);
                records = records.checked_add(1).ok_or_else(|| {
                    WireError::LimitExceeded("extent record count overflows u32".into())
                })?;
            }
            segments.push(ExtentSegment {
                local_node_id,
                first_file_offset,
                extents,
            });
        }
        validate_segments(&segments)?;
        if encoded.header.record_count != records {
            return Err(WireError::invalid(
                "extent batch",
                "record count does not match payload",
            ));
        }
        if encoded.header.cluster_id == [0; 16] {
            return Err(WireError::invalid(
                "extent batch",
                "cluster id must be non-zero",
            ));
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

fn validate_segments(segments: &[ExtentSegment]) -> WireResult<()> {
    if segments.is_empty() || segments.len() > MAX_SEGMENTS {
        return Err(WireError::LimitExceeded(
            "extent segment count is outside bounds".into(),
        ));
    }
    let mut previous_node = 0u32;
    let mut previous_end = 0u64;
    for (segment_index, segment) in segments.iter().enumerate() {
        if segment.local_node_id == 0 {
            return Err(WireError::invalid(
                "extent segment",
                "local node id must be non-zero",
            ));
        }
        if segment.extents.is_empty() || segment.extents.len() > MAX_EXTENTS_PER_SEGMENT {
            return Err(WireError::LimitExceeded(
                "extent segment count is outside bounds".into(),
            ));
        }
        if segment_index > 0 {
            if segment.local_node_id < previous_node {
                return Err(WireError::invalid(
                    "extent segments",
                    "segments are not sorted by local node id",
                ));
            }
            if segment.local_node_id == previous_node && segment.first_file_offset < previous_end {
                return Err(WireError::invalid(
                    "extent segments",
                    "segments overlap within one local node",
                ));
            }
        }
        let mut cursor = segment.first_file_offset;
        let mut segment_end = cursor;
        for extent in &segment.extents {
            if extent.logical_length == 0 || extent.slice_id == 0 {
                return Err(WireError::invalid(
                    "extent record",
                    "logical length and slice id must be non-zero",
                ));
            }
            cursor = cursor
                .checked_add(extent.gap_from_previous_end)
                .ok_or_else(|| {
                    WireError::LimitExceeded("extent file offset overflows u64".into())
                })?;
            let end = cursor.checked_add(extent.logical_length).ok_or_else(|| {
                WireError::LimitExceeded("extent file offset overflows u64".into())
            })?;
            let _slice_end = extent
                .slice_offset
                .checked_add(extent.logical_length)
                .ok_or_else(|| {
                    WireError::LimitExceeded("extent slice offset overflows u64".into())
                })?;
            cursor = end;
            segment_end = end;
        }
        previous_node = segment.local_node_id;
        previous_end = segment_end;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workspace_overlay::clustered_snapshot::batch::BatchHeader;

    fn batch() -> ExtentBatch {
        ExtentBatch {
            cluster_id: [1; 16],
            batch_id: 3,
            stream_ordinal: 7,
            predecessor_ordinal: BatchHeader::NO_PREDECESSOR,
            segments: vec![
                ExtentSegment {
                    local_node_id: 4,
                    first_file_offset: 0,
                    extents: vec![
                        ExtentRecord {
                            gap_from_previous_end: 0,
                            logical_length: 4096,
                            slice_id: 9,
                            slice_offset: 0,
                        },
                        ExtentRecord {
                            gap_from_previous_end: 4096,
                            logical_length: 8192,
                            slice_id: 10,
                            slice_offset: 128,
                        },
                    ],
                },
                ExtentSegment {
                    local_node_id: 5,
                    first_file_offset: 0,
                    extents: vec![ExtentRecord {
                        gap_from_previous_end: 0,
                        logical_length: 1024,
                        slice_id: 11,
                        slice_offset: 0,
                    }],
                },
            ],
        }
    }

    #[test]
    fn extent_batch_round_trips_with_holes_and_sorted_segments() {
        let expected = batch();
        let encoded = expected.encode().unwrap();
        let decoded = ExtentBatch::decode(&EncodedBatch::decode(&encoded.bytes).unwrap()).unwrap();
        assert_eq!(decoded, expected);
        assert_eq!(encoded.header.kind, BatchKind::Extent);
        assert_eq!(encoded.header.record_count, 3);
    }

    #[test]
    fn extent_batch_rejects_overlap_and_zero_slice() {
        let mut invalid = batch();
        invalid.segments[1].local_node_id = invalid.segments[0].local_node_id;
        invalid.segments[1].first_file_offset = 1;
        assert!(invalid.encode().is_err());
        let mut invalid = batch();
        invalid.segments[0].extents[0].slice_id = 0;
        assert!(invalid.encode().is_err());
    }

    #[test]
    fn extent_index_key_is_big_endian_tuple() {
        assert_eq!(
            extent_index_key(0x0102_0304, 0x0506_0708_090a_0b0c),
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        );
    }
}
