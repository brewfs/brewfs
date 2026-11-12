//! PM09 required regular-inode selectors and paged external logical extents.
//! Missing selectors are corruption, never an implicit all-hole file.

use super::{SnapshotIndexRecord, SnapshotIndexValue, SnapshotObjectKind, SnapshotObjectRef};
use crate::workspace::packed::wire::{PackedResult, PackedWireError, Reader, Writer};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SnapshotPlacement {
    Group {
        inode: u64,
        size: u64,
    },
    External {
        inode: u64,
        size: u64,
        data_bytes: u64,
        extent_count: u64,
        logical_digest: [u8; 32],
        extents: SnapshotObjectRef,
    },
}

fn invalid(message: &str) -> PackedWireError {
    PackedWireError::Invalid(format!("PM09 placement {message}"))
}

impl SnapshotPlacement {
    pub fn inode(&self) -> u64 {
        match self {
            Self::Group { inode, .. } | Self::External { inode, .. } => *inode,
        }
    }
    pub fn size(&self) -> u64 {
        match self {
            Self::Group { size, .. } | Self::External { size, .. } => *size,
        }
    }
    pub fn encode(&self) -> PackedResult<Vec<u8>> {
        if self.inode() == 0 || self.inode() > i64::MAX as u64 || self.size() > i64::MAX as u64 {
            return Err(invalid("inode/EOF exceeds supported range"));
        }
        let mut w = Writer::default();
        w.bytes(b"PS09");
        w.u64(self.inode());
        w.u64(self.size());
        w.u8(u8::from(matches!(self, Self::External { .. })));
        w.bytes(&[0; 7]);
        if let Self::External {
            data_bytes,
            extent_count,
            logical_digest,
            extents,
            ..
        } = self
        {
            if *data_bytes > self.size()
                || (*data_bytes == 0) != (*extent_count == 0)
                || *extent_count > *data_bytes
                || *logical_digest == [0; 32]
                || extents.kind != SnapshotObjectKind::LargeIndex
                || extents.object_len
                    > (super::SNAPSHOT_HEADER_LEN
                        + super::index::SNAPSHOT_INDEX_BODY_LIMIT
                        + super::SNAPSHOT_FOOTER_LEN) as u64
            {
                return Err(invalid("external counts/digest/index identity are invalid"));
            }
            let reference = extents.encode_value()?;
            w.u64(*data_bytes);
            w.u64(*extent_count);
            w.bytes(logical_digest);
            w.u32(reference.len() as u32);
            w.bytes(&reference);
        }
        Ok(w.finish())
    }
    pub fn decode(bytes: &[u8], inode: u64, size: u64) -> PackedResult<Self> {
        let mut r = Reader::new(bytes);
        if r.take(4)? != b"PS09" {
            return Err(PackedWireError::UnsupportedFormat(
                "PM09 selector version mismatch".into(),
            ));
        }
        let actual_inode = r.u64()?;
        let actual_size = r.u64()?;
        let kind = r.u8()?;
        r.skip_zeroes(7)?;
        let placement = match kind {
            0 => Self::Group {
                inode: actual_inode,
                size: actual_size,
            },
            1 => {
                let data_bytes = r.u64()?;
                let extent_count = r.u64()?;
                let logical_digest = r.array::<32>()?;
                let reference_len = r.u32()? as usize;
                if reference_len > 8192 {
                    return Err(invalid("reference exceeds value budget"));
                }
                Self::External {
                    inode: actual_inode,
                    size: actual_size,
                    data_bytes,
                    extent_count,
                    logical_digest,
                    extents: SnapshotObjectRef::decode_value(r.take(reference_len)?)?,
                }
            }
            _ => {
                return Err(PackedWireError::UnsupportedFormat(
                    "PM09 unknown required placement kind".into(),
                ));
            }
        };
        if !r.is_empty() || placement.inode() != inode || placement.size() != size {
            return Err(invalid("selector key/inode/EOF/trailing bytes mismatch"));
        }
        placement.encode()?;
        Ok(placement)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotLargeExtent {
    pub inode: u64,
    pub file_offset: u64,
    pub logical_len: u32,
    pub container_ordinal: u32,
    pub frame_ordinal: u32,
    pub raw_offset: u32,
    pub raw_len: u32,
}

pub(crate) fn extent_key(inode: u64, offset: u64) -> Vec<u8> {
    [inode.to_be_bytes(), offset.to_be_bytes()].concat()
}

impl SnapshotLargeExtent {
    fn end(&self) -> PackedResult<u64> {
        if self.inode == 0
            || self.inode > i64::MAX as u64
            || self.logical_len == 0
            || self.raw_len == 0
            || self.raw_len > 8 * 1024 * 1024
            || self
                .raw_offset
                .checked_add(self.logical_len)
                .is_none_or(|end| end > self.raw_len)
        {
            return Err(invalid("extent inode/length/raw range is invalid"));
        }
        self.file_offset
            .checked_add(u64::from(self.logical_len))
            .filter(|end| *end <= i64::MAX as u64)
            .ok_or_else(|| invalid("extent range overflows"))
    }
    pub fn record(&self) -> PackedResult<SnapshotIndexRecord> {
        let end = self.end()?;
        let mut w = Writer::default();
        w.bytes(b"LE09");
        w.u64(self.inode);
        w.u64(self.file_offset);
        w.u32(self.logical_len);
        w.u32(self.container_ordinal);
        w.u32(self.frame_ordinal);
        w.u32(self.raw_offset);
        w.u32(self.raw_len);
        Ok(SnapshotIndexRecord {
            first_key: extent_key(self.inode, self.file_offset),
            last_key: extent_key(self.inode, end - 1),
            value: SnapshotIndexValue::Leaf(w.finish()),
        })
    }
    pub fn decode_record(
        record: &SnapshotIndexRecord,
        inode: u64,
        size: u64,
    ) -> PackedResult<Self> {
        let SnapshotIndexValue::Leaf(value) = &record.value else {
            return Err(invalid("extent leaf contains branch"));
        };
        let mut r = Reader::new(value);
        if r.take(4)? != b"LE09" {
            return Err(PackedWireError::UnsupportedFormat(
                "PM09 external extent version mismatch".into(),
            ));
        }
        let extent = Self {
            inode: r.u64()?,
            file_offset: r.u64()?,
            logical_len: r.u32()?,
            container_ordinal: r.u32()?,
            frame_ordinal: r.u32()?,
            raw_offset: r.u32()?,
            raw_len: r.u32()?,
        };
        let expected = extent.record()?;
        if !r.is_empty()
            || extent.inode != inode
            || extent.end()? > size
            || expected.first_key != record.first_key
            || expected.last_key != record.last_key
        {
            return Err(invalid("extent inode/EOF/fences/trailing bytes mismatch"));
        }
        Ok(extent)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn required_selector_and_extent_identity_are_exact_and_bounded() {
        let group = SnapshotPlacement::Group {
            inode: 9,
            size: 72 * 1024 * 1024,
        };
        let bytes = group.encode().unwrap();
        assert_eq!(
            SnapshotPlacement::decode(&bytes, 9, group.size()).unwrap(),
            group
        );
        assert!(SnapshotPlacement::decode(&bytes, 10, group.size()).is_err());
        assert!(SnapshotPlacement::decode(&bytes, 9, 8).is_err());
        for length in 0..bytes.len() {
            assert!(SnapshotPlacement::decode(&bytes[..length], 9, group.size()).is_err());
        }
        let mut extra = bytes.clone();
        extra.push(0);
        assert!(SnapshotPlacement::decode(&extra, 9, group.size()).is_err());
        let mut unknown = bytes;
        unknown[20] = 2;
        assert!(SnapshotPlacement::decode(&unknown, 9, group.size()).is_err());
        let extent = SnapshotLargeExtent {
            inode: 9,
            file_offset: 4096,
            logical_len: 4096,
            container_ordinal: 3,
            frame_ordinal: 0,
            raw_offset: 0,
            raw_len: 4096,
        };
        let record = extent.record().unwrap();
        assert_eq!(
            SnapshotLargeExtent::decode_record(&record, 9, 8192).unwrap(),
            extent
        );
        assert!(SnapshotLargeExtent::decode_record(&record, 10, 8192).is_err());
        assert!(SnapshotLargeExtent::decode_record(&record, 9, 8191).is_err());
        let mut bad = record;
        bad.last_key = extent_key(9, 8192);
        assert!(SnapshotLargeExtent::decode_record(&bad, 9, 8193).is_err());
        let mut bad = extent;
        bad.raw_offset = 1;
        assert!(bad.record().is_err());
    }
}
