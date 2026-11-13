//! PM08 required source attributes. PM07 keeps its original synthetic-root
//! and logical-size block interpretation; these records are never inferred.

use super::{SnapshotObjectKind, SnapshotObjectRef};
use crate::workspace::packed::meta::validate_snapshot_hot_attributes;
use crate::workspace::packed::wire::{PackedResult, PackedWireError, Reader, Writer};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotRootAttributes {
    pub inode: u64,
    pub size: u64,
    /// POSIX st_blocks, in 512-byte units; independent of logical EOF.
    pub blocks: u64,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub nlink: u32,
    pub atime_ns: i64,
    pub mtime_ns: i64,
    pub ctime_ns: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotSourceAttributes {
    pub root: SnapshotRootAttributes,
    /// One SI05 allocation record per non-root inode, including special inodes.
    pub allocations: SnapshotObjectRef,
    /// PM09 requires a selector for every regular inode in LargePlacements.
    /// PM07/PM08 retain their original group placement interpretation.
    pub placement_contract: bool,
}

impl SnapshotRootAttributes {
    pub(crate) fn validate(&self) -> PackedResult<()> {
        validate_snapshot_hot_attributes(self.inode, 2, self.mode, self.nlink, 0)
    }

    pub(crate) fn encode(&self, w: &mut Writer) -> PackedResult<()> {
        self.validate()?;
        w.bytes(b"RA05");
        w.u64(self.inode);
        w.u64(self.size);
        w.u64(self.blocks);
        w.u32(self.mode);
        w.u32(self.uid);
        w.u32(self.gid);
        w.u32(self.nlink);
        w.i64(self.atime_ns);
        w.i64(self.mtime_ns);
        w.i64(self.ctime_ns);
        Ok(())
    }

    pub(crate) fn decode(r: &mut Reader<'_>) -> PackedResult<Self> {
        if r.take(4)? != b"RA05" {
            return Err(PackedWireError::UnsupportedFormat(
                "PM08 root attribute version mismatch".into(),
            ));
        }
        let root = Self {
            inode: r.u64()?,
            size: r.u64()?,
            blocks: r.u64()?,
            mode: r.u32()?,
            uid: r.u32()?,
            gid: r.u32()?,
            nlink: r.u32()?,
            atime_ns: r.i64()?,
            mtime_ns: r.i64()?,
            ctime_ns: r.i64()?,
        };
        root.validate()?;
        Ok(root)
    }
}

impl SnapshotSourceAttributes {
    pub(crate) fn validate(&self, root_inode: u64) -> PackedResult<()> {
        self.root.validate()?;
        self.allocations.encode_value()?;
        if self.root.inode != root_inode
            || self.allocations.kind != SnapshotObjectKind::SourceStatsIndex
            || self.allocations.object_len
                > (super::SNAPSHOT_HEADER_LEN
                    + super::index::SNAPSHOT_INDEX_BODY_LIMIT
                    + super::SNAPSHOT_FOOTER_LEN) as u64
        {
            return Err(PackedWireError::Invalid(
                "PM08 source root identity or allocation index mismatch".into(),
            ));
        }
        Ok(())
    }
}

pub(crate) fn encode_allocation(inode: u64, blocks: u64) -> PackedResult<Vec<u8>> {
    if inode == 0 || inode > i64::MAX as u64 {
        return Err(PackedWireError::Invalid("SI05 inode is invalid".into()));
    }
    let mut w = Writer::default();
    w.bytes(b"SI05");
    w.u64(inode);
    w.u64(blocks);
    Ok(w.finish())
}

pub(crate) fn decode_allocation(bytes: &[u8], expected_inode: u64) -> PackedResult<u64> {
    let mut r = Reader::new(bytes);
    if r.take(4)? != b"SI05" {
        return Err(PackedWireError::UnsupportedFormat(
            "SI05 allocation version mismatch".into(),
        ));
    }
    let inode = r.u64()?;
    let blocks = r.u64()?;
    if inode != expected_inode || !r.is_empty() {
        return Err(PackedWireError::Invalid(
            "SI05 allocation inode or length mismatch".into(),
        ));
    }
    encode_allocation(inode, blocks)?;
    Ok(blocks)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocation_identity_version_and_exact_length_are_required() {
        let bytes = encode_allocation(8, 3).unwrap();
        assert_eq!(decode_allocation(&bytes, 8).unwrap(), 3);
        assert!(decode_allocation(&bytes, 9).is_err());
        for length in 0..bytes.len() {
            assert!(decode_allocation(&bytes[..length], 8).is_err());
        }
        let mut extra = bytes.clone();
        extra.push(0);
        assert!(decode_allocation(&extra, 8).is_err());
        extra[..4].copy_from_slice(b"SI06");
        assert!(matches!(
            decode_allocation(&extra, 8),
            Err(PackedWireError::UnsupportedFormat(_))
        ));
        assert!(encode_allocation(0, 1).is_err());
        assert!(encode_allocation(u64::MAX, 1).is_err());
    }
}
