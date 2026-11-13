//! Authenticated inode locators reuse the existing hot POSIX record model.

use super::SnapshotGroupRef;
use crate::workspace::packed::wire::{PackedResult, PackedWireError, Reader, Writer};
use crate::workspace::packed::{GroupMetaEntry, PackedInodeIndexEntry};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotInodeLocation {
    pub hot: PackedInodeIndexEntry,
    pub group: SnapshotGroupRef,
}

impl SnapshotInodeLocation {
    pub fn encode_value(&self) -> PackedResult<Vec<u8>> {
        self.validate()?;
        let entry = &self.hot;
        let mut w = Writer::default();
        w.bytes(b"IL05");
        w.u64(entry.inode);
        w.u64(entry.parent_inode);
        w.bytes(&entry.parent_dir_key);
        w.u64(entry.group_id);
        w.u32(entry.entry_ordinal);
        w.u8(entry.kind);
        w.bytes(&[0; 3]);
        w.u32(entry.mode);
        w.u32(entry.uid);
        w.u32(entry.gid);
        w.u64(entry.rdev);
        w.u32(entry.nlink);
        w.i64(entry.atime_ns);
        w.i64(entry.mtime_ns);
        w.i64(entry.ctime_ns);
        w.u64(entry.size);
        w.u16(entry.name.len() as u16);
        w.bytes(&entry.name);
        let group = self.group.encode_value()?;
        w.u32(group.len() as u32);
        w.bytes(&group);
        Ok(w.finish())
    }

    pub fn decode_value(bytes: &[u8]) -> PackedResult<Self> {
        let mut r = Reader::new(bytes);
        if r.take(4)? != b"IL05" {
            return Err(PackedWireError::UnsupportedFormat(
                "wire 005 inode locator payload mismatch".into(),
            ));
        }
        let inode = r.u64()?;
        let parent_inode = r.u64()?;
        let parent_dir_key = r.array::<32>()?;
        let group_id = r.u64()?;
        let entry_ordinal = r.u32()?;
        let kind = r.u8()?;
        r.skip_zeroes(3)?;
        let mode = r.u32()?;
        let uid = r.u32()?;
        let gid = r.u32()?;
        let rdev = r.u64()?;
        let nlink = r.u32()?;
        let atime_ns = r.i64()?;
        let mtime_ns = r.i64()?;
        let ctime_ns = r.i64()?;
        let size = r.u64()?;
        let name_len = r.u16()? as usize;
        if name_len > 1024 {
            return Err(PackedWireError::LimitExceeded(
                "wire 005 inode name exceeds budget".into(),
            ));
        }
        let name = r.take(name_len)?.to_vec();
        let group_len = r.u32()? as usize;
        if group_len > 4096 {
            return Err(PackedWireError::LimitExceeded(
                "wire 005 inode group ref exceeds budget".into(),
            ));
        }
        let group = SnapshotGroupRef::decode_value(r.take(group_len)?)?;
        if !r.is_empty() {
            return Err(PackedWireError::Invalid(
                "wire 005 inode locator has trailing bytes".into(),
            ));
        }
        let location = Self {
            hot: PackedInodeIndexEntry {
                inode,
                parent_inode,
                parent_dir_key,
                group_id,
                entry_ordinal,
                name,
                kind,
                mode,
                uid,
                gid,
                rdev,
                nlink,
                atime_ns,
                mtime_ns,
                ctime_ns,
                size,
            },
            group,
        };
        location.validate()?;
        Ok(location)
    }

    fn validate(&self) -> PackedResult<()> {
        super::super::meta::validate_name(&self.hot.name)?;
        super::super::meta::validate_snapshot_hot_attributes(
            self.hot.inode,
            self.hot.kind,
            self.hot.mode,
            self.hot.nlink,
            self.hot.rdev,
        )?;
        if self.hot.inode == 0
            || self.hot.inode > i64::MAX as u64
            || self.hot.parent_inode == 0
            || self.hot.parent_inode > i64::MAX as u64
            || !(1..=7).contains(&self.hot.kind)
            || self.hot.nlink == 0
            || self.hot.group_id != self.group.group_id
            || self.hot.parent_dir_key != self.group.parent_dir_key
            || self.hot.entry_ordinal >= self.group.entry_count
            || self.hot.name < self.group.first_name
            || self.hot.name > self.group.last_name
        {
            return Err(PackedWireError::Invalid(
                "wire 005 inode identity/ordinal/name fences disagree".into(),
            ));
        }
        self.group.encode_value()?;
        Ok(())
    }

    pub fn reverse_key(&self) -> Vec<u8> {
        let mut key = self.hot.inode.to_be_bytes().to_vec();
        key.extend_from_slice(&self.hot.parent_inode.to_be_bytes());
        key.extend_from_slice(&self.hot.name);
        key
    }

    pub fn same_inode_attributes(&self, other: &Self) -> bool {
        let a = &self.hot;
        let b = &other.hot;
        (
            a.inode, a.kind, a.mode, a.uid, a.gid, a.rdev, a.nlink, a.atime_ns, a.mtime_ns,
            a.ctime_ns, a.size,
        ) == (
            b.inode, b.kind, b.mode, b.uid, b.gid, b.rdev, b.nlink, b.atime_ns, b.mtime_ns,
            b.ctime_ns, b.size,
        )
    }

    pub fn validate_entry(&self, entry: &GroupMetaEntry) -> PackedResult<()> {
        let hot = &self.hot;
        if entry.inode != hot.inode
            || entry.name != hot.name
            || entry.kind != hot.kind
            || entry.mode != hot.mode
            || entry.uid != hot.uid
            || entry.gid != hot.gid
            || entry.rdev != hot.rdev
            || entry.nlink != hot.nlink
            || entry.atime_ns != hot.atime_ns
            || entry.mtime_ns != hot.mtime_ns
            || entry.ctime_ns != hot.ctime_ns
            || entry.size != hot.size
        {
            return Err(PackedWireError::Invalid(
                "wire 005 inode index disagrees with GroupMeta entry".into(),
            ));
        }
        Ok(())
    }
}
