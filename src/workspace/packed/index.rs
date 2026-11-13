//! Pageable manifest indexes for large read-only snapshots.

use super::layout::AccessProfile;
use super::wire::{
    PackedEnvelope, PackedGroupIndexPageRef, PackedGroupRef, PackedInodeIndexPageRef,
    PackedObjectKind, PackedResult, PackedWireError, Reader, Writer, compare_group_key,
};

const MAX_PAGE_ENTRIES: u32 = 4096;
const MAX_INDEX_NAME_BYTES: u32 = 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackedGroupIndexPage {
    pub snapshot_id: [u8; 32],
    pub page_ordinal: u32,
    pub total_pages: u32,
    pub groups: Vec<PackedGroupRef>,
}

impl PackedGroupIndexPage {
    pub fn encode(&self) -> PackedResult<Vec<u8>> {
        validate_page_header(self.page_ordinal, self.total_pages, self.groups.len())?;
        validate_group_order(&self.groups)?;
        let mut writer = Writer::default();
        writer.bytes(b"GI04");
        writer.bytes(&self.snapshot_id);
        writer.u32(self.page_ordinal);
        writer.u32(self.total_pages);
        writer.u32(self.groups.len() as u32);
        for group in &self.groups {
            encode_group_ref(&mut writer, group)?;
        }
        PackedEnvelope::build(PackedObjectKind::GroupIndex, writer.finish())
    }

    pub fn decode(object: Vec<u8>) -> PackedResult<Self> {
        let envelope = PackedEnvelope::parse(object)?;
        if envelope.header.kind != PackedObjectKind::GroupIndex {
            return Err(PackedWireError::Invalid(
                "packed object is not a group index page".into(),
            ));
        }
        let mut reader = Reader::new(envelope.body());
        if reader.take(4)? != b"GI04" {
            return Err(PackedWireError::UnsupportedFormat(
                "packed group index page version mismatch".into(),
            ));
        }
        let snapshot_id = reader.array::<32>()?;
        let page_ordinal = reader.u32()?;
        let total_pages = reader.u32()?;
        let count = reader.u32()?;
        validate_page_header(page_ordinal, total_pages, count as usize)?;
        let mut groups = Vec::with_capacity(count as usize);
        for _ in 0..count {
            groups.push(decode_group_ref(&mut reader)?);
        }
        validate_group_order(&groups)?;
        if !reader.is_empty() {
            return Err(PackedWireError::Invalid(
                "packed group index page has trailing bytes".into(),
            ));
        }
        Ok(Self {
            snapshot_id,
            page_ordinal,
            total_pages,
            groups,
        })
    }

    /// Check that this page's first and last groups exactly match the
    /// authenticated manifest routing fence.
    pub fn validate_reference(&self, reference: &PackedGroupIndexPageRef) -> PackedResult<()> {
        let first = self
            .groups
            .first()
            .ok_or_else(|| PackedWireError::Invalid("packed group index page is empty".into()))?;
        let last = self
            .groups
            .last()
            .ok_or_else(|| PackedWireError::Invalid("packed group index page is empty".into()))?;
        if first.parent_dir_key != reference.first_parent_dir_key
            || first.first_name != reference.first_name
            || last.parent_dir_key != reference.last_parent_dir_key
            || last.last_name != reference.last_name
        {
            return Err(PackedWireError::Invalid(
                "packed group index page fence does not match manifest".into(),
            ));
        }
        Ok(())
    }

    pub fn contains(&self, parent_dir_key: [u8; 32], name: &[u8]) -> bool {
        self.groups.iter().any(|group| {
            group.parent_dir_key == parent_dir_key
                && group.first_name.as_slice() <= name
                && group.last_name.as_slice() >= name
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackedInodeIndexEntry {
    pub inode: u64,
    /// Parent identity is carried with every inode index value so lookup and
    /// getattr never need to fetch the complete directory group first.
    pub parent_inode: u64,
    pub parent_dir_key: [u8; 32],
    pub group_id: u64,
    pub entry_ordinal: u32,
    pub name: Vec<u8>,
    pub kind: u8,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub rdev: u64,
    pub nlink: u32,
    pub atime_ns: i64,
    pub mtime_ns: i64,
    pub ctime_ns: i64,
    pub size: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackedInodeIndexPage {
    pub snapshot_id: [u8; 32],
    pub page_ordinal: u32,
    pub total_pages: u32,
    pub entries: Vec<PackedInodeIndexEntry>,
}

impl PackedInodeIndexPage {
    pub fn encode(&self) -> PackedResult<Vec<u8>> {
        validate_page_header(self.page_ordinal, self.total_pages, self.entries.len())?;
        validate_inode_order(&self.entries)?;
        let mut writer = Writer::default();
        writer.bytes(b"II05");
        writer.bytes(&self.snapshot_id);
        writer.u32(self.page_ordinal);
        writer.u32(self.total_pages);
        writer.u32(self.entries.len() as u32);
        for entry in &self.entries {
            validate_index_name(&entry.name)?;
            writer.u64(entry.inode);
            writer.u64(entry.parent_inode);
            writer.bytes(&entry.parent_dir_key);
            writer.u64(entry.group_id);
            writer.u32(entry.entry_ordinal);
            writer.u8(entry.kind);
            writer.bytes(&[0, 0, 0]);
            writer.u32(entry.mode);
            writer.u32(entry.uid);
            writer.u32(entry.gid);
            writer.u64(entry.rdev);
            writer.u32(entry.nlink);
            writer.i64(entry.atime_ns);
            writer.i64(entry.mtime_ns);
            writer.i64(entry.ctime_ns);
            writer.u64(entry.size);
            writer.u32(entry.name.len() as u32);
            writer.bytes(&entry.name);
        }
        PackedEnvelope::build(PackedObjectKind::InodeIndex, writer.finish())
    }

    pub fn decode(object: Vec<u8>) -> PackedResult<Self> {
        let envelope = PackedEnvelope::parse(object)?;
        if envelope.header.kind != PackedObjectKind::InodeIndex {
            return Err(PackedWireError::Invalid(
                "packed object is not an inode index page".into(),
            ));
        }
        let mut reader = Reader::new(envelope.body());
        if reader.take(4)? != b"II05" {
            return Err(PackedWireError::UnsupportedFormat(
                "packed inode index page version mismatch".into(),
            ));
        }
        let snapshot_id = reader.array::<32>()?;
        let page_ordinal = reader.u32()?;
        let total_pages = reader.u32()?;
        let count = reader.u32()?;
        validate_page_header(page_ordinal, total_pages, count as usize)?;
        let mut entries = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let inode = reader.u64()?;
            let parent_inode = reader.u64()?;
            let parent_dir_key = reader.array::<32>()?;
            let group_id = reader.u64()?;
            let entry_ordinal = reader.u32()?;
            let kind = reader.u8()?;
            reader.skip_zeroes(3)?;
            let mode = reader.u32()?;
            let uid = reader.u32()?;
            let gid = reader.u32()?;
            let rdev = reader.u64()?;
            let nlink = reader.u32()?;
            let atime_ns = reader.i64()?;
            let mtime_ns = reader.i64()?;
            let ctime_ns = reader.i64()?;
            let size = reader.u64()?;
            let name_len = reader.u32()?;
            if name_len > MAX_INDEX_NAME_BYTES {
                return Err(PackedWireError::LimitExceeded(
                    "packed inode index name exceeds 1 KiB".into(),
                ));
            }
            let name = reader.bytes(name_len as usize)?.to_vec();
            validate_index_name(&name)?;
            entries.push(PackedInodeIndexEntry {
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
            });
        }
        validate_inode_order(&entries)?;
        if !reader.is_empty() {
            return Err(PackedWireError::Invalid(
                "packed inode index page has trailing bytes".into(),
            ));
        }
        Ok(Self {
            snapshot_id,
            page_ordinal,
            total_pages,
            entries,
        })
    }

    /// Check that the page's first and last inode exactly match its manifest
    /// routing range.
    pub fn validate_reference(&self, reference: &PackedInodeIndexPageRef) -> PackedResult<()> {
        let first = self
            .entries
            .first()
            .ok_or_else(|| PackedWireError::Invalid("packed inode index page is empty".into()))?;
        let last = self
            .entries
            .last()
            .ok_or_else(|| PackedWireError::Invalid("packed inode index page is empty".into()))?;
        if first.inode != reference.first_inode || last.inode != reference.last_inode {
            return Err(PackedWireError::Invalid(
                "packed inode index page range does not match manifest".into(),
            ));
        }
        Ok(())
    }

    pub fn contains(&self, inode: u64) -> bool {
        self.entries
            .binary_search_by_key(&inode, |entry| entry.inode)
            .is_ok()
    }
}

fn validate_group_order(groups: &[PackedGroupRef]) -> PackedResult<()> {
    for pair in groups.windows(2) {
        let previous = &pair[0];
        let current = &pair[1];
        if compare_group_key(
            previous.parent_dir_key,
            &previous.first_name,
            current.parent_dir_key,
            &current.first_name,
        ) != std::cmp::Ordering::Less
            || (previous.parent_dir_key == current.parent_dir_key
                && previous.last_name >= current.first_name)
        {
            return Err(PackedWireError::Invalid(
                "packed group index page groups are unsorted or overlapping".into(),
            ));
        }
    }
    Ok(())
}

fn validate_inode_order(entries: &[PackedInodeIndexEntry]) -> PackedResult<()> {
    if entries
        .windows(2)
        .any(|pair| pair[0].inode >= pair[1].inode)
    {
        return Err(PackedWireError::Invalid(
            "packed inode index page entries are not strictly ordered".into(),
        ));
    }
    Ok(())
}

fn validate_page_header(page_ordinal: u32, total_pages: u32, count: usize) -> PackedResult<()> {
    if total_pages == 0
        || page_ordinal >= total_pages
        || count == 0
        || count > MAX_PAGE_ENTRIES as usize
    {
        return Err(PackedWireError::Invalid(
            "packed index page ordinal, total, or entry count is invalid".into(),
        ));
    }
    Ok(())
}

fn encode_group_ref(writer: &mut Writer, group: &PackedGroupRef) -> PackedResult<()> {
    validate_group_ref(group)?;
    writer.u64(group.group_id);
    writer.u32(group.container_ordinal);
    writer.u8(group.layout_profile as u8);
    writer.bytes(&[0, 0, 0]);
    writer.bytes(&group.parent_dir_key);
    writer.u64(group.meta_offset);
    writer.u32(group.meta_len);
    writer.u64(group.data_offset);
    writer.u32(group.data_len);
    writer.u32(group.entry_count);
    writer.u32(group.file_count);
    writer.u32(group.frame_count);
    writer.u32(group.first_name.len() as u32);
    writer.u32(group.last_name.len() as u32);
    writer.bytes(&group.metadata_digest);
    writer.bytes(&group.data_digest);
    writer.bytes(&group.first_name);
    writer.bytes(&group.last_name);
    Ok(())
}

fn decode_group_ref(reader: &mut Reader<'_>) -> PackedResult<PackedGroupRef> {
    let group_id = reader.u64()?;
    let container_ordinal = reader.u32()?;
    let layout_profile = AccessProfile::from_u8(reader.u8()?)
        .map_err(|error| PackedWireError::Invalid(error.to_string()))?;
    reader.skip_zeroes(3)?;
    let parent_dir_key = reader.array::<32>()?;
    let meta_offset = reader.u64()?;
    let meta_len = reader.u32()?;
    let data_offset = reader.u64()?;
    let data_len = reader.u32()?;
    let entry_count = reader.u32()?;
    let file_count = reader.u32()?;
    let frame_count = reader.u32()?;
    let first_name_len = reader.u32()?;
    let last_name_len = reader.u32()?;
    if first_name_len > MAX_INDEX_NAME_BYTES || last_name_len > MAX_INDEX_NAME_BYTES {
        return Err(PackedWireError::LimitExceeded(
            "packed group index name exceeds 1 KiB".into(),
        ));
    }
    let metadata_digest = reader.array::<32>()?;
    let data_digest = reader.array::<32>()?;
    let first_name = reader.bytes(first_name_len as usize)?.to_vec();
    let last_name = reader.bytes(last_name_len as usize)?.to_vec();
    let group = PackedGroupRef {
        group_id,
        container_ordinal,
        parent_dir_key,
        first_name,
        last_name,
        meta_offset,
        meta_len,
        data_offset,
        data_len,
        entry_count,
        file_count,
        frame_count,
        layout_profile,
        metadata_digest,
        data_digest,
    };
    validate_group_ref(&group)?;
    Ok(group)
}

fn validate_group_ref(group: &PackedGroupRef) -> PackedResult<()> {
    validate_index_name(&group.first_name)?;
    validate_index_name(&group.last_name)?;
    if group.first_name > group.last_name {
        return Err(PackedWireError::Invalid(
            "packed group index name range is inverted".into(),
        ));
    }
    Ok(())
}

fn validate_index_name(name: &[u8]) -> PackedResult<()> {
    if name.is_empty()
        || name.len() as u32 > MAX_INDEX_NAME_BYTES
        || name.contains(&0)
        || name.contains(&b'/')
        || name == b"."
        || name == b".."
    {
        return Err(PackedWireError::Invalid(
            "packed index name is invalid".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn group() -> PackedGroupRef {
        PackedGroupRef {
            group_id: 1,
            container_ordinal: 0,
            parent_dir_key: [1; 32],
            first_name: b"a".to_vec(),
            last_name: b"z".to_vec(),
            meta_offset: 64,
            meta_len: 128,
            data_offset: 256,
            data_len: 1024,
            entry_count: 1,
            file_count: 1,
            frame_count: 1,
            layout_profile: AccessProfile::RandomSmallFile,
            metadata_digest: [2; 32],
            data_digest: [3; 32],
        }
    }

    #[test]
    fn group_index_page_round_trips() {
        let page = PackedGroupIndexPage {
            snapshot_id: [4; 32],
            page_ordinal: 0,
            total_pages: 1,
            groups: vec![group()],
        };
        assert_eq!(
            PackedGroupIndexPage::decode(page.encode().unwrap()).unwrap(),
            page
        );
    }

    #[test]
    fn group_index_page_rejects_overlapping_name_ranges() {
        let mut first = group();
        first.last_name = b"m".to_vec();
        let mut second = group();
        second.group_id = 2;
        second.first_name = b"l".to_vec();
        let page = PackedGroupIndexPage {
            snapshot_id: [4; 32],
            page_ordinal: 0,
            total_pages: 1,
            groups: vec![first, second],
        };
        assert!(page.encode().is_err());
    }

    #[test]
    fn inode_index_page_rejects_invalid_name() {
        let page = PackedInodeIndexPage {
            snapshot_id: [4; 32],
            page_ordinal: 0,
            total_pages: 1,
            entries: vec![PackedInodeIndexEntry {
                inode: 1,
                parent_inode: 0,
                parent_dir_key: [9; 32],
                group_id: 1,
                entry_ordinal: 0,
                name: b"bad/name".to_vec(),
                kind: 1,
                mode: 0o100644,
                uid: 1000,
                gid: 1000,
                rdev: 0,
                nlink: 1,
                atime_ns: 1,
                mtime_ns: 2,
                ctime_ns: 3,
                size: 0,
            }],
        };
        assert!(page.encode().is_err());
    }

    #[test]
    fn inode_index_page_round_trips_parent_identity_and_posix_attrs() {
        let page = PackedInodeIndexPage {
            snapshot_id: [4; 32],
            page_ordinal: 0,
            total_pages: 1,
            entries: vec![PackedInodeIndexEntry {
                inode: 10,
                parent_inode: 2,
                parent_dir_key: [7; 32],
                group_id: 3,
                entry_ordinal: 4,
                name: b"sample".to_vec(),
                kind: 1,
                mode: 0o100640,
                uid: 1001,
                gid: 1002,
                rdev: 0,
                nlink: 2,
                atime_ns: -11,
                mtime_ns: 22,
                ctime_ns: 33,
                size: 4096,
            }],
        };
        assert_eq!(
            PackedInodeIndexPage::decode(page.encode().unwrap()).unwrap(),
            page
        );
    }

    #[test]
    fn inode_index_page_rejects_unsorted_entries() {
        let entry = |inode| PackedInodeIndexEntry {
            inode,
            parent_inode: 2,
            parent_dir_key: [7; 32],
            group_id: 3,
            entry_ordinal: 4,
            name: format!("sample-{}", inode).into_bytes(),
            kind: 1,
            mode: 0o100640,
            uid: 1001,
            gid: 1002,
            rdev: 0,
            nlink: 1,
            atime_ns: 1,
            mtime_ns: 2,
            ctime_ns: 3,
            size: 4096,
        };
        let page = PackedInodeIndexPage {
            snapshot_id: [4; 32],
            page_ordinal: 0,
            total_pages: 1,
            entries: vec![entry(2), entry(1)],
        };
        assert!(page.encode().is_err());
    }
}
