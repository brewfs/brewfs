//! Directory projection of authenticated GM07. File placement and inline
//! payloads stay opaque until a caller asks for the full file record.

use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GroupDirectoryEntry {
    pub name: Vec<u8>,
    pub inode: u64,
    pub kind: u8,
    pub mode: u32,
}

#[derive(Debug)]
pub(crate) struct GroupDirectory {
    pub entries: Vec<GroupDirectoryEntry>,
}

impl GroupDirectory {
    pub(crate) fn from_metadata(meta: &GroupMeta) -> Self {
        Self {
            entries: meta
                .entries()
                .iter()
                .map(|entry| GroupDirectoryEntry {
                    name: entry.name.clone(),
                    inode: entry.inode,
                    kind: entry.kind,
                    mode: entry.mode,
                })
                .collect(),
        }
    }

    pub(crate) fn owned_memory_bytes(&self) -> u64 {
        (std::mem::size_of::<Self>()
            + 256
            + self.entries.capacity() * std::mem::size_of::<GroupDirectoryEntry>()
            + self
                .entries
                .iter()
                .map(|entry| entry.name.capacity())
                .sum::<usize>()) as u64
    }

    pub(crate) fn decode(bytes: &[u8]) -> PackedResult<Self> {
        let (count, runs) = restart_runs(bytes)?;
        let mut entries: Vec<GroupDirectoryEntry> = Vec::with_capacity(count);
        for (ordinal, run) in runs.into_iter().enumerate() {
            let mut reader = Reader::new(run);
            reader.take(4)?; // restart_runs validated the GM06 run tag.
            let length = reader.u32()? as usize;
            if length != (count - ordinal * 32).min(32)
                || length > (run.len() - GROUP_META_HEADER_LEN) / GROUP_META_MIN_ENTRY_BYTES
            {
                return Err(PackedWireError::Invalid(
                    "GM07 restart record count mismatch".into(),
                ));
            }
            reader.skip_zeroes(4)?;
            for index in 0..length {
                let previous = if index == 0 {
                    &[][..]
                } else {
                    entries.last().unwrap().name.as_slice()
                };
                let prefix = usize::from(reader.u16()?);
                let suffix = usize::from(reader.u16()?);
                if prefix > previous.len() || prefix + suffix > MAX_NAME_LEN {
                    return Err(PackedWireError::Invalid(
                        "group metadata name prefix/suffix is invalid".into(),
                    ));
                }
                let mut name = Vec::with_capacity(prefix + suffix);
                name.extend_from_slice(&previous[..prefix]);
                name.extend_from_slice(reader.take(suffix)?);
                if prefix != common_prefix_len(previous, &name) {
                    return Err(PackedWireError::Invalid(
                        "group metadata name prefix is not canonical".into(),
                    ));
                }
                validate_name(&name)?;
                if entries.last().is_some_and(|last| last.name >= name) {
                    return Err(PackedWireError::Invalid(
                        "group metadata names are not strictly sorted".into(),
                    ));
                }
                let kind = reader.u8()?;
                let flags = reader.u8()?;
                let mode = reader.u32()?;
                reader.take(8)?; // uid, gid
                let rdev = reader.u64()?;
                let nlink = reader.u32()?;
                reader.take(24)?; // atime, mtime, ctime
                let inode = reader.u64()?;
                let size = reader.u64()?;
                validate_snapshot_hot_attributes(inode, kind, mode, nlink, rdev)?;
                if flags & !INLINE_DATA_FLAG != 0 {
                    return Err(PackedWireError::UnsupportedFormat(
                        "GM07 entry has unknown flags".into(),
                    ));
                }
                let extents = usize::from(reader.u16()?);
                if extents > 256 || (kind != 1 && extents != 0) {
                    return Err(PackedWireError::Invalid(
                        "GM07 directory extent count/kind mismatch".into(),
                    ));
                }
                reader.take(extents * 24)?;
                let inline = reader.u32()? as usize;
                if inline >= INLINE_FILE_MAX_BYTES {
                    return Err(PackedWireError::LimitExceeded(
                        "inline group metadata file exceeds 256 KiB".into(),
                    ));
                }
                if flags & INLINE_DATA_FLAG != 0 {
                    if kind != 1 || extents != 0 || size != inline as u64 {
                        return Err(PackedWireError::Invalid(
                            "inline group metadata size/kind mismatch".into(),
                        ));
                    }
                } else if inline != 0 {
                    return Err(PackedWireError::Invalid(
                        "inline group metadata file is missing its inline flag".into(),
                    ));
                }
                reader.take(inline)?;
                entries.push(GroupDirectoryEntry {
                    name,
                    inode,
                    kind,
                    mode,
                });
            }
            if !reader.is_empty() {
                return Err(PackedWireError::Invalid(
                    "group metadata has trailing bytes".into(),
                ));
            }
        }
        Ok(Self { entries })
    }
}
