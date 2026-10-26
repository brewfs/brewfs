//! Bounded Linux regular-file capture for the 005 producer.
//!
//! This captures one dentry, with visible nlink=1. It does not establish an
//! atomic directory snapshot: production inventory must supply a frozen view
//! or a stronger namespace fence. Stat revalidation detects ordinary mutation,
//! including pathname replacement, but is not a filesystem snapshot protocol.

use std::fs::{File, Metadata};
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::Path;
use std::sync::Arc;

use super::source_root::{V3SourceEntry, V3SourceLocation, V3SourceRoot};
use super::{V3ColdAttributes, V3RootAttributes, V3Xattr};
use crate::workspace_overlay::packed_v3::wire::{PackedResult, PackedWireError};
use crate::workspace_overlay::packed_v3::{
    AccessProfile, GroupMeta, GroupMetaEntry, GroupMetaExtent, PackedFrameInput, PackedGroupInput,
    SizeClassTable,
};

#[derive(Clone, Copy, Debug)]
pub struct V3SourceFileLimits {
    pub max_logical_bytes: u64,
    pub max_data_bytes: usize,
    pub max_extents: usize,
}

impl Default for V3SourceFileLimits {
    fn default() -> Self {
        Self {
            max_logical_bytes: 64 * 1024 * 1024,
            max_data_bytes: 16 * 1024 * 1024,
            max_extents: 256,
        }
    }
}

impl V3SourceFileLimits {
    pub(super) fn validate(self) -> PackedResult<()> {
        if self.max_logical_bytes == 0
            || self.max_logical_bytes > 64 * 1024 * 1024
            || self.max_data_bytes == 0
            || self.max_data_bytes > 16 * 1024 * 1024
            || self.max_extents == 0
            || self.max_extents > 256
        {
            return Err(PackedWireError::Invalid(
                "invalid source capture limits".into(),
            ));
        }
        Ok(())
    }
}

pub struct CapturedV3SourceFile {
    file: File,
    location: V3SourceLocation,
    before: Metadata,
    entry: GroupMetaEntry,
    frames: Vec<PackedFrameInput>,
    cold: V3ColdAttributes,
}

/// Pinned source directory attributes. This is also a stat/path fence, not an
/// atomic namespace snapshot. Capturing a single child does not enumerate it.
pub struct CapturedV3SourceRoot {
    file: File,
    location: V3SourceLocation,
    before: Metadata,
    attributes: V3RootAttributes,
    cold: V3ColdAttributes,
}

impl CapturedV3SourceRoot {
    pub fn capture(path: &Path, inode: u64) -> PackedResult<Self> {
        let root = V3SourceRoot::open(path, super::V3SourceConsistency::BestEffortDetected)?;
        Self::capture_at(root.entry(vec![])?, inode)
    }

    pub(super) fn capture_at(entry: V3SourceEntry, inode: u64) -> PackedResult<Self> {
        let file = entry.open(libc::O_RDONLY | libc::O_DIRECTORY)?;
        let before = file.metadata().map_err(|_| failure("root stat"))?;
        let attributes = V3RootAttributes {
            inode,
            size: before.len(),
            blocks: before.blocks(),
            mode: before.mode(),
            uid: before.uid(),
            gid: before.gid(),
            nlink: u32::try_from(before.nlink()).map_err(|_| failure("root nlink bound"))?,
            atime_ns: ns(before.atime(), before.atime_nsec())?,
            mtime_ns: ns(before.mtime(), before.mtime_nsec())?,
            ctime_ns: ns(before.ctime(), before.ctime_nsec())?,
        };
        attributes.validate()?;
        let cold = capture_xattrs(&file, inode)?;
        let result = Self {
            file,
            location: V3SourceLocation::Rooted(entry),
            before,
            attributes,
            cold,
        };
        result.validate_unchanged()?;
        Ok(result)
    }

    pub fn attributes(&self) -> &V3RootAttributes {
        &self.attributes
    }
    pub fn cold_attributes(&self) -> &V3ColdAttributes {
        &self.cold
    }

    pub fn validate_unchanged(&self) -> PackedResult<()> {
        self.location.validate(&self.file, &self.before)
    }
}

fn failure(what: &str) -> PackedWireError {
    PackedWireError::Backend(format!("wire 005 source {what} failed"))
}

fn token(meta: &Metadata) -> (u64, u64, u64, u32, u32, u32, u64, i64, i64, i64, i64, u64) {
    (
        meta.dev(),
        meta.ino(),
        meta.len(),
        meta.mode(),
        meta.uid(),
        meta.gid(),
        meta.nlink(),
        meta.mtime(),
        meta.mtime_nsec(),
        meta.ctime(),
        meta.ctime_nsec(),
        meta.blocks(),
    )
}

pub(super) fn validate_source_path(
    file: &File,
    path: &Path,
    before: &Metadata,
) -> PackedResult<()> {
    let path = std::fs::symlink_metadata(path).map_err(|_| failure("path revalidation"))?;
    validate_source_metadata(file, &path, before)
}

pub(super) fn validate_source_metadata(
    file: &File,
    path: &Metadata,
    before: &Metadata,
) -> PackedResult<()> {
    let current = file.metadata().map_err(|_| failure("revalidation"))?;
    if token(&current) != token(before) || token(path) != token(before) {
        return Err(PackedWireError::Invalid(
            "wire 005 source changed during capture/publication".into(),
        ));
    }
    Ok(())
}

pub(super) fn ns(seconds: i64, nanos: i64) -> PackedResult<i64> {
    seconds
        .checked_mul(1_000_000_000)
        .and_then(|s| s.checked_add(nanos))
        .ok_or_else(|| PackedWireError::LimitExceeded("wire 005 source timestamp overflows".into()))
}

pub(super) fn seek(file: &File, offset: u64, whence: i32) -> PackedResult<Option<u64>> {
    let offset = i64::try_from(offset)
        .map_err(|_| PackedWireError::LimitExceeded("source offset exceeds off_t".into()))?;
    // SAFETY: the owned file descriptor is live, and offset fits off_t.
    let result = unsafe { libc::lseek(file.as_raw_fd(), offset, whence) };
    if result >= 0 {
        return Ok(Some(result as u64));
    }
    match std::io::Error::last_os_error().raw_os_error() {
        Some(libc::ENXIO) => Ok(None),
        Some(libc::EINVAL | libc::EOPNOTSUPP) => Err(PackedWireError::UnsupportedFormat(
            "source filesystem does not support SEEK_DATA/SEEK_HOLE".into(),
        )),
        _ => Err(failure("extent query")),
    }
}

pub(super) fn capture_xattrs(file: &File, inode: u64) -> PackedResult<V3ColdAttributes> {
    let fd = file.as_raw_fd();
    let cold = capture_xattrs_with(
        inode,
        None,
        // SAFETY: callers pass a live destination or null/zero size probe.
        |buffer, length| unsafe { libc::flistxattr(fd, buffer, length) },
        // SAFETY: the name is NUL terminated and the destination is live.
        |name, buffer, length| unsafe { libc::fgetxattr(fd, name, buffer, length) },
    )?;
    let metadata = file.metadata().map_err(|_| failure("ACL stat"))?;
    cold.validate_for_inode(if metadata.is_dir() { 2 } else { 1 }, metadata.mode())?;
    Ok(cold)
}

/// Metadata-only no-follow capture, including FIFO/socket/device/symlink
/// xattrs. Never opens those nodes for data; the caller supplies stat fences.
pub(super) fn capture_path_xattrs(
    path: &Path,
    inode: u64,
    target: Option<Vec<u8>>,
) -> PackedResult<V3ColdAttributes> {
    let metadata = std::fs::symlink_metadata(path).map_err(|_| failure("ACL path stat"))?;
    let path =
        std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(|_| failure("xattr path"))?;
    let cold = capture_xattrs_with(
        inode,
        target,
        // SAFETY: path is NUL terminated; the destination is live or a probe.
        |buffer, length| unsafe { libc::llistxattr(path.as_ptr(), buffer, length) },
        // SAFETY: both path/name are NUL terminated and the destination lives.
        |name, buffer, length| unsafe { libc::lgetxattr(path.as_ptr(), name, buffer, length) },
    )?;
    cold.validate_for_inode(
        if metadata.is_dir() {
            2
        } else if metadata.file_type().is_symlink() {
            3
        } else {
            1
        },
        metadata.mode(),
    )?;
    Ok(cold)
}

fn capture_xattrs_with(
    inode: u64,
    target: Option<Vec<u8>>,
    list: impl Fn(*mut libc::c_char, usize) -> isize,
    get: impl Fn(*const libc::c_char, *mut libc::c_void, usize) -> isize,
) -> PackedResult<V3ColdAttributes> {
    let size = list(std::ptr::null_mut(), 0);
    if size < 0 {
        return Err(failure("xattr list"));
    }
    if size as usize > 256 * 1024 {
        return Err(PackedWireError::LimitExceeded(
            "source xattr names exceed budget".into(),
        ));
    }
    let mut names = vec![0u8; size as usize];
    let count = list(names.as_mut_ptr().cast(), names.len());
    if count < 0 || count as usize > names.len() {
        return Err(failure("xattr list changed"));
    }
    names.truncate(count as usize);
    if !names.is_empty() && names.last() != Some(&0) {
        return Err(failure("xattr names framing"));
    }
    let mut xattrs = Vec::new();
    let mut bytes = 28usize + target.as_ref().map_or(0, Vec::len);
    for name in names.split(|b| *b == 0).filter(|name| !name.is_empty()) {
        if name.len() > 255 || xattrs.len() >= 1024 {
            return Err(PackedWireError::LimitExceeded(
                "source xattr count/name exceeds budget".into(),
            ));
        }
        let c_name = std::ffi::CString::new(name).map_err(|_| failure("xattr name"))?;
        let length = get(c_name.as_ptr(), std::ptr::null_mut(), 0);
        if length < 0 {
            return Err(failure("xattr size"));
        }
        if length > 65536 {
            return Err(PackedWireError::LimitExceeded(
                "source xattr value exceeds budget".into(),
            ));
        }
        bytes += 6 + name.len() + length as usize;
        if bytes > 256 * 1024 {
            return Err(PackedWireError::LimitExceeded(
                "source cold attributes exceed budget".into(),
            ));
        }
        // Allocate a non-empty buffer so a zero-length value still gets a real
        // read, rather than another size probe during a concurrent mutation.
        let mut value = vec![0u8; (length as usize).max(1)];
        let observed = get(c_name.as_ptr(), value.as_mut_ptr().cast(), value.len());
        if observed != length {
            return Err(failure("xattr value changed"));
        }
        value.truncate(length as usize);
        xattrs.push(V3Xattr {
            name: name.to_vec(),
            value,
        });
    }
    xattrs.sort_by(|a, b| a.name.cmp(&b.name));
    let cold = V3ColdAttributes {
        inode,
        symlink_target: target,
        xattrs,
        acl: Vec::new(),
    };
    cold.encode()?;
    Ok(cold)
}

impl CapturedV3SourceFile {
    pub fn capture(
        path: &Path,
        inode: u64,
        profile: AccessProfile,
        classes: SizeClassTable,
        p90: Option<u64>,
        limits: V3SourceFileLimits,
    ) -> PackedResult<Self> {
        Self::capture_inner(
            V3SourceLocation::Path(path.to_owned()),
            inode,
            profile,
            classes,
            p90,
            limits,
            (super::V3BuildPolicy::default(), false),
        )?
        .ok_or_else(|| PackedWireError::Invalid("bounded source capture lacks a result".into()))
    }

    /// Internal source importer routing. Exceeding a materialization limit
    /// selects external placement before any object upload; other errors keep
    /// their original meaning. Public `capture` retains its rejection policy.
    pub(super) fn capture_for_placement(
        path: &Path,
        inode: u64,
        profile: AccessProfile,
        classes: SizeClassTable,
        limits: V3SourceFileLimits,
        policy: super::V3BuildPolicy,
    ) -> PackedResult<Option<Self>> {
        Self::capture_inner(
            V3SourceLocation::Path(path.to_owned()),
            inode,
            profile,
            classes,
            None,
            limits,
            (policy, true),
        )
    }

    pub(super) fn capture_at_for_placement(
        entry: V3SourceEntry,
        inode: u64,
        profile: AccessProfile,
        classes: SizeClassTable,
        limits: V3SourceFileLimits,
        policy: super::V3BuildPolicy,
    ) -> PackedResult<Option<Self>> {
        Self::capture_inner(
            V3SourceLocation::Rooted(entry),
            inode,
            profile,
            classes,
            None,
            limits,
            (policy, true),
        )
    }

    fn capture_inner(
        location: V3SourceLocation,
        inode: u64,
        profile: AccessProfile,
        classes: SizeClassTable,
        p90: Option<u64>,
        limits: V3SourceFileLimits,
        admission: (super::V3BuildPolicy, bool),
    ) -> PackedResult<Option<Self>> {
        let (policy, allow_external) = admission;
        limits.validate()?;
        let name = location.basename()?;
        super::super::meta::validate_name(&name)?;
        if name.len() > crate::posix::NAME_MAX {
            return Err(PackedWireError::LimitExceeded(
                "source basename exceeds NAME_MAX".into(),
            ));
        }
        // NONBLOCK ensures FIFO input cannot block before fstat rejects it.
        let file = location.open_regular()?;
        let before = file.metadata().map_err(|_| failure("stat"))?;
        if !before.is_file() {
            return Err(PackedWireError::UnsupportedFormat(
                "source capture requires a regular file".into(),
            ));
        }
        if before.len() > limits.max_logical_bytes {
            if allow_external {
                return Ok(None);
            }
            return Err(PackedWireError::LimitExceeded(
                "source logical size exceeds capture budget".into(),
            ));
        }
        super::super::meta::validate_v3_hot_attributes(inode, 1, before.mode(), 1, 0)?;
        if p90.is_some() {
            return Err(PackedWireError::UnsupportedFormat(
                "source p90 policy requires authenticated histogram provenance".into(),
            ));
        }
        let decision = policy.select(before.len(), profile, classes)?;
        let mut frames = Vec::new();
        let mut extents = Vec::new();
        let mut cursor = 0u64;
        let mut data_bytes = 0usize;
        while cursor < before.len() {
            let Some(start) = seek(&file, cursor, libc::SEEK_DATA)? else {
                break;
            };
            if start < cursor || start >= before.len() {
                return Err(failure("data extent bounds"));
            }
            let end = seek(&file, start, libc::SEEK_HOLE)?
                .ok_or_else(|| failure("hole extent"))?
                .min(before.len());
            if end <= start {
                return Err(failure("hole extent bounds"));
            }
            let mut position = start;
            while position < end {
                let length = (end - position).min(decision.frame_raw_bytes) as usize;
                if extents.len() >= limits.max_extents
                    || data_bytes
                        .checked_add(length)
                        .is_none_or(|n| n > limits.max_data_bytes)
                {
                    if allow_external {
                        return Ok(None);
                    }
                    return Err(PackedWireError::LimitExceeded(
                        "source data/extents exceed capture budget".into(),
                    ));
                }
                let mut raw = vec![0; length];
                file.read_exact_at(&mut raw, position)
                    .map_err(|_| failure("payload read"))?;
                extents.push(GroupMetaExtent {
                    file_offset: position,
                    logical_len: length as u32,
                    frame_ordinal: frames.len() as u32,
                    raw_offset: 0,
                    raw_len: length as u32,
                });
                frames.push(PackedFrameInput {
                    raw,
                    size_class: decision.size_class,
                    codec: 0,
                    first_file_slot: 0,
                    last_file_slot: 0,
                });
                data_bytes += length;
                position += length as u64;
            }
            cursor = end;
        }
        let cold = capture_xattrs(&file, inode)?;
        let entry = GroupMetaEntry {
            name,
            inode,
            kind: 1,
            mode: before.mode(),
            uid: before.uid(),
            gid: before.gid(),
            rdev: 0,
            nlink: 1,
            atime_ns: ns(before.atime(), before.atime_nsec())?,
            mtime_ns: ns(before.mtime(), before.mtime_nsec())?,
            ctime_ns: ns(before.ctime(), before.ctime_nsec())?,
            size: before.len(),
            flags: 0,
            inline_data: Arc::from([]),
            extents,
        };
        GroupMeta::new(vec![entry.clone()])?.encode_restart()?;
        let result = Self {
            file,
            location,
            before,
            entry,
            frames,
            cold,
        };
        result.validate_unchanged()?;
        Ok(Some(result))
    }

    pub fn validate_unchanged(&self) -> PackedResult<()> {
        self.location.validate(&self.file, &self.before)
    }

    pub fn entry(&self) -> &GroupMetaEntry {
        &self.entry
    }
    pub fn frames(&self) -> &[PackedFrameInput] {
        &self.frames
    }
    pub fn cold_attributes(&self) -> &V3ColdAttributes {
        &self.cold
    }
    pub fn source_blocks(&self) -> u64 {
        self.before.blocks()
    }
    pub fn source_nlink(&self) -> u64 {
        self.before.nlink()
    }

    pub(super) fn source_metadata(&self) -> &Metadata {
        &self.before
    }

    pub(super) fn into_parts(self) -> (GroupMetaEntry, Vec<PackedFrameInput>, V3ColdAttributes) {
        (self.entry, self.frames, self.cold)
    }

    pub fn group(
        &self,
        group_id: u64,
        parent: [u8; 32],
        profile: AccessProfile,
    ) -> PackedResult<PackedGroupInput> {
        self.validate_unchanged()?;
        Ok(PackedGroupInput {
            group_id,
            parent_dir_key: parent,
            metadata: GroupMeta::new(vec![self.entry.clone()])?.encode()?,
            frame_ordinals: (0..self.frames.len() as u32).collect(),
            entry_count: 1,
            file_count: 1,
            layout_profile: profile,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;
    use std::io::{Seek, SeekFrom, Write};

    fn capture(path: &Path) -> PackedResult<CapturedV3SourceFile> {
        CapturedV3SourceFile::capture(
            path,
            7,
            AccessProfile::RandomSmallFile,
            SizeClassTable::default(),
            None,
            V3SourceFileLimits::default(),
        )
    }

    fn set_past_mtime(path: &Path) {
        // The production fence compares stat tokens. Give the next mutation
        // an observable mtime change even within one filesystem clock tick.
        File::open(path)
            .unwrap()
            .set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(1))
            .unwrap();
    }

    #[test]
    fn source_root_pins_real_attributes_and_detects_namespace_changes_and_replacement() {
        use std::os::unix::fs::PermissionsExt;
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("root");
        std::fs::create_dir(&path).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o750)).unwrap();
        set_past_mtime(&path);
        let meta = std::fs::symlink_metadata(&path).unwrap();
        let captured = CapturedV3SourceRoot::capture(&path, 1).unwrap();
        assert_eq!(
            (
                captured.attributes().size,
                captured.attributes().blocks,
                captured.attributes().mode
            ),
            (meta.len(), meta.blocks(), meta.mode())
        );
        assert_eq!(
            (
                captured.attributes().uid,
                captured.attributes().gid,
                captured.attributes().nlink
            ),
            (meta.uid(), meta.gid(), meta.nlink() as u32)
        );
        captured.validate_unchanged().unwrap();
        std::fs::write(path.join("new-entry"), b"x").unwrap();
        assert_ne!(
            std::fs::symlink_metadata(&path)
                .unwrap()
                .modified()
                .unwrap(),
            meta.modified().unwrap(),
            "namespace mutation must change the fixture's stat token",
        );
        assert!(captured.validate_unchanged().is_err());
        let captured = CapturedV3SourceRoot::capture(&path, 1).unwrap();
        std::fs::rename(&path, temp.path().join("old-root")).unwrap();
        std::fs::create_dir(&path).unwrap();
        assert!(captured.validate_unchanged().is_err());
        let alias = temp.path().join("root-link");
        std::os::unix::fs::symlink(&path, &alias).unwrap();
        assert!(CapturedV3SourceRoot::capture(&alias, 1).is_err());
    }

    #[test]
    fn actual_sparse_file_capture_reads_only_seek_data_and_preserves_eof() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("sparse");
        let mut file = File::create(&path).unwrap();
        file.set_len(8 * 1024 * 1024).unwrap();
        file.seek(SeekFrom::Start(4096)).unwrap();
        file.write_all(b"first").unwrap();
        file.seek(SeekFrom::Start(6 * 1024 * 1024)).unwrap();
        file.write_all(b"last").unwrap();
        file.sync_all().unwrap();
        let captured = capture(&path).unwrap();
        assert_eq!(captured.entry().size, 8 * 1024 * 1024);
        assert!(captured.frames().iter().map(|f| f.raw.len()).sum::<usize>() < 64 * 1024);
        assert_eq!(captured.entry().mode, file.metadata().unwrap().mode());
        assert_eq!(captured.source_blocks(), file.metadata().unwrap().blocks());
        assert!(
            captured
                .entry()
                .extents
                .iter()
                .all(|e| e.file_offset >= 4096)
        );
        let mut logical = vec![0; captured.entry().size as usize];
        for extent in &captured.entry().extents {
            logical[extent.file_offset as usize
                ..extent.file_offset as usize + extent.logical_len as usize]
                .copy_from_slice(&captured.frames()[extent.frame_ordinal as usize].raw);
        }
        assert_eq!(logical, std::fs::read(path).unwrap());
        captured.validate_unchanged().unwrap();
    }

    #[test]
    fn empty_and_all_hole_sources_preserve_size_without_payload() {
        let temp = tempfile::tempdir().unwrap();
        for size in [0, 1024 * 1024] {
            let path = temp.path().join(format!("hole-{size}"));
            File::create(&path).unwrap().set_len(size).unwrap();
            let source = capture(&path).unwrap();
            assert_eq!(source.entry().size, size);
            assert!(source.entry().extents.is_empty());
            assert!(source.frames().is_empty());
        }
    }

    #[test]
    fn source_mutation_replacement_symlink_and_limits_fail_closed() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("file");
        std::fs::write(&path, b"original").unwrap();
        set_past_mtime(&path);
        let source = capture(&path).unwrap();
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .write_all_at(b"changed!", 0)
            .unwrap();
        assert_ne!(
            std::fs::symlink_metadata(&path)
                .unwrap()
                .modified()
                .unwrap(),
            source.source_metadata().modified().unwrap(),
            "content mutation must change the fixture's stat token",
        );
        assert!(source.validate_unchanged().is_err());
        let source = capture(&path).unwrap();
        std::fs::rename(&path, temp.path().join("old")).unwrap();
        std::fs::write(&path, b"changed!").unwrap();
        assert!(source.validate_unchanged().is_err());
        std::os::unix::fs::symlink(&path, temp.path().join("link")).unwrap();
        assert!(capture(&temp.path().join("link")).is_err());
        let limits = V3SourceFileLimits {
            max_data_bytes: 4,
            ..Default::default()
        };
        assert!(
            CapturedV3SourceFile::capture(
                &path,
                7,
                AccessProfile::RandomSmallFile,
                SizeClassTable::default(),
                None,
                limits
            )
            .is_err()
        );
        assert!(capture(temp.path()).is_err());
    }

    #[test]
    fn source_capture_preserves_binary_xattrs_and_explicit_visible_link_policy() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("file");
        std::fs::write(&path, b"contents").unwrap();
        std::fs::hard_link(&path, temp.path().join("alias")).unwrap();
        let file = File::open(&path).unwrap();
        let name = c"user.brewfs.capture";
        let value = [0u8, 255, 7];
        // SAFETY: descriptor and buffers remain live throughout fsetxattr.
        assert_eq!(
            unsafe {
                libc::fsetxattr(
                    file.as_raw_fd(),
                    name.as_ptr(),
                    value.as_ptr().cast(),
                    value.len(),
                    0,
                )
            },
            0
        );
        let source = capture(&path).unwrap();
        assert_eq!(source.source_nlink(), 2);
        assert_eq!(source.entry().nlink, 1);
        assert_eq!(
            source.cold_attributes().xattrs,
            vec![V3Xattr {
                name: name.to_bytes().to_vec(),
                value: value.to_vec()
            }]
        );
    }

    #[tokio::test]
    async fn captured_sparse_source_publishes_and_roundtrips_authenticated_partial_reads() {
        use super::super::{
            AuthenticatedV3Snapshot, V3IndexReader, V3ProducerOptions, V3SnapshotProducer,
        };
        use crate::cadapter::{client::ObjectClient, localfs::LocalFsBackend};
        use crate::workspace_overlay::packed_v3::PackedCodec;
        let source_dir = tempfile::tempdir().unwrap();
        let path = source_dir.path().join("sparse");
        let mut file = File::create(&path).unwrap();
        file.set_len(8 * 1024 * 1024).unwrap();
        file.seek(SeekFrom::Start(4096)).unwrap();
        file.write_all(b"begin").unwrap();
        file.seek(SeekFrom::Start(6 * 1024 * 1024)).unwrap();
        file.write_all(b"end").unwrap();
        file.sync_all().unwrap();
        let source = capture(&path).unwrap();
        let expected = std::fs::read(&path).unwrap();
        for codec in [PackedCodec::Raw, PackedCodec::Zstd] {
            let objects = tempfile::tempdir().unwrap();
            let spool = tempfile::tempdir().unwrap();
            let client = ObjectClient::new(LocalFsBackend::new(objects.path()));
            let options = V3ProducerOptions {
                snapshot_id: [1; 32],
                root_dir_key: [2; 32],
                root_inode: 1,
                profile: AccessProfile::RandomSmallFile,
                size_classes: SizeClassTable::default(),
                build_policy: Default::default(),
                metadata_codec: codec,
                data_codec: codec,
            };
            let mut producer =
                V3SnapshotProducer::new(client.clone(), spool.path(), "capture".into(), options)
                    .await
                    .unwrap();
            producer
                .add_container(
                    1,
                    &[source
                        .group(1, [2; 32], AccessProfile::RandomSmallFile)
                        .unwrap()],
                    source.frames(),
                    &[1],
                )
                .await
                .unwrap();
            producer
                .add_cold_attributes(source.cold_attributes())
                .await
                .unwrap();
            source.validate_unchanged().unwrap();
            let reference = producer.finish().await.unwrap();
            source.validate_unchanged().unwrap();
            let snapshot = AuthenticatedV3Snapshot::open(&client, &reference)
                .await
                .unwrap();
            let reader = V3IndexReader::new(client.clone(), 0);
            for (offset, length) in [
                (0, expected.len()),
                (0, 8192),
                (6 * 1024 * 1024 - 2048, 8192),
                (expected.len() - 1000, 1000),
            ] {
                let mut output = vec![0; length];
                snapshot
                    .read_inode_range(
                        &client,
                        &reader,
                        7,
                        offset as u64,
                        &mut output,
                        32 * 1024 * 1024,
                    )
                    .await
                    .unwrap();
                assert_eq!(output, expected[offset..offset + length]);
            }
            assert_eq!(
                snapshot.cold_attributes(&client, &reader, 7).await.unwrap(),
                Some(source.cold_attributes().clone())
            );
        }
    }
}
