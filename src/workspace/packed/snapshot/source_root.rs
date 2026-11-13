//! Root-FD source access and a verified readonly Btrfs snapshot lease.
//!
//! The provider trusts the source administrator to keep the snapshot readonly.
//! It does not claim to resist a privileged actor who changes mount topology or
//! toggles subvolume flags. Identity/readonly/generation fences detect ordinary
//! changes and provider failures. No ordinary directory or readonly bind mount
//! is promoted to SnapshotBacked.

use sha2::{Digest, Sha256};
use std::ffi::{CString, OsString};
use std::fs::{File, Metadata};
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::MetadataExt;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use super::SnapshotSourceConsistency;
use crate::workspace::packed::wire::{PackedResult, PackedWireError};

pub(super) const MAX_SOURCE_PATH_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct SnapshotSourceProvenance {
    pub consistency: &'static str,
    pub provider: &'static str,
    pub root_device: u64,
    pub root_inode: u64,
    pub root_mount_id: u64,
    pub root_stat_token: String,
    pub filesystem_uuid: Option<String>,
    pub snapshot_uuid: Option<String>,
    pub parent_snapshot_uuid: Option<String>,
    pub subvolume_id: Option<u64>,
    pub generation: Option<u64>,
    pub change_transaction: Option<u64>,
    pub subvolume_flags: Option<u64>,
    pub root_item_flags: Option<u64>,
    pub readonly_guard: &'static str,
    pub trust_boundary: &'static str,
}

fn invalid(message: &str) -> PackedWireError {
    PackedWireError::Invalid(format!("source root {message}"))
}
fn failure(message: &str) -> PackedWireError {
    PackedWireError::Backend(format!("source root {message} failed"))
}
fn unsupported(message: &str) -> PackedWireError {
    PackedWireError::UnsupportedFormat(format!("source root {message}"))
}

fn open_at(parent: &File, name: &[u8], flags: i32) -> PackedResult<File> {
    let name = CString::new(name).map_err(|_| invalid("NUL in component"))?;
    // SAFETY: parent is live; name is NUL terminated; successful fd is owned.
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            flags | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
        )
    };
    if fd < 0 {
        return Err(failure("no-follow openat"));
    }
    Ok(unsafe { File::from_raw_fd(fd) })
}

fn mount_id(file: &File) -> PackedResult<u64> {
    let mut stat: libc::statx = unsafe { std::mem::zeroed() };
    // SAFETY: the descriptor and output live; an empty path addresses the fd.
    let result = unsafe {
        libc::statx(
            file.as_raw_fd(),
            c"".as_ptr(),
            libc::AT_EMPTY_PATH | libc::AT_SYMLINK_NOFOLLOW,
            libc::STATX_MNT_ID,
            &mut stat,
        )
    };
    if result < 0 || stat.stx_mask & libc::STATX_MNT_ID == 0 {
        return Err(unsupported(
            "requires Linux statx mount identity (kernel >= 5.8)",
        ));
    }
    Ok(stat.stx_mnt_id)
}

/// No user-supplied provider callbacks: only the built-in checked provider may
/// issue a frozen lease. A future application-freeze provider must carry a
/// trusted lifetime guard, not a string claiming a directory is frozen.
struct FrozenSourceLease {
    identity: BtrfsSnapshotIdentity,
}

trait FrozenSourceProvider {
    fn acquire(root: &File) -> PackedResult<FrozenSourceLease>;
}
struct ReadonlyBtrfsSnapshotProvider;
impl FrozenSourceProvider for ReadonlyBtrfsSnapshotProvider {
    fn acquire(root: &File) -> PackedResult<FrozenSourceLease> {
        Ok(FrozenSourceLease {
            identity: BtrfsSnapshotIdentity::read(root)?,
        })
    }
}
impl FrozenSourceLease {
    fn validate(&self, root: &File) -> PackedResult<()> {
        if BtrfsSnapshotIdentity::read(root)? != self.identity {
            return Err(invalid(
                "snapshot identity/readonly/generation changed during lease",
            ));
        }
        Ok(())
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct BtrfsTime {
    seconds: u64,
    nanos: u32,
    padding: u32,
}
#[repr(C)]
struct BtrfsSubvolumeInfo {
    tree_id: u64,
    name: [u8; 256],
    parent_id: u64,
    directory_id: u64,
    generation: u64,
    flags: u64,
    uuid: [u8; 16],
    parent_uuid: [u8; 16],
    received_uuid: [u8; 16],
    change_transaction: u64,
    origin_transaction: u64,
    send_transaction: u64,
    receive_transaction: u64,
    times: [BtrfsTime; 4],
    reserved: [u64; 8],
}

const BTRFS_SUBVOLUME_INFO_SIZE: usize = 504;
const _: () = assert!(std::mem::size_of::<BtrfsSubvolumeInfo>() == BTRFS_SUBVOLUME_INFO_SIZE);
fn read_ioctl(number: u32, size: usize) -> libc::c_ulong {
    ((2u32 << 30) | ((size as u32) << 16) | (0x94 << 8) | number) as libc::c_ulong
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BtrfsSnapshotIdentity {
    filesystem_uuid: [u8; 16],
    uuid: [u8; 16],
    parent_uuid: [u8; 16],
    tree_id: u64,
    generation: u64,
    change_transaction: u64,
    flags: u64,
    root_item_flags: u64,
}
impl BtrfsSnapshotIdentity {
    fn read(root: &File) -> PackedResult<Self> {
        let mut fs: libc::statfs = unsafe { std::mem::zeroed() };
        // SAFETY: root descriptor and output are live.
        if unsafe { libc::fstatfs(root.as_raw_fd(), &mut fs) } < 0 {
            return Err(failure("filesystem identity"));
        }
        if fs.f_type as u64 != 0x9123_683e
            || root.metadata().map_err(|_| failure("snapshot stat"))?.ino() != 256
        {
            return Err(unsupported("SnapshotBacked requires a Btrfs snapshot root"));
        }
        let mut info: BtrfsSubvolumeInfo = unsafe { std::mem::zeroed() };
        let mut fs_info = [0u64; 128]; // aligned btrfs_ioctl_fs_info_args (1024B)
        let mut flags = 0u64;
        // ioctl reports failure with -1; a nonnegative command-specific result
        // is successful (GET_SUBVOL_INFO can return a positive result).
        // SAFETY: UAPI structures have fixed verified sizes and aligned storage.
        let ok = unsafe {
            libc::ioctl(root.as_raw_fd(), read_ioctl(60, 504), &mut info) >= 0
                && libc::ioctl(root.as_raw_fd(), read_ioctl(31, 1024), fs_info.as_mut_ptr()) >= 0
                && libc::ioctl(root.as_raw_fd(), read_ioctl(25, 8), &mut flags) >= 0
        };
        if !ok {
            return Err(unsupported(
                "Btrfs snapshot identity ioctls are unavailable",
            ));
        }
        // GETFLAGS uses BTRFS_SUBVOL_RDONLY (1 << 1), whereas GET_SUBVOL_INFO
        // exposes on-disk BTRFS_ROOT_SUBVOL_RDONLY (1 << 0). They are distinct
        // UAPI flag domains and must never be compared for numeric equality.
        if flags & 2 == 0
            || info.flags & 1 == 0
            || info.uuid == [0; 16]
            || info.parent_uuid == [0; 16]
            || info.tree_id == 0
        {
            return Err(unsupported(
                "requires a readonly snapshot with a parent UUID",
            ));
        }
        // FSID lives at byte offset 16 in the 1024B Linux UAPI structure.
        let filesystem_uuid =
            unsafe { std::slice::from_raw_parts(fs_info.as_ptr().cast::<u8>().add(16), 16) }
                .try_into()
                .unwrap();
        Ok(Self {
            filesystem_uuid,
            uuid: info.uuid,
            parent_uuid: info.parent_uuid,
            tree_id: info.tree_id,
            generation: info.generation,
            change_transaction: info.change_transaction,
            flags,
            root_item_flags: info.flags,
        })
    }
}

pub(super) struct SnapshotSourceRoot {
    file: Arc<File>,
    anchor_parent: File,
    anchor_name: Vec<u8>,
    before: Metadata,
    lease: Option<FrozenSourceLease>,
    provenance: SnapshotSourceProvenance,
}

impl SnapshotSourceRoot {
    pub(super) fn open(
        path: &Path,
        consistency: SnapshotSourceConsistency,
    ) -> PackedResult<Arc<Self>> {
        let absolute = if path.is_absolute() {
            path.to_owned()
        } else {
            std::env::current_dir()
                .map_err(|_| failure("working directory"))?
                .join(path)
        };
        if absolute.as_os_str().as_bytes().len() > MAX_SOURCE_PATH_BYTES {
            return Err(PackedWireError::LimitExceeded(
                "source root exceeds 64KiB budget".into(),
            ));
        }
        let mut names = Vec::new();
        for component in absolute.components() {
            match component {
                Component::RootDir | Component::CurDir => {}
                Component::Normal(name) => names.push(name.as_bytes().to_vec()),
                _ => return Err(invalid("root path must not contain parent traversal")),
            }
        }
        let mut parent = File::open("/").map_err(|_| failure("filesystem root open"))?;
        let final_name = names.pop().unwrap_or_else(|| b".".to_vec());
        for name in names {
            parent = open_at(&parent, &name, libc::O_RDONLY | libc::O_DIRECTORY)?;
        }
        let file = open_at(&parent, &final_name, libc::O_RDONLY | libc::O_DIRECTORY)?;
        let before = file.metadata().map_err(|_| failure("root stat"))?;
        let root_mount_id = mount_id(&file)?;
        let lease = match consistency {
            SnapshotSourceConsistency::BestEffortDetected => None,
            SnapshotSourceConsistency::SnapshotBacked => {
                Some(ReadonlyBtrfsSnapshotProvider::acquire(&file)?)
            }
        };
        let id = lease.as_ref().map(|l| &l.identity);
        let provenance = SnapshotSourceProvenance {
            consistency: consistency.as_str(),
            provider: if id.is_some() {
                "linux-btrfs-readonly-snapshot"
            } else {
                "linux-root-fd-stat-fences"
            },
            root_device: before.dev(),
            root_inode: before.ino(),
            root_mount_id,
            root_stat_token: hex::encode(super::source_namespace::source_token(&before)),
            filesystem_uuid: id.map(|i| hex::encode(i.filesystem_uuid)),
            snapshot_uuid: id.map(|i| hex::encode(i.uuid)),
            parent_snapshot_uuid: id.map(|i| hex::encode(i.parent_uuid)),
            subvolume_id: id.map(|i| i.tree_id),
            generation: id.map(|i| i.generation),
            change_transaction: id.map(|i| i.change_transaction),
            subvolume_flags: id.map(|i| i.flags),
            root_item_flags: id.map(|i| i.root_item_flags),
            readonly_guard: if id.is_some() {
                "Btrfs readonly/UUID/FSID/generation/change-transaction checked through final manifest fence"
            } else {
                "stat fences only; no atomic frozen view"
            },
            trust_boundary: "source administrator and mount namespace are trusted; privileged flag/topology changes are outside the freeze protocol",
        };
        let root = Arc::new(Self {
            file: Arc::new(file),
            anchor_parent: parent,
            anchor_name: final_name,
            before,
            lease,
            provenance,
        });
        root.validate_unchanged()?;
        Ok(root)
    }

    pub(super) fn entry(self: &Arc<Self>, relative: Vec<u8>) -> PackedResult<SnapshotSourceEntry> {
        if relative.len() > MAX_SOURCE_PATH_BYTES {
            return Err(PackedWireError::LimitExceeded(
                "source relative path exceeds 64KiB budget".into(),
            ));
        }
        if !relative.is_empty() {
            for name in relative.split(|b| *b == b'/') {
                super::super::meta::validate_name(name)?;
                if name.len() > crate::posix::NAME_MAX {
                    return Err(invalid("component exceeds NAME_MAX"));
                }
            }
        }
        Ok(SnapshotSourceEntry {
            root: self.clone(),
            relative,
        })
    }
    pub(super) fn validate_unchanged(&self) -> PackedResult<()> {
        let anchor = open_at(
            &self.anchor_parent,
            &self.anchor_name,
            libc::O_PATH | libc::O_DIRECTORY,
        )?;
        super::source_file::validate_source_metadata(
            &self.file,
            &anchor.metadata().map_err(|_| failure("anchor stat"))?,
            &self.before,
        )?;
        self.check_boundary(&anchor)?;
        if let Some(lease) = &self.lease {
            lease.validate(&self.file)?;
        }
        Ok(())
    }
    fn check_boundary(&self, file: &File) -> PackedResult<()> {
        let metadata = file.metadata().map_err(|_| failure("entry stat"))?;
        if metadata.dev() != self.before.dev() || mount_id(file)? != self.provenance.root_mount_id {
            return Err(unsupported(
                "nested mount/subvolume crosses the leased root",
            ));
        }
        Ok(())
    }
    pub(super) fn provenance(&self) -> SnapshotSourceProvenance {
        self.provenance.clone()
    }
    /// Encode actual checked FD/provider identity, never caller-writable
    /// reporting provenance. The final importer fence is the only consumer.
    pub(super) fn checked_fence_digest(&self) -> PackedResult<[u8; 32]> {
        self.validate_unchanged()?;
        let mut hash = Sha256::new();
        hash.update(b"BrewFS packed v3 checked root provider\0");
        hash.update(super::source_namespace::source_token(&self.before));
        hash.update(mount_id(&self.file)?.to_le_bytes());
        hash.update([u8::from(self.lease.is_some())]);
        if let Some(lease) = &self.lease {
            // validate_unchanged above performed real readonly snapshot ioctls.
            let id = &lease.identity;
            hash.update(id.filesystem_uuid);
            hash.update(id.uuid);
            hash.update(id.parent_uuid);
            for field in [
                id.tree_id,
                id.generation,
                id.change_transaction,
                id.flags,
                id.root_item_flags,
            ] {
                hash.update(field.to_le_bytes());
            }
        }
        Ok(hash.finalize().into())
    }
    pub(super) fn contains_directory(&self, path: &Path) -> PackedResult<bool> {
        let mut file = std::fs::File::open(path).map_err(|_| failure("spool directory open"))?;
        loop {
            let current = file
                .metadata()
                .map_err(|_| failure("spool ancestor stat"))?;
            if (current.dev(), current.ino()) == (self.before.dev(), self.before.ino()) {
                return Ok(true);
            }
            let parent = open_at(&file, b"..", libc::O_RDONLY | libc::O_DIRECTORY)?;
            let above = parent
                .metadata()
                .map_err(|_| failure("spool ancestor stat"))?;
            if (current.dev(), current.ino()) == (above.dev(), above.ino()) {
                return Ok(false);
            }
            file = parent;
        }
    }
}

#[derive(Clone)]
pub(super) struct SnapshotSourceEntry {
    root: Arc<SnapshotSourceRoot>,
    relative: Vec<u8>,
}
impl SnapshotSourceEntry {
    pub(super) fn open(&self, flags: i32) -> PackedResult<File> {
        self.root.validate_unchanged()?;
        if self.relative.is_empty() {
            return open_at(&self.root.file, b".", flags);
        }
        let (parent, name) = self.parent_and_name()?;
        let file = open_at(&parent, &name, flags)?;
        self.root.check_boundary(&file)?;
        Ok(file)
    }
    fn parent_and_name(&self) -> PackedResult<(File, Vec<u8>)> {
        self.root.validate_unchanged()?;
        let mut parent = self
            .root
            .file
            .try_clone()
            .map_err(|_| failure("root descriptor clone"))?;
        let mut components = self.relative.split(|b| *b == b'/').peekable();
        while let Some(name) = components.next() {
            if components.peek().is_none() {
                return Ok((parent, name.to_vec()));
            }
            parent = open_at(&parent, name, libc::O_RDONLY | libc::O_DIRECTORY)?;
            self.root.check_boundary(&parent)?;
        }
        Err(invalid("empty relative entry"))
    }
    pub(super) fn metadata(&self) -> PackedResult<Metadata> {
        self.open(libc::O_PATH)?
            .metadata()
            .map_err(|_| failure("entry stat"))
    }
    pub(super) fn read_link(&self) -> PackedResult<Vec<u8>> {
        let (parent, name) = self.parent_and_name()?;
        let name = CString::new(name).map_err(|_| invalid("link name"))?;
        let mut bytes = vec![0u8; MAX_SOURCE_PATH_BYTES + 1];
        // SAFETY: live parent, NUL terminated name, bounded live output.
        let length = unsafe {
            libc::readlinkat(
                parent.as_raw_fd(),
                name.as_ptr(),
                bytes.as_mut_ptr().cast(),
                bytes.len(),
            )
        };
        if length < 0 {
            return Err(failure("readlinkat"));
        }
        if length as usize > MAX_SOURCE_PATH_BYTES {
            return Err(invalid("symlink target exceeds budget"));
        }
        bytes.truncate(length as usize);
        Ok(bytes)
    }
    pub(super) fn capture_xattrs(
        &self,
        inode: u64,
        target: Option<Vec<u8>>,
    ) -> PackedResult<super::SnapshotColdAttributes> {
        let metadata = self.metadata()?;
        if metadata.is_file() || metadata.is_dir() {
            let flags = libc::O_RDONLY
                | if metadata.is_dir() {
                    libc::O_DIRECTORY
                } else {
                    0
                };
            return super::source_file::capture_xattrs(&self.open(flags)?, inode);
        }
        // Linux l*xattr has no dirfd form on older supported kernels. Keep the
        // parent descriptor live and pass only ONE <= NAME_MAX component via
        // procfs. This never reconstructs a deep full pathname, never follows
        // the final symlink, and never opens FIFO/socket/device payloads.
        let (parent, name) = self.parent_and_name()?;
        let short = PathBuf::from(format!("/proc/self/fd/{}", parent.as_raw_fd()))
            .join(OsString::from_vec(name));
        let cold = super::source_file::capture_path_xattrs(&short, inode, target)?;
        drop(parent);
        Ok(cold)
    }
    pub(super) fn basename(&self) -> PackedResult<Vec<u8>> {
        self.relative
            .rsplit(|b| *b == b'/')
            .next()
            .filter(|name| !name.is_empty())
            .map(|name| name.to_vec())
            .ok_or_else(|| invalid("entry basename"))
    }
    pub(super) fn read_dir(&self) -> PackedResult<SnapshotSourceDirectory> {
        let file = self.open(libc::O_RDONLY | libc::O_DIRECTORY)?;
        let fd = file.into_raw_fd();
        // SAFETY: owned directory descriptor transferred only on success.
        let directory = unsafe { libc::fdopendir(fd) };
        if directory.is_null() {
            unsafe { libc::close(fd) };
            return Err(failure("fdopendir"));
        }
        Ok(SnapshotSourceDirectory { directory })
    }
}

pub(super) struct SnapshotSourceDirectory {
    directory: *mut libc::DIR,
}
// A directory iterator has exclusive ownership and is moved, never shared.
unsafe impl Send for SnapshotSourceDirectory {}
impl SnapshotSourceDirectory {
    pub(super) fn next_name(&mut self) -> PackedResult<Option<Vec<u8>>> {
        loop {
            // SAFETY: exclusively owned live DIR; errno is thread local.
            let item = unsafe {
                *libc::__errno_location() = 0;
                libc::readdir(self.directory)
            };
            if item.is_null() {
                return if unsafe { *libc::__errno_location() } == 0 {
                    Ok(None)
                } else {
                    Err(failure("readdir"))
                };
            }
            let name = unsafe { std::ffi::CStr::from_ptr((*item).d_name.as_ptr()) }.to_bytes();
            if name == b"." || name == b".." {
                continue;
            }
            super::super::meta::validate_name(name)?;
            if name.len() > crate::posix::NAME_MAX {
                return Err(invalid("directory name exceeds NAME_MAX"));
            }
            return Ok(Some(name.to_vec()));
        }
    }
}
impl Drop for SnapshotSourceDirectory {
    fn drop(&mut self) {
        unsafe { libc::closedir(self.directory) };
    }
}

#[derive(Clone)]
pub(super) enum SnapshotSourceLocation {
    Path(PathBuf),
    Rooted(SnapshotSourceEntry),
}
impl SnapshotSourceLocation {
    pub(super) fn basename(&self) -> PackedResult<Vec<u8>> {
        match self {
            Self::Path(path) => path
                .file_name()
                .map(|n| n.as_bytes().to_vec())
                .ok_or_else(|| invalid("basename")),
            Self::Rooted(entry) => entry.basename(),
        }
    }
    pub(super) fn open_regular(&self) -> PackedResult<File> {
        use std::os::unix::fs::OpenOptionsExt;
        match self {
            Self::Path(path) => std::fs::OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC)
                .open(path)
                .map_err(|_| failure("regular file open")),
            Self::Rooted(entry) => entry.open(libc::O_RDONLY),
        }
    }
    pub(super) fn validate(&self, file: &File, before: &Metadata) -> PackedResult<()> {
        match self {
            Self::Path(path) => super::source_file::validate_source_path(file, path, before),
            Self::Rooted(entry) => {
                super::source_file::validate_source_metadata(file, &entry.metadata()?, before)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_entry_rejects_intermediate_symlink_escape_and_root_replacement() {
        let source = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::create_dir(source.path().join("root")).unwrap();
        std::fs::create_dir(source.path().join("root/top")).unwrap();
        std::fs::create_dir(source.path().join("root/top/parent")).unwrap();
        std::fs::write(source.path().join("root/top/parent/file"), b"original").unwrap();
        std::fs::write(outside.path().join("file"), b"outside").unwrap();
        let root_path = source.path().join("root");
        let root =
            SnapshotSourceRoot::open(&root_path, SnapshotSourceConsistency::BestEffortDetected)
                .unwrap();
        let entry = root.entry(b"top/parent/file".to_vec()).unwrap();
        std::fs::rename(
            root_path.join("top/parent"),
            root_path.join("top/old-parent"),
        )
        .unwrap();
        std::os::unix::fs::symlink(outside.path(), root_path.join("top/parent")).unwrap();
        assert!(entry.open(libc::O_RDONLY).is_err());
        assert!(entry.capture_xattrs(1, None).is_err());
        std::fs::rename(&root_path, source.path().join("old-root")).unwrap();
        std::fs::create_dir(&root_path).unwrap();
        assert!(root.validate_unchanged().is_err());
    }

    #[test]
    #[ignore = "requires an owned Linux Btrfs readonly snapshot and admin flag-change permission"]
    fn source_btrfs_lease_rejects_revoked_readonly_guard() {
        let path = PathBuf::from(
            std::env::var_os("BREWFS_TEST_BTRFS_SNAPSHOT").expect("owned readonly snapshot"),
        );
        let root =
            SnapshotSourceRoot::open(&path, SnapshotSourceConsistency::SnapshotBacked).unwrap();
        struct RestoreReadonly(PathBuf);
        impl Drop for RestoreReadonly {
            fn drop(&mut self) {
                let status = std::process::Command::new("btrfs")
                    .args(["property", "set", "-ts"])
                    .arg(&self.0)
                    .args(["ro", "true"])
                    .status();
                assert!(
                    status.is_ok_and(|status| status.success()),
                    "owned snapshot readonly restoration failed"
                );
            }
        }
        let restore = RestoreReadonly(path.clone());
        let status = std::process::Command::new("btrfs")
            .args(["property", "set", "-ts"])
            .arg(&path)
            .args(["ro", "false"])
            .status()
            .unwrap();
        assert!(status.success());
        assert!(root.validate_unchanged().is_err());
        assert!(
            root.entry(b"frozen".to_vec())
                .unwrap()
                .open(libc::O_RDONLY)
                .is_err()
        );
        drop(restore);
    }
}
