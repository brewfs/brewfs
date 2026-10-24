//! Resolve clone sources from kernel descriptor identity, never pathname suffixes.

use std::fs::File;
use std::io::{self, Read};
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

use asyncfuse::{Errno, Result};

const MAX_MOUNTINFO_BYTES: u64 = 2 << 20;

fn read_proc(path: impl AsRef<Path>, limit: u64) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(limit + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "proc file too large",
        ));
    }
    Ok(bytes)
}

struct FuseMount<'a> {
    id: u64,
    device: (u32, u32),
    path: &'a [u8],
}

fn fuse_mounts(bytes: &[u8]) -> impl Iterator<Item = FuseMount<'_>> {
    bytes.split(|byte| *byte == b'\n').filter_map(|line| {
        let separator = line.windows(3).position(|part| part == b" - ")?;
        let fs_type = line[separator + 3..].split(|byte| *byte == b' ').next()?;
        if fs_type != b"fuse" && !fs_type.starts_with(b"fuse.") {
            return None;
        }
        let mut fields = line[..separator].split(|byte| *byte == b' ');
        let id = std::str::from_utf8(fields.next()?).ok()?.parse().ok()?;
        fields.next()?; // parent mount ID
        let device = std::str::from_utf8(fields.next()?).ok()?;
        let (major, minor) = device.split_once(':')?;
        fields.next()?; // root within the filesystem (may be a bind mount)
        Some(FuseMount {
            id,
            device: (major.parse().ok()?, minor.parse().ok()?),
            path: fields.next()?,
        })
    })
}

/// Called only after mounting, with a path canonicalized before mounting. Do not
/// stat our own FUSE filesystem from an ioctl callback: that can deadlock its
/// request workers. The kernel's mount table needs no FUSE requests.
pub(super) fn mounted_device(mount_point: &Path) -> io::Result<(u32, u32)> {
    let mut escaped = Vec::new();
    for byte in mount_point.as_os_str().as_bytes() {
        match byte {
            b' ' => escaped.extend_from_slice(b"\\040"),
            b'\t' => escaped.extend_from_slice(b"\\011"),
            b'\n' => escaped.extend_from_slice(b"\\012"),
            b'\\' => escaped.extend_from_slice(b"\\134"),
            byte => escaped.push(*byte),
        }
    }
    let mounts = read_proc("/proc/self/mountinfo", MAX_MOUNTINFO_BYTES)?;
    let mut matches = fuse_mounts(&mounts).filter(|mount| mount.path == escaped);
    match (matches.next(), matches.next()) {
        (Some(mount), None) => Ok(mount.device),
        _ => Err(io::Error::other(
            "cannot identify the mounted BrewFS filesystem",
        )),
    }
}

pub(crate) struct SourceFile {
    pub(crate) ino: i64,
    // O_PATH pins the kernel inode without opening a new FUSE handle (which
    // would recursively enter our worker pool). The data path separately pins
    // and validates the existing readable VFS handle it actually uses.
    _file: File,
}

pub(super) fn source_file(pid: u32, fd: i64, device: (u32, u32)) -> Result<SourceFile> {
    let fd = i32::try_from(fd)
        .ok()
        .filter(|fd| *fd >= 0)
        .ok_or_else(|| Errno::from(libc::EBADF))?;
    let file = File::options()
        .read(true)
        .custom_flags(libc::O_PATH | libc::O_CLOEXEC)
        .open(format!("/proc/{pid}/fd/{fd}"))
        .map_err(|_| Errno::from(libc::EBADF))?;
    let pinned = read_proc(format!("/proc/self/fdinfo/{}", file.as_raw_fd()), 4096)
        .map_err(|_| Errno::from(libc::EBADF))?;
    let (mount_id, ino, _) = fd_identity(&pinned)?;
    let original = read_proc(format!("/proc/{pid}/fdinfo/{fd}"), 4096)
        .map_err(|_| Errno::from(libc::EBADF))?;
    let (original_mount, original_ino, flags) = fd_identity(&original)?;
    // The caller may have closed/reused its descriptor while we pinned it.
    // Validate read authority only for the inode our owned reference pins.
    if (mount_id, ino) != (original_mount, original_ino) {
        return Err(libc::EBADF.into());
    }
    let access = flags & libc::O_ACCMODE as u32;
    if flags & libc::O_PATH as u32 != 0
        || (access != libc::O_RDONLY as u32 && access != libc::O_RDWR as u32)
    {
        return Err(libc::EBADF.into());
    }

    // The owned reference retains the original mount, including a bind mount
    // visible only in the caller's namespace. Inspect that namespace's table.
    let mounts = read_proc(format!("/proc/{pid}/mountinfo"), MAX_MOUNTINFO_BYTES)
        .map_err(|_| Errno::from(libc::EXDEV))?;
    if !fuse_mounts(&mounts).any(|mount| mount.id == mount_id && mount.device == device) {
        return Err(libc::EXDEV.into());
    }
    Ok(SourceFile { ino, _file: file })
}

fn fd_identity(info: &[u8]) -> Result<(u64, i64, u32)> {
    let info = std::str::from_utf8(info).map_err(|_| Errno::from(libc::EBADF))?;
    let field = |key| {
        info.lines()
            .find_map(|line| line.strip_prefix(key))
            .map(str::trim)
    };
    let flags = field("flags:")
        .and_then(|value| u32::from_str_radix(value, 8).ok())
        .ok_or_else(|| Errno::from(libc::EBADF))?;
    let mount_id: u64 = field("mnt_id:")
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| Errno::from(libc::EXDEV))?;
    let ino: i64 = field("ino:")
        .and_then(|value| value.parse().ok())
        .filter(|ino| *ino > 0)
        .ok_or_else(|| Errno::from(libc::EBADF))?;

    Ok((mount_id, ino, flags))
}
