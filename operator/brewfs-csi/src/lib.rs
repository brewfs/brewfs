//! Shared CSI state and BrewFS volume identity handling.
//!
//! CSI volumes refer to existing, operator-provisioned BrewFS workspaces;
//! dynamic provisioning is unsupported. A workspace is an operator-owned
//! resource, and its ID is carried by an immutable CSI volume ID.
//! The node side is the only component that starts
//! a BrewFS process; that process acquires the normal BrewFS sole-writer lease.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
#[cfg(target_os = "linux")]
use std::ffi::CString;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::fd::{AsRawFd, FromRawFd};
#[cfg(target_os = "linux")]
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Mutex;
use uuid::Uuid;

pub mod proto {
    tonic::include_proto!("csi.v1");
}

pub const DRIVER_NAME: &str = "csi.brewfs.io";
pub const VOLUME_SCHEME: &str = "brewfs";
const DEFAULT_BINARY: &str = "/usr/local/bin/brewfs";
const DEFAULT_MOUNT_ROOT: &str = "/var/lib/brewfs-csi/mounts";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumeId {
    pub cluster: String,
    pub workspace: Uuid,
}

impl VolumeId {
    pub fn new(cluster: impl Into<String>, workspace: Uuid) -> Result<Self, VolumeIdError> {
        let cluster = cluster.into();
        validate_component("cluster", &cluster)?;
        Ok(Self { cluster, workspace })
    }

    pub fn parse(raw: &str) -> Result<Self, VolumeIdError> {
        let prefix = format!("{VOLUME_SCHEME}://");
        let value = raw
            .strip_prefix(&prefix)
            .ok_or_else(|| VolumeIdError::Scheme(raw.to_owned()))?;
        let (cluster, workspace) = value
            .split_once('/')
            .ok_or_else(|| VolumeIdError::Shape(raw.to_owned()))?;
        if workspace.contains('/') || cluster.is_empty() || workspace.is_empty() {
            return Err(VolumeIdError::Shape(raw.to_owned()));
        }
        let workspace = Uuid::parse_str(workspace)
            .map_err(|_| VolumeIdError::Workspace(workspace.to_owned()))?;
        Self::new(cluster.to_owned(), workspace)
    }

    pub fn as_string(&self) -> String {
        format!("{VOLUME_SCHEME}://{}/{}", self.cluster, self.workspace)
    }
}

fn validate_component(name: &'static str, value: &str) -> Result<(), VolumeIdError> {
    if value.is_empty()
        || value.len() > 253
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
    {
        return Err(VolumeIdError::Component {
            name,
            value: value.to_owned(),
        });
    }
    Ok(())
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum VolumeIdError {
    #[error("volume ID is not a {VOLUME_SCHEME} URI: {0}")]
    Scheme(String),
    #[error("volume ID has invalid shape: {0}")]
    Shape(String),
    #[error("volume ID has invalid workspace UUID: {0}")]
    Workspace(String),
    #[error("volume ID has invalid {name} component: {value}")]
    Component { name: &'static str, value: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MountSpec {
    pub volume: VolumeId,
    pub config: PathBuf,
    pub binary: PathBuf,
    pub mount_root: PathBuf,
    pub read_only: bool,
}

impl MountSpec {
    pub fn from_context(volume_id: &str, context: &[(String, String)]) -> Result<Self, CsiError> {
        let volume = VolumeId::parse(volume_id).map_err(CsiError::InvalidVolumeId)?;
        let value = |key: &str| {
            context
                .iter()
                .find(|(name, _)| name == key)
                .map(|(_, value)| value.as_str())
        };
        let config = value("config")
            .or_else(|| value("brewfs.config"))
            .filter(|path| !path.is_empty())
            .ok_or_else(|| CsiError::InvalidContext("config is required".to_owned()))?;
        let binary = value("binary")
            .or_else(|| value("brewfs.binary"))
            .filter(|path| !path.is_empty())
            .unwrap_or(DEFAULT_BINARY);
        let mount_root = value("mount_root")
            .or_else(|| value("brewfs.mount_root"))
            .filter(|path| !path.is_empty())
            .unwrap_or(DEFAULT_MOUNT_ROOT);
        let read_only = value("read_only")
            .or_else(|| value("readonly"))
            .is_some_and(|value| matches!(value, "1" | "true" | "TRUE" | "yes" | "YES"));
        for (name, path) in [
            ("config", config),
            ("binary", binary),
            ("mount_root", mount_root),
        ] {
            if !Path::new(path).is_absolute() {
                return Err(CsiError::InvalidContext(format!(
                    "{name} must be an absolute path"
                )));
            }
        }
        Ok(Self {
            volume,
            config: PathBuf::from(config),
            binary: PathBuf::from(binary),
            mount_root: PathBuf::from(mount_root),
            read_only,
        })
    }

    pub fn shared_mount(&self) -> PathBuf {
        let mut digest = Sha256::new();
        digest.update(self.volume.as_string().as_bytes());
        self.mount_root.join(hex::encode(digest.finalize()))
    }
}

fn state_key(volume: &VolumeId) -> String {
    let mut digest = Sha256::new();
    digest.update(volume.as_string().as_bytes());
    hex::encode(digest.finalize())
}

#[derive(Debug, Error)]
pub enum CsiError {
    #[error("invalid volume ID: {0}")]
    InvalidVolumeId(#[source] VolumeIdError),
    #[error("invalid volume context: {0}")]
    InvalidContext(String),
    #[error("workspace is busy: {0}")]
    Busy(String),
    #[error("workspace is still published at: {0}")]
    Published(String),
    #[error("mount ownership or mode cannot be verified: {0}")]
    MountConflict(String),
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("mount command failed: {0}")]
    MountCommand(String),
}

impl From<VolumeIdError> for CsiError {
    fn from(error: VolumeIdError) -> Self {
        Self::InvalidVolumeId(error)
    }
}

/// Kernel identity of a mount. Bind mounts share the device, root, filesystem
/// type and source, but have independent read-only flags and mount IDs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MountInfo {
    /// Identifies this particular mount in the current namespace. It is not
    /// persisted: a plugin restart may enter a different mount namespace.
    #[serde(skip)]
    pub mount_id: u64,
    /// Keep the mount alive so Linux cannot recycle its ID before unbind's
    /// final validation. Persisted state contains filesystem identity only.
    #[serde(skip)]
    anchor: Option<UnmountAnchor>,
    pub device: String,
    pub root: String,
    pub fs_type: String,
    pub source: String,
    pub read_only: bool,
}

/// File descriptors retained from the validated target.  The target fd is
/// used to open a mount fd with the new mount API; the parent/name pair is
/// used for quarantine and cleanup without resolving the target pathname
/// again.
#[derive(Debug, Clone)]
struct UnmountAnchor {
    target: Arc<File>,
    parent: Option<Arc<File>>,
    name: Option<std::ffi::OsString>,
}

impl PartialEq for MountInfo {
    fn eq(&self, other: &Self) -> bool {
        self.mount_id == other.mount_id
            && self.same_filesystem_root(other)
            && self.read_only == other.read_only
    }
}

impl Eq for MountInfo {}

impl MountInfo {
    fn same_filesystem_root(&self, other: &Self) -> bool {
        self.device == other.device
            && self.root == other.root
            && self.fs_type == other.fs_type
            && self.source == other.source
    }

    fn is_brewfs(&self) -> bool {
        (self.fs_type == "fuse" || self.fs_type.starts_with("fuse."))
            && matches!(
                self.source.as_str(),
                "brewfs" | "brewfs-entity-experimental"
            )
    }
}

/// The small set of host operations needed by the node plugin.  Keeping this
/// boundary explicit makes all idempotency behavior unit-testable without
/// mounting FUSE in a test process.
pub trait MountOps: Send + Sync + 'static {
    fn is_mounted(&self, path: &Path) -> io::Result<bool>;
    fn inspect_mount(&self, path: &Path) -> io::Result<Option<MountInfo>>;
    fn process_alive(&self, pid: u32) -> io::Result<bool>;
    fn start_brewfs(&self, spec: &MountSpec, target: &Path) -> io::Result<u32>;
    fn bind(&self, source: &Path, target: &Path, read_only: bool) -> io::Result<()>;
    /// Refuse a replacement of the inspected mount at the unmount boundary.
    fn unbind(&self, target: &Path, expected: MountInfo) -> io::Result<()>;
    fn unmount_brewfs(&self, target: &Path) -> io::Result<()>;
}

impl<T: MountOps> MountOps for Arc<T> {
    fn is_mounted(&self, path: &Path) -> io::Result<bool> {
        (**self).is_mounted(path)
    }

    fn inspect_mount(&self, path: &Path) -> io::Result<Option<MountInfo>> {
        (**self).inspect_mount(path)
    }

    fn process_alive(&self, pid: u32) -> io::Result<bool> {
        (**self).process_alive(pid)
    }

    fn start_brewfs(&self, spec: &MountSpec, target: &Path) -> io::Result<u32> {
        (**self).start_brewfs(spec, target)
    }

    fn bind(&self, source: &Path, target: &Path, read_only: bool) -> io::Result<()> {
        (**self).bind(source, target, read_only)
    }

    fn unbind(&self, target: &Path, expected: MountInfo) -> io::Result<()> {
        (**self).unbind(target, expected)
    }

    fn unmount_brewfs(&self, target: &Path) -> io::Result<()> {
        (**self).unmount_brewfs(target)
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SystemMountOps;

impl MountOps for SystemMountOps {
    fn is_mounted(&self, path: &Path) -> io::Result<bool> {
        let status = Command::new("mountpoint").arg("-q").arg(path).status()?;
        Ok(status.success())
    }

    fn inspect_mount(&self, path: &Path) -> io::Result<Option<MountInfo>> {
        let anchor = match open_mount_anchor(path) {
            Ok(anchor) => anchor,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let fd = anchor.target.as_raw_fd();
        let path = fs::read_link(format!("/proc/self/fd/{fd}"))?;
        // Pin inspection to the visible mount. A path match alone can select
        // an owned mount hidden beneath a foreign mount at it or an ancestor.
        let fdinfo = fs::read_to_string(format!("/proc/self/fdinfo/{fd}"))?;
        let mount_id = fdinfo
            .lines()
            .find_map(|line| line.strip_prefix("mnt_id:"))
            .and_then(|id| id.trim().parse().ok())
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "missing mount ID in fdinfo")
            })?;
        let mount = mount_at(
            &fs::read_to_string("/proc/self/mountinfo")?,
            &path,
            mount_id,
        )?;
        Ok(mount.map(|mut mount| {
            mount.anchor = Some(anchor);
            mount
        }))
    }

    fn process_alive(&self, pid: u32) -> io::Result<bool> {
        let command_line = match fs::read(Path::new("/proc").join(pid.to_string()).join("cmdline"))
        {
            Ok(command_line) => command_line,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error),
        };
        Ok(command_line
            .split(|byte| *byte == 0)
            .next()
            .is_some_and(|command| command.ends_with(b"/brewfs") || command == b"brewfs"))
    }

    fn start_brewfs(&self, spec: &MountSpec, target: &Path) -> io::Result<u32> {
        fs::create_dir_all(target)?;
        let mut child = Command::new(&spec.binary)
            .arg("mount")
            .arg("--config")
            .arg(&spec.config)
            .arg("--workspace")
            .arg(spec.volume.workspace.to_string())
            .arg("--privileged")
            .arg(target)
            .spawn()
            .map_err(|error| io::Error::new(error.kind(), format!("start BrewFS: {error}")))?;
        // NodeStageVolume must not return until the mount is visible to the
        // host. Otherwise a retry can start a second FUSE process while the
        // first process is still negotiating its lease.
        for _ in 0..300 {
            if self.is_mounted(target)? {
                // The mount process owns the FUSE session and therefore the
                // workspace lease. It intentionally outlives this request.
                let pid = child.id();
                std::mem::forget(child);
                return Ok(pid);
            }
            if let Some(status) = child.try_wait()? {
                return Err(io::Error::other(format!(
                    "BrewFS exited before mounting {target:?}: {status}"
                )));
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        let _ = child.kill();
        // Reap the child before the final mount check so it cannot finish
        // mounting after cleanup. kill() alone neither waits nor unmounts.
        child.wait()?;
        if self.is_mounted(target)? {
            self.unmount_brewfs(target)?;
        }
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            format!("BrewFS mount did not become visible at {target:?}"),
        ))
    }

    fn bind(&self, source: &Path, target: &Path, read_only: bool) -> io::Result<()> {
        fs::create_dir_all(target)?;
        run_command(Command::new("mount").arg("--bind").arg(source).arg(target))?;
        if read_only {
            run_command(
                Command::new("mount")
                    .arg("-o")
                    .arg("remount,bind,ro")
                    .arg(target),
            )?;
        }
        Ok(())
    }

    #[cfg(target_os = "linux")]
    fn unbind(&self, target: &Path, mut expected: MountInfo) -> io::Result<()> {
        let anchor = expected.anchor.take().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "unmount requires a retained mount observation",
            )
        })?;
        let UnmountAnchor {
            target: target_fd,
            parent,
            name,
        } = anchor;
        let (parent, name) = match (parent.as_ref(), name.as_ref()) {
            (Some(parent), Some(name)) => (parent, name),
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "verified unmount requires a parent directory",
                ));
            }
        };
        let target_name = CString::new(name.as_bytes())
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;

        // Convert the retained target fd into a mount fd.  The mount fd is a
        // reference to the exact mount object observed by inspect_mount, not
        // a pathname that can be redirected by a later overmount.
        let target_path = CString::new(format!("/proc/self/fd/{}", target_fd.as_raw_fd()))
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        let mount_fd = unsafe {
            libc::syscall(
                libc::SYS_open_tree,
                libc::AT_FDCWD,
                target_path.as_ptr(),
                libc::OPEN_TREE_CLOEXEC,
            )
        } as libc::c_int;
        if mount_fd < 0 {
            return Err(io::Error::last_os_error());
        }
        let mount_fd = unsafe { File::from_raw_fd(mount_fd) };

        // Re-check after opening the mount fd.  A replacement that happened
        // before open_tree is rejected; a replacement after this check is
        // harmless because the mount fd below remains anchored to expected.
        if self.inspect_mount(target)?.as_ref() != Some(&expected) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "target mount changed before unmount",
            ));
        }

        let parent_fd = parent.as_raw_fd();
        let mut quarantine = None;
        for attempt in 0..64u32 {
            let candidate = format!(
                ".brewfs-csi-unmount-{}-{}-{attempt}",
                std::process::id(),
                mount_fd.as_raw_fd()
            );
            let candidate = CString::new(candidate).expect("quarantine name has no NUL");
            let result = unsafe { libc::mkdirat(parent_fd, candidate.as_ptr(), 0o700) };
            if result == 0 {
                quarantine = Some(candidate);
                break;
            }
            let error = io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::EEXIST) {
                return Err(error);
            }
        }
        let quarantine = quarantine.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::AlreadyExists,
                "could not allocate an unmount quarantine path",
            )
        })?;

        // Move the exact mount into a private, uniquely-created directory
        // before invoking the test hook or doing any unmount.  Once this
        // succeeds, a replacement at `target` cannot affect the mount being
        // removed.
        let moved = unsafe {
            libc::syscall(
                libc::SYS_move_mount,
                mount_fd.as_raw_fd(),
                c"".as_ptr(),
                parent_fd,
                quarantine.as_ptr(),
                libc::MOVE_MOUNT_F_EMPTY_PATH,
            )
        } as libc::c_int;
        if moved < 0 {
            let error = io::Error::last_os_error();
            let _ = unsafe { libc::unlinkat(parent_fd, quarantine.as_ptr(), libc::AT_REMOVEDIR) };
            return Err(error);
        }

        // The test hook runs after the exact mount has left the request path.
        // In production there is no check-to-unmount pathname window: the
        // mount fd is already quarantined, and umount2 addresses that mount
        // object rather than `target`.
        #[cfg(test)]
        tests::run_before_unmount_syscall();

        // Release the descriptors that would otherwise make a normal
        // unmount report EBUSY.  An independent caller's open handle still
        // correctly keeps the mount busy.
        drop(target_fd);
        drop(mount_fd);
        let quarantine_path = CString::new(format!(
            "/proc/self/fd/{parent_fd}/{}",
            quarantine.to_string_lossy()
        ))
        .expect("quarantine path has no NUL");
        if unsafe { libc::umount2(quarantine_path.as_ptr(), 0) } != 0 {
            let error = io::Error::last_os_error();
            // Restore the mount to its original location when the normal
            // unmount was refused (for example, because another process still
            // holds a handle).  Never stack it over a replacement that became
            // visible while it was quarantined.
            if matches!(self.inspect_mount(target), Ok(None)) {
                let restore_fd = unsafe {
                    libc::syscall(
                        libc::SYS_open_tree,
                        libc::AT_FDCWD,
                        quarantine_path.as_ptr(),
                        libc::OPEN_TREE_CLOEXEC,
                    )
                } as libc::c_int;
                if restore_fd >= 0 {
                    let restore_fd = unsafe { File::from_raw_fd(restore_fd) };
                    let restored = unsafe {
                        libc::syscall(
                            libc::SYS_move_mount,
                            restore_fd.as_raw_fd(),
                            c"".as_ptr(),
                            parent_fd,
                            target_name.as_ptr(),
                            libc::MOVE_MOUNT_F_EMPTY_PATH,
                        )
                    } as libc::c_int;
                    if restored == 0 {
                        let _ = unsafe {
                            libc::unlinkat(parent_fd, quarantine.as_ptr(), libc::AT_REMOVEDIR)
                        };
                    }
                }
            }
            return Err(error);
        }

        let result = unsafe { libc::unlinkat(parent_fd, quarantine.as_ptr(), libc::AT_REMOVEDIR) };
        if result != 0 {
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::NotFound {
                return Err(error);
            }
        }
        // The original target directory is still the directory opened in
        // `anchor`; remove it without following any replacement path.
        let result = unsafe { libc::unlinkat(parent_fd, target_name.as_ptr(), libc::AT_REMOVEDIR) };
        if result != 0 {
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::NotFound {
                return Err(error);
            }
        }
        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    fn unbind(&self, _target: &Path, _expected: MountInfo) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "verified unmount requires Linux",
        ))
    }

    fn unmount_brewfs(&self, target: &Path) -> io::Result<()> {
        let mount = self
            .inspect_mount(target)?
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "mount disappeared"))?;
        self.unbind(target, mount)
    }
}

#[cfg(target_os = "linux")]
fn open_mount_anchor(path: &Path) -> io::Result<UnmountAnchor> {
    use std::os::unix::ffi::OsStrExt;
    use std::path::Component;

    if !path.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "mount target must be absolute",
        ));
    }
    // `Path::components` normalizes `.` away, so inspect the original bytes
    // first.  Accepting a lexical alias would let an unmount request validate
    // one path spelling and then operate on another object.
    if path
        .as_os_str()
        .as_bytes()
        .split(|byte| *byte == b'/')
        .any(|component| component == b"." || component == b"..")
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "mount target aliases are not accepted",
        ));
    }
    let components: Vec<_> = path.components().collect();
    if components.len() == 1 && components[0] == Component::RootDir {
        let root = openat_path(
            None,
            std::ffi::OsStr::new("/"),
            libc::O_PATH | libc::O_DIRECTORY,
        )?;
        return Ok(UnmountAnchor {
            target: Arc::new(root),
            parent: None,
            name: None,
        });
    }
    let mut normal = Vec::new();
    for component in components {
        match component {
            Component::RootDir => {}
            Component::Normal(value) => normal.push(value.to_owned()),
            Component::CurDir | Component::ParentDir => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "mount target aliases are not accepted",
                ));
            }
            Component::Prefix(_) => unreachable!("prefix on Unix"),
        }
    }
    let name = normal
        .pop()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "empty mount target"))?;
    let mut parent = openat_path(
        None,
        std::ffi::OsStr::new("/"),
        libc::O_PATH | libc::O_DIRECTORY,
    )?;
    for component in normal {
        parent = openat_path(Some(&parent), &component, libc::O_PATH | libc::O_DIRECTORY)?;
    }
    let target = openat_path(Some(&parent), &name, libc::O_PATH)?;
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    if unsafe { libc::fstat(target.as_raw_fd(), stat.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let stat = unsafe { stat.assume_init() };
    if stat.st_mode & libc::S_IFMT != libc::S_IFDIR {
        return Err(io::Error::from_raw_os_error(libc::ENOTDIR));
    }
    Ok(UnmountAnchor {
        target: Arc::new(target),
        parent: Some(Arc::new(parent)),
        name: Some(name),
    })
}

#[cfg(target_os = "linux")]
fn openat_path(parent: Option<&File>, name: &std::ffi::OsStr, flags: i32) -> io::Result<File> {
    let name = CString::new(name.as_bytes())
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    let dirfd = parent.map_or(libc::AT_FDCWD, AsRawFd::as_raw_fd);
    let fd = unsafe {
        libc::openat(
            dirfd,
            name.as_ptr(),
            flags | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { File::from_raw_fd(fd) })
}

fn run_command(command: &mut Command) -> io::Result<()> {
    let output = command.output()?;
    if output.status.success() {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "{}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        )))
    }
}

fn mount_at(mountinfo: &str, path: &Path, visible_mount_id: u64) -> io::Result<Option<MountInfo>> {
    let invalid = || io::Error::new(io::ErrorKind::InvalidData, "invalid mountinfo");
    for line in mountinfo.lines() {
        let (mount, filesystem) = line.split_once(" - ").ok_or_else(invalid)?;
        let mount: Vec<_> = mount.split(' ').collect();
        let filesystem: Vec<_> = filesystem.split(' ').collect();
        if mount.len() < 6 || filesystem.len() < 3 {
            return Err(invalid());
        }
        if mount[0].parse::<u64>().map_err(|_| invalid())? != visible_mount_id {
            continue;
        }
        if Path::new(&unescape_mount_field(mount[4])?) != path {
            // The descriptor belongs to an ancestor mount: this directory is
            // not itself a mountpoint, even if mountinfo lists hidden mounts here.
            return Ok(None);
        }
        let mount_ro = mount_read_only(mount[5])?;
        let filesystem_ro = mount_read_only(filesystem[2])?;
        return Ok(Some(MountInfo {
            mount_id: visible_mount_id,
            anchor: None,
            device: mount[2].to_owned(),
            root: unescape_mount_field(mount[3])?,
            fs_type: filesystem[0].to_owned(),
            source: unescape_mount_field(filesystem[1])?,
            read_only: mount_ro || filesystem_ro,
        }));
    }
    Err(io::Error::other(format!(
        "mount {visible_mount_id} disappeared while inspecting {path:?}"
    )))
}

fn mount_read_only(options: &str) -> io::Result<bool> {
    let ro = options.split(',').any(|option| option == "ro");
    let rw = options.split(',').any(|option| option == "rw");
    if ro == rw {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid mount mode",
        ));
    }
    Ok(ro)
}

fn unescape_mount_field(field: &str) -> io::Result<String> {
    let mut bytes = field.bytes();
    let mut decoded = Vec::new();
    while let Some(byte) = bytes.next() {
        decoded.push(if byte == b'\\' {
            match (bytes.next(), bytes.next(), bytes.next()) {
                (Some(b'0'), Some(b'4'), Some(b'0')) => b' ',
                (Some(b'0'), Some(b'1'), Some(b'1')) => b'\t',
                (Some(b'0'), Some(b'1'), Some(b'2')) => b'\n',
                (Some(b'1'), Some(b'3'), Some(b'4')) => b'\\',
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "invalid mount escape",
                    ));
                }
            }
        } else {
            byte
        });
    }
    String::from_utf8(decoded).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct MountState {
    volume_id: String,
    shared_mount: String,
    config: String,
    binary: String,
    mount_root: String,
    read_only: bool,
    /// PID of the lease-owning BrewFS process when started by this plugin.
    /// `None` permits reading legacy state, but cannot authenticate a live mount.
    #[serde(default)]
    pid: Option<u32>,
    #[serde(default)]
    targets: BTreeSet<String>,
    /// Captured after starting BrewFS; never inferred from an existing target.
    #[serde(default)]
    shared_identity: Option<MountInfo>,
    #[serde(default)]
    target_read_only: BTreeMap<String, bool>,
}

/// Node-side state manager.  The shared mount path is derived only from the
/// volume ID, so every actor on a node reuses one FUSE process and one BrewFS
/// lease.  State is written atomically and survives CSI plugin restarts.
#[derive(Debug, Clone)]
pub struct MountManager<O = SystemMountOps> {
    ops: Arc<O>,
    lock: Arc<Mutex<()>>,
    state_root: PathBuf,
}

struct StageOutcome {
    shared: PathBuf,
    state: MountState,
    started: bool,
    previous_state: Option<Vec<u8>>,
}

impl<O: MountOps> MountManager<O> {
    pub fn new(ops: O) -> Self {
        Self::new_with_root(ops, PathBuf::from(DEFAULT_MOUNT_ROOT))
    }

    pub fn new_with_root(ops: O, state_root: PathBuf) -> Self {
        Self {
            ops: Arc::new(ops),
            lock: Arc::new(Mutex::new(())),
            state_root,
        }
    }

    pub async fn stage(&self, spec: &MountSpec) -> Result<PathBuf, CsiError> {
        let _guard = self.lock.lock().await;
        Ok(self.stage_locked(spec, true)?.shared)
    }

    pub async fn publish(
        &self,
        spec: &MountSpec,
        target: &Path,
        read_only: bool,
    ) -> Result<(), CsiError> {
        let _guard = self.lock.lock().await;
        let mut state = self.load_state(spec)?;
        let read_only = read_only || spec.read_only;
        if let Some(mount) = self.ops.inspect_mount(target)? {
            // Reject existing foreign targets before stage can start BrewFS,
            // create directories, or persist any state. An owned retry only
            // needs to validate and record the existing bind mount.
            self.verify_target(spec, &state, target, &mount, read_only)?;
        } else {
            let outcome = self.stage_locked(spec, false)?;
            let shared = outcome.shared;
            let started = outcome.started;
            let previous_state = outcome.previous_state;
            state = outcome.state;
            let mut bound = None;
            let result = (|| {
                self.ops.bind(&shared, target, read_only)?;
                let mount = self.ops.inspect_mount(target)?.ok_or_else(|| {
                    CsiError::MountConflict(format!("bind mount missing at {target:?}"))
                })?;
                bound = Some(mount.clone());
                self.verify_target(spec, &state, target, &mount, read_only)
            })();
            if let Err(error) = result {
                self.rollback_publish(
                    spec,
                    &shared,
                    target,
                    bound,
                    started,
                    previous_state.as_deref(),
                );
                return Err(error);
            }
        }
        state.targets.insert(target.display().to_string());
        state
            .target_read_only
            .insert(target.display().to_string(), read_only);
        self.store_state(spec, &state)
    }

    fn rollback_publish(
        &self,
        spec: &MountSpec,
        shared: &Path,
        target: &Path,
        bound: Option<MountInfo>,
        started: bool,
        previous_state: Option<&[u8]>,
    ) {
        if let Some(bound) = bound
            && self.ops.inspect_mount(target).ok().flatten().as_ref() == Some(&bound)
        {
            let _ = self.ops.unbind(target, bound);
        }
        if started && self.ops.is_mounted(shared).unwrap_or(false) {
            let _ = self.ops.unmount_brewfs(shared);
            if previous_state.is_none() {
                let _ = fs::remove_dir(shared);
                let _ = fs::remove_dir(&spec.mount_root);
                let _ = fs::remove_file(self.state_path(spec));
            } else {
                let _ = fs::write(self.state_path(spec), previous_state.unwrap());
            }
        }
    }

    pub async fn unpublish(&self, spec: &MountSpec, target: &Path) -> Result<(), CsiError> {
        let _guard = self.lock.lock().await;
        self.unpublish_locked(spec, target)
    }

    pub async fn unpublish_by_id(&self, volume_id: &str, target: &Path) -> Result<(), CsiError> {
        let _guard = self.lock.lock().await;
        let volume = VolumeId::parse(volume_id).map_err(CsiError::InvalidVolumeId)?;
        let probe = MountSpec {
            volume,
            config: PathBuf::new(),
            binary: PathBuf::new(),
            mount_root: self.state_root.clone(),
            read_only: false,
        };
        if !self.state_path(&probe).exists() {
            if self.ops.inspect_mount(target)?.is_some() {
                return Err(CsiError::MountConflict(format!(
                    "no volume state for {target:?}"
                )));
            }
            return Ok(());
        }
        let spec = self.spec_from_state(volume_id)?;
        self.unpublish_locked(&spec, target)
    }

    fn unpublish_locked(&self, spec: &MountSpec, target: &Path) -> Result<(), CsiError> {
        let mut state = self.load_state(spec)?;
        if let Some(mount) = self.ops.inspect_mount(target)? {
            let read_only = state
                .target_read_only
                .get(&target.display().to_string())
                .copied()
                .ok_or_else(|| {
                    CsiError::MountConflict(format!("no recorded target mode for {target:?}"))
                })?;
            self.verify_target(spec, &state, target, &mount, read_only)?;
            self.ops.unbind(target, mount)?;
        }
        state.targets.remove(&target.display().to_string());
        state.target_read_only.remove(&target.display().to_string());
        self.store_state(spec, &state)
    }

    fn verify_shared(&self, spec: &MountSpec, state: &MountState) -> Result<MountInfo, CsiError> {
        let shared = spec.shared_mount();
        let mount = self.ops.inspect_mount(&shared)?;
        match (mount, &state.shared_identity) {
            (Some(mount), Some(recorded))
                if mount.is_brewfs()
                    && mount.same_filesystem_root(recorded)
                    && mount.read_only == recorded.read_only
                    && state.volume_id == spec.volume.as_string()
                    && Path::new(&state.shared_mount) == shared =>
            {
                Ok(mount)
            }
            _ => Err(CsiError::MountConflict(format!(
                "shared mount changed or has no recorded identity at {shared:?}"
            ))),
        }
    }

    fn verify_target(
        &self,
        spec: &MountSpec,
        state: &MountState,
        target: &Path,
        mount: &MountInfo,
        read_only: bool,
    ) -> Result<(), CsiError> {
        let shared = self.verify_shared(spec, state)?;
        if mount.mount_id == shared.mount_id {
            return Err(CsiError::MountConflict(format!(
                "target aliases the shared mount at {target:?}"
            )));
        }
        if !mount.same_filesystem_root(&shared) || mount.read_only != read_only {
            return Err(CsiError::MountConflict(format!(
                "foreign source or incorrect read-only mode at {target:?}"
            )));
        }
        Ok(())
    }

    pub async fn unstage(&self, spec: &MountSpec) -> Result<(), CsiError> {
        let _guard = self.lock.lock().await;
        let state = self.load_state(spec)?;
        if !state.targets.is_empty() {
            return Err(CsiError::Published(
                state.targets.iter().cloned().collect::<Vec<_>>().join(", "),
            ));
        }
        let shared = spec.shared_mount();
        if self.ops.is_mounted(&shared)? {
            self.ops.unmount_brewfs(&shared)?;
        }
        let path = self.state_path(spec);
        match fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(CsiError::Io(error)),
        }
    }

    pub async fn unstage_by_id(&self, volume_id: &str) -> Result<(), CsiError> {
        let _guard = self.lock.lock().await;
        let volume = VolumeId::parse(volume_id).map_err(CsiError::InvalidVolumeId)?;
        let probe = MountSpec {
            volume,
            config: PathBuf::new(),
            binary: PathBuf::new(),
            mount_root: self.state_root.clone(),
            read_only: false,
        };
        if !self.state_path(&probe).exists() {
            return Ok(());
        }
        let spec = self.spec_from_state(volume_id)?;
        let state = self.load_state(&spec)?;
        if !state.targets.is_empty() {
            return Err(CsiError::Published(
                state.targets.iter().cloned().collect::<Vec<_>>().join(", "),
            ));
        }
        let shared = spec.shared_mount();
        if self.ops.is_mounted(&shared)? {
            self.ops.unmount_brewfs(&shared)?;
        }
        match fs::remove_file(self.state_path(&spec)) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(CsiError::Io(error)),
        }
    }

    fn stage_locked(&self, spec: &MountSpec, persist: bool) -> Result<StageOutcome, CsiError> {
        fs::create_dir_all(&spec.mount_root)?;
        let shared = spec.shared_mount();
        let previous_state = fs::read(self.state_path(spec)).ok();
        let mut state = self.load_state(spec)?;
        let mut mounted = self.ops.is_mounted(&shared)?;
        let mut started = false;
        if mounted && state.pid.is_none() {
            return Err(CsiError::MountCommand(format!(
                "cannot reuse mount at {shared:?} without a recorded BrewFS PID; unmount it before retrying"
            )));
        }
        if mounted {
            // A recorded PID alone cannot authenticate a replaced shared mount.
            self.verify_shared(spec, &state)?;
        }
        let stale = match state.pid {
            Some(pid) => mounted && !self.ops.process_alive(pid)?,
            None => false,
        };
        if stale {
            // The previous plugin process left a stale mount behind. Remove
            // only the mount owned by this volume before reacquiring the
            // workspace lease; a live PID is always reused.
            self.ops.unmount_brewfs(&shared)?;
            mounted = false;
            state = MountState::default();
        }
        if !mounted {
            // A state file without a live mount is stale after a process crash.
            // Replacing it is safe: the new BrewFS process must reacquire the
            // same workspace lease before serving any request.
            let pid = self.ops.start_brewfs(spec, &shared)?;
            started = true;
            state = MountState::default();
            state.pid = Some(pid);
            let mount = self
                .ops
                .inspect_mount(&shared)?
                .filter(MountInfo::is_brewfs)
                .ok_or_else(|| CsiError::MountConflict(format!("no BrewFS mount at {shared:?}")))?;
            state.shared_identity = Some(mount);
        }
        state.volume_id = spec.volume.as_string();
        state.shared_mount = shared.display().to_string();
        state.config = spec.config.display().to_string();
        state.binary = spec.binary.display().to_string();
        state.mount_root = spec.mount_root.display().to_string();
        state.read_only = spec.read_only;
        if persist {
            self.store_state(spec, &state)?;
        }
        Ok(StageOutcome {
            shared,
            state,
            started,
            previous_state,
        })
    }

    fn spec_from_state(&self, volume_id: &str) -> Result<MountSpec, CsiError> {
        let volume = VolumeId::parse(volume_id).map_err(CsiError::InvalidVolumeId)?;
        // The state file name is independent of the context and can be found
        // only after reading the plugin's configured mount roots.  The node
        // service supplies the same root through the volume context on stage;
        // unpublish/unstage use the persisted absolute paths below.
        let probe = MountSpec {
            volume: volume.clone(),
            config: PathBuf::new(),
            binary: PathBuf::new(),
            mount_root: self.state_root.clone(),
            read_only: false,
        };
        if let Ok(state) = self.load_state(&probe) {
            return Ok(MountSpec {
                volume,
                config: PathBuf::from(state.config),
                binary: PathBuf::from(state.binary),
                mount_root: PathBuf::from(state.mount_root),
                read_only: state.read_only,
            });
        }
        Err(CsiError::InvalidContext(
            "volume has not been staged or its state was lost".to_owned(),
        ))
    }

    fn load_state(&self, spec: &MountSpec) -> Result<MountState, CsiError> {
        match fs::read(self.state_path(spec)) {
            Ok(data) => {
                serde_json::from_slice(&data).map_err(|error| CsiError::Io(io::Error::other(error)))
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(MountState::default()),
            Err(error) => Err(CsiError::Io(error)),
        }
    }

    fn store_state(&self, spec: &MountSpec, state: &MountState) -> Result<(), CsiError> {
        fs::create_dir_all(&self.state_root)?;
        let path = self.state_path(spec);
        let tmp = path.with_extension("json.tmp");
        let data = serde_json::to_vec_pretty(state)
            .map_err(|error| CsiError::Io(io::Error::other(error)))?;
        fs::write(&tmp, data)?;
        fs::rename(tmp, path)?;
        Ok(())
    }

    fn state_path(&self, spec: &MountSpec) -> PathBuf {
        self.state_root
            .join(format!("{}.json", state_key(&spec.volume)))
    }
}

/// Acquire an advisory lock for a shared state directory.  This is used by
/// the server before constructing a manager and prevents two node-plugin
/// processes from starting independent FUSE writers for the same node.
pub fn lock_state_dir(path: &Path) -> io::Result<File> {
    fs::create_dir_all(path)?;
    let lock = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(path.join(".lock"))?;
    fs2::FileExt::lock_exclusive(&lock)?;
    Ok(lock)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;
    use tempfile::TempDir;

    thread_local! {
        static BEFORE_UNMOUNT_SYSCALL: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
    }

    pub(super) fn run_before_unmount_syscall() {
        BEFORE_UNMOUNT_SYSCALL.with(|hook| {
            if let Some(hook) = hook.borrow_mut().take() {
                hook();
            }
        });
    }

    #[derive(Debug, Default)]
    struct FakeOps {
        mounted: StdMutex<BTreeSet<String>>,
        binds: StdMutex<BTreeSet<String>>,
        starts: StdMutex<usize>,
        unmounts: StdMutex<usize>,
        identities: StdMutex<BTreeMap<String, MountInfo>>,
        inspection_errors: StdMutex<BTreeSet<PathBuf>>,
        unbinds: StdMutex<usize>,
        replace_on_unbind: StdMutex<Option<(PathBuf, MountInfo)>>,
        next_mount_id: StdMutex<u64>,
        corrupt_bind_mode: StdMutex<bool>,
    }

    impl FakeOps {
        fn allocate_mount_id(&self) -> u64 {
            let mut id = self.next_mount_id.lock().unwrap();
            *id += 1;
            *id
        }
    }

    impl MountOps for FakeOps {
        fn inspect_mount(&self, path: &Path) -> io::Result<Option<MountInfo>> {
            if self.inspection_errors.lock().unwrap().contains(path) {
                return Err(io::Error::other("mount inspection failed"));
            }
            if !self.is_mounted(path)? {
                return Ok(None);
            }
            Ok(Some(
                self.identities
                    .lock()
                    .unwrap()
                    .get(&path.display().to_string())
                    .cloned()
                    .unwrap_or(MountInfo {
                        mount_id: 0,
                        anchor: None,
                        device: "8:1".to_owned(),
                        root: "/".to_owned(),
                        fs_type: "ext4".to_owned(),
                        source: "/dev/sda1".to_owned(),
                        read_only: false,
                    }),
            ))
        }

        fn is_mounted(&self, path: &Path) -> io::Result<bool> {
            Ok(self
                .mounted
                .lock()
                .unwrap()
                .contains(&path.display().to_string())
                || self
                    .binds
                    .lock()
                    .unwrap()
                    .contains(&path.display().to_string()))
        }

        fn process_alive(&self, _pid: u32) -> io::Result<bool> {
            Ok(true)
        }

        fn start_brewfs(&self, _spec: &MountSpec, target: &Path) -> io::Result<u32> {
            *self.starts.lock().unwrap() += 1;
            self.mounted
                .lock()
                .unwrap()
                .insert(target.display().to_string());
            self.identities.lock().unwrap().insert(
                target.display().to_string(),
                MountInfo {
                    mount_id: self.allocate_mount_id(),
                    anchor: None,
                    device: format!("0:{}", *self.starts.lock().unwrap()),
                    root: "/".to_owned(),
                    fs_type: "fuse".to_owned(),
                    source: "brewfs".to_owned(),
                    read_only: false,
                },
            );
            Ok(42)
        }

        fn bind(&self, source: &Path, target: &Path, read_only: bool) -> io::Result<()> {
            let mut mount = self
                .inspect_mount(source)?
                .ok_or_else(|| io::Error::other("source is not mounted"))?;
            mount.read_only = read_only;
            if *self.corrupt_bind_mode.lock().unwrap() {
                mount.read_only = !read_only;
            }
            mount.mount_id = self.allocate_mount_id();
            self.identities
                .lock()
                .unwrap()
                .insert(target.display().to_string(), mount);
            self.binds
                .lock()
                .unwrap()
                .insert(target.display().to_string());
            Ok(())
        }

        fn unbind(&self, target: &Path, expected: MountInfo) -> io::Result<()> {
            if let Some((path, foreign)) = self.replace_on_unbind.lock().unwrap().take() {
                assert_eq!(path, target);
                self.identities
                    .lock()
                    .unwrap()
                    .insert(path.display().to_string(), foreign);
            }
            if self.inspect_mount(target)?.as_ref() != Some(&expected) {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "target mount changed before unmount",
                ));
            }
            *self.unbinds.lock().unwrap() += 1;
            self.binds
                .lock()
                .unwrap()
                .remove(&target.display().to_string());
            Ok(())
        }

        fn unmount_brewfs(&self, target: &Path) -> io::Result<()> {
            *self.unmounts.lock().unwrap() += 1;
            self.mounted
                .lock()
                .unwrap()
                .remove(&target.display().to_string());
            Ok(())
        }
    }

    fn spec(temp: &TempDir) -> MountSpec {
        MountSpec {
            volume: VolumeId::new("cluster-a", Uuid::from_u128(7)).unwrap(),
            config: temp.path().join("mount.yaml"),
            binary: PathBuf::from("brewfs"),
            mount_root: temp.path().join("mounts"),
            read_only: false,
        }
    }

    #[test]
    fn volume_ids_round_trip_and_reject_ambiguous_values() {
        let id = VolumeId::new("cluster-a", Uuid::from_u128(7)).unwrap();
        assert_eq!(VolumeId::parse(&id.as_string()).unwrap(), id);
        assert!(matches!(
            VolumeId::parse("other://cluster/00000000-0000-0000-0000-000000000007"),
            Err(VolumeIdError::Scheme(_))
        ));
        assert!(VolumeId::parse("brewfs://cluster/not-a-uuid").is_err());
        assert!(VolumeId::parse("brewfs://cluster/a/b").is_err());
    }

    #[tokio::test]
    async fn stage_refuses_untracked_mount() {
        // Cover both a timeout with no state and legacy state without a PID.
        for legacy_state in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let ops = Arc::new(FakeOps::default());
            let manager = MountManager::new_with_root(ops.clone(), temp.path().join("state"));
            let spec = spec(&temp);
            let shared = spec.shared_mount();
            ops.mounted
                .lock()
                .unwrap()
                .insert(shared.display().to_string());
            if legacy_state {
                manager.store_state(&spec, &MountState::default()).unwrap();
            }
            let previous_state = fs::read(manager.state_path(&spec)).ok();

            assert!(matches!(
                manager.stage(&spec).await,
                Err(CsiError::MountCommand(_))
            ));
            assert_eq!(*ops.starts.lock().unwrap(), 0);
            assert_eq!(*ops.unmounts.lock().unwrap(), 0);
            assert!(ops.is_mounted(&shared).unwrap());
            assert_eq!(fs::read(manager.state_path(&spec)).ok(), previous_state);
        }
    }

    #[tokio::test]
    async fn stage_reuses_tracked_mount_after_restart() {
        let temp = tempfile::tempdir().unwrap();
        let ops = Arc::new(FakeOps::default());
        let state_root = temp.path().join("state");
        let spec = spec(&temp);
        MountManager::new_with_root(ops.clone(), state_root.clone())
            .stage(&spec)
            .await
            .unwrap();
        MountManager::new_with_root(ops.clone(), state_root)
            .stage(&spec)
            .await
            .unwrap();
        assert_eq!(*ops.starts.lock().unwrap(), 1);
        assert_eq!(*ops.unmounts.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn stage_publish_and_unpublish_are_idempotent() {
        let temp = tempfile::tempdir().unwrap();
        let ops = Arc::new(FakeOps::default());
        let manager = MountManager::new_with_root(ops.clone(), temp.path().join("state"));
        let spec = spec(&temp);
        let target = temp.path().join("target");

        manager.stage(&spec).await.unwrap();
        manager.stage(&spec).await.unwrap();
        assert_eq!(*ops.starts.lock().unwrap(), 1);

        manager.publish(&spec, &target, false).await.unwrap();
        manager.publish(&spec, &target, false).await.unwrap();
        manager.unpublish(&spec, &target).await.unwrap();
        manager.unpublish(&spec, &target).await.unwrap();
        manager.unstage(&spec).await.unwrap();
        manager.unstage(&spec).await.unwrap();
        assert_eq!(*ops.unmounts.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn ownership_self_review_failed_verification_rolls_back() {
        let temp = tempfile::tempdir().unwrap();
        let ops = Arc::new(FakeOps::default());
        *ops.corrupt_bind_mode.lock().unwrap() = true;
        let manager = MountManager::new_with_root(ops.clone(), temp.path().join("state"));
        let spec = spec(&temp);
        let target = temp.path().join("target");
        assert!(matches!(
            manager.publish(&spec, &target, true).await,
            Err(CsiError::MountConflict(_))
        ));
        assert!(
            !manager.state_path(&spec).exists(),
            "failed ownership verification left persisted state"
        );
        assert!(
            !ops.is_mounted(&spec.shared_mount()).unwrap(),
            "failed ownership verification left a started mount"
        );
        assert!(!ops.is_mounted(&target).unwrap());
    }

    #[cfg(target_os = "linux")]
    struct ReviewMountFixture(TempDir);

    #[cfg(target_os = "linux")]
    impl ReviewMountFixture {
        fn new() -> Self {
            assert_eq!(std::env::var("CSI_PRIVATE_MOUNT_TEST").as_deref(), Ok("1"));
            assert_ne!(
                fs::read_link("/proc/self/ns/mnt").unwrap(),
                fs::read_link("/proc/1/ns/mnt").unwrap()
            );
            assert!(
                !fs::read_to_string("/proc/self/mountinfo")
                    .unwrap()
                    .lines()
                    .any(|line| line.split(" - ").next().unwrap().contains(" shared:"))
            );
            let fixture = Self(tempfile::tempdir().unwrap());
            for name in ["owned", "foreign"] {
                let path = fixture.path(name);
                fs::create_dir(&path).unwrap();
                run_command(Command::new("mount").args(["-t", "tmpfs", name]).arg(path)).unwrap();
            }
            SystemMountOps
                .bind(&fixture.path("owned"), &fixture.path("target"), false)
                .unwrap();
            fixture
        }

        fn path(&self, name: &str) -> PathBuf {
            self.0.path().join(name)
        }
    }

    #[cfg(target_os = "linux")]
    impl Drop for ReviewMountFixture {
        fn drop(&mut self) {
            for name in ["target", "owned", "foreign"] {
                let path = CString::new(self.path(name).as_os_str().as_bytes()).unwrap();
                // Test-only cleanup; all mounts are private fixtures.
                while unsafe { libc::umount2(path.as_ptr(), libc::MNT_DETACH) } == 0 {}
            }
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "requires a private mount namespace and CSI_PRIVATE_MOUNT_TEST=1"]
    fn ownership_self_review_last_syscall_window_preserves_foreign_mount() {
        let fixture = ReviewMountFixture::new();
        let target = fixture.path("target");
        let expected = SystemMountOps.inspect_mount(&target).unwrap().unwrap();
        let foreign = fixture.path("foreign");
        let replaced_target = target.clone();
        BEFORE_UNMOUNT_SYSCALL.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                SystemMountOps
                    .bind(&foreign, &replaced_target, false)
                    .unwrap();
            }));
        });
        let result = SystemMountOps.unbind(&target, expected);
        let visible = SystemMountOps.inspect_mount(&target).unwrap().unwrap();
        assert_eq!(
            visible.source, "foreign",
            "foreign overmount was unmounted after the final validation; result={result:?}"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "requires a private mount namespace and CSI_PRIVATE_MOUNT_TEST=1"]
    fn ownership_self_review_aliases_are_rejected() {
        let mut accepted = Vec::new();
        for alias_type in ["symlink", "dot-component"] {
            let fixture = ReviewMountFixture::new();
            let expected = SystemMountOps
                .inspect_mount(&fixture.path("target"))
                .unwrap()
                .unwrap();
            let alias = if alias_type == "symlink" {
                let alias = fixture.path("alias");
                std::os::unix::fs::symlink(fixture.path("target"), &alias).unwrap();
                alias
            } else {
                fixture.0.path().join(".").join("target")
            };
            if SystemMountOps.unbind(&alias, expected).is_ok() {
                accepted.push(alias_type);
            }
        }
        assert!(
            accepted.is_empty(),
            "aliased targets were accepted and unmounted: {accepted:?}"
        );
    }

    #[tokio::test]
    async fn ownership_unpublish_rejects_replacement_during_unmount() {
        for by_id in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let ops = Arc::new(FakeOps::default());
            let manager = MountManager::new_with_root(ops.clone(), temp.path().join("state"));
            let spec = spec(&temp);
            let target = temp.path().join("target");
            manager.publish(&spec, &target, false).await.unwrap();
            let before = fs::read(manager.state_path(&spec)).unwrap();
            let mut foreign = ops.inspect_mount(&target).unwrap().unwrap();
            foreign.device = "8:99".to_owned();
            foreign.fs_type = "ext4".to_owned();
            foreign.source = "/dev/foreign".to_owned();
            *ops.replace_on_unbind.lock().unwrap() = Some((target.clone(), foreign.clone()));
            let result = if by_id {
                manager
                    .unpublish_by_id(&spec.volume.as_string(), &target)
                    .await
            } else {
                manager.unpublish(&spec, &target).await
            };
            assert_eq!(
                ops.inspect_mount(&target).unwrap(),
                Some(foreign),
                "replacement was unmounted (by_id={by_id})"
            );
            assert!(result.is_err());
            assert_eq!(fs::read(manager.state_path(&spec)).unwrap(), before);
        }
    }

    #[tokio::test]
    async fn ownership_rejection_has_no_stage_side_effects() {
        for previously_staged in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let ops = Arc::new(FakeOps::default());
            let state_root = temp.path().join("state");
            let manager = MountManager::new_with_root(ops.clone(), state_root.clone());
            let mut spec = spec(&temp);
            if previously_staged {
                manager.stage(&spec).await.unwrap();
                // A rejected request must not rewrite an existing volume context.
                spec.read_only = true;
            }
            let before = fs::read(manager.state_path(&spec)).ok();
            let target = temp.path().join("target");
            ops.mounted
                .lock()
                .unwrap()
                .insert(target.display().to_string());
            assert!(matches!(
                manager.publish(&spec, &target, false).await,
                Err(CsiError::MountConflict(_))
            ));
            assert_eq!(
                *ops.starts.lock().unwrap(),
                usize::from(previously_staged),
                "rejection started BrewFS"
            );
            assert_eq!(
                ops.is_mounted(&spec.shared_mount()).unwrap(),
                previously_staged
            );
            assert_eq!(
                state_root.exists(),
                previously_staged,
                "rejection persisted state"
            );
            assert_eq!(fs::read(manager.state_path(&spec)).ok(), before);
            assert_eq!(
                spec.mount_root.exists(),
                previously_staged,
                "rejection created the mount root"
            );
            assert!(ops.is_mounted(&target).unwrap());
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "run with sudo unshare --mount --fork --propagation private and CSI_PRIVATE_MOUNT_TEST=1"]
    fn ownership_kernel_replacements_are_not_unmounted() {
        assert_eq!(std::env::var("CSI_PRIVATE_MOUNT_TEST").as_deref(), Ok("1"));
        assert_ne!(
            fs::read_link("/proc/self/ns/mnt").unwrap(),
            fs::read_link("/proc/1/ns/mnt").unwrap()
        );
        assert!(
            !fs::read_to_string("/proc/self/mountinfo")
                .unwrap()
                .lines()
                .any(|line| line.split(" - ").next().unwrap().contains(" shared:")),
            "mount propagation must be private"
        );
        let temp = tempfile::tempdir().unwrap();
        let ops = SystemMountOps;
        let source = temp.path().join("source");
        let foreign = temp.path().join("foreign");
        for (name, path) in [("owned", &source), ("foreign", &foreign)] {
            fs::create_dir(path).unwrap();
            run_command(Command::new("mount").args(["-t", "tmpfs", name]).arg(path)).unwrap();
        }
        let foreign_identity = ops.inspect_mount(&foreign).unwrap().unwrap();
        for replacement in ["overmount", "symlink", "same-filesystem"] {
            let target = temp.path().join("target");
            ops.bind(&source, &target, false).unwrap();
            let expected = ops.inspect_mount(&target).unwrap().unwrap();
            let alias = temp.path().join("alias");
            let request_path = if replacement == "symlink" {
                std::os::unix::fs::symlink(&target, &alias).unwrap();
                alias.as_path()
            } else {
                target.as_path()
            };
            if replacement == "symlink" {
                assert!(
                    ops.inspect_mount(request_path).is_err(),
                    "symlink alias was followed during validation"
                );
            } else {
                assert_eq!(
                    ops.inspect_mount(request_path).unwrap().as_ref(),
                    Some(&expected)
                );
            }
            match replacement {
                "overmount" => ops.bind(&foreign, &target, false).unwrap(),
                "symlink" => {
                    fs::remove_file(&alias).unwrap();
                    std::os::unix::fs::symlink(&foreign, &alias).unwrap();
                }
                _ => {
                    // Simulate another actor detaching and replacing the
                    // mount. The observation must retain its ID until checked.
                    run_command(Command::new("umount").arg("-l").arg(&target)).unwrap();
                    ops.bind(&source, &target, false).unwrap();
                }
            }
            if replacement == "symlink" {
                assert!(
                    ops.unbind(request_path, expected).is_err(),
                    "symlink replacement was accepted"
                );
                assert_eq!(
                    ops.inspect_mount(&foreign).unwrap().as_ref(),
                    Some(&foreign_identity)
                );
                fs::remove_file(&alias).unwrap();
                ops.unbind(&target, ops.inspect_mount(&target).unwrap().unwrap())
                    .unwrap();
                continue;
            }
            let replacement_identity = ops.inspect_mount(request_path).unwrap().unwrap();
            assert_ne!(replacement_identity.mount_id, expected.mount_id);
            assert!(
                ops.unbind(request_path, expected).is_err(),
                "accepted {replacement}"
            );
            assert_eq!(
                ops.inspect_mount(request_path).unwrap(),
                Some(replacement_identity)
            );
            assert_eq!(
                ops.inspect_mount(&foreign).unwrap().as_ref(),
                Some(&foreign_identity)
            );
            if replacement == "overmount" {
                run_command(Command::new("umount").arg(&target)).unwrap();
            }
            let owned = ops.inspect_mount(&target).unwrap().unwrap();
            // Ordinary unmount must still reject a busy target, not detach it.
            let busy = open_mount_anchor(&target).unwrap();
            assert_eq!(
                ops.unbind(&target, owned.clone())
                    .unwrap_err()
                    .raw_os_error(),
                Some(libc::EBUSY)
            );
            assert_eq!(ops.inspect_mount(&target).unwrap().as_ref(), Some(&owned));
            drop(busy);
            ops.unbind(&target, owned).unwrap();
            assert!(!target.exists());
        }
        drop(foreign_identity);
        for path in [&source, &foreign] {
            ops.unbind(path, ops.inspect_mount(path).unwrap().unwrap())
                .unwrap();
        }
    }

    #[tokio::test]
    async fn ownership_publish_rejects_another_volume_even_with_a_recorded_target() {
        for recorded in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let ops = Arc::new(FakeOps::default());
            let manager = MountManager::new_with_root(ops.clone(), temp.path().join("state"));
            let spec = spec(&temp);
            let target = temp.path().join("target");
            fs::create_dir(&target).unwrap();
            if recorded {
                manager.publish(&spec, &target, false).await.unwrap();
                ops.unbind(&target, ops.inspect_mount(&target).unwrap().unwrap())
                    .unwrap();
            }
            let mut other = spec.clone();
            other.volume = VolumeId::new("cluster-a", Uuid::from_u128(8)).unwrap();
            let foreign = manager.stage(&other).await.unwrap();
            ops.bind(&foreign, &target, false).unwrap();

            assert!(
                manager.publish(&spec, &target, false).await.is_err(),
                "publish accepted another volume's mount (recorded={recorded})"
            );
            assert!(ops.is_mounted(&target).unwrap());
            assert_eq!(
                manager
                    .load_state(&spec)
                    .unwrap()
                    .targets
                    .contains(&target.display().to_string()),
                recorded
            );
        }
    }

    #[tokio::test]
    async fn ownership_unpublish_rejects_replaced_target() {
        for by_id in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let ops = Arc::new(FakeOps::default());
            let manager = MountManager::new_with_root(ops.clone(), temp.path().join("state"));
            let spec = spec(&temp);
            let target = temp.path().join("target");
            manager.publish(&spec, &target, false).await.unwrap();
            ops.unbind(&target, ops.inspect_mount(&target).unwrap().unwrap())
                .unwrap();
            let mut other = spec.clone();
            other.volume = VolumeId::new("cluster-a", Uuid::from_u128(8)).unwrap();
            let foreign = manager.stage(&other).await.unwrap();
            ops.bind(&foreign, &target, false).unwrap();

            let result = if by_id {
                manager
                    .unpublish_by_id(&spec.volume.as_string(), &target)
                    .await
            } else {
                manager.unpublish(&spec, &target).await
            };
            assert!(
                result.is_err(),
                "unpublish accepted a replaced mount (by_id={by_id})"
            );
            assert!(
                ops.is_mounted(&target).unwrap(),
                "foreign mount was removed"
            );
            assert!(
                manager
                    .load_state(&spec)
                    .unwrap()
                    .targets
                    .contains(&target.display().to_string())
            );
        }
    }

    #[tokio::test]
    async fn ownership_unpublish_rejects_mounted_target_without_state() {
        for by_id in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let ops = Arc::new(FakeOps::default());
            let manager = MountManager::new_with_root(ops.clone(), temp.path().join("state"));
            let spec = spec(&temp);
            let target = temp.path().join("target");
            ops.mounted
                .lock()
                .unwrap()
                .insert(target.display().to_string());
            let result = if by_id {
                manager
                    .unpublish_by_id(&spec.volume.as_string(), &target)
                    .await
            } else {
                manager.unpublish(&spec, &target).await
            };
            assert!(
                result.is_err(),
                "unpublish trusted a mount without state (by_id={by_id})"
            );
            assert!(ops.is_mounted(&target).unwrap());
        }
    }

    #[tokio::test]
    async fn ownership_publish_rejects_read_only_mismatch() {
        for read_only in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let manager =
                MountManager::new_with_root(FakeOps::default(), temp.path().join("state"));
            let spec = spec(&temp);
            let target = temp.path().join("target");
            manager.publish(&spec, &target, read_only).await.unwrap();
            assert!(
                manager.publish(&spec, &target, !read_only).await.is_err(),
                "publish accepted the wrong read-only mode"
            );
        }
    }

    #[tokio::test]
    async fn ownership_owned_target_is_idempotent_after_restart() {
        for read_only in [false, true] {
            for by_id in [false, true] {
                let temp = tempfile::tempdir().unwrap();
                let ops = Arc::new(FakeOps::default());
                let state_root = temp.path().join("state");
                let manager = MountManager::new_with_root(ops.clone(), state_root.clone());
                let spec = spec(&temp);
                let target = temp.path().join("target");
                manager.publish(&spec, &target, read_only).await.unwrap();
                let restarted = MountManager::new_with_root(ops.clone(), state_root);
                restarted.publish(&spec, &target, read_only).await.unwrap();
                for _ in 0..2 {
                    if by_id {
                        restarted
                            .unpublish_by_id(&spec.volume.as_string(), &target)
                            .await
                            .unwrap();
                    } else {
                        restarted.unpublish(&spec, &target).await.unwrap();
                    }
                }
                assert!(!ops.is_mounted(&target).unwrap());
                assert_eq!(*ops.starts.lock().unwrap(), 1);
            }
        }
    }

    #[tokio::test]
    async fn ownership_rejects_changed_identity_and_mode_without_unbinding() {
        for field in ["device", "root", "fs_type", "source", "read_only"] {
            let temp = tempfile::tempdir().unwrap();
            let ops = Arc::new(FakeOps::default());
            let state_root = temp.path().join("state");
            let manager = MountManager::new_with_root(ops.clone(), state_root.clone());
            let spec = spec(&temp);
            let target = temp.path().join("target");
            manager.publish(&spec, &target, false).await.unwrap();
            {
                let mut identities = ops.identities.lock().unwrap();
                let identity = identities.get_mut(&target.display().to_string()).unwrap();
                match field {
                    "device" => identity.device = "8:1".to_owned(),
                    "root" => identity.root = "/subdirectory".to_owned(),
                    "fs_type" => identity.fs_type = "ext4".to_owned(),
                    "source" => identity.source = "other-fuse".to_owned(),
                    _ => identity.read_only = true,
                }
            }
            let restarted = MountManager::new_with_root(ops.clone(), state_root);
            let before = fs::read(restarted.state_path(&spec)).unwrap();
            assert!(
                matches!(
                    restarted.publish(&spec, &target, false).await,
                    Err(CsiError::MountConflict(_))
                ),
                "publish accepted changed {field}"
            );
            assert!(
                matches!(
                    restarted.unpublish(&spec, &target).await,
                    Err(CsiError::MountConflict(_))
                ),
                "unpublish accepted changed {field}"
            );
            assert!(
                matches!(
                    restarted
                        .unpublish_by_id(&spec.volume.as_string(), &target)
                        .await,
                    Err(CsiError::MountConflict(_))
                ),
                "unpublish_by_id accepted changed {field}"
            );
            assert_eq!(fs::read(restarted.state_path(&spec)).unwrap(), before);
            assert_eq!(*ops.unbinds.lock().unwrap(), 0);
            assert!(ops.is_mounted(&target).unwrap());
        }
    }

    #[tokio::test]
    async fn ownership_rejects_host_mount_and_replaced_or_legacy_shared_mount() {
        for scenario in ["host-target", "replaced-shared", "legacy-state"] {
            let temp = tempfile::tempdir().unwrap();
            let ops = Arc::new(FakeOps::default());
            let manager = MountManager::new_with_root(ops.clone(), temp.path().join("state"));
            let spec = spec(&temp);
            let target = temp.path().join("target");
            manager.stage(&spec).await.unwrap();
            match scenario {
                "host-target" => {
                    ops.mounted
                        .lock()
                        .unwrap()
                        .insert(target.display().to_string());
                }
                "replaced-shared" => {
                    // Even a live recorded PID must not authenticate a host mount.
                    ops.identities
                        .lock()
                        .unwrap()
                        .remove(&spec.shared_mount().display().to_string());
                }
                _ => {
                    let mut state = manager.load_state(&spec).unwrap();
                    state.shared_identity = None;
                    manager.store_state(&spec, &state).unwrap();
                }
            }
            assert!(
                matches!(
                    manager.publish(&spec, &target, false).await,
                    Err(CsiError::MountConflict(_))
                ),
                "accepted {scenario}"
            );
            assert!(manager.load_state(&spec).unwrap().targets.is_empty());
            assert_eq!(*ops.unbinds.lock().unwrap(), 0);
            assert_eq!(*ops.unmounts.lock().unwrap(), 0);
        }
    }

    #[tokio::test]
    async fn ownership_inspection_errors_are_returned_without_unbinding() {
        let temp = tempfile::tempdir().unwrap();
        let ops = Arc::new(FakeOps::default());
        let manager = MountManager::new_with_root(ops.clone(), temp.path().join("state"));
        let spec = spec(&temp);
        let target = temp.path().join("target");
        manager.publish(&spec, &target, false).await.unwrap();
        ops.inspection_errors.lock().unwrap().insert(target.clone());
        assert!(matches!(
            manager.publish(&spec, &target, false).await,
            Err(CsiError::Io(_))
        ));
        assert!(matches!(
            manager.unpublish(&spec, &target).await,
            Err(CsiError::Io(_))
        ));
        assert!(matches!(
            manager
                .unpublish_by_id(&spec.volume.as_string(), &target)
                .await,
            Err(CsiError::Io(_))
        ));
        assert_eq!(*ops.unbinds.lock().unwrap(), 0);
        assert!(ops.is_mounted(&target).unwrap());
    }

    #[test]
    fn mountinfo_decodes_paths_and_distinguishes_bind_modes() {
        let info = "41 1 0:55 / /shared rw,relatime shared:7 - fuse brewfs rw,user_id=0\n\
                    42 1 0:55 / /target\\040one ro,relatime master:7 - fuse brewfs rw,user_id=0\n\
                    43 1 0:55 /sub\\134dir /subdir rw - fuse brewfs rw\n";
        let shared = mount_at(info, Path::new("/shared"), 41).unwrap().unwrap();
        let target = mount_at(info, Path::new("/target one"), 42)
            .unwrap()
            .unwrap();
        assert!(shared.is_brewfs());
        assert!(target.same_filesystem_root(&shared));
        assert!(!shared.read_only);
        assert!(target.read_only);
        let subdir = mount_at(info, Path::new("/subdir"), 43).unwrap().unwrap();
        assert_eq!(subdir.root, "/sub\\dir");
        assert!(!subdir.same_filesystem_root(&shared));
        assert!(mount_at(info, Path::new("/missing"), 41).unwrap().is_none());
        assert_eq!(
            unescape_mount_field(r"a\011b\012c\134d").unwrap(),
            "a\tb\nc\\d"
        );
        // mountinfo separates fields with spaces; a literal carriage return
        // in a path is not a field separator.
        assert!(
            mount_at(
                "44 1 0:55 / /target\rname rw - fuse brewfs rw",
                Path::new("/target\rname"),
                44
            )
            .unwrap()
            .is_some()
        );
        let super_ro = "41 1 0:55 / /shared rw - fuse brewfs ro";
        assert!(
            mount_at(super_ro, Path::new("/shared"), 41)
                .unwrap()
                .unwrap()
                .read_only
        );
    }

    #[test]
    fn mountinfo_rejects_malformed_entries_and_uses_visible_mount() {
        for info in [
            "41 1 0:55 / /target rw",
            "41 - fuse brewfs rw",
            "41 1 0:55 / /target rw - fuse",
            "41 1 0:55 / /target defaults - fuse brewfs rw",
            "41 1 0:55 / /target ro,rw - fuse brewfs rw",
            r"41 1 0:55 / /target\999 rw - fuse brewfs rw",
        ] {
            assert!(
                mount_at(info, Path::new("/target"), 41).is_err(),
                "accepted {info}"
            );
        }
        let stacked =
            "41 1 0:55 / /target rw - fuse brewfs rw\n42 1 8:1 / /target rw - ext4 /dev/sda1 rw";
        let visible = mount_at(stacked, Path::new("/target"), 42)
            .unwrap()
            .unwrap();
        assert_eq!(visible.fs_type, "ext4");
        assert!(!visible.is_brewfs());
        assert!(mount_at(stacked, Path::new("/target"), 99).is_err());
        // An ancestor overmount hides the old target mount entirely.
        let covered = "41 1 0:55 / /parent/target rw - fuse brewfs rw\n42 1 8:1 / /parent rw - ext4 /dev/sda1 rw";
        assert!(
            mount_at(covered, Path::new("/parent/target"), 42)
                .unwrap()
                .is_none()
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn mountinfo_system_inspection_distinguishes_mounts_from_directories() {
        let temp = tempfile::tempdir().unwrap();
        assert!(
            SystemMountOps
                .inspect_mount(Path::new("/"))
                .unwrap()
                .is_some()
        );
        assert!(SystemMountOps.inspect_mount(temp.path()).unwrap().is_none());
        assert!(
            SystemMountOps
                .inspect_mount(&temp.path().join("missing"))
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn unstage_refuses_live_published_targets() {
        let temp = tempfile::tempdir().unwrap();
        let manager = MountManager::new_with_root(FakeOps::default(), temp.path().join("state"));
        let spec = spec(&temp);
        let target = temp.path().join("target");
        manager.publish(&spec, &target, false).await.unwrap();
        assert!(matches!(
            manager.unstage(&spec).await,
            Err(CsiError::Published(_))
        ));
    }

    #[tokio::test]
    async fn unpublish_and_unstage_by_id_are_idempotent_without_state() {
        let temp = tempfile::tempdir().unwrap();
        let manager = MountManager::new_with_root(FakeOps::default(), temp.path().join("state"));
        let spec = spec(&temp);
        let target = temp.path().join("target");
        let id = spec.volume.as_string();
        manager.unpublish_by_id(&id, &target).await.unwrap();
        manager.unstage_by_id(&id).await.unwrap();
    }
}
