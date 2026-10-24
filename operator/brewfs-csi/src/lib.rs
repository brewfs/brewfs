//! Shared CSI state and BrewFS volume identity handling.
//!
//! CSI volumes refer to existing, operator-provisioned BrewFS workspaces;
//! dynamic provisioning is unsupported. A workspace is an operator-owned
//! resource, and its ID is carried by an immutable CSI volume ID.
//! The node side is the only component that starts
//! a BrewFS process; that process acquires the normal BrewFS sole-writer lease.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io;
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

/// The small set of host operations needed by the node plugin.  Keeping this
/// boundary explicit makes all idempotency behavior unit-testable without
/// mounting FUSE in a test process.
pub trait MountOps: Send + Sync + 'static {
    fn is_mounted(&self, path: &Path) -> io::Result<bool>;
    fn process_alive(&self, pid: u32) -> io::Result<bool>;
    fn start_brewfs(&self, spec: &MountSpec, target: &Path) -> io::Result<u32>;
    fn bind(&self, source: &Path, target: &Path, read_only: bool) -> io::Result<()>;
    fn unbind(&self, target: &Path) -> io::Result<()>;
    fn unmount_brewfs(&self, target: &Path) -> io::Result<()>;
}

impl<T: MountOps> MountOps for Arc<T> {
    fn is_mounted(&self, path: &Path) -> io::Result<bool> {
        (**self).is_mounted(path)
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

    fn unbind(&self, target: &Path) -> io::Result<()> {
        (**self).unbind(target)
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

    fn unbind(&self, target: &Path) -> io::Result<()> {
        run_command(Command::new("umount").arg(target))?;
        match fs::remove_dir(target) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }

    fn unmount_brewfs(&self, target: &Path) -> io::Result<()> {
        run_command(Command::new("umount").arg(target))
    }
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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct MountState {
    volume_id: String,
    shared_mount: String,
    config: String,
    binary: String,
    mount_root: String,
    read_only: bool,
    /// PID of the lease-owning BrewFS process when started by this plugin.
    /// `None` preserves compatibility with state written before PID tracking.
    #[serde(default)]
    pid: Option<u32>,
    #[serde(default)]
    targets: BTreeSet<String>,
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
        self.stage_locked(spec)
    }

    pub async fn publish(
        &self,
        spec: &MountSpec,
        target: &Path,
        read_only: bool,
    ) -> Result<(), CsiError> {
        let _guard = self.lock.lock().await;
        let shared = self.stage_locked(spec)?;
        if self.ops.is_mounted(target)? {
            let mut state = self.load_state(spec)?;
            state.targets.insert(target.display().to_string());
            self.store_state(spec, &state)?;
            return Ok(());
        }
        self.ops
            .bind(&shared, target, read_only || spec.read_only)?;
        let mut state = self.load_state(spec)?;
        state.targets.insert(target.display().to_string());
        self.store_state(spec, &state)
    }

    pub async fn unpublish(&self, spec: &MountSpec, target: &Path) -> Result<(), CsiError> {
        let _guard = self.lock.lock().await;
        if self.ops.is_mounted(target)? {
            self.ops.unbind(target)?;
        }
        let mut state = self.load_state(spec)?;
        state.targets.remove(&target.display().to_string());
        self.store_state(spec, &state)
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
            return Ok(());
        }
        let spec = self.spec_from_state(volume_id)?;
        if self.ops.is_mounted(target)? {
            self.ops.unbind(target)?;
        }
        let mut state = self.load_state(&spec)?;
        state.targets.remove(&target.display().to_string());
        self.store_state(&spec, &state)
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

    fn stage_locked(&self, spec: &MountSpec) -> Result<PathBuf, CsiError> {
        fs::create_dir_all(&spec.mount_root)?;
        let shared = spec.shared_mount();
        let mut state = self.load_state(spec)?;
        let mut mounted = self.ops.is_mounted(&shared)?;
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
            state = MountState::default();
            state.pid = Some(pid);
        }
        state.volume_id = spec.volume.as_string();
        state.shared_mount = shared.display().to_string();
        state.config = spec.config.display().to_string();
        state.binary = spec.binary.display().to_string();
        state.mount_root = spec.mount_root.display().to_string();
        state.read_only = spec.read_only;
        self.store_state(spec, &state)?;
        Ok(shared)
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

    #[derive(Debug, Default)]
    struct FakeOps {
        mounted: StdMutex<BTreeSet<String>>,
        binds: StdMutex<BTreeSet<String>>,
        starts: StdMutex<usize>,
        unmounts: StdMutex<usize>,
    }

    impl MountOps for FakeOps {
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
            Ok(42)
        }

        fn bind(&self, _source: &Path, target: &Path, _read_only: bool) -> io::Result<()> {
            self.binds
                .lock()
                .unwrap()
                .insert(target.display().to_string());
            Ok(())
        }

        fn unbind(&self, target: &Path) -> io::Result<()> {
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
