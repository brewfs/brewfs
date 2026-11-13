//! Kernel evidence for CLI cleanup, independent of vendor Session completion.
//! A retained-error marker keeps outer SDK shutdown from destroying live work.

use crate::workspace::packed::snapshot::SnapshotMountBudget;
#[cfg(target_os = "linux")]
use crate::workspace::packed::snapshot::{SnapshotBudgetPool, SnapshotOwnedPermit};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(thiserror::Error)]
#[error(
    "packed cutoff/drain is unverified; reader pin, workspace lease and metadata backend retained"
)]
pub(super) struct RetainPackedRuntime {
    _owner: Arc<dyn Send + Sync>,
}
impl std::fmt::Debug for RetainPackedRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("RetainPackedRuntime")
    }
}
impl RetainPackedRuntime {
    pub(super) fn holding<T: Send + Sync + 'static>(owner: T) -> Self {
        Self {
            _owner: Arc::new(owner),
        }
    }
}

pub(super) fn requires_retention(result: &anyhow::Result<()>) -> bool {
    result
        .as_ref()
        .err()
        .is_some_and(|error| error.is::<RetainPackedRuntime>())
}

#[cfg(target_os = "linux")]
struct OwnedMountInfo {
    bytes: Vec<u8>,
    _permit: SnapshotOwnedPermit,
}
#[cfg(target_os = "linux")]
impl std::ops::Deref for OwnedMountInfo {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &self.bytes
    }
}

#[cfg(target_os = "linux")]
fn read_mountinfo(budget: &Arc<SnapshotMountBudget>) -> std::io::Result<OwnedMountInfo> {
    use std::io::Read;
    const MAX_BYTES: usize = 2 << 20;
    let permit = budget
        .admit(&[(SnapshotBudgetPool::Metadata, 2 * (MAX_BYTES as u64 + 1))])
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    let mut bytes = Vec::with_capacity(MAX_BYTES + 1);
    std::fs::File::open("/proc/self/mountinfo")?
        .take(MAX_BYTES as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "mountinfo exceeds fixed bound",
        ));
    }
    Ok(OwnedMountInfo {
        bytes,
        _permit: permit,
    })
}

#[cfg(target_os = "linux")]
fn invalid(what: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, what)
}

#[cfg(target_os = "linux")]
fn mount_id(raw: &[u8]) -> std::io::Result<u64> {
    let id: u64 = std::str::from_utf8(raw)
        .map_err(|_| invalid("mount ID encoding"))?
        .parse()
        .map_err(|_| invalid("mount ID encoding"))?;
    if id == 0 {
        return Err(invalid("zero mount ID"));
    }
    Ok(id)
}

#[cfg(target_os = "linux")]
fn decode_path(raw: &[u8]) -> std::io::Result<Vec<u8>> {
    let mut path = Vec::with_capacity(raw.len());
    let mut index = 0;
    while index < raw.len() {
        if raw[index] != b'\\' {
            path.push(raw[index]);
            index += 1;
            continue;
        }
        let escaped = raw
            .get(index + 1..index + 4)
            .ok_or_else(|| invalid("short mount path escape"))?;
        path.push(match escaped {
            b"040" => b' ',
            b"011" => b'\t',
            b"012" => b'\n',
            b"134" => b'\\',
            _ => return Err(invalid("unknown mount path escape")),
        });
        index += 4;
    }
    Ok(path)
}

#[cfg(target_os = "linux")]
fn id_at_path(bytes: &[u8], expected: &[u8]) -> std::io::Result<Option<u64>> {
    let mut found = None;
    for line in bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let mut fields = line.split(|byte| *byte == b' ');
        let id = mount_id(
            fields
                .next()
                .ok_or_else(|| invalid("mountinfo has no ID"))?,
        )?;
        let path = fields
            .nth(3)
            .ok_or_else(|| invalid("mountinfo has no mount point"))?;
        if decode_path(path)? == expected && found.replace(id).is_some() {
            return Err(invalid("ambiguous stacked mount point"));
        }
    }
    Ok(found)
}

#[cfg(target_os = "linux")]
pub(super) fn prepare(path: &Path, budget: &Arc<SnapshotMountBudget>) -> std::io::Result<PathBuf> {
    use std::os::unix::ffi::OsStrExt;
    let path = path.canonicalize()?;
    if id_at_path(&read_mountinfo(budget)?, path.as_os_str().as_bytes())?.is_some() {
        return Err(invalid("packed mount point already has a kernel mount"));
    }
    Ok(path)
}

#[cfg(target_os = "linux")]
pub(super) fn mounted_id(path: &Path, budget: &Arc<SnapshotMountBudget>) -> std::io::Result<u64> {
    use std::os::unix::ffi::OsStrExt;
    id_at_path(&read_mountinfo(budget)?, path.as_os_str().as_bytes())?
        .ok_or_else(|| invalid("successful mount has no visible kernel mount ID"))
}

#[cfg(target_os = "linux")]
pub(super) fn is_absent(id: u64, budget: &Arc<SnapshotMountBudget>) -> std::io::Result<bool> {
    let info = read_mountinfo(budget)?;
    for line in info
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        if mount_id(
            line.split(|byte| *byte == b' ')
                .next()
                .ok_or_else(|| invalid("mountinfo ID"))?,
        )? == id
        {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(not(target_os = "linux"))]
pub(super) fn prepare(
    _path: &Path,
    _budget: &Arc<SnapshotMountBudget>,
) -> std::io::Result<PathBuf> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "packed FUSE cutoff requires Linux",
    ))
}
#[cfg(not(target_os = "linux"))]
pub(super) fn mounted_id(_path: &Path, _budget: &Arc<SnapshotMountBudget>) -> std::io::Result<u64> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "packed FUSE cutoff requires Linux",
    ))
}
#[cfg(not(target_os = "linux"))]
pub(super) fn is_absent(_id: u64, _budget: &Arc<SnapshotMountBudget>) -> std::io::Result<bool> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "packed FUSE cutoff requires Linux",
    ))
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    #[test]
    fn kernel_paths_keep_raw_bytes_and_decode_only_mountinfo_escapes() {
        let input = b"47 1 0:1 / /tmp/a\\040b\\134x\xff rw - fuse brewfs rw\n";
        assert_eq!(id_at_path(input, b"/tmp/a b\\x\xff").unwrap(), Some(47));
        assert!(decode_path(b"/tmp/\\000").is_err());
        assert!(decode_path(b"/tmp/\\0").is_err());
    }

    #[test]
    fn stacked_mounts_cannot_issue_one_cutoff_identity() {
        let input =
            b"47 1 0:1 / /tmp/m rw - fuse brewfs rw\n48 1 0:1 / /tmp/m rw - fuse other rw\n";
        assert!(id_at_path(input, b"/tmp/m").is_err());
        assert_eq!(id_at_path(input, b"/tmp/else").unwrap(), None);
    }

    #[test]
    fn retained_error_marker_survives_original_errno_context() {
        let error = anyhow::Error::from(std::io::Error::from_raw_os_error(libc::EBUSY))
            .context(RetainPackedRuntime::holding(()));
        assert!(error.downcast_ref::<std::io::Error>().is_some());
        let result = Err(error);
        assert!(requires_retention(&result));
        assert!(!requires_retention(&Ok(())));
    }
}

// Append inside cli/packed_mount_cutoff.rs; keep all old helpers/tests unchanged.
// The owning CLI must capture this after its actual mount call, from that same VFS.

pub(super) fn operator_mount_reference(
    view: &crate::workspace::model::ViewContext,
) -> Result<
    crate::workspace::stores::kv_store::packed_admin::PackedReleasedMountReference,
    crate::workspace::error::WorkspaceError,
> {
    use crate::workspace::catalog::HeadGuard;
    use crate::workspace::error::WorkspaceError;
    let uid = |key: &str| {
        std::env::var(key)
            .ok()
            .and_then(|value| uuid::Uuid::parse_str(&value).ok())
            .filter(|value| !value.is_nil())
            .ok_or(WorkspaceError::Fenced)
    };
    Ok(
        crate::workspace::stores::kv_store::packed_admin::PackedReleasedMountReference {
            guard: HeadGuard {
                workspace_id: view.workspace_id,
                expected_head_layer_id: view.head_layer_id,
                expected_head_epoch: view.head_epoch,
                lease_id: view.lease_id,
                holder_generation: view.holder_generation,
            },
            mount_uid: uid("BREWFS_PACKED_MOUNT_UID")?,
            pod_uid: uid("BREWFS_PACKED_POD_UID")?,
        },
    )
}

pub(crate) struct OwnedPackedMountIdentity<S, M>
where
    S: crate::chunk::BlockStore + Send + Sync + 'static,
    M: crate::meta::layer::MetaLayer + Send + Sync + 'static,
{
    vfs: crate::vfs::fs::VFS<S, M>,
    mount_id: u64,
    mount_namespace: (u64, u64),
    budget: Arc<SnapshotMountBudget>,
}

pub(crate) struct VerifiedPackedKernelCutoff<S, M>
where
    S: crate::chunk::BlockStore + Send + Sync + 'static,
    M: crate::meta::layer::MetaLayer + Send + Sync + 'static,
{
    identity: OwnedPackedMountIdentity<S, M>,
}

#[cfg(target_os = "linux")]
fn current_mount_namespace() -> std::io::Result<(u64, u64)> {
    use std::os::unix::fs::MetadataExt;
    let metadata = std::fs::metadata("/proc/self/ns/mnt")?;
    Ok((metadata.dev(), metadata.ino()))
}

#[cfg(not(target_os = "linux"))]
fn current_mount_namespace() -> std::io::Result<(u64, u64)> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "packed kernel cutoff",
    ))
}

impl<S, M> OwnedPackedMountIdentity<S, M>
where
    S: crate::chunk::BlockStore + Send + Sync + 'static,
    M: crate::meta::layer::MetaLayer + Send + Sync + 'static,
{
    pub(super) fn capture(
        path: &Path,
        vfs: crate::vfs::fs::VFS<S, M>,
        budget: Arc<SnapshotMountBudget>,
    ) -> std::io::Result<Self> {
        let namespace = current_mount_namespace()?;
        let mount_id = mounted_id(path, &budget)?;
        if current_mount_namespace()? != namespace {
            return Err(std::io::Error::other(
                "packed mount namespace changed during capture",
            ));
        }
        Ok(Self {
            vfs,
            mount_id,
            mount_namespace: namespace,
            budget,
        })
    }

    // Invoke only after the original Session/unmount and ordinary-worker joins.
    pub(super) fn after_worker_join(self) -> std::io::Result<VerifiedPackedKernelCutoff<S, M>> {
        if current_mount_namespace()? != self.mount_namespace
            || !is_absent(self.mount_id, &self.budget)?
        {
            return Err(std::io::Error::other(
                "owned packed kernel mount still present",
            ));
        }
        Ok(VerifiedPackedKernelCutoff { identity: self })
    }
}

impl<S, M> VerifiedPackedKernelCutoff<S, M>
where
    S: crate::chunk::BlockStore + Send + Sync + 'static,
    M: crate::meta::layer::MetaLayer + Send + Sync + 'static,
{
    pub(crate) fn validate(&self) -> std::io::Result<()> {
        if current_mount_namespace()? != self.identity.mount_namespace
            || !is_absent(self.identity.mount_id, &self.identity.budget)?
        {
            return Err(std::io::Error::other(
                "packed kernel cutoff authority changed",
            ));
        }
        Ok(())
    }

    pub(crate) fn matches_drain(&self, drain: &crate::vfs::fs::PackedVfsDrainFence<S, M>) -> bool {
        drain.is_same_vfs(&self.identity.vfs)
    }

    pub(crate) fn uses_budget(&self, budget: &Arc<SnapshotMountBudget>) -> bool {
        Arc::ptr_eq(&self.identity.budget, budget)
    }
}

#[cfg(all(test, target_os = "linux"))]
pub(crate) mod packed_original_shutdown_tests;

#[cfg(all(test, target_os = "linux"))]
pub(crate) mod packed_fork_fixture;
