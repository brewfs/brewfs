//! Private same-PVC identity evidence for an original packed mount.
//! This marker never certifies that replay or remote uploads have completed.

use super::packed_admin::PackedReleasedMountReference;
use super::*;
use crate::vfs::cache::config::WriteBackMode;
use crate::vfs::config::VFSConfig;
use crate::workspace::packed::snapshot::{SnapshotBudgetPool, SnapshotMountBudget};
use sha2::{Digest, Sha256};
use std::path::Path;

const MARKER_MAGIC: &[u8; 5] = b"PMW3\x01";
const MAX_MARKER_BYTES: usize = 4096;
const MAX_SCOPE_BYTES: usize = 256;
const MAX_ROOT_PATH_BYTES: usize = 4096;
const MARKER_ADMISSION: u64 = 64 << 10;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PackedMountWritebackIdentity {
    workspace_id: WorkspaceId,
    head_layer_id: LayerId,
    head_epoch: u64,
    original_lease_id: LeaseId,
    original_holder_generation: u64,
    mount_uid: uuid::Uuid,
    pod_uid: uuid::Uuid,
    binding_sha256: [u8; 32],
    volume_scope: Option<String>,
    commit_before_upload: bool,
}

fn expected_marker(
    reference: &PackedReleasedMountReference,
    binding: &PackedLowerBindingRecord,
    config: &VFSConfig,
    initializing: bool,
) -> Result<Vec<u8>, WorkspaceError> {
    let guard = &reference.guard;
    if guard.workspace_id.as_uuid().is_nil()
        || guard.expected_head_layer_id.as_uuid().is_nil()
        || guard.expected_head_epoch == 0
        || guard.lease_id.as_uuid().is_nil()
        || guard.holder_generation == 0
        || reference.mount_uid.is_nil()
        || reference.pod_uid.is_nil()
        || binding.workspace_id != guard.workspace_id
        || binding.head_layer_id != guard.expected_head_layer_id
        || binding.head_epoch != guard.expected_head_epoch
        || config
            .cache
            .volume_scope
            .as_ref()
            .is_some_and(|scope| scope.len() > MAX_SCOPE_BYTES)
        || config.cache.writeback_mode != config.write.writeback_mode
        || (initializing && config.workspace_writer_epoch != guard.holder_generation)
        || (!initializing && config.workspace_writer_epoch <= guard.holder_generation)
    {
        return Err(WorkspaceError::Fenced);
    }
    let binding_sha256 = Sha256::digest(binding.encode()?).into();
    let identity = PackedMountWritebackIdentity {
        workspace_id: guard.workspace_id,
        head_layer_id: guard.expected_head_layer_id,
        head_epoch: guard.expected_head_epoch,
        original_lease_id: guard.lease_id,
        original_holder_generation: guard.holder_generation,
        mount_uid: reference.mount_uid,
        pod_uid: reference.pod_uid,
        binding_sha256,
        volume_scope: config.cache.volume_scope.clone(),
        commit_before_upload: matches!(
            config.cache.writeback_mode,
            WriteBackMode::CommitBeforeUpload
        ),
    };
    let mut bytes = MARKER_MAGIC.to_vec();
    bytes.extend(serde_json::to_vec(&identity).map_err(|_| WorkspaceError::Fenced)?);
    if bytes.len() > MAX_MARKER_BYTES {
        return Err(WorkspaceError::Fenced);
    }
    Ok(bytes)
}

fn marker_name(reference: &PackedReleasedMountReference) -> String {
    format!(".packed-mount-{}.json", reference.guard.lease_id)
}

fn configured_root(config: &VFSConfig) -> Result<&Path, WorkspaceError> {
    let root = config
        .workspace_writeback_root
        .as_deref()
        .ok_or(WorkspaceError::Fenced)?;
    if root.as_os_str().len() > MAX_ROOT_PATH_BYTES {
        return Err(WorkspaceError::Fenced);
    }
    Ok(root)
}

#[cfg(target_os = "linux")]
mod filesystem {
    use super::*;
    use std::ffi::CString;
    use std::fs::{File, OpenOptions};
    use std::io::{Read, Write};
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::fs::OpenOptionsExt;

    pub(super) fn open_root(root: &Path) -> Result<File, WorkspaceError> {
        // Pin the real directory. Relative openat operations below cannot be
        // redirected by replacing the path after this handle was acquired.
        let metadata = std::fs::symlink_metadata(root)?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(WorkspaceError::Fenced);
        }
        let directory = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(root)?;
        if !directory.metadata()?.is_dir() {
            return Err(WorkspaceError::Fenced);
        }
        Ok(directory)
    }

    fn open_at(directory: &File, name: &str, creating: bool) -> Result<File, std::io::Error> {
        let name = CString::new(name)
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
        let flags = libc::O_NOFOLLOW
            | libc::O_CLOEXEC
            | libc::O_NONBLOCK
            | if creating {
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL
            } else {
                libc::O_RDONLY
            };
        // SAFETY: directory owns a live fd, name is a valid terminated CString,
        // and the returned fd is transferred to one File exactly once.
        let fd = unsafe { libc::openat(directory.as_raw_fd(), name.as_ptr(), flags, 0o600) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(unsafe { File::from_raw_fd(fd) })
    }

    fn read_exact_marker(
        directory: &File,
        name: &str,
        expected: &[u8],
    ) -> Result<File, WorkspaceError> {
        let mut file = open_at(directory, name, false)?;
        let metadata = file.metadata()?;
        if !metadata.is_file() || metadata.len() > MAX_MARKER_BYTES as u64 {
            return Err(WorkspaceError::Fenced);
        }
        let mut bytes = Vec::with_capacity(MAX_MARKER_BYTES + 1);
        (&mut file)
            .take((MAX_MARKER_BYTES + 1) as u64)
            .read_to_end(&mut bytes)?;
        // Comparing canonical expected bytes also rejects unknown fields,
        // duplicate JSON fields, alternate encodings, and trailing data.
        if bytes.as_slice() != expected {
            return Err(WorkspaceError::Fenced);
        }
        Ok(file)
    }

    pub(super) fn initialize(
        root: &Path,
        name: &str,
        expected: &[u8],
    ) -> Result<(), WorkspaceError> {
        let directory = open_root(root)?;
        match open_at(&directory, name, true) {
            Ok(mut file) => {
                file.write_all(expected)?;
                file.sync_all()?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                // An interrupted matching install can finish durability. Old
                // mismatching or incomplete evidence is never overwritten.
                read_exact_marker(&directory, name, expected)?.sync_all()?;
            }
            Err(error) => return Err(error.into()),
        }
        directory.sync_all()?;
        Ok(())
    }

    pub(super) fn verify(root: &Path, name: &str, expected: &[u8]) -> Result<(), WorkspaceError> {
        let directory = open_root(root)?;
        read_exact_marker(&directory, name, expected)?;
        Ok(())
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn matching_install_is_idempotent_and_mismatch_preserves_original() {
            let root = tempfile::tempdir().unwrap();
            initialize(root.path(), "marker", b"original").unwrap();
            initialize(root.path(), "marker", b"original").unwrap();
            assert!(initialize(root.path(), "marker", b"different").is_err());
            assert_eq!(
                std::fs::read(root.path().join("marker")).unwrap(),
                b"original"
            );
            verify(root.path(), "marker", b"original").unwrap();
        }

        #[test]
        fn missing_original_marker_and_empty_replacement_root_fail_closed() {
            let root = tempfile::tempdir().unwrap();
            assert!(verify(root.path(), "missing", b"original").is_err());
            let missing = root.path().join("missing-root");
            assert!(initialize(&missing, "marker", b"original").is_err());
            assert!(!missing.exists());
        }

        #[test]
        fn symlink_roots_and_marker_aliases_are_rejected() {
            use std::os::unix::fs::symlink;
            let root = tempfile::tempdir().unwrap();
            let real = root.path().join("real");
            std::fs::create_dir(&real).unwrap();
            let alias = root.path().join("alias");
            symlink(&real, &alias).unwrap();
            assert!(initialize(&alias, "marker", b"original").is_err());
            std::fs::write(real.join("target"), b"original").unwrap();
            symlink(real.join("target"), real.join("marker")).unwrap();
            assert!(verify(&real, "marker", b"original").is_err());
            assert!(initialize(&real, "marker", b"original").is_err());
        }

        #[test]
        fn oversized_existing_marker_is_rejected_before_read_materialization() {
            let root = tempfile::tempdir().unwrap();
            let file = File::create(root.path().join("marker")).unwrap();
            file.set_len((MAX_MARKER_BYTES + 1) as u64).unwrap();
            assert!(verify(root.path(), "marker", b"original").is_err());
            assert_eq!(
                file.metadata().unwrap().len(),
                (MAX_MARKER_BYTES + 1) as u64
            );
        }
    }
}

pub(in crate::workspace::stores::kv_store) async fn init_packed_mount_writeback_identity(
    reference: &PackedReleasedMountReference,
    binding: &PackedLowerBindingRecord,
    budget: &Arc<SnapshotMountBudget>,
    config: &VFSConfig,
) -> Result<(), WorkspaceError> {
    let owner = budget
        .admit(&[(SnapshotBudgetPool::Metadata, MARKER_ADMISSION)])
        .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
    let expected = expected_marker(reference, binding, config, true)?;
    let root = configured_root(config)?.to_path_buf();
    let name = marker_name(reference);
    #[cfg(target_os = "linux")]
    return tokio::task::spawn_blocking(move || {
        let _owner = owner;
        filesystem::initialize(&root, &name, &expected)
    })
    .await
    .map_err(|_| {
        WorkspaceError::Backend("packed writeback identity install driver failed".into())
    })?;
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (owner, expected, root, name);
        Err(WorkspaceError::UnsupportedCapability(
            "packed mount writeback identity requires Linux",
        ))
    }
}

pub(in crate::workspace::stores::kv_store) async fn verify_packed_mount_writeback_identity(
    reference: &PackedReleasedMountReference,
    binding: &PackedLowerBindingRecord,
    budget: &Arc<SnapshotMountBudget>,
    config: &VFSConfig,
) -> Result<(), WorkspaceError> {
    let owner = budget
        .admit(&[(SnapshotBudgetPool::Metadata, MARKER_ADMISSION)])
        .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
    let expected = expected_marker(reference, binding, config, false)?;
    let root = configured_root(config)?.to_path_buf();
    let name = marker_name(reference);
    #[cfg(target_os = "linux")]
    return tokio::task::spawn_blocking(move || {
        let _owner = owner;
        filesystem::verify(&root, &name, &expected)
    })
    .await
    .map_err(|_| {
        WorkspaceError::Backend("packed writeback identity verification driver failed".into())
    })?;
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (owner, expected, root, name);
        Err(WorkspaceError::UnsupportedCapability(
            "packed mount writeback identity requires Linux",
        ))
    }
}
