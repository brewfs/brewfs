//! Workspace-specific durable staging paths.

use std::path::{Path, PathBuf};

use uuid::Uuid;

use super::error::WorkspaceError;
use super::ids::WorkspaceId;

pub fn writeback_root(
    cache_root: &Path,
    volume_id: Uuid,
    workspace_id: WorkspaceId,
    head_epoch: u64,
) -> Result<PathBuf, WorkspaceError> {
    std::fs::create_dir_all(cache_root)?;
    let cache_root = cache_root.canonicalize()?;
    let candidate = cache_root
        .join("workspace-v1")
        .join(volume_id.to_string())
        .join(workspace_id.to_string())
        .join(head_epoch.to_string());
    std::fs::create_dir_all(&candidate)?;
    let candidate = candidate.canonicalize()?;
    candidate.strip_prefix(&cache_root).map_err(|_| {
        WorkspaceError::CorruptMetadata(
            "workspace writeback path escapes the configured cache root".into(),
        )
    })?;
    Ok(candidate)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writeback_path_contains_the_complete_workspace_scope() {
        let root = tempfile::tempdir().unwrap();
        let volume = Uuid::from_u128(41);
        let workspace = WorkspaceId::from_uuid(Uuid::from_u128(42));
        let path = writeback_root(root.path(), volume, workspace, 7).unwrap();

        assert!(path.starts_with(root.path().canonicalize().unwrap()));
        assert!(
            path.ends_with(
                Path::new("workspace-v1")
                    .join(volume.to_string())
                    .join(workspace.to_string())
                    .join("7")
            )
        );
    }
}
