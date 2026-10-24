//! Durable trash record shared by namespace backends and the control plane.
pub(crate) const CONTROL_TRASH_XATTR_NAME: &str = "system.brewfs.trash";

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct ControlTrashMetadata {
    pub original_path: String,
    pub deleted_at: String,
}
