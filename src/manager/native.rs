//! Manager adapter for the explicitly admitted native P1 catalog-lower profile.

use crate::native_base::runtime::{
    NativeP1Workspace, NativeRuntimeCapabilities, NativeVolumeHeader, RuntimeAdmissionError,
    ensure_p1_control, initialize_volume, inspect_p1_control, load_volume_header, mark_p1_deleting,
    p1_storage_namespace_id, validate_p1_volume,
};
use crate::native_base::write::keys::Keys;
use crate::native_base::write::store::{ControlStore, Txn};
use crate::workspace_overlay::error::WorkspaceError;
use crate::workspace_overlay::ids::WorkspaceId;
use crate::workspace_overlay::model::VolumeHeader;
use uuid::Uuid;

use super::catalog::CatalogConnection;
use super::{ManagerError, NativeCreation, Result, catalog_error, conflict};

fn control(conn: &CatalogConnection) -> Result<(&dyn ControlStore, &str)> {
    match (conn.native.as_deref(), conn.namespace()) {
        (Some(control), Some(namespace)) => Ok((control, namespace)),
        _ => Err(ManagerError::new(
            "unsupported",
            "native P1 requires a configured Redis or TiKV control namespace",
        )),
    }
}

fn require_guard(conn: &CatalogConnection) -> Result<()> {
    conn.guard()
        .ok_or_else(|| conflict("native mutations require the Manager catalog claim"))?;
    Ok(())
}

fn admission_error(error: RuntimeAdmissionError) -> ManagerError {
    match error {
        RuntimeAdmissionError::Store(_) => unavailable(),
        RuntimeAdmissionError::MissingHeader(_)
        | RuntimeAdmissionError::NamespaceExists
        | RuntimeAdmissionError::InvalidHeader(_)
        | RuntimeAdmissionError::Wire(_) => {
            conflict("native volume header is missing or differs from the recorded identity")
        }
        _ => ManagerError::new(
            "unsupported",
            "native volume format, features, or namespace are unsupported",
        ),
    }
}

fn unavailable() -> ManagerError {
    ManagerError::new(
        "recovery_required",
        "native control outcome requires reconciliation; retry the original request after restoring backend access",
    )
}

fn creation_error(error: WorkspaceError) -> ManagerError {
    match error {
        // The native control rows may already have committed before a lost
        // reply, then progressed through a mount. Strict replay must retain
        // the original IDs and require reconciliation, never reset the head
        // or downgrade that uncertain outcome to a definitive failure.
        WorkspaceError::Conflict(_) => ManagerError::new(
            "recovery_required",
            "recorded native creation identity cannot be reconciled; retain the journal and inspect the catalog",
        ),
        other => catalog_error(other),
    }
}

pub(super) async fn header(
    conn: &CatalogConnection,
    catalog_header: &VolumeHeader,
) -> Result<NativeVolumeHeader> {
    let (control, namespace) = control(conn)?;
    let header = load_volume_header(control, namespace, NativeRuntimeCapabilities::compiled())
        .await
        .map_err(admission_error)?;
    validate_p1_volume(&header, namespace, catalog_header).map_err(catalog_error)?;
    Ok(header)
}

/// Read-only admission before claiming or creating the catalog carrier. A
/// malformed or foreign native locator must never be overwritten or adopted.
pub(super) async fn check_initialize_target(
    conn: &CatalogConnection,
    volume_id: Uuid,
) -> Result<()> {
    let namespace = conn.namespace().ok_or_else(|| {
        ManagerError::new(
            "unsupported",
            "native P1 requires a shared control namespace",
        )
    })?;
    let expected = NativeVolumeHeader::p1(
        *volume_id.as_bytes(),
        p1_storage_namespace_id(namespace, &volume_id),
    );
    if conn
        .native_locator()
        .await?
        .is_some_and(|bytes| bytes != expected.encode())
    {
        return Err(conflict(
            "native namespace locator differs from the recorded initialization identity",
        ));
    }
    Ok(())
}

pub(super) async fn initialize(
    conn: &CatalogConnection,
    volume_id: Uuid,
) -> Result<NativeVolumeHeader> {
    require_guard(conn)?;
    let (control, namespace) = control(conn)?;
    let expected = NativeVolumeHeader::p1(
        *volume_id.as_bytes(),
        p1_storage_namespace_id(namespace, &volume_id),
    );
    match load_volume_header(control, namespace, NativeRuntimeCapabilities::compiled()).await {
        Ok(found) if found != expected => {
            return Err(conflict(
                "native namespace already belongs to a different volume identity",
            ));
        }
        Ok(_) => {}
        Err(RuntimeAdmissionError::MissingHeader(_)) => {
            // An uncertain reply is reconciled below against the identical
            // expected header. No retry ever replaces an existing header.
            if let Err(error) = initialize_volume(
                control,
                namespace,
                &expected,
                NativeRuntimeCapabilities::compiled(),
            )
            .await
            {
                match load_volume_header(control, namespace, NativeRuntimeCapabilities::compiled())
                    .await
                {
                    Ok(found) if found == expected => {}
                    Ok(_) => {
                        return Err(conflict(
                            "native namespace initialization collided with a different identity",
                        ));
                    }
                    Err(_) => return Err(admission_error(error)),
                }
            }
        }
        Err(error) => return Err(admission_error(error)),
    }
    // Even an idempotent existing-header result must validate the current
    // ownership bytes and deadline inside GuardedControlStore's transaction.
    control
        .run(Txn::new().check_bytes(Keys::volume_header(namespace), expected.encode()))
        .await
        .map_err(|_| unavailable())?;
    Ok(expected)
}

pub(super) async fn ensure(
    conn: &CatalogConnection,
    header: &NativeVolumeHeader,
    workspace_id: WorkspaceId,
    creation: &NativeCreation,
) -> Result<()> {
    require_guard(conn)?;
    let (control, namespace) = control(conn)?;
    ensure_p1_control(
        control,
        header,
        namespace,
        &NativeP1Workspace {
            workspace_id,
            domain_id: creation.domain_id,
            owner_id: creation.owner_id,
            initial_epoch: 0,
            initial_generation: 1,
        },
    )
    .await
    .map_err(creation_error)?;
    Ok(())
}

pub(super) async fn inspect(
    conn: &CatalogConnection,
    header: &NativeVolumeHeader,
    workspace_id: WorkspaceId,
) -> Result<serde_json::Value> {
    let (control, namespace) = control(conn)?;
    let view = inspect_p1_control(control, header, namespace, workspace_id)
        .await
        .map_err(catalog_error)?;
    serde_json::to_value(view).map_err(|_| unavailable())
}

pub(super) async fn mark_deleting(
    conn: &CatalogConnection,
    header: &NativeVolumeHeader,
    workspace_id: WorkspaceId,
) -> Result<()> {
    require_guard(conn)?;
    let (control, namespace) = control(conn)?;
    mark_p1_deleting(control, header, namespace, workspace_id)
        .await
        .map_err(catalog_error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn malformed_headers_are_conflicts_and_native_backend_details_stay_private() {
        let corrupted = admission_error(RuntimeAdmissionError::InvalidHeader(
            "checksum mismatch: private backend diagnostic".into(),
        ));
        assert_eq!(corrupted.code, "conflict");
        assert!(!corrupted.message.contains("private backend diagnostic"));
        let backend = admission_error(RuntimeAdmissionError::Store(
            crate::native_base::write::store::StoreError::Backend(
                "redis://private-user:private-token@private-host/0".into(),
            ),
        ));
        assert_eq!(backend.code, "recovery_required");
        for private in ["private-user", "private-token", "private-host"] {
            assert!(!backend.message.contains(private));
        }
        let frozen = admission_error(RuntimeAdmissionError::UnavailableRequiredFeatures(
            crate::native_base::runtime::FROZEN_METADATA_FEATURE,
        ));
        assert_eq!(frozen.code, "unsupported");
    }

    #[test]
    fn native_creation_identity_conflicts_preserve_an_uncertain_outcome() {
        let error = creation_error(WorkspaceError::Conflict(
            crate::workspace_overlay::error::ConflictDetail {
                path: b"private-native-key".to_vec(),
                reason: "private progressed head identity".into(),
            },
        ));
        assert_eq!(error.code, "recovery_required");
        assert!(!error.message.contains("private"));
        assert_eq!(
            creation_error(WorkspaceError::UnsupportedCapability("native profile")).code,
            "unsupported"
        );
    }
}
