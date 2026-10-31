//! Administration of the native P1 runtime's immutable catalog lower.
//!
//! This profile uses the same workspace-v1 namespace and data source as the
//! native CLI. It does not accept PublishedRevision or frozen metadata and
//! never manufactures publication, drain, or physical cleanup evidence.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::native_base::wire::bnct::{
    ControlRecord, DomainKind, DomainState, HeadRef, OwnershipDomain,
};
use crate::native_base::write::domain::decode_domain;
use crate::native_base::write::keys::Keys;
use crate::native_base::write::records::HeadState;
use crate::native_base::write::store::{ControlStore, StoreError, Txn};
use crate::workspace_overlay::admin::validate_volume_header;
use crate::workspace_overlay::error::{ConflictDetail, WorkspaceError};
use crate::workspace_overlay::ids::WorkspaceId;
use crate::workspace_overlay::model::{BaseRevision, VolumeHeader};

use super::{NativeRuntimeCapabilities, NativeVolumeHeader};

/// A catalog lower selected under an exact native volume identity. A native
/// published snapshot is a different revision type and is not represented here.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeP1Revision {
    pub volume_id: Uuid,
    pub storage_namespace_id: [u8; 16],
    pub catalog_revision: BaseRevision,
}

/// Persist these identities before either the native or catalog creation.
/// The native head ID equals workspace_id, as required by the P1 runtime;
/// the catalog writable-layer ID is independently persisted by its caller.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeP1Workspace {
    pub workspace_id: WorkspaceId,
    pub domain_id: Uuid,
    pub owner_id: Uuid,
    pub initial_epoch: u64,
    pub initial_generation: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NativeP1HeadView {
    pub head_id: Uuid,
    pub epoch: u64,
    pub commit_seq: u64,
    pub writer_generation: u64,
    pub domain_id: Uuid,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NativeP1DomainView {
    pub domain_id: Uuid,
    pub namespace_id: Uuid,
    pub owner_id: Uuid,
    pub owner_generation: u64,
    pub state: String,
    pub entity_version: u64,
    pub inventory_seq: u64,
    pub retention_seq: u64,
    // These are actual persisted fields, not evidence of a completed drain.
    pub outstanding_attempts: u64,
    pub open_operations: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NativeP1ControlView {
    pub workspace_id: WorkspaceId,
    pub head: Option<NativeP1HeadView>,
    pub domain: Option<NativeP1DomainView>,
    pub writer_lease_present: bool,
    pub deleting: bool,
}

/// The namespace derivation used by the existing native CLI.
pub fn p1_storage_namespace_id(namespace: &str, volume_id: &Uuid) -> [u8; 16] {
    let mut digest = Sha256::new();
    digest.update(b"brewfs/native-storage-namespace/v2");
    digest.update(namespace.as_bytes());
    digest.update(volume_id.as_bytes());
    digest.finalize()[..16].try_into().unwrap()
}

/// Validate both headers without treating the native format as a catalog
/// schema upgrade. P1 deliberately retains the workspace-v1 schema-1 catalog.
pub fn validate_p1_volume(
    header: &NativeVolumeHeader,
    namespace: &str,
    catalog: &VolumeHeader,
) -> Result<(), WorkspaceError> {
    validate_p1_header(header, namespace)?;
    validate_volume_header(catalog, Uuid::from_bytes(header.volume_id))
}

fn validate_p1_header(header: &NativeVolumeHeader, namespace: &str) -> Result<(), WorkspaceError> {
    header
        .validate(NativeRuntimeCapabilities::compiled())
        .map_err(|error| WorkspaceError::UnsupportedVolumeFormat(error.to_string()))?;
    if header.required_features != 0 {
        return Err(WorkspaceError::UnsupportedCapability(
            "native P1 catalog lower without frozen metadata",
        ));
    }
    if namespace.is_empty()
        || namespace.len() > 128
        || !namespace
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
    {
        return Err(WorkspaceError::CorruptMetadata(
            "invalid native control namespace".into(),
        ));
    }
    if header.storage_namespace_id
        != p1_storage_namespace_id(namespace, &Uuid::from_bytes(header.volume_id))
    {
        return Err(conflict("native storage namespace identity changed"));
    }
    Ok(())
}

fn deletion_bytes(header: &NativeVolumeHeader, workspace_id: WorkspaceId) -> Vec<u8> {
    let mut bytes = b"BNMD\x01".to_vec();
    bytes.extend_from_slice(&header.volume_id);
    bytes.extend_from_slice(workspace_id.as_bytes());
    bytes
}

struct Snapshot {
    checks: Txn,
    head: Option<HeadState>,
    domain: Option<OwnershipDomain>,
    deleting: bool,
    writer_lease_present: bool,
}

impl Snapshot {
    fn view(&self, workspace_id: WorkspaceId) -> NativeP1ControlView {
        NativeP1ControlView {
            workspace_id,
            head: self.head.as_ref().map(|head| NativeP1HeadView {
                head_id: Uuid::from_bytes(head.head.head_id),
                epoch: head.head.epoch,
                commit_seq: head.head.commit_seq,
                writer_generation: head.writer_generation,
                domain_id: Uuid::from_bytes(head.write_domain_id),
            }),
            domain: self.domain.as_ref().map(|domain| NativeP1DomainView {
                domain_id: Uuid::from_bytes(domain.domain_id),
                namespace_id: Uuid::from_bytes(domain.namespace_id),
                owner_id: Uuid::from_bytes(domain.owner_id),
                owner_generation: domain.owner_generation,
                state: match domain.state {
                    DomainState::Active => "active",
                    DomainState::Draining => "draining",
                    DomainState::Quarantined => "quarantined",
                    DomainState::Closed => "closed",
                    DomainState::Cleaning => "cleaning",
                    DomainState::Cleaned => "cleaned",
                }
                .into(),
                entity_version: domain.entity_version,
                inventory_seq: domain.inventory_seq,
                retention_seq: domain.retention_seq,
                outstanding_attempts: domain.outstanding_attempts,
                open_operations: domain.open_operations,
            }),
            writer_lease_present: self.writer_lease_present,
            deleting: self.deleting,
        }
    }
}

async fn checked_read(
    store: &dyn ControlStore,
    checks: &mut Txn,
    key: Vec<u8>,
) -> Result<Option<Vec<u8>>, WorkspaceError> {
    let bytes = store.get(&key).await.map_err(store_error)?;
    checks.checks.push((
        key,
        match &bytes {
            Some(bytes) => crate::native_base::write::store::Expect::Bytes(bytes.clone()),
            None => crate::native_base::write::store::Expect::Absent,
        },
    ));
    Ok(bytes)
}

async fn snapshot(
    store: &dyn ControlStore,
    header: &NativeVolumeHeader,
    namespace: &str,
    workspace_id: WorkspaceId,
) -> Result<Snapshot, WorkspaceError> {
    validate_p1_header(header, namespace)?;
    if workspace_id.as_uuid().is_nil() {
        return Err(conflict("native workspace ID must be nonzero"));
    }
    let keys = Keys::new(&header.volume_id);
    let id = workspace_id.as_bytes();
    let mut checks = Txn::new();
    if checked_read(store, &mut checks, Keys::volume_header(namespace)).await?
        != Some(header.encode())
    {
        return Err(conflict("native volume header changed or is missing"));
    }
    for key in [keys.workspace_view(id), keys.fork_base(id)] {
        if checked_read(store, &mut checks, key).await?.is_some() {
            return Err(WorkspaceError::UnsupportedCapability(
                "native published snapshots require a different lower-data adapter",
            ));
        }
    }
    let marker = checked_read(store, &mut checks, keys.workspace_deleted(id)).await?;
    if marker
        .as_ref()
        .is_some_and(|bytes| *bytes != deletion_bytes(header, workspace_id))
    {
        return Err(WorkspaceError::CorruptMetadata(
            "native deletion marker has a different identity or encoding".into(),
        ));
    }
    let writer_lease_present = checked_read(store, &mut checks, keys.writer_lease(id))
        .await?
        .is_some();
    let head = checked_read(store, &mut checks, keys.head(id))
        .await?
        .map(|bytes| HeadState::decode(&bytes).map_err(record_error))
        .transpose()?;
    let domain = if let Some(head) = &head {
        if head.head.head_id != *id || head.write_domain_id == [0; 16] {
            return Err(conflict(
                "native head has a different P1 workspace identity",
            ));
        }
        let bytes = checked_read(store, &mut checks, keys.domain(&head.write_domain_id))
            .await?
            .ok_or_else(|| WorkspaceError::CorruptMetadata("native head has no domain".into()))?;
        let domain = decode_domain(&bytes).map_err(record_error)?;
        if domain.domain_id != head.write_domain_id
            || domain.volume_id != header.volume_id
            || domain.namespace_id != *id
            || domain.domain_kind != DomainKind::Workspace
        {
            return Err(conflict("native ownership domain has a different identity"));
        }
        Some(domain)
    } else {
        None
    };
    Ok(Snapshot {
        checks,
        head,
        domain,
        deleting: marker.is_some(),
        writer_lease_present,
    })
}

/// Read actual control state. A zero-write conditional transaction validates
/// all rows together, so the result never mixes two head/domain generations.
pub async fn inspect_p1_control(
    store: &dyn ControlStore,
    header: &NativeVolumeHeader,
    namespace: &str,
    workspace_id: WorkspaceId,
) -> Result<NativeP1ControlView, WorkspaceError> {
    for _ in 0..4 {
        let found = snapshot(store, header, namespace, workspace_id).await?;
        let view = found.view(workspace_id);
        match store.run(found.checks).await {
            Ok(()) => return Ok(view),
            Err(StoreError::Conflict) => continue,
            Err(error) => return Err(store_error(error)),
        }
    }
    Err(WorkspaceError::Busy)
}

/// Create the P1 head and private domain together before exposing the catalog
/// child. Replay accepts only the exact untouched creation identity. A mount
/// or mutation which already advanced it requires explicit recovery.
pub async fn ensure_p1_control(
    store: &dyn ControlStore,
    header: &NativeVolumeHeader,
    namespace: &str,
    request: &NativeP1Workspace,
) -> Result<NativeP1ControlView, WorkspaceError> {
    if request.domain_id.is_nil()
        || request.owner_id.is_nil()
        || request.initial_epoch != 0
        || request.initial_generation != 1
    {
        return Err(conflict("invalid initial native P1 creation identity"));
    }
    let id = request.workspace_id.as_bytes();
    let keys = Keys::new(&header.volume_id);
    let expected_head = HeadState {
        head: HeadRef {
            head_id: *id,
            epoch: 0,
            commit_seq: 0,
        },
        writer_generation: 1,
        write_domain_id: *request.domain_id.as_bytes(),
    };
    let expected_domain = OwnershipDomain {
        domain_id: *request.domain_id.as_bytes(),
        volume_id: header.volume_id,
        namespace_id: *id,
        domain_kind: DomainKind::Workspace,
        owner_id: *request.owner_id.as_bytes(),
        owner_generation: 1,
        state: DomainState::Active,
        entity_version: 1,
        inventory_seq: 0,
        retention_seq: 0,
        outstanding_attempts: 0,
        open_operations: 0,
        accepted_ticket_end: None,
        close_ref: None,
    };
    let mut last_error = WorkspaceError::Busy;
    for _ in 0..4 {
        let mut found = snapshot(store, header, namespace, request.workspace_id).await?;
        if found.deleting {
            return Err(WorkspaceError::Fenced);
        }
        if found.writer_lease_present {
            return Err(WorkspaceError::Busy);
        }
        if found.head.is_some() {
            if found.head.as_ref() != Some(&expected_head)
                || found.domain.as_ref() != Some(&expected_domain)
            {
                return Err(conflict(
                    "native P1 creation identity changed or progressed",
                ));
            }
        } else {
            if checked_read(
                store,
                &mut found.checks,
                keys.domain(request.domain_id.as_bytes()),
            )
            .await?
            .is_some()
            {
                return Err(conflict("native P1 domain ID is already owned"));
            }
            found.checks = found.checks.put(keys.head(id), expected_head.encode()).put(
                keys.domain(request.domain_id.as_bytes()),
                ControlRecord::OwnershipDomain(expected_domain.clone()).encode(),
            );
            found.head = Some(expected_head.clone());
            found.domain = Some(expected_domain.clone());
        }
        let view = found.view(request.workspace_id);
        match store.run(found.checks).await {
            Ok(()) => return Ok(view),
            // Re-read after an ambiguous reply as well as a CAS race. The
            // following iteration accepts only the exact pristine identity.
            Err(error) => last_error = store_error(error),
        }
    }
    Err(last_error)
}

/// Persist an admission/publication fence after the caller's lease-aware
/// catalog deletion marker. The marker never deletes objects, changes a
/// retention record, or claims that accepted uploads have drained.
pub async fn mark_p1_deleting(
    store: &dyn ControlStore,
    header: &NativeVolumeHeader,
    namespace: &str,
    workspace_id: WorkspaceId,
) -> Result<(), WorkspaceError> {
    let keys = Keys::new(&header.volume_id);
    let mut last_error = WorkspaceError::Busy;
    for _ in 0..4 {
        let mut found = snapshot(store, header, namespace, workspace_id).await?;
        if found.writer_lease_present {
            return Err(WorkspaceError::Busy);
        }
        if !found.deleting {
            found.checks = found.checks.put(
                keys.workspace_deleted(workspace_id.as_bytes()),
                deletion_bytes(header, workspace_id),
            );
        }
        if let Some(mut domain) = found.domain {
            if domain.state == DomainState::Active {
                // Quarantine already fences the old runtime's registration
                // and commit paths. Preserve every inventory/retention field:
                // this is authority rollback, never a close/drain certificate.
                domain.state = DomainState::Quarantined;
                domain.entity_version = domain.entity_version.checked_add(1).ok_or_else(|| {
                    WorkspaceError::CorruptMetadata("native domain version overflow".into())
                })?;
                found.checks = found.checks.put(
                    keys.domain(&domain.domain_id),
                    ControlRecord::OwnershipDomain(domain).encode(),
                );
            }
        } else {
            // A mount can have acquired the catalog lease before deletion,
            // then reach lazy native initialization much later. A real
            // quarantined head/domain also fences older clients which do not
            // understand the new marker. IDs are deterministic for recovery.
            let mut digest = Sha256::new();
            digest.update(b"brewfs/native-p1/deletion-domain/v1");
            digest.update(header.volume_id);
            digest.update(workspace_id.as_bytes());
            let domain_id: [u8; 16] = digest.finalize()[..16].try_into().unwrap();
            if checked_read(store, &mut found.checks, keys.domain(&domain_id))
                .await?
                .is_some()
            {
                return Err(conflict(
                    "native deletion sentinel domain ID is already owned",
                ));
            }
            let domain = OwnershipDomain {
                domain_id,
                volume_id: header.volume_id,
                namespace_id: *workspace_id.as_bytes(),
                domain_kind: DomainKind::Workspace,
                owner_id: *workspace_id.as_bytes(),
                owner_generation: 1,
                state: DomainState::Quarantined,
                entity_version: 1,
                inventory_seq: 0,
                retention_seq: 0,
                outstanding_attempts: 0,
                open_operations: 0,
                accepted_ticket_end: None,
                close_ref: None,
            };
            let head = HeadState {
                head: HeadRef {
                    head_id: *workspace_id.as_bytes(),
                    epoch: 0,
                    commit_seq: 0,
                },
                writer_generation: 1,
                write_domain_id: domain_id,
            };
            found.checks = found
                .checks
                .put(keys.head(workspace_id.as_bytes()), head.encode())
                .put(
                    keys.domain(&domain_id),
                    ControlRecord::OwnershipDomain(domain).encode(),
                );
        }
        match store.run(found.checks).await {
            Ok(()) => return Ok(()),
            Err(error) => last_error = store_error(error),
        }
    }
    Err(last_error)
}

fn record_error(error: impl std::fmt::Display) -> WorkspaceError {
    WorkspaceError::CorruptMetadata(error.to_string())
}

fn store_error(error: StoreError) -> WorkspaceError {
    match error {
        StoreError::Conflict => conflict("native control state changed concurrently"),
        StoreError::Backend(error) => WorkspaceError::Backend(error),
    }
}

fn conflict(reason: &str) -> WorkspaceError {
    WorkspaceError::Conflict(ConflictDetail {
        path: Vec::new(),
        reason: reason.into(),
    })
}
