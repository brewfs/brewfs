//! Ownership domains and upload registration (spec 20 §2/§6).
//!
//! The order is fixed and non-negotiable: an object is *registered* in an
//! ACTIVE write domain under a valid head guard (unique object/attempt
//! reservation, inventory sequence appended), only then *dispatched* to the
//! object backend, and only after the upload is durable does the commit
//! transaction verify the registration and flip it to `Verified`. Nothing
//! ever dispatches an unregistered object, and nothing commits against an
//! unverified one.

use crate::native_base::wire::bnct::{
    ControlRecord, DomainState, ObjectRegistration, OwnershipDomain, RegistrationState,
};
use crate::native_base::wire::refs::{Hash32, ObjectId, ObjectRef};

use super::error::WriteError;
use super::keys::Keys;
use super::records::HeadState;
use super::store::{ControlStore, Txn};

/// Decode a kind 1 envelope into an [`OwnershipDomain`].
pub fn decode_domain(bytes: &[u8]) -> Result<OwnershipDomain, WriteError> {
    match ControlRecord::decode(bytes)? {
        ControlRecord::OwnershipDomain(d) => Ok(d),
        other => Err(WriteError::Record(format!(
            "expected ownership domain, got kind {}",
            other.kind().as_u16()
        ))),
    }
}

/// Decode a kind 2 envelope into an [`ObjectRegistration`].
pub fn decode_registration(bytes: &[u8]) -> Result<ObjectRegistration, WriteError> {
    match ControlRecord::decode(bytes)? {
        ControlRecord::ObjectRegistration(r) => Ok(r),
        other => Err(WriteError::Record(format!(
            "expected object registration, got kind {}",
            other.kind().as_u16()
        ))),
    }
}

/// Create a domain. Fails with a conflict if it already exists.
pub async fn create_domain(
    store: &dyn ControlStore,
    keys: &Keys,
    domain: OwnershipDomain,
) -> Result<(), WriteError> {
    let key = keys.domain(&domain.domain_id);
    let encoded = ControlRecord::OwnershipDomain(domain).encode();
    store
        .run(Txn::new().check_absent(key.clone()).put(key, encoded))
        .await?;
    Ok(())
}

/// The head guard every registration runs under: the workspace identity and
/// the exact head state the caller holds.
#[derive(Debug, Clone)]
pub struct HeadGuard {
    pub workspace_id: [u8; 16],
    pub expected_head: HeadState,
}

/// The result of a successful (or previously completed) registration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadReservation {
    pub object_ref: ObjectRef,
    pub registration: ObjectRegistration,
    pub inventory_seq: u64,
}

/// Register an upload: reserve the unique `(namespace, object key) ->
/// ObjectId` binding, append the inventory sequence, and write the
/// registration in `Registered` state — all in one atomic transaction under
/// the head guard (spec 20 §6).
///
/// Idempotent: if this exact object is already registered the existing
/// reservation is returned unchanged. Registering the same key for a
/// different `ObjectId` is a permanent [`WriteError::RegistryConflict`]
/// (spec 01 §6: one registry key, one identity).
pub async fn register_upload(
    store: &dyn ControlStore,
    keys: &Keys,
    guard: &HeadGuard,
    domain_id: &[u8; 16],
    object_ref: ObjectRef,
    upload_plan_hash: Hash32,
) -> Result<UploadReservation, WriteError> {
    // Read the domain and registry outside the transaction, then guard both
    // reads with expected-value checks inside it (optimistic concurrency).
    let domain_key = keys.domain(domain_id);
    let domain_bytes = store
        .get(&domain_key)
        .await?
        .ok_or_else(|| WriteError::Record("ownership domain not found".into()))?;
    let domain = decode_domain(&domain_bytes)?;
    if domain.state != DomainState::Active {
        return Err(WriteError::DomainNotActive(format!(
            "domain state is {:?}, must be Active to register uploads",
            domain.state
        )));
    }
    if domain.volume_id != keys_volume_id(keys) {
        return Err(WriteError::Record(
            "domain belongs to a different volume".into(),
        ));
    }

    let registry_key = keys.registry(&domain.namespace_id, &object_ref.key);
    if let Some(existing) = store.get(&registry_key).await? {
        let existing_id: ObjectId = existing
            .as_slice()
            .try_into()
            .map_err(|_| WriteError::Record("registry binding is not 16 bytes".into()))?;
        if existing_id != object_ref.object_id {
            return Err(WriteError::RegistryConflict(format!(
                "object key already bound to object id {:02x?}",
                existing_id
            )));
        }
        // Same identity: return the existing registration (idempotent).
        let object_key = keys.object(domain_id, &object_ref.object_id);
        let registration =
            decode_registration(&store.get(&object_key).await?.ok_or_else(|| {
                WriteError::Record("registry binding without registration".into())
            })?)?;
        return Ok(UploadReservation {
            object_ref,
            registration,
            inventory_seq: domain.inventory_seq,
        });
    }

    let object_key = keys.object(domain_id, &object_ref.object_id);
    if let Some(existing) = store.get(&object_key).await? {
        let registration = decode_registration(&existing)?;
        if registration.object_ref.object_id != object_ref.object_id
            || registration.domain_id != *domain_id
        {
            return Err(WriteError::RegistrationMismatch(
                "object id already registered with a different identity".into(),
            ));
        }
        // Registration exists but the registry binding is missing: not
        // something to paper over.
        return Err(WriteError::Record(
            "object registration exists without its registry binding".into(),
        ));
    }

    let inventory_seq = domain.inventory_seq + 1;
    let registration = ObjectRegistration {
        object_ref: object_ref.clone(),
        domain_id: *domain_id,
        upload_plan_hash,
        registration_seq: inventory_seq,
        attempt_generation: 1,
        state: RegistrationState::Registered,
    };

    let mut next_domain = domain.clone();
    next_domain.inventory_seq = inventory_seq;
    next_domain.entity_version += 1;

    let head_key = keys.head(&guard.workspace_id);
    let txn = Txn::new()
        .check_bytes(head_key, guard.expected_head.encode())
        .check_bytes(domain_key, domain_bytes)
        .check_absent(registry_key.clone())
        .check_absent(object_key.clone())
        .put(registry_key, object_ref.object_id.to_vec())
        .put(
            object_key,
            ControlRecord::ObjectRegistration(registration.clone()).encode(),
        )
        .put(
            keys.inventory(domain_id, inventory_seq),
            object_ref.object_id.to_vec(),
        )
        .put(
            keys.domain(domain_id),
            ControlRecord::OwnershipDomain(next_domain).encode(),
        );
    store.run(txn).await.map_err(|e| match e {
        super::store::StoreError::Conflict => {
            WriteError::Conflict("upload registration raced with a concurrent writer".into())
        }
        other => other.into(),
    })?;

    Ok(UploadReservation {
        object_ref,
        registration,
        inventory_seq,
    })
}

/// Mark a registration `Dispatched`: the upload is handed to the object
/// backend (spec 20 §6: register, then dispatch).
pub async fn mark_dispatched(
    store: &dyn ControlStore,
    keys: &Keys,
    domain_id: &[u8; 16],
    object_id: &ObjectId,
) -> Result<ObjectRegistration, WriteError> {
    let object_key = keys.object(domain_id, object_id);
    let bytes = store
        .get(&object_key)
        .await?
        .ok_or_else(|| WriteError::Record("object registration not found".into()))?;
    let registration = decode_registration(&bytes)?;
    match registration.state {
        RegistrationState::Dispatched | RegistrationState::Verified => return Ok(registration),
        RegistrationState::Registered => {}
        other => {
            return Err(WriteError::RegistrationMismatch(format!(
                "registration state {other:?} cannot dispatch"
            )));
        }
    }
    let mut dispatched = registration.clone();
    dispatched.state = RegistrationState::Dispatched;
    store
        .run(Txn::new().check_bytes(object_key.clone(), bytes).put(
            object_key,
            ControlRecord::ObjectRegistration(dispatched.clone()).encode(),
        ))
        .await?;
    Ok(dispatched)
}

/// The registry binding for one object key, for commit-time verification.
pub async fn registry_binding(
    store: &dyn ControlStore,
    keys: &Keys,
    namespace_id: &[u8; 16],
    object_key: &[u8],
) -> Result<Option<ObjectId>, WriteError> {
    let bytes = store.get(&keys.registry(namespace_id, object_key)).await?;
    match bytes {
        None => Ok(None),
        Some(v) => v
            .as_slice()
            .try_into()
            .map(Some)
            .map_err(|_| WriteError::Record("registry binding is not 16 bytes".into())),
    }
}

/// Volume id of the `Keys` instance — domains registered through this
/// pipeline must belong to the same volume.
fn keys_volume_id(keys: &Keys) -> [u8; 16] {
    keys.volume_id()
}
