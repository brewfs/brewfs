//! Lease and lower-pin authority for the reference current-entity namespace.
//!
//! An expired lease fences mutations, but does not prove that old readers have
//! stopped. There is deliberately no timeout-only takeover or reclamation API.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use tokio::sync::Notify;
use uuid::Uuid;

use super::backend::{
    CommitOutcome, EntityBackend, EntityPlan, RowCheck, RowSnapshot, RowWrite, TransactionAuthority,
};
use super::codec::{decode, encode};
use super::keys::EntityKeys;
use super::model::{ENTITY_FORMAT, ENTITY_WRITEBACK_FORMAT, LowerBinding};
use super::ownership::{SliceCensus, SliceState};
use super::{EntityError, Result};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LeaseIdentity {
    pub lease_id: Uuid,
    pub holder_generation: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum NamespaceState {
    Ready,
    Active,
    Quiescent,
    Deleting,
    Deleted,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NamespaceDescriptor {
    pub format: String,
    pub namespace_id: Uuid,
    pub lower: LowerBinding,
    pub state: NamespaceState,
    pub generation: u64,
    pub active_lease: Option<LeaseIdentity>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WriterLease {
    pub namespace_id: Uuid,
    pub identity: LeaseIdentity,
    pub expires_ns: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LowerRegistry {
    pub binding: LowerBinding,
    pub owners: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LowerOwner {
    pub namespace_id: Uuid,
    pub binding: LowerBinding,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct MaintenanceRecord {
    namespace_id: Uuid,
    lease: LeaseIdentity,
    token: Uuid,
}

#[derive(Debug, Default)]
struct AdmissionState {
    closed: bool,
    active: usize,
}

#[derive(Debug, Default)]
struct Admission {
    state: Mutex<AdmissionState>,
    drained: Notify,
}

/// Every retained read plan, open handle and outstanding upload must retain an
/// admission guard. A metadata call's guard alone does not protect later I/O.
#[derive(Clone, Debug)]
pub struct WriterSession {
    namespace_id: Uuid,
    namespace_prefix: Vec<u8>,
    identity: LeaseIdentity,
    /// Immutable identity authenticated when this writer incarnation starts.
    /// Runtime reads may refresh authority, but cannot retarget retained handles
    /// or lower cache entries to another manifest or inode namespace boundary.
    lower: LowerBinding,
    admission: Arc<Admission>,
    /// All wrappers for this writer share affected-inode coordination.
    pub(super) inode_locks: Arc<Mutex<super::namespace::InodeLockRegistry>>,
    io_failures: Arc<Mutex<BTreeMap<i64, String>>>,
}

#[derive(Debug)]
pub struct OperationGuard {
    admission: Arc<Admission>,
}

impl Drop for OperationGuard {
    fn drop(&mut self) {
        let mut state = self
            .admission
            .state
            .lock()
            .expect("admission mutex poisoned");
        state.active -= 1;
        if state.active == 0 {
            self.admission.drained.notify_waiters();
        }
    }
}

impl WriterSession {
    pub(super) fn remember_io_failure(&self, ino: i64, message: String) {
        self.io_failures
            .lock()
            .expect("I/O failure registry poisoned")
            .entry(ino)
            .or_insert(message);
    }
    pub(super) fn io_failure(&self, ino: i64) -> Option<String> {
        self.io_failures
            .lock()
            .expect("I/O failure registry poisoned")
            .get(&ino)
            .cloned()
    }
    pub(super) fn matches_keys(&self, keys: &EntityKeys) -> bool {
        self.namespace_id == keys.namespace_id() && self.namespace_prefix == keys.prefix()
    }
    pub fn namespace_id(&self) -> Uuid {
        self.namespace_id
    }
    pub fn identity(&self) -> LeaseIdentity {
        self.identity
    }
    pub(super) fn lower_binding(&self) -> &LowerBinding {
        &self.lower
    }
    pub fn is_closed(&self) -> bool {
        self.admission
            .state
            .lock()
            .expect("admission mutex poisoned")
            .closed
    }
    pub fn admit(&self) -> Result<OperationGuard> {
        let mut state = self
            .admission
            .state
            .lock()
            .expect("admission mutex poisoned");
        if state.closed {
            return Err(EntityError::Fenced);
        }
        state.active = state.active.checked_add(1).ok_or(EntityError::Overflow)?;
        Ok(OperationGuard {
            admission: self.admission.clone(),
        })
    }
    pub(super) async fn close_and_drain(&self) {
        loop {
            // Register the waiter before inspecting the count, avoiding the
            // last guard's notification racing the first await.
            let notified = self.admission.drained.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut state = self
                    .admission
                    .state
                    .lock()
                    .expect("admission mutex poisoned");
                state.closed = true;
                if state.active == 0 {
                    return;
                }
            }
            notified.await;
        }
    }
}

/// Constructible only after this process has closed and drained its session.
/// This reference token is not a cross-process fencing certificate.
#[derive(Clone, Debug)]
pub struct MaintenanceToken {
    namespace_id: Uuid,
    namespace_prefix: Vec<u8>,
    record: MaintenanceRecord,
}

/// Terminal proof for releasing the corresponding collector-visible packed
/// lower owner. The namespace cannot reopen, and its upper objects are already
/// retired/deleted. The packed collector must still transact its own owner/root
/// predicates; this value is not permission to delete immutable objects directly.
#[derive(Clone, Debug)]
pub struct DeletedLowerOwnerProof {
    volume_id: Uuid,
    namespace_id: Uuid,
    namespace_prefix: Vec<u8>,
    binding: LowerBinding,
}

impl DeletedLowerOwnerProof {
    pub fn volume_id(&self) -> Uuid {
        self.volume_id
    }
    pub fn namespace_id(&self) -> Uuid {
        self.namespace_id
    }
    pub fn namespace_prefix(&self) -> &[u8] {
        &self.namespace_prefix
    }
    pub fn binding(&self) -> &LowerBinding {
        &self.binding
    }
}

pub struct Lifecycle<B> {
    backend: Arc<B>,
    keys: EntityKeys,
}

impl<B: EntityBackend> Lifecycle<B> {
    pub fn new(backend: Arc<B>, keys: EntityKeys) -> Self {
        Self { backend, keys }
    }
    pub fn keys(&self) -> &EntityKeys {
        &self.keys
    }

    pub async fn descriptor(&self) -> Result<NamespaceDescriptor> {
        let snapshot = self.backend.read(&[self.keys.descriptor()]).await?;
        self.decode_descriptor(snapshot.rows[0].as_deref().ok_or(EntityError::NotFound)?)
    }

    fn decode_descriptor(&self, bytes: &[u8]) -> Result<NamespaceDescriptor> {
        let row: NamespaceDescriptor = decode(bytes)?;
        row.lower.validate()?;
        if !matches!(row.format.as_str(), ENTITY_FORMAT | ENTITY_WRITEBACK_FORMAT)
            || row.namespace_id != self.keys.namespace_id()
            || (row.state == NamespaceState::Active) != row.active_lease.is_some()
            || row.active_lease.is_some_and(|lease| {
                lease.lease_id.is_nil()
                    || lease.holder_generation == 0
                    || lease.holder_generation != row.generation
            })
        {
            return Err(EntityError::Corrupt("invalid namespace descriptor".into()));
        }
        Ok(row)
    }

    /// Reference/checkpoint initialization only: this does not authenticate
    /// packed objects or protect them from legacy collectors. Production attach
    /// remains disabled until a qualified immutable-ownership bridge exists.
    /// Initializes the namespace and adds one whole-manifest owner atomically.
    pub async fn provision(&self, lower: &LowerBinding) -> Result<()> {
        lower.validate()?;
        let keys = vec![
            self.keys.descriptor(),
            self.keys.lease(),
            self.keys.lower_pin(),
            self.keys.lower_registry(&lower.manifest_digest),
            self.keys
                .lower_owner(&lower.manifest_digest, self.keys.namespace_id()),
            self.keys.maintenance(),
            self.keys.counter("inode"),
            self.keys.counter("slice"),
        ];
        let snapshot = self.backend.read(&keys).await?;
        if snapshot.rows[0].is_some() {
            return Err(EntityError::Exists);
        }
        if [1, 2, 4, 5, 6]
            .iter()
            .any(|index| snapshot.rows[*index].is_some())
        {
            return Err(EntityError::Corrupt(
                "uninitialized namespace has lifecycle rows".into(),
            ));
        }
        // A missing descriptor is not permission to reinterpret abandoned
        // current rows under a different lower binding or reset their allocator.
        // Before initialization no legitimate writer can add namespace rows;
        // competing initializers are excluded by the descriptor predicate.
        if !self
            .backend
            .scan_prefix(self.keys.prefix(), 4096)
            .await?
            .is_empty()
        {
            return Err(EntityError::Corrupt(
                "uninitialized namespace has entity rows".into(),
            ));
        }
        if let Some(counter) = snapshot.rows[7].as_deref() {
            match super::backend::next_counter(counter) {
                Ok(_) | Err(EntityError::Exhausted) => {}
                Err(error) => return Err(error),
            }
        }
        let mut registry = match &snapshot.rows[3] {
            Some(bytes) => {
                let row: LowerRegistry = decode(bytes)?;
                if &row.binding != lower {
                    return Err(EntityError::Corrupt(
                        "invalid lower ownership registry".into(),
                    ));
                }
                row
            }
            None => LowerRegistry {
                binding: lower.clone(),
                owners: 0,
            },
        };
        registry.owners = registry
            .owners
            .checked_add(1)
            .ok_or(EntityError::Overflow)?;
        let descriptor = NamespaceDescriptor {
            format: ENTITY_FORMAT.into(),
            namespace_id: self.keys.namespace_id(),
            lower: lower.clone(),
            state: NamespaceState::Ready,
            generation: 0,
            active_lease: None,
        };
        let owner = LowerOwner {
            namespace_id: self.keys.namespace_id(),
            binding: lower.clone(),
        };
        let initialize_slice_counter = snapshot.rows[7].is_none();
        let checks = checked_rows(&keys, snapshot.rows);
        let authority = TransactionAuthority::from_validated(checks.clone(), None)?;
        let mut plan = EntityPlan {
            checks,
            writes: vec![
                put(keys[0].clone(), &descriptor)?,
                put(keys[2].clone(), &owner)?,
                put(keys[3].clone(), &registry)?,
                put(keys[4].clone(), &owner)?,
                RowWrite::Put {
                    key: keys[6].clone(),
                    value: lower.max_ino.to_string().into_bytes(),
                },
            ],
        };
        if initialize_slice_counter {
            plan.writes.push(RowWrite::Put {
                key: keys[7].clone(),
                value: b"0".to_vec(),
            });
        }
        committed(self.backend.commit(&authority, &plan).await?)
    }

    /// Canonical authority prefix for a combined authority/entity snapshot.
    /// The session's immutable binding supplies the keys, so discovering them
    /// never needs a separate descriptor read.
    pub(super) fn authority_keys(&self, session: &WriterSession) -> Result<Vec<Vec<u8>>> {
        if !session.matches_keys(&self.keys) {
            return Err(EntityError::Fenced);
        }
        Ok(vec![
            self.keys.descriptor(),
            self.keys.lease(),
            self.keys.lower_pin(),
            self.keys
                .lower_owner(&session.lower.manifest_digest, self.keys.namespace_id()),
            self.keys.lower_registry(&session.lower.manifest_digest),
        ])
    }

    fn decode_runtime_rows(
        &self,
        session: &WriterSession,
        keys: &[Vec<u8>],
        snapshot: RowSnapshot,
    ) -> Result<(NamespaceDescriptor, WriterLease, Vec<RowCheck>, i64)> {
        if snapshot.rows.len() != keys.len() {
            return Err(EntityError::Corrupt(
                "invalid authority snapshot size".into(),
            ));
        }
        let current =
            self.decode_descriptor(snapshot.rows[0].as_deref().ok_or(EntityError::Fenced)?)?;
        if current.lower != session.lower
            || current.state != NamespaceState::Active
            || current.active_lease != Some(session.identity)
        {
            return Err(EntityError::Fenced);
        }
        let lease: WriterLease = decode(snapshot.rows[1].as_deref().ok_or(EntityError::Fenced)?)?;
        if lease.namespace_id != session.namespace_id
            || lease.identity != session.identity
            || lease.expires_ns <= snapshot.now_ns
        {
            return Err(EntityError::Fenced);
        }
        validate_pin(&snapshot.rows[2], &snapshot.rows[3], &current)?;
        validate_registry(&snapshot.rows[4], &current)?;
        Ok((
            current,
            lease,
            checked_rows(keys, snapshot.rows),
            snapshot.now_ns,
        ))
    }

    async fn runtime_rows(
        &self,
        session: &WriterSession,
    ) -> Result<(NamespaceDescriptor, WriterLease, Vec<RowCheck>, i64)> {
        let keys = self.authority_keys(session)?;
        let snapshot = self.backend.read(&keys).await?;
        self.decode_runtime_rows(session, &keys, snapshot)
    }

    /// Validate the authority prefix from one backend snapshot. The eventual
    /// commit still checks these exact rows and backend-time lease expiry.
    pub(super) fn authority_from_snapshot(
        &self,
        session: &WriterSession,
        snapshot: RowSnapshot,
    ) -> Result<TransactionAuthority> {
        if session.is_closed() {
            return Err(EntityError::Fenced);
        }
        let keys = self.authority_keys(session)?;
        let (_, lease, checks, _) = self.decode_runtime_rows(session, &keys, snapshot)?;
        TransactionAuthority::from_validated(checks, Some(lease.expires_ns))
    }

    pub async fn authority(&self, session: &WriterSession) -> Result<TransactionAuthority> {
        if session.is_closed() {
            return Err(EntityError::Fenced);
        }
        let keys = self.authority_keys(session)?;
        let snapshot = self.backend.read(&keys).await?;
        self.authority_from_snapshot(session, snapshot)
    }

    /// Only a never-mounted Ready namespace can be acquired. Lease expiry alone
    /// cannot allow a new writer because retained read/upload obligations remain.
    pub async fn acquire(&self, lease_id: Uuid, ttl_ns: i64) -> Result<WriterSession> {
        if lease_id.is_nil() || ttl_ns <= 0 {
            return Err(EntityError::Invalid("invalid lease request".into()));
        }
        let descriptor = self.descriptor().await?;
        let keys = vec![
            self.keys.descriptor(),
            self.keys.lease(),
            self.keys.lower_pin(),
            self.keys
                .lower_owner(&descriptor.lower.manifest_digest, self.keys.namespace_id()),
            self.keys.lower_registry(&descriptor.lower.manifest_digest),
        ];
        let snapshot = self.backend.read(&keys).await?;
        let mut current =
            self.decode_descriptor(snapshot.rows[0].as_deref().ok_or(EntityError::NotFound)?)?;
        if current != descriptor {
            return Err(EntityError::Conflict);
        }
        if current.state != NamespaceState::Ready || snapshot.rows[1].is_some() {
            return Err(EntityError::Busy);
        }
        validate_pin(&snapshot.rows[2], &snapshot.rows[3], &current)?;
        validate_registry(&snapshot.rows[4], &current)?;
        let generation = current
            .generation
            .checked_add(1)
            .ok_or(EntityError::Exhausted)?;
        let identity = LeaseIdentity {
            lease_id,
            holder_generation: generation,
        };
        let lease = WriterLease {
            namespace_id: self.keys.namespace_id(),
            identity,
            expires_ns: snapshot
                .now_ns
                .checked_add(ttl_ns)
                .ok_or(EntityError::Overflow)?,
        };
        current.generation = generation;
        current.active_lease = Some(identity);
        current.state = NamespaceState::Active;
        let checks = checked_rows(&keys, snapshot.rows);
        let authority =
            TransactionAuthority::from_validated(checks.clone(), Some(lease.expires_ns))?;
        committed(
            self.backend
                .commit(
                    &authority,
                    &EntityPlan {
                        checks,
                        writes: vec![
                            put(keys[0].clone(), &current)?,
                            put(keys[1].clone(), &lease)?,
                        ],
                    },
                )
                .await?,
        )?;
        Ok(WriterSession {
            namespace_id: self.keys.namespace_id(),
            namespace_prefix: self.keys.prefix().to_vec(),
            identity,
            lower: current.lower,
            admission: Arc::default(),
            inode_locks: Arc::default(),
            io_failures: Arc::default(),
        })
    }

    pub async fn renew(&self, session: &WriterSession, ttl_ns: i64) -> Result<()> {
        // Draining closes new admission, but the exact writer must keep its
        // lease alive until retained operations and remote debt have drained.
        if ttl_ns <= 0 {
            return Err(EntityError::Invalid("invalid lease duration".into()));
        }
        let (_, mut lease, checks, now_ns) = self.runtime_rows(session).await?;
        let authority =
            TransactionAuthority::from_validated(checks.clone(), Some(lease.expires_ns))?;
        lease.expires_ns = now_ns.checked_add(ttl_ns).ok_or(EntityError::Overflow)?;
        committed(
            self.backend
                .commit(
                    &authority,
                    &EntityPlan {
                        checks,
                        writes: vec![put(self.keys.lease(), &lease)?],
                    },
                )
                .await?,
        )
    }

    pub async fn enter_maintenance(&self, session: &WriterSession) -> Result<MaintenanceToken> {
        if !session.matches_keys(&self.keys) {
            return Err(EntityError::Fenced);
        }
        session.close_and_drain().await;
        // Resume an uncertain transition only with the same locally drained
        // session, never by adopting an expired or foreign writer identity.
        let retry = self
            .backend
            .read(&[self.keys.descriptor(), self.keys.maintenance()])
            .await?;
        let descriptor =
            self.decode_descriptor(retry.rows[0].as_deref().ok_or(EntityError::Fenced)?)?;
        if descriptor.lower != session.lower {
            return Err(EntityError::Fenced);
        }
        if descriptor.state == NamespaceState::Quiescent {
            let record: MaintenanceRecord =
                decode(retry.rows[1].as_deref().ok_or(EntityError::Fenced)?)?;
            if record.namespace_id != session.namespace_id || record.lease != session.identity {
                return Err(EntityError::Fenced);
            }
            let token = MaintenanceToken {
                namespace_id: session.namespace_id,
                namespace_prefix: self.keys.prefix().to_vec(),
                record,
            };
            self.maintenance_authority(&token).await?;
            return Ok(token);
        }
        let (mut descriptor, lease, mut checks, _) = self.runtime_rows(session).await?;
        if self.backend.read(&[self.keys.remote_pending()]).await?.rows[0].is_some() {
            return Err(EntityError::Busy);
        }
        checks.push(RowCheck {
            key: self.keys.remote_pending(),
            expected: None,
        });
        let record = MaintenanceRecord {
            namespace_id: self.keys.namespace_id(),
            lease: session.identity,
            token: Uuid::new_v4(),
        };
        checks.push(RowCheck {
            key: self.keys.maintenance(),
            expected: None,
        });
        let authority =
            TransactionAuthority::from_validated(checks.clone(), Some(lease.expires_ns))?;
        descriptor.state = NamespaceState::Quiescent;
        descriptor.active_lease = None;
        committed(
            self.backend
                .commit(
                    &authority,
                    &EntityPlan {
                        checks,
                        writes: vec![
                            put(self.keys.descriptor(), &descriptor)?,
                            put(self.keys.maintenance(), &record)?,
                            RowWrite::Delete {
                                key: self.keys.lease(),
                            },
                        ],
                    },
                )
                .await?,
        )?;
        Ok(MaintenanceToken {
            namespace_id: self.keys.namespace_id(),
            namespace_prefix: self.keys.prefix().to_vec(),
            record,
        })
    }

    pub async fn maintenance_authority(
        &self,
        token: &MaintenanceToken,
    ) -> Result<TransactionAuthority> {
        if token.namespace_id != self.keys.namespace_id()
            || token.namespace_prefix != self.keys.prefix()
        {
            return Err(EntityError::Fenced);
        }
        let binding = self.descriptor().await?.lower;
        let keys = vec![
            self.keys.descriptor(),
            self.keys.maintenance(),
            self.keys.lease(),
            self.keys.lower_pin(),
            self.keys
                .lower_owner(&binding.manifest_digest, self.keys.namespace_id()),
            self.keys.lower_registry(&binding.manifest_digest),
            self.keys.remote_pending(),
        ];
        let snapshot = self.backend.read(&keys).await?;
        if snapshot.rows[6].is_some() {
            return Err(EntityError::Busy);
        }
        let descriptor =
            self.decode_descriptor(snapshot.rows[0].as_deref().ok_or(EntityError::Fenced)?)?;
        if !matches!(
            descriptor.state,
            NamespaceState::Quiescent | NamespaceState::Deleting
        ) || snapshot.rows[1].as_deref() != Some(encode(&token.record)?.as_slice())
            || snapshot.rows[2].is_some()
        {
            return Err(EntityError::Fenced);
        }
        if descriptor.lower != binding {
            return Err(EntityError::Fenced);
        }
        validate_pin(&snapshot.rows[3], &snapshot.rows[4], &descriptor)?;
        validate_registry(&snapshot.rows[5], &descriptor)?;
        TransactionAuthority::from_validated(checked_rows(&keys, snapshot.rows), None)
    }

    /// Bounded final discard for an already empty reference namespace. A general
    /// namespace sweeper must first retire its entities and finish all upper
    /// object deletion; this method refuses residual rows rather than guessing.
    /// Zero lower owners is retained in the registry: this API never authorizes
    /// physical deletion of the immutable graph or a legacy packed object.
    pub async fn discard_empty(&self, token: &MaintenanceToken) -> Result<()> {
        if self.discard_completed(token).await? {
            return Ok(());
        }
        let authority = self.maintenance_authority(token).await?;
        let mut descriptor = self.descriptor().await?;
        let allowed = [
            self.keys.descriptor(),
            self.keys.lease(),
            self.keys.lower_pin(),
            self.keys.maintenance(),
            self.keys.counter("inode"),
            self.keys.slice_delete_fence(),
        ];
        let namespace_rows = self.backend.scan_prefix(self.keys.prefix(), 4096).await?;
        if namespace_rows.iter().any(|(key, _)| !allowed.contains(key)) {
            return Err(EntityError::Busy);
        }
        let mut census = SliceCensus::new(self.backend.as_ref(), &self.keys).await?;
        while let Some(rows) = census.next_page(self.backend.as_ref(), &self.keys).await? {
            for row in rows {
                if !matches!(row.state, SliceState::Deleted { .. }) {
                    return Err(EntityError::Busy);
                }
            }
        }
        let keys = vec![
            self.keys.lower_registry(&descriptor.lower.manifest_digest),
            self.keys
                .lower_owner(&descriptor.lower.manifest_digest, self.keys.namespace_id()),
        ];
        let snapshot = self.backend.read(&keys).await?;
        let mut registry: LowerRegistry =
            decode(snapshot.rows[0].as_deref().ok_or(EntityError::Fenced)?)?;
        let owner: LowerOwner = decode(snapshot.rows[1].as_deref().ok_or(EntityError::Fenced)?)?;
        if registry.binding != descriptor.lower
            || registry.owners == 0
            || owner.namespace_id != self.keys.namespace_id()
            || owner.binding != descriptor.lower
        {
            return Err(EntityError::Corrupt(
                "invalid lower ownership on discard".into(),
            ));
        }
        registry.owners -= 1;
        descriptor.state = NamespaceState::Deleted;
        let mut checks = checked_rows(&keys, snapshot.rows);
        checks.extend(namespace_rows.into_iter().map(|(key, value)| RowCheck {
            key,
            expected: Some(value),
        }));
        committed(
            self.backend
                .commit(
                    &authority,
                    &EntityPlan {
                        checks,
                        writes: vec![
                            put(self.keys.descriptor(), &descriptor)?,
                            put(keys[0].clone(), &registry)?,
                            RowWrite::Delete {
                                key: keys[1].clone(),
                            },
                            RowWrite::Delete {
                                key: self.keys.lower_pin(),
                            },
                            RowWrite::Delete {
                                key: self.keys.counter("inode"),
                            },
                            RowWrite::Delete {
                                key: self.keys.slice_delete_fence(),
                            },
                        ],
                    },
                )
                .await?,
        )
    }

    /// Persist discard admission before any namespace sweep. The same exact
    /// maintenance receipt remains usable across uncertain transaction replies.
    pub async fn begin_discard(&self, token: &MaintenanceToken) -> Result<()> {
        let authority = self.maintenance_authority(token).await?;
        let keys = vec![self.keys.descriptor()];
        let snapshot = self.backend.read(&keys).await?;
        let mut descriptor =
            self.decode_descriptor(snapshot.rows[0].as_deref().ok_or(EntityError::Fenced)?)?;
        if descriptor.state == NamespaceState::Deleting {
            return Ok(());
        }
        if descriptor.state != NamespaceState::Quiescent {
            return Err(EntityError::Fenced);
        }
        descriptor.state = NamespaceState::Deleting;
        committed(
            self.backend
                .commit(
                    &authority,
                    &EntityPlan {
                        checks: checked_rows(&keys, snapshot.rows),
                        writes: vec![put(self.keys.descriptor(), &descriptor)?],
                    },
                )
                .await?,
        )
    }

    /// Orderly reopen from a locally proven drained session. This does not
    /// manufacture quiescence after a process crash or lease expiry. Recovery
    /// must finish pending inode/upload/deletion obligations first. Retired
    /// objects may keep conservative holds across the new writer incarnation.
    pub async fn resume_after_maintenance(
        &self,
        token: &MaintenanceToken,
        new_lease_id: Uuid,
        ttl_ns: i64,
    ) -> Result<WriterSession> {
        if new_lease_id.is_nil() || ttl_ns <= 0 {
            return Err(EntityError::Invalid("invalid resumed lease request".into()));
        }
        let maintenance = self.maintenance_authority(token).await?;
        let keys = vec![
            self.keys.descriptor(),
            self.keys.lease(),
            self.keys.slice_delete_fence(),
        ];
        let snapshot = self.backend.read(&keys).await?;
        let mut descriptor =
            self.decode_descriptor(snapshot.rows[0].as_deref().ok_or(EntityError::Fenced)?)?;
        if descriptor.state != NamespaceState::Quiescent || snapshot.rows[1].is_some() {
            return Err(EntityError::Fenced);
        }
        for suffix in [b"truncate/".as_slice(), b"orphan/"] {
            let prefix = [self.keys.prefix(), suffix].concat();
            if !self.backend.scan_prefix(&prefix, 4096).await?.is_empty() {
                return Err(EntityError::Busy);
            }
        }
        // Maintenance fences new uploads/publications. Abandon and completion
        // only remove blockers; reserve_delete is the only maintenance change
        // that adds one and atomically changes slice_delete_fence. Checking it
        // with the exact maintenance authority keeps this final CAS bounded.
        let checks = checked_rows(&keys, snapshot.rows);
        let mut census = SliceCensus::new(self.backend.as_ref(), &self.keys).await?;
        while let Some(rows) = census.next_page(self.backend.as_ref(), &self.keys).await? {
            for row in rows {
                if row.staging_hold
                    || matches!(
                        row.state,
                        SliceState::Pending { .. } | SliceState::Deleting { .. }
                    )
                {
                    return Err(EntityError::Busy);
                }
            }
        }
        let identity = LeaseIdentity {
            lease_id: new_lease_id,
            holder_generation: descriptor
                .generation
                .checked_add(1)
                .ok_or(EntityError::Exhausted)?,
        };
        let lease = WriterLease {
            namespace_id: self.keys.namespace_id(),
            identity,
            expires_ns: snapshot
                .now_ns
                .checked_add(ttl_ns)
                .ok_or(EntityError::Overflow)?,
        };
        descriptor.generation = identity.holder_generation;
        descriptor.active_lease = Some(identity);
        descriptor.state = NamespaceState::Active;
        let authority = TransactionAuthority::from_validated(
            maintenance.checks.clone(),
            Some(lease.expires_ns),
        )?;
        committed(
            self.backend
                .commit(
                    &authority,
                    &EntityPlan {
                        checks,
                        writes: vec![
                            put(self.keys.descriptor(), &descriptor)?,
                            put(self.keys.lease(), &lease)?,
                            RowWrite::Delete {
                                key: self.keys.maintenance(),
                            },
                        ],
                    },
                )
                .await?,
        )?;
        Ok(WriterSession {
            namespace_id: self.keys.namespace_id(),
            namespace_prefix: self.keys.prefix().to_vec(),
            identity,
            lower: descriptor.lower,
            admission: Arc::default(),
            inode_locks: Arc::default(),
            io_failures: Arc::default(),
        })
    }

    /// A retained terminal receipt makes a lost final-discard reply reconcilable
    /// without ever decrementing the lower owner count twice.
    pub async fn discard_completed(&self, token: &MaintenanceToken) -> Result<bool> {
        if token.namespace_id != self.keys.namespace_id()
            || token.namespace_prefix != self.keys.prefix()
        {
            return Err(EntityError::Fenced);
        }
        let keys = vec![
            self.keys.descriptor(),
            self.keys.maintenance(),
            self.keys.lease(),
            self.keys.lower_pin(),
        ];
        let snapshot = self.backend.read(&keys).await?;
        let descriptor =
            self.decode_descriptor(snapshot.rows[0].as_deref().ok_or(EntityError::Fenced)?)?;
        if snapshot.rows[1].as_deref() != Some(encode(&token.record)?.as_slice())
            || snapshot.rows[2].is_some()
        {
            return Err(EntityError::Fenced);
        }
        if descriptor.state != NamespaceState::Deleted {
            return Ok(false);
        }
        let owner_key = self
            .keys
            .lower_owner(&descriptor.lower.manifest_digest, self.keys.namespace_id());
        if snapshot.rows[3].is_some() || self.backend.read(&[owner_key]).await?.rows[0].is_some() {
            return Err(EntityError::Corrupt(
                "deleted namespace retains a lower owner".into(),
            ));
        }
        Ok(true)
    }

    pub async fn deleted_lower_owner_proof(
        &self,
        token: &MaintenanceToken,
    ) -> Result<DeletedLowerOwnerProof> {
        if !self.discard_completed(token).await? {
            return Err(EntityError::Busy);
        }
        let descriptor = self.descriptor().await?;
        Ok(DeletedLowerOwnerProof {
            volume_id: self.keys.volume_id(),
            namespace_id: self.keys.namespace_id(),
            namespace_prefix: self.keys.prefix().to_vec(),
            binding: descriptor.lower,
        })
    }
}

fn validate_pin(
    pin: &Option<Vec<u8>>,
    owner: &Option<Vec<u8>>,
    descriptor: &NamespaceDescriptor,
) -> Result<()> {
    let expected = LowerOwner {
        namespace_id: descriptor.namespace_id,
        binding: descriptor.lower.clone(),
    };
    for bytes in [pin, owner] {
        let actual: LowerOwner = decode(bytes.as_deref().ok_or(EntityError::Fenced)?)?;
        if actual != expected {
            return Err(EntityError::Fenced);
        }
    }
    Ok(())
}

fn validate_registry(bytes: &Option<Vec<u8>>, descriptor: &NamespaceDescriptor) -> Result<()> {
    let registry: LowerRegistry = decode(bytes.as_deref().ok_or(EntityError::Fenced)?)?;
    if registry.binding != descriptor.lower || registry.owners == 0 {
        return Err(EntityError::Fenced);
    }
    Ok(())
}

pub(super) fn put<T: Serialize>(key: Vec<u8>, row: &T) -> Result<RowWrite> {
    Ok(RowWrite::Put {
        key,
        value: encode(row)?,
    })
}

pub(super) fn checked_rows(keys: &[Vec<u8>], rows: Vec<Option<Vec<u8>>>) -> Vec<RowCheck> {
    keys.iter()
        .cloned()
        .zip(rows)
        .map(|(key, expected)| RowCheck { key, expected })
        .collect()
}

pub(super) fn committed(outcome: CommitOutcome) -> Result<()> {
    match outcome {
        CommitOutcome::Committed => Ok(()),
        CommitOutcome::Conflict => Err(EntityError::Conflict),
    }
}

#[cfg(test)]
#[path = "lifecycle_tests.rs"]
mod tests;
