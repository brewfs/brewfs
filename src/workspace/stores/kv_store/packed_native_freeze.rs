//! Real workspace-KV quiescence, before trusted local drain and PM11 publish.
//! This fence proves committed catalog topology, never completion of dirty I/O.

use super::super::kv_backend::KvReadLimits;
use super::*;
use crate::workspace::packed::snapshot::{
    SnapshotBudgetPool, SnapshotMountBudget, SnapshotOwnedPermit,
};
use sha2::{Digest, Sha256};

mod native_begin;
mod native_delta;
#[cfg(target_os = "linux")]
mod native_phase;
mod native_recovery;
mod native_seed;
mod recovery_claim;
pub(crate) use native_delta::{
    FrozenNativeDeltaHash, NativeDeltaHashCounts, NativeDeltaHashLimits,
};
#[cfg(target_os = "linux")]
pub(crate) use native_phase::{
    PackedNativePhaseFence, promote_verified_data_drained, promote_verified_hashed,
    reissue_verified_native_phase,
};
pub(crate) use native_recovery::PackedNativeRecoveryReadFence;
use native_seed::NativePrepareRecoveryAuthority;
pub(crate) use native_seed::{NativeJournalOwnerHandoff, NativePrepareRecoveryRequest};
pub(crate) use recovery_claim::NativePackedRecoveryClaimRequest;
pub(crate) use recovery_claim::PackedNativeRecoveryClaimFence;

const FREEZE_METADATA_BYTES: u64 = 16 << 20;
const FREEZE_POINT_MAX_BYTES: usize = 48 << 10;

#[cfg(test)]
pub(super) fn native_authority_diagnostic(stage: &str, error: &WorkspaceError) {
    let variant = match error {
        WorkspaceError::Fenced => "Fenced",
        WorkspaceError::Busy => "Busy",
        WorkspaceError::Backend(_) => "Backend",
        WorkspaceError::CorruptMetadata(_) => "CorruptMetadata",
        WorkspaceError::InvalidReadPlan(_) => "InvalidReadPlan",
        _ => "Other",
    };
    eprintln!("[packed-native-authority-diag] stage={stage} error={variant}");
}

/// The exact predecessor catalog and requested next head identity. Publication
/// must independently install a legal sealed carrier and cannot reinterpret
/// the historical head/base chain as the fixed PWB3 layer pair.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackedNativePlannedRotation {
    old_guard: HeadGuard,
    old_layers: [LayerRecord; 2],
    journal_id: JournalId,
    planned_head_layer_id: LayerId,
    planned_head_epoch: u64,
}
impl PackedNativePlannedRotation {
    pub fn old_guard(&self) -> &HeadGuard {
        &self.old_guard
    }
    pub fn old_layers(&self) -> &[LayerRecord; 2] {
        &self.old_layers
    }
    pub fn journal_id(&self) -> JournalId {
        self.journal_id
    }
    pub fn planned_head_layer_id(&self) -> LayerId {
        self.planned_head_layer_id
    }
    pub fn planned_head_epoch(&self) -> u64 {
        self.planned_head_epoch
    }
    pub fn native_sealed_source_layer_id(&self) -> LayerId {
        self.old_layers[0].layer_id
    }
}

struct NativeQuiesceSources<B> {
    clean: Option<Arc<super::packed_admin::PackedCleanPublicationAuthority<B>>>,
    bootstrap: Option<Arc<super::packed_admin::PackedInitialBootstrapAuthority<B>>>,
}

/// Constructible only through real catalog begin/advance and a locked-clock
/// authority check. Keeping this token does not grant a lower mount or GC DELETE.
/// It deliberately has no conversion into a full effective-view/source proof.
pub struct PackedNativeQuiesceFence<B> {
    store: Arc<KvWorkspaceStore<B>>,
    mapping: PackedNativePlannedRotation,
    binding: PackedLowerBindingRecord,
    frozen_head: LayerRecord,
    journal: SealJournal,
    canonical: Vec<u8>,
    // Present only after actual durable-basis and current-phase reissuance.
    // The original journal/canonical stay immutable; this is read authority.
    recovery: Option<Arc<super::packed_journal::PackedNativeRecoveryBasisReceipt<B>>>,
    recovery_claim: Option<Arc<PackedNativeRecoveryClaimFence<B>>>,
    seed_authority: Option<Arc<NativePrepareRecoveryAuthority<B>>>,
    clean_source: Option<Arc<super::packed_admin::PackedCleanPublicationAuthority<B>>>,
    bootstrap_source: Option<Arc<super::packed_admin::PackedInitialBootstrapAuthority<B>>>,
    budget: Arc<SnapshotMountBudget>,
    _permit: SnapshotOwnedPermit,
}
impl<B: WorkspaceKvBackend> PackedNativeQuiesceFence<B> {
    pub(crate) fn belongs_to_store(&self, store: &KvWorkspaceStore<B>) -> bool {
        std::ptr::eq(self.store.as_ref(), store)
    }
    pub(crate) fn is_same_store(&self, store: &Arc<KvWorkspaceStore<B>>) -> bool {
        Arc::ptr_eq(&self.store, store)
    }

    pub(crate) fn mount_budget(&self) -> Arc<SnapshotMountBudget> {
        self.budget.clone()
    }

    pub(crate) fn initial_publication_origin(
        &self,
    ) -> Option<Arc<super::packed_admin::PackedInitialBootstrapAuthority<B>>> {
        self.bootstrap_source.clone()
    }

    pub(crate) fn clean_publication_origin(
        &self,
    ) -> Option<Arc<super::packed_admin::PackedCleanPublicationAuthority<B>>> {
        self.clean_source.clone()
    }

    pub(crate) async fn captured_recovery_checks_before(
        &self,
    ) -> Result<(Vec<KvCheck>, i64), WorkspaceError> {
        if self.recovery.is_none() {
            return Err(WorkspaceError::Fenced);
        }
        self.store.frozen_source_authority_checks(self).await
    }

    pub(crate) fn recovery_basis(
        &self,
    ) -> Option<&Arc<super::packed_journal::PackedNativeRecoveryBasisReceipt<B>>> {
        self.recovery.as_ref()
    }

    /// Current actual read authority. Original source identity/canonical bytes
    /// always remain bound to mapping.old_guard, including after takeover.
    pub(crate) fn source_guard(&self) -> &HeadGuard {
        if let Some(authority) = &self.seed_authority {
            return authority.source_guard();
        }
        self.recovery_claim
            .as_ref()
            .map_or(&self.mapping.old_guard, |claim| claim.guard())
    }

    /// Recovery retains owner authority even when it keeps the original lease.
    pub(crate) fn requires_recovery_owner(&self) -> bool {
        self.seed_authority.is_some() || self.recovery_claim.is_some()
    }

    async fn read_phase_authority(
        &self,
        expected: Option<&SealJournal>,
    ) -> Result<NativeQuiesceRead, WorkspaceError> {
        use super::native_read_conflict::{
            FirstRead, MAX_NATIVE_READ_ATTEMPTS, NATIVE_READ_REBUILD_BYTES,
        };
        // Declare admission before retained original checks: checks are dropped
        // first on every return. Existing fence/read tiers cover fresh packets.
        let mut _owner = None;
        let mut first = FirstRead::new();
        for _ in 0..MAX_NATIVE_READ_ATTEMPTS {
            if self.budget.state().closed {
                return Err(WorkspaceError::Fenced);
            }
            let (mut read, root_only_conflict, root_seen) =
                self.read_phase_authority_once(expected).await?;
            if read.now <= 0 || read.now >= read.authority_deadline_ns || self.budget.state().closed
            {
                return Err(WorkspaceError::Fenced);
            }
            if !root_only_conflict && _owner.is_none() {
                return Ok(read);
            }
            if _owner.is_none() {
                _owner = Some(
                    self.budget
                        .admit(&[(SnapshotBudgetPool::Metadata, NATIVE_READ_REBUILD_BYTES)])
                        .map_err(native_freeze_error)?,
                );
            }
            let checks = read
                .keys
                .iter()
                .cloned()
                .zip(read.values.iter().cloned())
                .map(|(key, expected)| KvCheck { key, expected })
                .collect();
            let (aligned, deadline) = first.observe(
                checks,
                read.authority_deadline_ns,
                root_only_conflict,
                root_seen,
            )?;
            if read.now <= 0 || read.now >= deadline || self.budget.state().closed {
                return Err(WorkspaceError::Fenced);
            }
            if aligned {
                read.authority_deadline_ns = deadline;
                return Ok(read);
            }
            // Only a typed complete root-only snapshot mismatch reaches here.
            // Busy/Fenced/read errors propagate through ? without a retry.
            tokio::task::yield_now().await;
        }
        Err(WorkspaceError::Busy)
    }

    async fn read_phase_authority_once(
        &self,
        expected: Option<&SealJournal>,
    ) -> Result<(NativeQuiesceRead, bool, u64), WorkspaceError> {
        if self.seed_authority.is_some()
            && (self.recovery.is_some() || self.recovery_claim.is_some())
        {
            return Err(WorkspaceError::Fenced);
        }
        let mut read = self
            .store
            .read_packed_native_phase_for_guard(
                &self.mapping,
                &self.binding,
                expected,
                self.source_guard(),
            )
            .await?;
        let mut merged = super::native_read_conflict::normalize(
            read.keys
                .iter()
                .cloned()
                .zip(read.values.iter().cloned())
                .map(|(key, expected)| KvCheck { key, expected })
                .collect(),
        )?;
        let mut root_only_conflict = false;
        let mut root_seen = 0;
        if let Some(claim) = &self.recovery_claim {
            let basis = self.recovery.as_ref().ok_or(WorkspaceError::Fenced)?;
            if !claim.matches_basis(basis) || !Arc::ptr_eq(claim.mount_budget(), &self.budget) {
                return Err(WorkspaceError::Fenced);
            }
            let (additional, deadline) = claim.authority_checks_before().await?;
            let alignment = super::native_read_conflict::align(merged, additional, true)?;
            root_only_conflict |= alignment.root_only_conflict;
            root_seen = root_seen.max(alignment.root_seen);
            merged = alignment.checks;
            read.authority_deadline_ns = read.authority_deadline_ns.min(deadline);
        }
        if let Some(authority) = &self.seed_authority {
            if !authority.belongs_to_store(&self.store)
                || !Arc::ptr_eq(authority.mount_budget(), &self.budget)
            {
                return Err(WorkspaceError::Fenced);
            }
            authority.matches_original(
                &self.mapping,
                &self.binding,
                &self.journal,
                &self.canonical,
            )?;
            let (additional, deadline) = authority.authority_checks_before().await?;
            let alignment = super::native_read_conflict::align(merged, additional, true)?;
            root_only_conflict |= alignment.root_only_conflict;
            root_seen = root_seen.max(alignment.root_seen);
            merged = alignment.checks;
            read.authority_deadline_ns = read.authority_deadline_ns.min(deadline);
        }
        if let Some(clean) = &self.clean_source {
            if !clean.belongs_to_store(&self.store)
                || clean.guard() != self.source_guard()
                || !Arc::ptr_eq(clean.mount_budget(), &self.budget)
            {
                return Err(WorkspaceError::Fenced);
            }
            let (additional, deadline) = clean.authority_checks_before().await?;
            let alignment = super::native_read_conflict::align(merged, additional, true)?;
            root_only_conflict |= alignment.root_only_conflict;
            root_seen = root_seen.max(alignment.root_seen);
            merged = alignment.checks;
            read.authority_deadline_ns = read.authority_deadline_ns.min(deadline);
        }
        if self.clean_source.is_some() && self.bootstrap_source.is_some() {
            return Err(WorkspaceError::Fenced);
        }
        if let Some(initial) = &self.bootstrap_source {
            if !initial.belongs_to_store(&self.store)
                || initial.guard() != self.source_guard()
                || !Arc::ptr_eq(initial.mount_budget(), &self.budget)
            {
                return Err(WorkspaceError::Fenced);
            }
            let (additional, deadline) = initial.authority_checks_before().await?;
            let alignment = super::native_read_conflict::align(merged, additional, true)?;
            root_only_conflict |= alignment.root_only_conflict;
            root_seen = root_seen.max(alignment.root_seen);
            merged = alignment.checks;
            read.authority_deadline_ns = read.authority_deadline_ns.min(deadline);
        }
        // A plain original-Q fence has one complete packet and no merge.
        if root_seen == 0 {
            let raw = merged
                .iter()
                .find(|check| check.key.as_slice() == PACKED_ROOT_GENERATION_KEY)
                .and_then(|check| check.expected.as_deref())
                .ok_or(WorkspaceError::Fenced)?;
            root_seen = decode::<u64>(raw)?;
        }
        (read.keys, read.values) = merged
            .into_iter()
            .map(|check| (check.key, check.expected))
            .unzip();
        Ok((read, root_only_conflict, root_seen))
    }

    async fn frozen_page<T: serde::de::DeserializeOwned>(
        &self,
        prefix: Vec<u8>,
        after: Option<&[u8]>,
        identity: impl Fn(&T) -> Vec<u8>,
    ) -> Result<FrozenNativeRows<T>, WorkspaceError> {
        self.frozen_page_bounded(
            prefix,
            after,
            identity,
            KvReadLimits {
                max_records: 32,
                max_key_bytes: 1024,
                max_value_bytes: 48 << 10,
                max_total_bytes: 48 << 10,
                max_response_bytes: 64 << 10,
                max_data_requests: 32,
            },
            256 << 10,
        )
        .await
    }

    async fn frozen_page_bounded<T: serde::de::DeserializeOwned>(
        &self,
        prefix: Vec<u8>,
        after: Option<&[u8]>,
        identity: impl Fn(&T) -> Vec<u8>,
        limits: KvReadLimits,
        decoded_owner_bytes: u64,
    ) -> Result<FrozenNativeRows<T>, WorkspaceError> {
        let permit = self
            .budget
            .admit(&[(SnapshotBudgetPool::Metadata, decoded_owner_bytes)])
            .map_err(native_freeze_error)?;
        self.validate().await.inspect_err(|_| {
            #[cfg(test)]
            eprintln!(
                "[packed-native-page-diag] stage=before-page-authority has_cursor={}",
                after.is_some()
            );
        })?;
        let raw = self
            .store
            .backend
            .scan_prefix_page_with_byte_limits(&prefix, after, limits)
            .await
            .inspect_err(|_| {
                #[cfg(test)]
                eprintln!(
                    "[packed-native-page-diag] stage=bounded-page-scan has_cursor={}",
                    after.is_some()
                );
            })?;
        let mut rows = Vec::with_capacity(raw.len());
        let mut previous = after.unwrap_or_default().to_vec();
        for entry in raw {
            if !entry.key.starts_with(&prefix) || entry.key <= previous {
                return Err(native_freeze_error(
                    "frozen page cursor/order/prefix mismatch",
                ));
            }
            let row: T = decode_open_value(&entry.value, limits.max_value_bytes)?;
            if identity(&row) != entry.key {
                return Err(native_freeze_error(
                    "frozen row identity disagrees with key",
                ));
            }
            previous = entry.key;
            rows.push(row);
        }
        self.validate().await.inspect_err(|_| {
            #[cfg(test)]
            eprintln!(
                "[packed-native-page-diag] stage=after-page-authority has_cursor={}",
                after.is_some()
            );
        })?;
        Ok(FrozenNativeRows {
            rows,
            after: (!previous.is_empty()).then_some(previous),
            _permit: permit,
        })
    }

    fn source_layer(&self, ordinal: usize) -> Result<LayerId, WorkspaceError> {
        self.mapping
            .old_layers
            .get(ordinal)
            .map(|layer| layer.layer_id)
            .ok_or_else(|| native_freeze_error("frozen source layer ordinal"))
    }

    pub(crate) async fn frozen_dentry_page(
        &self,
        ordinal: usize,
        parent: i64,
        after: Option<&[u8]>,
    ) -> Result<FrozenNativeRows<DentryDelta>, WorkspaceError> {
        self.frozen_page(
            dentry_parent_prefix(self.source_layer(ordinal)?, parent),
            after,
            dentry_key,
        )
        .await
    }

    pub(crate) async fn frozen_xattr_page(
        &self,
        ordinal: usize,
        ino: i64,
        after: Option<&[u8]>,
    ) -> Result<FrozenNativeRows<XattrDelta>, WorkspaceError> {
        // A legal Linux/PM11 xattr can contain 64 KiB before its native KV
        // envelope. One row per page avoids rejecting two dense legal values
        // merely because their combined size exceeds this fixed response tier.
        self.frozen_page_bounded(
            xattr_inode_prefix(self.source_layer(ordinal)?, ino),
            after,
            xattr_key,
            KvReadLimits {
                max_records: 1,
                max_key_bytes: 1024,
                max_value_bytes: 96 << 10,
                max_total_bytes: 97 << 10,
                max_response_bytes: 128 << 10,
                max_data_requests: 32,
            },
            512 << 10,
        )
        .await
    }

    pub(crate) async fn frozen_acl_page(
        &self,
        ordinal: usize,
        ino: i64,
        after: Option<&[u8]>,
    ) -> Result<FrozenNativeRows<AclDelta>, WorkspaceError> {
        self.frozen_page(
            acl_inode_prefix(self.source_layer(ordinal)?, ino),
            after,
            acl_key,
        )
        .await
    }

    pub(crate) async fn frozen_extent_page(
        &self,
        ordinal: usize,
        ino: i64,
        chunk: u64,
        after: Option<&[u8]>,
    ) -> Result<FrozenNativeRows<DataExtentDelta>, WorkspaceError> {
        self.frozen_page(
            extent_chunk_prefix(self.source_layer(ordinal)?, ino, chunk),
            after,
            extent_key,
        )
        .await
    }

    pub(crate) async fn frozen_inode(
        &self,
        ino: i64,
    ) -> Result<FrozenNativeRows<InodeDelta>, WorkspaceError> {
        let permit = self
            .budget
            .admit(&[(SnapshotBudgetPool::Metadata, 256 << 10)])
            .map_err(native_freeze_error)?;
        let keys: Vec<_> = self
            .mapping
            .old_layers
            .iter()
            .map(|layer| inode_identity_key(layer.layer_id, ino))
            .collect();
        self.validate().await?;
        let (raw, _) = self
            .store
            .backend
            .get_many_consistent_with_time_bounded(
                &keys,
                KvReadLimits {
                    max_records: 2,
                    max_key_bytes: 1024,
                    max_value_bytes: 48 << 10,
                    max_total_bytes: 48 << 10,
                    max_response_bytes: 64 << 10,
                    // Two original keys plus at most two certified Get reads.
                    max_data_requests: 4,
                },
            )
            .await?;
        if raw.len() != keys.len() {
            return Err(native_freeze_error("frozen inode response count"));
        }
        let mut rows = Vec::with_capacity(2);
        for (key, value) in keys.into_iter().zip(raw) {
            if let Some(value) = value {
                let row: InodeDelta = decode_open_value(&value, FREEZE_POINT_MAX_BYTES)?;
                if inode_key(&row) != key {
                    return Err(native_freeze_error("frozen inode identity"));
                }
                rows.push(row);
            }
        }
        self.validate().await?;
        Ok(FrozenNativeRows {
            rows,
            after: None,
            _permit: permit,
        })
    }

    pub fn mapping(&self) -> &PackedNativePlannedRotation {
        &self.mapping
    }
    pub fn binding(&self) -> &PackedLowerBindingRecord {
        &self.binding
    }

    pub(crate) fn canonical_receipt_bytes(&self) -> &[u8] {
        &self.canonical
    }
    pub(crate) fn canonical_receipt_digest(&self) -> [u8; 32] {
        Sha256::digest(&self.canonical).into()
    }

    /// Persist the actual original Quiesced journal alongside its canonical
    /// receipt. Later phase journals have a different update timestamp and
    /// cannot reconstruct these immutable source facts during recovery.
    pub(crate) fn quiesced_journal_bytes(&self) -> Result<Vec<u8>, WorkspaceError> {
        encode(&self.journal)
    }

    pub(crate) fn quiesced_native_basis_bytes(&self) -> Result<[Vec<u8>; 2], WorkspaceError> {
        Ok([
            encode(&self.frozen_head)?,
            encode(&self.mapping.old_layers[1])?,
        ])
    }

    /// Exact authority checks for a typed journal rebind. The consumer must
    /// apply these in its timed backend CAS; this method does not authorize a
    /// phase transition, publication, native seal or topology rotation.
    pub(crate) async fn authority_checks_before(
        &self,
    ) -> Result<(Vec<KvCheck>, i64), WorkspaceError> {
        if self.recovery.is_some() {
            // Rebinding a new PPJ requires the original live Q token. A
            // recovered read token uses its separate exact basis route.
            return Err(WorkspaceError::Fenced);
        }
        let read = self.read_phase_authority(None).await?;
        if read.head != self.frozen_head || read.journal != self.journal {
            return Err(WorkspaceError::Fenced);
        }
        Ok((
            read.keys
                .into_iter()
                .zip(read.values)
                .map(|(key, expected)| KvCheck { key, expected })
                .collect(),
            read.authority_deadline_ns,
        ))
    }

    /// Revalidate this exact workspace/native domain. A phase change, native
    /// sequence change, binding replacement, expired lease or takeover fences it.
    pub async fn validate(&self) -> Result<(), WorkspaceError> {
        self.store.validate_packed_native_quiesce(self).await
    }

    pub(crate) async fn validate_context(
        &self,
        guard: &HeadGuard,
        layers: &[LayerRecord; 2],
        binding: &PackedLowerBindingRecord,
    ) -> Result<(), WorkspaceError> {
        if guard != self.source_guard()
            || layers != &self.mapping.old_layers
            || binding != &self.binding
        {
            return Err(WorkspaceError::Fenced);
        }
        self.validate().await
    }
}

impl<B: WorkspaceKvBackend + 'static> PackedNativeQuiesceFence<B> {
    /// The complete returned owner must remain alive through the packed
    /// journal writer's actual CAS and uncertain-response terminal.
    pub(crate) async fn prepare_native_owner_handoff(
        &self,
        record: &super::packed_journal::PackedJournalRecord,
    ) -> Result<Option<NativeJournalOwnerHandoff>, WorkspaceError> {
        let Some(authority) = &self.seed_authority else {
            if self.recovery.is_some() || self.recovery_claim.is_some() {
                return Err(WorkspaceError::Fenced);
            }
            return self
                .store
                .prepare_original_native_journal_link(self, record)
                .await
                .map(Some);
        };
        if !authority.belongs_to_store(&self.store)
            || !Arc::ptr_eq(authority.mount_budget(), &self.budget)
            || self.recovery.is_some()
            || self.recovery_claim.is_some()
        {
            return Err(WorkspaceError::Fenced);
        }
        authority.matches_original(&self.mapping, &self.binding, &self.journal, &self.canonical)?;
        Ok(Some(authority.prepare_packed_handoff(record).await?))
    }
}

pub(crate) struct FrozenNativeRows<T> {
    rows: Vec<T>,
    pub(crate) after: Option<Vec<u8>>,
    _permit: SnapshotOwnedPermit,
}
impl<T> std::ops::Deref for FrozenNativeRows<T> {
    type Target = [T];
    fn deref(&self) -> &Self::Target {
        &self.rows
    }
}

struct NativeQuiesceRead {
    keys: Vec<Vec<u8>>,
    values: Vec<Option<Vec<u8>>>,
    lease: SnapshotLease,
    head: LayerRecord,
    journal: SealJournal,
    now: i64,
    authority_deadline_ns: i64,
}

fn native_freeze_error(message: impl std::fmt::Display) -> WorkspaceError {
    WorkspaceError::CorruptMetadata(format!("packed native quiesce: {message}"))
}
fn canonical_field(output: &mut Vec<u8>, bytes: &[u8]) -> Result<(), WorkspaceError> {
    let length = u32::try_from(bytes.len()).map_err(native_freeze_error)?;
    output.extend_from_slice(&length.to_le_bytes());
    output.extend_from_slice(bytes);
    Ok(())
}
fn canonical_quiesce(
    mapping: &PackedNativePlannedRotation,
    binding: &PackedLowerBindingRecord,
    frozen_head: &LayerRecord,
    journal: &SealJournal,
) -> Result<Vec<u8>, WorkspaceError> {
    let mut bytes = b"BrewFS-packed-native-catalog-quiesce\0".to_vec();
    bytes.extend_from_slice(mapping.old_guard.workspace_id.as_bytes());
    bytes.extend_from_slice(mapping.old_guard.expected_head_layer_id.as_bytes());
    bytes.extend_from_slice(&mapping.old_guard.expected_head_epoch.to_le_bytes());
    bytes.extend_from_slice(mapping.old_guard.lease_id.as_bytes());
    bytes.extend_from_slice(&mapping.old_guard.holder_generation.to_le_bytes());
    bytes.extend_from_slice(mapping.journal_id.as_bytes());
    bytes.extend_from_slice(mapping.planned_head_layer_id.as_bytes());
    bytes.extend_from_slice(&mapping.planned_head_epoch.to_le_bytes());
    for layer in &mapping.old_layers {
        canonical_field(&mut bytes, &encode(layer)?)?;
    }
    canonical_field(&mut bytes, &encode(frozen_head)?)?;
    canonical_field(&mut bytes, &encode(journal)?)?;
    canonical_field(&mut bytes, &binding.encode()?)?;
    Ok(bytes)
}

impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    /// Resume actual durable Prepare/Q, retaining its original source facts.
    /// Native phase and publication still require fresh drain/compare/hash.
    pub(crate) async fn recover_packed_native_prepare(
        self: &Arc<Self>,
        request: NativePrepareRecoveryRequest,
        budget: Arc<SnapshotMountBudget>,
    ) -> Result<Arc<PackedNativeQuiesceFence<B>>, WorkspaceError>
    where
        B: 'static,
    {
        if budget.state().closed {
            return Err(WorkspaceError::Fenced);
        }
        let store = self.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let result = async {
                let recovered = store
                    .recover_native_prepare_seed(request, budget.clone())
                    .await?;
                if !recovered.authority.belongs_to_store(&store)
                    || !Arc::ptr_eq(recovered.authority.mount_budget(), &budget)
                    || budget.state().closed
                {
                    return Err(WorkspaceError::Fenced);
                }
                recovered.authority.matches_original(
                    &recovered.mapping,
                    &recovered.binding,
                    &recovered.journal,
                    &recovered.canonical,
                )?;
                let permit = budget
                    .admit(&[(SnapshotBudgetPool::Metadata, 128 << 10)])
                    .map_err(native_freeze_error)?;
                let mut fence = PackedNativeQuiesceFence {
                    store: store.clone(),
                    mapping: recovered.mapping,
                    binding: recovered.binding,
                    frozen_head: recovered.frozen_head,
                    journal: recovered.journal,
                    canonical: recovered.canonical,
                    recovery: None,
                    recovery_claim: None,
                    seed_authority: Some(recovered.authority),
                    clean_source: None,
                    bootstrap_source: None,
                    budget,
                    _permit: permit,
                };
                fence.clean_source = store.restore_clean_native_origin(&fence).await?;
                fence.bootstrap_source = store.restore_initial_native_origin(&fence).await?;
                let fence = Arc::new(fence);
                fence.validate().await?;
                Ok(fence)
            }
            .await;
            let _ = sender.send(result);
        });
        receiver
            .await
            .map_err(|_| native_freeze_error("native Prepare materialization driver stopped"))?
    }

    /// Stop native metadata mutation in the real workspace KV domain and return
    /// its exact committed-source fence. Callers must separately stop admission
    /// and drain actual VFS dirty writes before requesting full publication.
    /// No NoopDurableRemoteBarrier, caller boolean or phase flag certifies drain.
    pub async fn begin_packed_native_quiesce(
        self: Arc<Self>,
        guard: HeadGuard,
        expected_layers: [LayerRecord; 2],
        journal_id: JournalId,
        new_head_layer_id: LayerId,
        budget: Arc<SnapshotMountBudget>,
    ) -> Result<PackedNativeQuiesceFence<B>, WorkspaceError>
    where
        B: 'static,
    {
        self.require_admin_access()?;
        let (sender, receiver) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let result = self
                .begin_packed_native_quiesce_owned(
                    guard,
                    expected_layers,
                    journal_id,
                    new_head_layer_id,
                    budget,
                    NativeQuiesceSources {
                        clean: None,
                        bootstrap: None,
                    },
                )
                .await;
            let _ = sender.send(result);
        });
        receiver.await.expect("packed native begin driver stopped")
    }

    pub(crate) async fn begin_clean_packed_native_quiesce(
        self: Arc<Self>,
        clean: Arc<super::packed_admin::PackedCleanPublicationAuthority<B>>,
        expected_layers: [LayerRecord; 2],
        journal_id: JournalId,
        new_head_layer_id: LayerId,
    ) -> Result<PackedNativeQuiesceFence<B>, WorkspaceError> {
        let budget = clean.mount_budget().clone();
        let guard = clean.guard().clone();
        self.begin_packed_native_quiesce_owned(
            guard,
            expected_layers,
            journal_id,
            new_head_layer_id,
            budget,
            NativeQuiesceSources {
                clean: Some(clean),
                bootstrap: None,
            },
        )
        .await
    }

    pub(crate) async fn begin_initial_packed_native_quiesce(
        self: Arc<Self>,
        initial: Arc<super::packed_admin::PackedInitialBootstrapAuthority<B>>,
        expected_layers: [LayerRecord; 2],
        journal_id: JournalId,
        new_head_layer_id: LayerId,
    ) -> Result<PackedNativeQuiesceFence<B>, WorkspaceError> {
        let budget = initial.mount_budget().clone();
        let guard = initial.guard().clone();
        self.begin_packed_native_quiesce_owned(
            guard,
            expected_layers,
            journal_id,
            new_head_layer_id,
            budget,
            NativeQuiesceSources {
                clean: None,
                bootstrap: Some(initial),
            },
        )
        .await
    }

    async fn begin_packed_native_quiesce_owned(
        self: Arc<Self>,
        guard: HeadGuard,
        expected_layers: [LayerRecord; 2],
        journal_id: JournalId,
        new_head_layer_id: LayerId,
        budget: Arc<SnapshotMountBudget>,
        sources: NativeQuiesceSources<B>,
    ) -> Result<PackedNativeQuiesceFence<B>, WorkspaceError> {
        let NativeQuiesceSources {
            clean: clean_source,
            bootstrap: bootstrap_source,
        } = sources;
        if journal_id.as_bytes() == &[0; 16] || new_head_layer_id.as_bytes() == &[0; 16] {
            return Err(native_freeze_error("nil journal or planned head identity"));
        }
        self.configure_packed_reader_pin_budget(budget.clone())?;
        let permit = budget
            .admit(&[(SnapshotBudgetPool::Metadata, FREEZE_METADATA_BYTES)])
            .map_err(native_freeze_error)?;
        validate_permission_layers(&expected_layers)?;
        if expected_layers[0].layer_id != guard.expected_head_layer_id
            || new_head_layer_id == guard.expected_head_layer_id
            || new_head_layer_id == expected_layers[1].layer_id
        {
            return Err(WorkspaceError::Fenced);
        }
        let mapping = PackedNativePlannedRotation {
            planned_head_epoch: guard
                .expected_head_epoch
                .checked_add(1)
                .ok_or_else(|| native_freeze_error("head epoch exhausted"))?,
            old_guard: guard.clone(),
            old_layers: expected_layers,
            journal_id,
            planned_head_layer_id: new_head_layer_id,
        };
        if let Some(clean) = &clean_source
            && (!clean.belongs_to_store(&self)
                || clean.guard() != &guard
                || !Arc::ptr_eq(clean.mount_budget(), &budget))
        {
            return Err(WorkspaceError::Fenced);
        }
        if clean_source.is_some() && bootstrap_source.is_some() {
            return Err(WorkspaceError::Fenced);
        }
        if let Some(initial) = &bootstrap_source
            && (!initial.belongs_to_store(&self)
                || initial.guard() != &guard
                || !Arc::ptr_eq(initial.mount_budget(), &budget))
        {
            return Err(WorkspaceError::Fenced);
        }
        let (binding, read) = self
            .bounded_native_begin_catalog(
                &mapping,
                clean_source.as_deref(),
                bootstrap_source.as_deref(),
            )
            .await?;
        let canonical = canonical_quiesce(&mapping, &binding, &read.head, &read.journal)?;
        let fence = PackedNativeQuiesceFence {
            store: self.clone(),
            mapping,
            binding,
            frozen_head: read.head,
            journal: read.journal,
            canonical,
            recovery: None,
            recovery_claim: None,
            seed_authority: None,
            clean_source,
            bootstrap_source,
            budget,
            _permit: permit,
        };
        fence.validate().await?;
        Ok(fence)
    }

    async fn read_packed_native_quiesce(
        &self,
        mapping: &PackedNativePlannedRotation,
        binding: &PackedLowerBindingRecord,
    ) -> Result<NativeQuiesceRead, WorkspaceError> {
        self.read_packed_native_phase(mapping, binding, None).await
    }

    async fn read_packed_native_phase(
        &self,
        mapping: &PackedNativePlannedRotation,
        binding: &PackedLowerBindingRecord,
        expected_journal: Option<&SealJournal>,
    ) -> Result<NativeQuiesceRead, WorkspaceError> {
        self.read_packed_native_phase_for_guard(
            mapping,
            binding,
            expected_journal,
            &mapping.old_guard,
        )
        .await
    }

    async fn read_packed_native_phase_for_guard(
        &self,
        mapping: &PackedNativePlannedRotation,
        binding: &PackedLowerBindingRecord,
        expected_journal: Option<&SealJournal>,
        guard: &HeadGuard,
    ) -> Result<NativeQuiesceRead, WorkspaceError> {
        if guard.workspace_id != mapping.old_guard.workspace_id
            || guard.expected_head_layer_id != mapping.old_guard.expected_head_layer_id
            || guard.expected_head_epoch != mapping.old_guard.expected_head_epoch
        {
            #[cfg(test)]
            eprintln!("[packed-native-authority-diag] stage=phase-guard-identity error=Fenced");
            return Err(WorkspaceError::Fenced);
        }
        let keys = vec![
            hot_journal_key(guard.workspace_id, mapping.journal_id),
            hot_workspace_key(guard.workspace_id),
            hot_layer_key(guard.expected_head_layer_id),
            hot_layer_key(mapping.old_layers[1].layer_id),
            hot_lease_key(guard.workspace_id, guard.lease_id),
            hot_layer_key(mapping.planned_head_layer_id),
            packed_current_key(guard.workspace_id),
            packed_claim_key(guard.workspace_id),
            packed_history_key(guard.workspace_id, binding.binding.binding_version),
            packed_history_key(guard.workspace_id, 1),
            PACKED_ROOT_GENERATION_KEY.to_vec(),
            LAYER_INVENTORY_GENERATION_KEY.to_vec(),
        ];
        let (values, now) = self
            .backend
            .get_many_consistent_with_time_bounded(
                &keys,
                KvReadLimits {
                    max_records: 12,
                    max_key_bytes: 1024,
                    max_value_bytes: FREEZE_POINT_MAX_BYTES,
                    max_total_bytes: FREEZE_POINT_MAX_BYTES,
                    max_response_bytes: 64 << 10,
                    // Preserve the twelve-key proof; reserve only two rereads.
                    max_data_requests: 14,
                },
            )
            .await
            .inspect_err(|_error| {
                #[cfg(test)]
                native_authority_diagnostic("phase-bounded-read", _error);
            })?;
        if values.len() != keys.len() {
            return Err(native_freeze_error("short authority read"));
        }
        let total = values.iter().try_fold(0usize, |total, value| {
            total
                .checked_add(value.as_ref().map_or(0, Vec::len))
                .ok_or_else(|| native_freeze_error("authority bytes overflow"))
        })?;
        if total > FREEZE_POINT_MAX_BYTES {
            return Err(native_freeze_error("authority byte bound"));
        }
        let bounded_required = |index: usize| -> Result<&[u8], WorkspaceError> {
            values[index].as_deref().ok_or(WorkspaceError::Fenced)
        };
        let journal: SealJournal = decode_open_value(bounded_required(0)?, FREEZE_POINT_MAX_BYTES)?;
        let workspace: WorkspaceRecord =
            decode_open_value(bounded_required(1)?, FREEZE_POINT_MAX_BYTES)?;
        let head: LayerRecord = decode_open_value(bounded_required(2)?, FREEZE_POINT_MAX_BYTES)?;
        let base: LayerRecord = decode_open_value(bounded_required(3)?, FREEZE_POINT_MAX_BYTES)?;
        let lease: SnapshotLease = decode_open_value(bounded_required(4)?, FREEZE_POINT_MAX_BYTES)?;
        let mut expected_head = mapping.old_layers[0].clone();
        expected_head.state = LayerState::Sealing;
        let current = decode_packed_pair(guard.workspace_id, &values[6], &values[7], &values[8])?
            .ok_or(WorkspaceError::Fenced)?;
        let anchor = PackedLowerBindingRecord::decode(
            values[9]
                .as_deref()
                .ok_or_else(|| native_freeze_error("version-1 anchor missing"))?,
        )?;
        if workspace.workspace_id != guard.workspace_id
            || workspace.state != WorkspaceState::Sealing
            || workspace.active_lease != Some(guard.lease_id)
            || workspace.head_layer_id != guard.expected_head_layer_id
            || workspace.head_epoch != guard.expected_head_epoch
            || head != expected_head
            || base != mapping.old_layers[1]
            || lease.lease_id != guard.lease_id
            || lease.workspace_id != guard.workspace_id
            || lease.holder_generation != guard.holder_generation
            || !lease.writable
            || lease.state != LeaseState::Active
            || lease.expires_at_ns <= now
            || values[5].is_some()
            || &current != binding
            || anchor.workspace_id != guard.workspace_id
            || anchor.binding.binding_version != 1
            || journal.journal_id != mapping.journal_id
            || journal.workspace_id != guard.workspace_id
            || journal.old_head_layer_id != guard.expected_head_layer_id
            || journal.expected_head_epoch != guard.expected_head_epoch
            || journal.new_head_layer_id != Some(mapping.planned_head_layer_id)
        {
            #[cfg(test)]
            eprintln!(
                "[packed-native-authority-diag] stage=phase-record-mismatch error=Fenced workspace_identity={} workspace_sealing={} head={} base={} lease_identity={} lease_writable={} lease_active={} lease_live={} next_head_absent={} binding={} anchor={} journal={}",
                workspace.workspace_id == guard.workspace_id
                    && workspace.head_layer_id == guard.expected_head_layer_id
                    && workspace.head_epoch == guard.expected_head_epoch,
                workspace.state == WorkspaceState::Sealing,
                head == expected_head,
                base == mapping.old_layers[1],
                lease.lease_id == guard.lease_id
                    && lease.workspace_id == guard.workspace_id
                    && lease.holder_generation == guard.holder_generation,
                lease.writable,
                lease.state == LeaseState::Active,
                lease.expires_at_ns > now,
                values[5].is_none(),
                &current == binding,
                anchor.workspace_id == guard.workspace_id && anchor.binding.binding_version == 1,
                journal.journal_id == mapping.journal_id
                    && journal.workspace_id == guard.workspace_id
                    && journal.old_head_layer_id == guard.expected_head_layer_id
                    && journal.expected_head_epoch == guard.expected_head_epoch
                    && journal.new_head_layer_id == Some(mapping.planned_head_layer_id),
            );
            return Err(WorkspaceError::Fenced);
        }
        let valid_phase = match expected_journal {
            None => {
                journal.phase == SealPhase::Quiesced
                    && journal.delta_digest.is_none()
                    && journal.root_hash.is_none()
            }
            Some(expected) if &journal == expected => match journal.phase {
                SealPhase::DataDrained => {
                    journal.pending_bytes == 0
                        && journal.delta_digest.is_none()
                        && journal.root_hash.is_none()
                }
                SealPhase::Hashed => {
                    journal.pending_bytes == 0
                        && journal.delta_digest.zip(journal.root_hash).is_some_and(
                            |(digest, root)| {
                                base.root_hash
                                    .is_some_and(|parent| root_hash(parent, digest) == root)
                            },
                        )
                }
                _ => false,
            },
            Some(_) => false,
        };
        if !valid_phase {
            #[cfg(test)]
            eprintln!("[packed-native-authority-diag] stage=phase-journal-phase error=Fenced");
            return Err(WorkspaceError::Fenced);
        }
        next_packed_root_generation(&values[10])?;
        layer_inventory_generation(&values[11])?;
        Ok(NativeQuiesceRead {
            keys,
            values,
            authority_deadline_ns: lease.expires_at_ns,
            lease,
            head,
            journal,
            now,
        })
    }

    async fn validate_packed_native_quiesce(
        &self,
        fence: &PackedNativeQuiesceFence<B>,
    ) -> Result<(), WorkspaceError> {
        for _ in 0..CAS_MAX_RETRIES {
            let (checks, deadline) = self
                .frozen_source_authority_checks(fence)
                .await
                .inspect_err(|_error| {
                    #[cfg(test)]
                    native_authority_diagnostic("validate-source-packet", _error);
                })?;
            if self
                .backend
                .compare_and_swap_before(&checks, &[], deadline)
                .await
                .inspect_err(|_error| {
                    #[cfg(test)]
                    native_authority_diagnostic("validate-noop-cas", _error);
                })?
            {
                return Ok(());
            }
            #[cfg(test)]
            eprintln!("[packed-native-authority-diag] stage=validate-noop-cas result=false");
            tokio::task::yield_now().await;
        }
        Err(WorkspaceError::Busy)
    }
}

#[cfg(test)]
mod packed_native_freeze_tests;
