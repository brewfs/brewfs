//! Actual verified-source promotion through bounded, lease-guarded native CAS.
//! Only the strong effective-source owned driver calls these borrowed helpers.
//! No constructor accepts a raw digest, a caller phase flag, or a drain boolean.

use super::native_delta::FrozenNativeDeltaHash;
use super::*;
use crate::chunk::BlockStore;
use crate::workspace::packed::snapshot::VerifiedFrozenNativeView;
use crate::workspace::packed_reader_lifecycle::PackedReaderSession;

const PHASE_OPERATION_BYTES: u64 = 2 << 20;

/// Every instance is made only after its exact successor committed, including
/// confirmation of an uncertain CAS through the phase-specific authority read.
/// The predecessor's canonical receipt remains immutable across phase changes.
pub(crate) struct PackedNativePhaseFence<B: WorkspaceKvBackend + 'static> {
    original: Arc<PackedNativeQuiesceFence<B>>,
    journal: SealJournal,
    hash: Arc<FrozenNativeDeltaHash<B>>,
    session: Arc<dyn PackedReaderSession>,
    _permit: SnapshotOwnedPermit,
}

/// CAS may have committed before later validation failed. The strong driver
/// must retain the complete source and install this committed authority even
/// when returning an error, rather than pretending the source is still Q.
pub(crate) struct NativePhasePromotionError<B: WorkspaceKvBackend + 'static> {
    pub(crate) error: WorkspaceError,
    pub(crate) committed: Option<Arc<PackedNativePhaseFence<B>>>,
    /// An attempted journal is diagnostic recovery input, never authority.
    /// Some means a submitted CAS whose exact outcome may need revalidation.
    pub(crate) attempted_successor: Option<Box<SealJournal>>,
}

impl<B: WorkspaceKvBackend + 'static> NativePhasePromotionError<B> {
    fn before_commit(error: WorkspaceError) -> Self {
        Self {
            error,
            committed: None,
            attempted_successor: None,
        }
    }
}

fn phase_error(message: impl std::fmt::Display) -> WorkspaceError {
    WorkspaceError::Backend(format!("packed native phase: {message}"))
}

fn checks(read: &NativeQuiesceRead) -> Vec<KvCheck> {
    read.keys
        .iter()
        .cloned()
        .zip(read.values.iter().cloned())
        .map(|(key, expected)| KvCheck { key, expected })
        .collect()
}

impl<B: WorkspaceKvBackend + 'static> PackedNativePhaseFence<B> {
    pub(crate) fn native_quiesce(&self) -> &Arc<PackedNativeQuiesceFence<B>> {
        &self.original
    }
    pub(crate) fn mapping(&self) -> &PackedNativePlannedRotation {
        &self.original.mapping
    }
    pub(crate) fn source_guard(&self) -> &HeadGuard {
        self.original.source_guard()
    }
    pub(crate) fn binding(&self) -> &PackedLowerBindingRecord {
        &self.original.binding
    }
    pub(crate) fn journal(&self) -> &SealJournal {
        &self.journal
    }
    pub(crate) fn native_delta_hash(&self) -> &Arc<FrozenNativeDeltaHash<B>> {
        &self.hash
    }
    pub(crate) fn is_hashed(&self) -> bool {
        self.journal.phase == SealPhase::Hashed
    }
    pub(crate) fn canonical_receipt_bytes(&self) -> &[u8] {
        self.original.canonical_receipt_bytes()
    }
    pub(crate) fn canonical_receipt_digest(&self) -> [u8; 32] {
        self.original.canonical_receipt_digest()
    }

    async fn read_exact(&self) -> Result<NativeQuiesceRead, WorkspaceError> {
        self.session.validate().await?;
        if !self.hash.matches_reader_session(&self.session)
            || !Arc::ptr_eq(self.hash.native_quiesce(), &self.original)
            || self.hash.predecessor_receipt_digest() != self.original.canonical_receipt_digest()
            || !matches!(
                self.journal.phase,
                SealPhase::DataDrained | SealPhase::Hashed
            )
        {
            return Err(WorkspaceError::Fenced);
        }
        let read = self
            .original
            .read_phase_authority(Some(&self.journal))
            .await?;
        if read.head != self.original.frozen_head || read.journal != self.journal {
            return Err(WorkspaceError::Fenced);
        }
        if self.journal.phase == SealPhase::Hashed
            && (self.journal.delta_digest != Some(self.hash.delta_digest())
                || self.journal.root_hash != Some(self.hash.root_hash()))
        {
            return Err(WorkspaceError::Fenced);
        }
        Ok(read)
    }

    /// These checks must be included in the actual final timed publication CAS.
    /// The successful read alone does not authorize a catalog or registry write.
    pub(crate) async fn authority_checks_before(
        &self,
    ) -> Result<(Vec<KvCheck>, i64), WorkspaceError> {
        let read = self.read_exact().await?;
        Ok((checks(&read), read.authority_deadline_ns))
    }

    pub(crate) async fn validate(&self) -> Result<(), WorkspaceError> {
        for _ in 0..CAS_MAX_RETRIES {
            let read = self.read_exact().await?;
            if self
                .original
                .store
                .backend
                .compare_and_swap_before(&checks(&read), &[], read.authority_deadline_ns)
                .await?
            {
                self.session.validate().await?;
                return Ok(());
            }
            tokio::task::yield_now().await;
        }
        Err(WorkspaceError::Busy)
    }
}

async fn verified_context<B, S>(
    source: &VerifiedFrozenNativeView<B, S>,
    hash: &Arc<FrozenNativeDeltaHash<B>>,
) -> Result<Arc<dyn PackedReaderSession>, WorkspaceError>
where
    B: WorkspaceKvBackend + 'static,
    S: BlockStore + Send + Sync + 'static,
{
    source.validate().await.map_err(phase_error)?;
    let session = source.native_reader_session();
    if !Arc::ptr_eq(source.native_quiesce(), hash.native_quiesce())
        || !hash.matches_reader_session(&session)
        || hash.predecessor_receipt_digest() != source.native_quiesce().canonical_receipt_digest()
    {
        return Err(WorkspaceError::Fenced);
    }
    if let Some(basis) = source.native_quiesce().recovery_basis() {
        let record = basis.record();
        let candidate_matches = match record.commit_target.as_ref() {
            Some(target) => &target.binding.manifest == source.candidate_manifest(),
            None => record.phase
                == crate::workspace::stores::kv_store::packed_journal::PackedJournalPhase::Building
                && source.has_produced_candidate(),
        };
        if record.source.effective_view_digest != source.source_digest()
            || !record.source.snapshot_backed
            || record.source.frozen_view_token != source.native_quiesce().canonical_receipt_digest()
            || !candidate_matches
        {
            return Err(WorkspaceError::Fenced);
        }
    }
    session.validate().await?;
    Ok(session)
}

/// Fresh process DD/Hashed authority requires both newly compared effective
/// source and newly scanned original native hash. Durable flags/digests alone
/// never reach this constructor. The exact durable PPJ and current native
/// journal/lease are revalidated together in a real timed no-op CAS.
pub(crate) async fn reissue_verified_native_phase<B, S>(
    source: &VerifiedFrozenNativeView<B, S>,
    hash: Arc<FrozenNativeDeltaHash<B>>,
) -> Result<Arc<PackedNativePhaseFence<B>>, NativePhasePromotionError<B>>
where
    B: WorkspaceKvBackend + 'static,
    S: BlockStore + Send + Sync + 'static,
{
    if source.native_phase_authority().is_some() {
        return Err(NativePhasePromotionError::before_commit(
            WorkspaceError::Fenced,
        ));
    }
    let session = verified_context(source, &hash)
        .await
        .map_err(NativePhasePromotionError::before_commit)?;
    hash.validate()
        .await
        .map_err(NativePhasePromotionError::before_commit)?;
    let original = source.native_quiesce().clone();
    let journal = original
        .recovery_basis()
        .ok_or_else(|| NativePhasePromotionError::before_commit(WorkspaceError::Fenced))?
        .current_native_journal()
        .clone();
    if !matches!(journal.phase, SealPhase::DataDrained | SealPhase::Hashed)
        || (journal.phase == SealPhase::Hashed
            && (journal.delta_digest != Some(hash.delta_digest())
                || journal.root_hash != Some(hash.root_hash())))
    {
        return Err(NativePhasePromotionError::before_commit(
            WorkspaceError::Fenced,
        ));
    }
    let permit = original
        .budget
        .admit(&[(SnapshotBudgetPool::Metadata, PHASE_OPERATION_BYTES)])
        .map_err(|error| NativePhasePromotionError::before_commit(phase_error(error)))?;
    for _ in 0..CAS_MAX_RETRIES {
        session
            .validate()
            .await
            .map_err(NativePhasePromotionError::before_commit)?;
        let (exact, deadline) = original
            .store
            .frozen_source_authority_checks(&original)
            .await
            .map_err(NativePhasePromotionError::before_commit)?;
        if !original
            .store
            .backend
            .compare_and_swap_before(&exact, &[], deadline)
            .await
            .map_err(NativePhasePromotionError::before_commit)?
        {
            tokio::task::yield_now().await;
            continue;
        }
        let fence = Arc::new(PackedNativePhaseFence {
            original,
            journal,
            hash,
            session,
            _permit: permit,
        });
        return match fence.validate().await {
            Ok(()) => Ok(fence),
            Err(error) => Err(NativePhasePromotionError {
                error,
                committed: Some(fence),
                attempted_successor: None,
            }),
        };
    }
    Err(NativePhasePromotionError::before_commit(
        WorkspaceError::Busy,
    ))
}

/// Q -> DD. The parameter is an actual successful effective comparison that
/// still owns the real VFS drain, authenticated lower and reader generation.
/// Invoke only while the strong source driver owns the complete source.
pub(crate) async fn promote_verified_data_drained<B, S>(
    source: &VerifiedFrozenNativeView<B, S>,
    hash: Arc<FrozenNativeDeltaHash<B>>,
) -> Result<Arc<PackedNativePhaseFence<B>>, NativePhasePromotionError<B>>
where
    B: WorkspaceKvBackend + 'static,
    S: BlockStore + Send + Sync + 'static,
{
    let session = verified_context(source, &hash)
        .await
        .map_err(NativePhasePromotionError::before_commit)?;
    if source.native_phase_authority().is_some() {
        return Err(NativePhasePromotionError::before_commit(
            WorkspaceError::Fenced,
        ));
    }
    hash.validate()
        .await
        .map_err(NativePhasePromotionError::before_commit)?;
    let original = source.native_quiesce().clone();
    let predecessor = original.journal.clone();
    transition(original, hash, session, predecessor, SealPhase::DataDrained).await
}

/// DD -> Hashed. The source must already delegate to the exact committed DD
/// token returned above. No old-Q validation is attempted after the first CAS.
pub(crate) async fn promote_verified_hashed<B, S>(
    source: &VerifiedFrozenNativeView<B, S>,
    drained: Arc<PackedNativePhaseFence<B>>,
) -> Result<Arc<PackedNativePhaseFence<B>>, NativePhasePromotionError<B>>
where
    B: WorkspaceKvBackend + 'static,
    S: BlockStore + Send + Sync + 'static,
{
    if drained.journal.phase != SealPhase::DataDrained
        || source
            .native_phase_authority()
            .is_none_or(|current| !Arc::ptr_eq(current, &drained))
    {
        return Err(NativePhasePromotionError::before_commit(
            WorkspaceError::Fenced,
        ));
    }
    let session = verified_context(source, &drained.hash)
        .await
        .map_err(NativePhasePromotionError::before_commit)?;
    drained
        .validate()
        .await
        .map_err(NativePhasePromotionError::before_commit)?;
    transition(
        drained.original.clone(),
        drained.hash.clone(),
        session,
        drained.journal.clone(),
        SealPhase::Hashed,
    )
    .await
}

/// Private common CAS, reached only after successful actual source validation.
async fn transition<B: WorkspaceKvBackend + 'static>(
    original: Arc<PackedNativeQuiesceFence<B>>,
    hash: Arc<FrozenNativeDeltaHash<B>>,
    session: Arc<dyn PackedReaderSession>,
    predecessor: SealJournal,
    target: SealPhase,
) -> Result<Arc<PackedNativePhaseFence<B>>, NativePhasePromotionError<B>> {
    if !matches!(
        (predecessor.phase, target),
        (SealPhase::Quiesced, SealPhase::DataDrained) | (SealPhase::DataDrained, SealPhase::Hashed)
    ) {
        return Err(NativePhasePromotionError::before_commit(
            WorkspaceError::Fenced,
        ));
    }
    let permit = original
        .budget
        .admit(&[(SnapshotBudgetPool::Metadata, PHASE_OPERATION_BYTES)])
        .map_err(|error| NativePhasePromotionError::before_commit(phase_error(error)))?;
    for _ in 0..CAS_MAX_RETRIES {
        session
            .validate()
            .await
            .map_err(NativePhasePromotionError::before_commit)?;
        let expected = if predecessor.phase == SealPhase::Quiesced {
            None
        } else {
            Some(&predecessor)
        };
        let read = original
            .read_phase_authority(expected)
            .await
            .map_err(NativePhasePromotionError::before_commit)?;
        if read.head != original.frozen_head || read.journal != predecessor {
            return Err(NativePhasePromotionError::before_commit(
                WorkspaceError::Fenced,
            ));
        }
        let next = successor(
            &predecessor,
            target,
            read.now,
            hash.delta_digest(),
            hash.root_hash(),
        )
        .map_err(NativePhasePromotionError::before_commit)?;
        let raw = read
            .values
            .first()
            .and_then(Option::as_ref)
            .ok_or_else(|| NativePhasePromotionError::before_commit(WorkspaceError::Fenced))?;
        let current: SealJournal = decode_open_value(raw, FREEZE_POINT_MAX_BYTES)
            .map_err(NativePhasePromotionError::before_commit)?;
        if current != predecessor {
            return Err(NativePhasePromotionError::before_commit(
                WorkspaceError::Fenced,
            ));
        }
        let encoded = encode(&next).map_err(NativePhasePromotionError::before_commit)?;
        if encoded.len() > FREEZE_POINT_MAX_BYTES {
            return Err(NativePhasePromotionError::before_commit(phase_error(
                "bounded journal successor exceeds fixed limit",
            )));
        }
        let write = KvWrite::Put {
            key: hot_journal_key(predecessor.workspace_id, predecessor.journal_id),
            value: encoded,
        };
        let packet = original
            .store
            .prepare_topology_packet(checks(&read), vec![write], Some(read.authority_deadline_ns))
            .await
            .map_err(NativePhasePromotionError::before_commit)?;
        let cas = original
            .store
            .commit_prepared_topology_packet(&packet)
            .await;
        match cas {
            Ok(true) => {}
            Ok(false) => {
                tokio::task::yield_now().await;
                continue;
            }
            Err(error) => {
                // Resolve uncertainty using exact attempted successor, not a
                // matching phase alone. Do not blindly retransmit this write.
                let exact_packet = original
                    .store
                    .confirm_prepared_topology_packet(&packet)
                    .await;
                match exact_packet {
                    Ok(true) if confirm_successor(&original, &session, &next).await.is_ok() => {}
                    _ => {
                        return Err(NativePhasePromotionError {
                            error,
                            committed: None,
                            attempted_successor: Some(Box::new(next)),
                        });
                    }
                }
            }
        }
        let attempted_successor = Some(Box::new(next.clone()));
        let fence = Arc::new(PackedNativePhaseFence {
            original,
            journal: next,
            hash,
            session,
            _permit: permit,
        });
        return match fence.validate().await {
            Ok(()) => Ok(fence),
            Err(error) => Err(NativePhasePromotionError {
                error,
                committed: Some(fence),
                attempted_successor,
            }),
        };
    }
    Err(NativePhasePromotionError::before_commit(
        WorkspaceError::Busy,
    ))
}

async fn confirm_successor<B: WorkspaceKvBackend + 'static>(
    original: &PackedNativeQuiesceFence<B>,
    session: &Arc<dyn PackedReaderSession>,
    next: &SealJournal,
) -> Result<(), WorkspaceError> {
    for _ in 0..CAS_MAX_RETRIES {
        session.validate().await?;
        let read = original.read_phase_authority(Some(next)).await?;
        if read.head != original.frozen_head || read.journal != *next {
            return Err(WorkspaceError::Fenced);
        }
        if original
            .store
            .backend
            .compare_and_swap_before(&checks(&read), &[], read.authority_deadline_ns)
            .await?
        {
            return Ok(());
        }
        tokio::task::yield_now().await;
    }
    Err(WorkspaceError::Busy)
}

fn successor(
    previous: &SealJournal,
    target: SealPhase,
    now: i64,
    native_delta_digest: [u8; 32],
    native_root: [u8; 32],
) -> Result<SealJournal, WorkspaceError> {
    if now <= 0
        || previous.delta_digest.is_some()
        || previous.root_hash.is_some()
        || !matches!(
            (previous.phase, target),
            (SealPhase::Quiesced, SealPhase::DataDrained)
                | (SealPhase::DataDrained, SealPhase::Hashed)
        )
    {
        return Err(WorkspaceError::Fenced);
    }
    let mut next = previous.clone();
    next.phase = target;
    next.pending_bytes = 0;
    next.last_error = None;
    next.updated_at_ns = now;
    if target == SealPhase::Hashed {
        next.delta_digest = Some(native_delta_digest);
        next.root_hash = Some(native_root);
    }
    Ok(next)
}

#[cfg(test)]
#[path = "native_phase_tests.rs"]
mod tests;
