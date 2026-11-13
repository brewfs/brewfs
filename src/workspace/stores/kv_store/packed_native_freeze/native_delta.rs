//! Complete frozen old-head table scans and owned native BLAKE3 capture.
//! Intentionally scans table prefixes, never only the reachable effective view.

use super::*;
use crate::workspace::digest::native_stream::{CanonicalNativeRow, NativeDeltaHasher};
use crate::workspace::packed_reader_lifecycle::{PackedReaderRequestOwner, PackedReaderSession};

const HASH_OPERATION_BYTES: u64 = 2 << 20;
const HASH_ROW_BYTES: usize = 96 << 10;

#[cfg(test)]
fn native_hash_diagnostic(stage: &str, error: &WorkspaceError) {
    let variant = match error {
        WorkspaceError::Fenced => "Fenced",
        WorkspaceError::Busy => "Busy",
        WorkspaceError::Backend(_) => "Backend",
        WorkspaceError::CorruptMetadata(_) => "CorruptMetadata",
        WorkspaceError::InvalidReadPlan(_) => "InvalidReadPlan",
        _ => "Other",
    };
    eprintln!("[packed-native-hash-diag] stage={stage} error={variant}");
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct NativeDeltaHashLimits {
    pub max_native_delta_rows: u64,
    pub max_canonical_bytes: u64,
}

impl NativeDeltaHashLimits {
    fn validate(self) -> Result<(), WorkspaceError> {
        if self.max_native_delta_rows == 0 || self.max_canonical_bytes < 57 {
            return Err(native_freeze_error("invalid native hash quotas"));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct NativeDeltaHashCounts {
    pub tables: [u64; 5],
    pub canonical_bytes: u64,
}

/// Constructible only by owned, complete bounded scans of this exact fence.
/// A private field binds the native digest to its original immutable receipt.
/// This alone does not assert data drain, Hashed phase, or publication authority.
pub(crate) struct FrozenNativeDeltaHash<B: WorkspaceKvBackend> {
    native: Arc<PackedNativeQuiesceFence<B>>,
    predecessor_digest: [u8; 32],
    digest: [u8; 32],
    root: [u8; 32],
    counts: NativeDeltaHashCounts,
    session: Arc<dyn PackedReaderSession>,
    _reader_owner: PackedReaderRequestOwner,
    _permit: SnapshotOwnedPermit,
}

impl<B: WorkspaceKvBackend> FrozenNativeDeltaHash<B> {
    pub(crate) fn native_quiesce(&self) -> &Arc<PackedNativeQuiesceFence<B>> {
        &self.native
    }
    pub(crate) fn predecessor_receipt_digest(&self) -> [u8; 32] {
        self.predecessor_digest
    }
    pub(crate) fn delta_digest(&self) -> [u8; 32] {
        self.digest
    }
    pub(crate) fn root_hash(&self) -> [u8; 32] {
        self.root
    }
    pub(crate) fn counts(&self) -> NativeDeltaHashCounts {
        self.counts
    }

    pub(crate) fn matches_reader_session(&self, session: &Arc<dyn PackedReaderSession>) -> bool {
        Arc::ptr_eq(&self.session, session)
    }

    pub(crate) async fn validate(&self) -> Result<(), WorkspaceError> {
        self.session.validate().await.inspect_err(|_error| {
            #[cfg(test)]
            native_hash_diagnostic("final-session-validate", _error);
        })?;
        self.native.validate().await.inspect_err(|_error| {
            #[cfg(test)]
            native_hash_diagnostic("final-native-validate", _error);
        })?;
        if self.native.canonical_receipt_digest() != self.predecessor_digest {
            #[cfg(test)]
            eprintln!("[packed-native-hash-diag] stage=final-canonical-identity error=Fenced");
            return Err(WorkspaceError::Fenced);
        }
        Ok(())
    }

    /// The driver owns each actual backend wait, session/generation and budget.
    /// Dropping the caller receiver cannot cancel a transport or release its
    /// authority while a bounded response is still in flight.
    pub(crate) async fn capture(
        native: Arc<PackedNativeQuiesceFence<B>>,
        session: Arc<dyn PackedReaderSession>,
        limits: NativeDeltaHashLimits,
    ) -> Result<Self, WorkspaceError>
    where
        B: 'static,
    {
        limits.validate()?;
        if session.binding() != &native.binding.binding
            || !Arc::ptr_eq(&session.mount_budget(), &native.budget)
        {
            #[cfg(test)]
            eprintln!(
                "[packed-native-hash-diag] stage=capture-identity error=Fenced binding_matches={} budget_matches={}",
                session.binding() == &native.binding.binding,
                Arc::ptr_eq(&session.mount_budget(), &native.budget),
            );
            return Err(WorkspaceError::Fenced);
        }
        let reader_owner = session.retain_request().inspect_err(|_error| {
            #[cfg(test)]
            native_hash_diagnostic("capture-session-retain", _error);
        })?;
        let permit = native
            .budget
            .admit(&[(SnapshotBudgetPool::Metadata, HASH_OPERATION_BYTES)])
            .map_err(native_freeze_error)?;
        let (sender, receiver) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let result = Self::capture_owned(native, session, limits, reader_owner, permit).await;
            let _ = sender.send(result);
        });
        receiver
            .await
            .map_err(|_| native_freeze_error("native hash driver stopped"))?
    }

    async fn capture_owned(
        native: Arc<PackedNativeQuiesceFence<B>>,
        session: Arc<dyn PackedReaderSession>,
        limits: NativeDeltaHashLimits,
        reader_owner: PackedReaderRequestOwner,
        permit: SnapshotOwnedPermit,
    ) -> Result<Self, WorkspaceError> {
        native.validate().await.inspect_err(|_error| {
            #[cfg(test)]
            {
                native_hash_diagnostic("initial-native-validate", _error);
                eprintln!(
                    "[packed-native-hash-diag] stage=initial-native-source seed_authority={} recovery_basis={} recovery_claim={} original_guard_matches_current={}",
                    native.seed_authority.is_some(),
                    native.recovery.is_some(),
                    native.recovery_claim.is_some(),
                    native.source_guard() == &native.mapping.old_guard,
                );
            }
        })?;
        session.validate().await.inspect_err(|_error| {
            #[cfg(test)]
            native_hash_diagnostic("initial-session-validate", _error);
        })?;
        let predecessor_digest = native.canonical_receipt_digest();
        let mut counts = NativeDeltaHashCounts::default();
        let mut total = 0_u64;
        macro_rules! count_table {
            ($ty:ty, $ordinal:literal) => {
                let mut after: Option<Vec<u8>> = None;
                loop {
                    let page = native
                        .frozen_native_delta_page::<$ty>(after.as_deref())
                        .await
                        .map_err(|error| {
                            #[cfg(test)]
                            {
                                native_hash_diagnostic("count-page", &error);
                                eprintln!(
                                    "[packed-native-hash-diag] pass=count table={} has_cursor={}",
                                    $ordinal,
                                    after.is_some(),
                                );
                            }
                            error
                        })?;
                    if page.is_empty() {
                        break;
                    }
                    total = total
                        .checked_add(page.len() as u64)
                        .filter(|rows| *rows <= limits.max_native_delta_rows)
                        .ok_or_else(|| native_freeze_error("native delta row quota"))?;
                    counts.tables[$ordinal] = counts.tables[$ordinal]
                        .checked_add(page.len() as u64)
                        .ok_or_else(|| native_freeze_error("native delta table count overflow"))?;
                    // Validate even the count pass, including tombstone payloads.
                    for row in page.iter() {
                        drop(row.canonical_body(HASH_ROW_BYTES)?);
                    }
                    after = page.after.clone();
                }
            };
        }
        count_table!(DentryDelta, 0);
        count_table!(InodeDelta, 1);
        count_table!(XattrDelta, 2);
        count_table!(AclDelta, 3);
        count_table!(DataExtentDelta, 4);
        let mut hash = NativeDeltaHasher::new(
            native.frozen_head.layer_id,
            limits.max_canonical_bytes,
            HASH_ROW_BYTES,
        )?;
        macro_rules! hash_table {
            ($ty:ty, $ordinal:literal) => {
                hash.begin_table::<$ty>(counts.tables[$ordinal])?;
                let mut after: Option<Vec<u8>> = None;
                loop {
                    let page = native
                        .frozen_native_delta_page::<$ty>(after.as_deref())
                        .await
                        .map_err(|error| {
                            #[cfg(test)]
                            {
                                native_hash_diagnostic("hash-page", &error);
                                eprintln!(
                                    "[packed-native-hash-diag] pass=hash table={} has_cursor={}",
                                    $ordinal,
                                    after.is_some(),
                                );
                            }
                            error
                        })?;
                    if page.is_empty() {
                        break;
                    }
                    for row in page.iter() {
                        hash.row(row)?;
                    }
                    after = page.after.clone();
                }
            };
        }
        hash_table!(DentryDelta, 0);
        hash_table!(InodeDelta, 1);
        hash_table!(XattrDelta, 2);
        hash_table!(AclDelta, 3);
        hash_table!(DataExtentDelta, 4);
        let (digest, canonical_bytes) = hash.finish()?;
        counts.canonical_bytes = canonical_bytes;
        let base = &native.mapping.old_layers[1];
        if native.frozen_head.parent_layer_id != Some(base.layer_id)
            || base.state != LayerState::Sealed
        {
            #[cfg(test)]
            eprintln!(
                "[packed-native-hash-diag] stage=sealed-parent-shape error=Fenced parent_matches={} parent_sealed={}",
                native.frozen_head.parent_layer_id == Some(base.layer_id),
                base.state == LayerState::Sealed,
            );
            return Err(WorkspaceError::Fenced);
        }
        let parent_hash = base
            .root_hash
            .ok_or_else(|| native_freeze_error("native sealed parent has no root hash"))?;
        let root = crate::workspace::digest::root_hash(parent_hash, digest);
        let captured = Self {
            native,
            predecessor_digest,
            digest,
            root,
            counts,
            session,
            _reader_owner: reader_owner,
            _permit: permit,
        };
        captured.validate().await?;
        Ok(captured)
    }
}

/// Only these five built-in row types can select a frozen native full prefix.
/// Key constructors include the signed sign-bit flip used by canonical order.
trait FrozenDeltaRow: CanonicalNativeRow + DeserializeOwned {
    fn prefix(layer: LayerId) -> Vec<u8>;
    fn physical_key(&self) -> Vec<u8>;
}

macro_rules! table {
    ($ty:ty, $prefix:ident, $key:ident) => {
        impl FrozenDeltaRow for $ty {
            fn prefix(layer: LayerId) -> Vec<u8> {
                $prefix(layer)
            }
            fn physical_key(&self) -> Vec<u8> {
                $key(self)
            }
        }
    };
}
table!(DentryDelta, dentry_layer_prefix, dentry_key);
table!(InodeDelta, inode_layer_prefix, inode_key);
table!(XattrDelta, xattr_layer_prefix, xattr_key);
table!(AclDelta, acl_layer_prefix, acl_key);
table!(DataExtentDelta, extent_layer_prefix, extent_key);

impl<B: WorkspaceKvBackend> PackedNativeQuiesceFence<B> {
    async fn frozen_native_delta_page<R: FrozenDeltaRow>(
        &self,
        after: Option<&[u8]>,
    ) -> Result<FrozenNativeRows<R>, WorkspaceError> {
        // One row prevents a pair of legal 64-KiB xattrs from exceeding the
        // aggregate envelope. The SDK must expose this fixed 128-KiB tier.
        self.frozen_page_bounded(
            R::prefix(self.frozen_head.layer_id),
            after,
            R::physical_key,
            KvReadLimits {
                max_records: 1,
                max_key_bytes: 1024,
                max_value_bytes: 96 << 10,
                max_total_bytes: (96 << 10) + 1024,
                max_response_bytes: 128 << 10,
                // Empty TiKV regions consume requests without yielding a row.
                // A short page is not an EOF, and the traversal stays bounded.
                max_data_requests: 32,
            },
            512 << 10,
        )
        .await
    }
}

#[cfg(test)]
#[path = "native_delta_tests.rs"]
mod tests;
