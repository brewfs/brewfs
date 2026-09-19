//! Protected upload receipts (WRITE-006 / INV-11).
//!
//! A native write uploads its object bytes and then commits through the
//! control plane.  The two steps are separate on purpose, which means the
//! upload can be durable while the KV step fails: the objects exist, no head
//! references them, and a cleaner looking only at reachability would delete
//! live data.  The receipt for that upload therefore enters a *protected
//! orphan* set — an explicit, durable record that says "these objects are
//! uncommitted but must not be collected" — until the operation is resolved
//! one way or the other:
//!
//! - `Committed`: a later commit references the objects, so the protection is
//!   released and the objects are ordinary retained objects;
//! - `Abandoned`: the operation will never commit, so the protection is
//!   released and the objects become ordinary garbage.
//!
//! Protection is released exactly once and keyed on the recorded bytes, so a
//! concurrent write of a different record is never deleted by a resolution
//! that was derived from another one.  A KV failure that happened *before*
//! anything was uploaded protects nothing: there is no receipt yet.

use std::collections::BTreeSet;

use sha2::{Digest, Sha256};

use crate::native_base::wire::bnct::Id16;
use crate::native_base::wire::error::{WireError, WireResult};
use crate::native_base::wire::refs::{Hash32, ObjectId, ObjectRef, RootRef};
use crate::native_base::wire::uvarint::{Reader, Writer};
use crate::native_base::write::error::WriteError;
use crate::native_base::write::keys::Keys;
use crate::native_base::write::store::{ControlStore, Txn};

pub const ORPHAN_RECEIPT_MAGIC: &[u8] = b"BrewFS.UploadReceipt.v1\0";
pub const MAX_ORPHAN_REASON: usize = 1024;
pub const MAX_ORPHAN_OBJECTS: usize = 1 << 16;

/// How far a write got before its control-plane step failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum KvStep {
    /// The object registration transaction failed; nothing was uploaded yet.
    Registration,
    /// The data objects were put, but the receipts container was not built.
    DataUploaded,
    /// The receipts container was built and uploaded; only the commit is left.
    ReceiptsUploaded,
    /// The commit transaction itself failed after a complete upload.
    Commit,
}

impl KvStep {
    pub fn as_u8(self) -> u8 {
        match self {
            Self::Registration => 0,
            Self::DataUploaded => 1,
            Self::ReceiptsUploaded => 2,
            Self::Commit => 3,
        }
    }

    pub fn from_u8(code: u8) -> WireResult<Self> {
        match code {
            0 => Ok(Self::Registration),
            1 => Ok(Self::DataUploaded),
            2 => Ok(Self::ReceiptsUploaded),
            3 => Ok(Self::Commit),
            other => Err(WireError::invalid(
                "orphan receipt",
                format!("unknown kv step {other}"),
            )),
        }
    }

    /// Whether object bytes are already durable at this step.
    pub fn upload_is_durable(self) -> bool {
        self >= Self::DataUploaded
    }
}

/// The durable result of one operation's upload, as far as it got.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrphanReceipt {
    pub operation_id: Id16,
    pub workspace_id: Id16,
    pub domain_id: Id16,
    pub step: KvStep,
    pub reason: String,
    /// Present once the receipts container itself was uploaded.  The receipt is
    /// the audit trail of the upload, so losing it would lose the only record
    /// of what those objects were for.
    pub receipts: Option<RootRef>,
    /// Every object the upload made durable, ascending by object id.
    pub objects: Vec<ObjectRef>,
}

impl OrphanReceipt {
    /// Build the protection record for a failed control-plane step.  Returns
    /// `None` when nothing was uploaded: without durable objects there is
    /// nothing to protect and no receipt to preserve.
    pub fn after_kv_failure(
        operation_id: Id16,
        workspace_id: Id16,
        domain_id: Id16,
        step: KvStep,
        reason: impl Into<String>,
        receipts: Option<RootRef>,
        objects: impl IntoIterator<Item = ObjectRef>,
    ) -> Result<Option<Self>, WriteError> {
        if !step.upload_is_durable() {
            return Ok(None);
        }
        let mut objects: Vec<ObjectRef> = objects.into_iter().collect();
        objects.sort_by(|a, b| a.object_id.cmp(&b.object_id));
        // Two identical blocks are one object (content dedup), so the record
        // lists it once instead of refusing a legitimate upload.
        objects.dedup_by(|a, b| a.object_id == b.object_id);
        let receipt = Self {
            operation_id,
            workspace_id,
            domain_id,
            step,
            reason: reason.into(),
            receipts,
            objects,
        };
        receipt.validate()?;
        Ok(Some(receipt))
    }

    pub fn validate(&self) -> Result<(), WriteError> {
        if self.operation_id == [0u8; 16] || self.workspace_id == [0u8; 16] {
            return Err(WriteError::Record(
                "orphan receipt ids must be non-zero".into(),
            ));
        }
        if !self.step.upload_is_durable() {
            return Err(WriteError::Record(
                "an orphan receipt needs a durable upload".into(),
            ));
        }
        if self.objects.is_empty() {
            return Err(WriteError::Record(
                "an orphan receipt protects at least one object".into(),
            ));
        }
        if self.objects.len() > MAX_ORPHAN_OBJECTS {
            return Err(WriteError::Record(
                "orphan receipt object count exceeds the limit".into(),
            ));
        }
        if self.reason.is_empty() || self.reason.len() > MAX_ORPHAN_REASON {
            return Err(WriteError::Record(
                "orphan receipt reason is empty or too long".into(),
            ));
        }
        for pair in self.objects.windows(2) {
            if pair[0].object_id >= pair[1].object_id {
                return Err(WriteError::Record(
                    "orphan receipt objects must strictly ascend by object id".into(),
                ));
            }
        }
        for object in &self.objects {
            if object.object_len == 0 {
                return Err(WriteError::Record(
                    "an orphan receipt object cannot be empty".into(),
                ));
            }
        }
        if let Some(receipts) = &self.receipts {
            if !self
                .objects
                .iter()
                .any(|object| object.object_id == receipts.object.object_id)
            {
                return Err(WriteError::Record(
                    "the receipts container must itself be protected".into(),
                ));
            }
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>, WriteError> {
        self.validate()?;
        let mut w = Writer::new();
        w.put(ORPHAN_RECEIPT_MAGIC);
        w.put(&self.operation_id);
        w.put(&self.workspace_id);
        w.put(&self.domain_id);
        w.u8(self.step.as_u8());
        w.bytes(self.reason.as_bytes());
        match &self.receipts {
            Some(root) => {
                w.u8(1);
                root.encode_into(&mut w);
            }
            None => w.u8(0),
        }
        w.u32(self.objects.len() as u32);
        for object in &self.objects {
            object.encode_into(&mut w);
        }
        Ok(w.into_bytes())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, WriteError> {
        let what = "orphan receipt";
        let mut r = Reader::new(bytes);
        if r.take(ORPHAN_RECEIPT_MAGIC.len(), what)? != ORPHAN_RECEIPT_MAGIC {
            return Err(WriteError::Record("orphan receipt magic mismatch".into()));
        }
        let operation_id: Id16 = r.take(16, what)?.try_into().unwrap();
        let workspace_id: Id16 = r.take(16, what)?.try_into().unwrap();
        let domain_id: Id16 = r.take(16, what)?.try_into().unwrap();
        let step = KvStep::from_u8(r.u8(what)?)?;
        let reason = String::from_utf8(r.bytes(what)?.to_vec())
            .map_err(|_| WriteError::Record("orphan receipt reason is not utf-8".into()))?;
        let receipts = match r.u8(what)? {
            0 => None,
            1 => Some(RootRef::decode(&mut r)?),
            other => {
                return Err(WriteError::Record(format!(
                    "orphan receipt receipts tag {other}"
                )));
            }
        };
        let count = r.u32(what)? as usize;
        if count > MAX_ORPHAN_OBJECTS {
            return Err(WriteError::Record(
                "orphan receipt object count exceeds the limit".into(),
            ));
        }
        let mut objects = Vec::with_capacity(count);
        for _ in 0..count {
            objects.push(ObjectRef::decode(&mut r)?);
        }
        if !r.is_empty() {
            return Err(WriteError::Record(
                "orphan receipt has trailing bytes".into(),
            ));
        }
        let receipt = Self {
            operation_id,
            workspace_id,
            domain_id,
            step,
            reason,
            receipts,
            objects,
        };
        receipt.validate()?;
        Ok(receipt)
    }

    pub fn digest(&self) -> Result<Hash32, WriteError> {
        Ok(Sha256::digest(self.encode()?).into())
    }

    /// The object ids this record protects.
    pub fn protected_ids(&self) -> BTreeSet<ObjectId> {
        self.objects.iter().map(|object| object.object_id).collect()
    }
}

/// The union of every protection record's object ids.
pub fn protected_object_ids<'a>(
    records: impl IntoIterator<Item = &'a OrphanReceipt>,
) -> BTreeSet<ObjectId> {
    records
        .into_iter()
        .flat_map(|record| record.objects.iter().map(|object| object.object_id))
        .collect()
}

/// Durable protection state for one operation.
pub async fn record_orphan_receipt(
    store: &dyn ControlStore,
    keys: &Keys,
    receipt: &OrphanReceipt,
) -> Result<bool, WriteError> {
    let bytes = receipt.encode()?;
    let key = keys.orphan_receipt(&receipt.operation_id);
    if let Some(existing) = store.get(&key).await? {
        if existing != bytes {
            return Err(WriteError::OperationIdMismatch(format!(
                "operation {:02x?} already has another protected upload receipt",
                receipt.operation_id
            )));
        }
        return Ok(false);
    }
    store
        .run(Txn::new().check_absent(key.clone()).put(key, bytes))
        .await
        .map_err(|error| match error {
            super::store::StoreError::Conflict => WriteError::Conflict(
                "the protected upload receipt raced with another writer".into(),
            ),
            other => other.into(),
        })?;
    Ok(true)
}

/// Read the protection record of one operation, if any.
pub async fn read_orphan_receipt(
    store: &dyn ControlStore,
    keys: &Keys,
    operation_id: &Id16,
) -> Result<Option<OrphanReceipt>, WriteError> {
    match store.get(&keys.orphan_receipt(operation_id)).await? {
        Some(bytes) => Ok(Some(OrphanReceipt::decode(&bytes)?)),
        None => Ok(None),
    }
}

/// Every protection record of the volume.
pub async fn scan_orphan_receipts(
    store: &dyn ControlStore,
    keys: &Keys,
) -> Result<Vec<OrphanReceipt>, WriteError> {
    let rows = store.scan(&keys.orphan_receipts_prefix()).await?;
    rows.into_iter()
        .map(|(_, value)| OrphanReceipt::decode(&value))
        .collect()
}

/// Why a protection record is released.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrphanResolution {
    /// A commit now references the uploaded objects.
    Committed,
    /// The operation will never commit; the objects are ordinary garbage.
    Abandoned,
}

impl OrphanResolution {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Committed => "committed",
            Self::Abandoned => "abandoned",
        }
    }
}

/// What releasing a protection record did.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrphanRelease {
    pub operation_id: Id16,
    pub resolution: OrphanResolution,
    pub released_objects: Vec<ObjectId>,
}

/// Release protection exactly once, keyed on the recorded bytes.
///
/// A resolution derived from a *different* record is refused rather than
/// deleting someone else's protection: the transaction checks the bytes it
/// read, so a concurrent replacement makes the release a conflict.
pub async fn resolve_orphan_receipt(
    store: &dyn ControlStore,
    keys: &Keys,
    operation_id: &Id16,
    resolution: OrphanResolution,
) -> Result<OrphanRelease, WriteError> {
    let key = keys.orphan_receipt(operation_id);
    let bytes = store.get(&key).await?.ok_or_else(|| {
        WriteError::Record(format!(
            "operation {operation_id:02x?} has no protected upload receipt to release"
        ))
    })?;
    let receipt = OrphanReceipt::decode(&bytes)?;
    let released_objects = receipt.objects.iter().map(|o| o.object_id).collect();
    store
        .run(Txn::new().check_bytes(key.clone(), bytes).delete(key))
        .await
        .map_err(|error| match error {
            super::store::StoreError::Conflict => WriteError::Conflict(
                "the protected upload receipt changed before it could be released".into(),
            ),
            other => other.into(),
        })?;
    Ok(OrphanRelease {
        operation_id: *operation_id,
        resolution,
        released_objects,
    })
}

/// Result of planning a collection pass.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CollectionPlan {
    /// Candidates that may be deleted.
    pub collectable: BTreeSet<ObjectId>,
    /// Candidates that are still protected by an uncommitted upload.
    pub blocked_by_protection: BTreeSet<ObjectId>,
}

/// Split a candidate set into collectable and protected objects.  A protected
/// candidate is moved aside, never silently deleted.
pub fn plan_collection(
    candidates: &BTreeSet<ObjectId>,
    protected: &BTreeSet<ObjectId>,
) -> CollectionPlan {
    CollectionPlan {
        collectable: candidates.difference(protected).copied().collect(),
        blocked_by_protection: candidates.intersection(protected).copied().collect(),
    }
}

/// The guard a cleaner calls before it deletes: fail closed if a protected
/// object reached the delete set, instead of relying on upstream bookkeeping.
pub fn ensure_not_collected(
    plan: &CollectionPlan,
    protected: &BTreeSet<ObjectId>,
) -> Result<(), WriteError> {
    if let Some(object) = plan.collectable.intersection(protected).next() {
        return Err(WriteError::Record(format!(
            "object {} is protected by an uncommitted upload receipt",
            hex::encode(object)
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native_base::wire::container::{Codec, ObjectKind};
    use crate::native_base::wire::refs::{ObjectRef, PageAddress, PageKind, RootRef};
    use crate::native_base::write::memory::MemoryControlStore;

    const VOLUME: Id16 = [9u8; 16];

    fn object(seed: u8, len: u64) -> ObjectRef {
        ObjectRef {
            object_id: [seed; 16],
            kind: ObjectKind::FrozenMetadata.as_u8(),
            object_len: len,
            full_hash: [seed; 32],
            key: format!("loose/{seed}").into_bytes(),
        }
    }

    fn root(object: ObjectRef) -> RootRef {
        RootRef {
            object,
            address: PageAddress {
                offset: 0,
                stored_len: 0,
                raw_len: 0,
                codec: Codec::None,
                page_kind: PageKind::GenericKeyValue,
                level: 0,
                entry_count: 0,
                stored_digest: [0u8; 32],
            },
        }
    }

    fn receipt(objects: Vec<ObjectRef>, with_receipts_root: bool) -> OrphanReceipt {
        let receipts = with_receipts_root.then(|| root(objects[0].clone()));
        OrphanReceipt::after_kv_failure(
            [7u8; 16],
            [8u8; 16],
            [6u8; 16],
            if with_receipts_root {
                KvStep::Commit
            } else {
                KvStep::DataUploaded
            },
            "simulated KV failure",
            receipts,
            objects,
        )
        .unwrap()
        .unwrap()
    }

    /// WRITE-006 / INV-11: a KV failure that happened before anything was
    /// uploaded protects nothing; once object bytes are durable the receipt is
    /// recorded, round-trips byte-exactly and refuses tampering.
    #[test]
    fn only_a_durable_upload_needs_protection_and_the_record_is_fail_closed() {
        assert!(
            OrphanReceipt::after_kv_failure(
                [7u8; 16],
                [8u8; 16],
                [6u8; 16],
                KvStep::Registration,
                "registration transaction failed",
                None,
                Vec::new(),
            )
            .unwrap()
            .is_none(),
            "nothing was uploaded, so there is nothing to protect"
        );
        assert!(KvStep::Registration.upload_is_durable() == false);
        assert!(KvStep::DataUploaded.upload_is_durable());
        assert!(KvStep::ReceiptsUploaded > KvStep::DataUploaded);

        let record = receipt(vec![object(1, 64), object(2, 64)], false);
        assert_eq!(record.step, KvStep::DataUploaded);
        assert!(record.receipts.is_none());
        let encoded = record.encode().unwrap();
        assert_eq!(OrphanReceipt::decode(&encoded).unwrap(), record);
        assert_eq!(record.digest().unwrap(), record.digest().unwrap());
        let mut corrupted = encoded.clone();
        corrupted[0] ^= 0xff;
        assert!(OrphanReceipt::decode(&corrupted).is_err());
        let mut trailing = encoded.clone();
        trailing.push(0);
        assert!(OrphanReceipt::decode(&trailing).is_err());

        // An empty object set, a non-durable step and a receipts root that is
        // not itself protected are all refused.
        assert!(
            OrphanReceipt::after_kv_failure(
                [7u8; 16],
                [8u8; 16],
                [6u8; 16],
                KvStep::Commit,
                "reason",
                None,
                Vec::new(),
            )
            .is_err()
        );
        let stranger = receipt(vec![object(3, 64)], false);
        let mut stray_root = stranger.clone();
        stray_root.receipts = Some(root(object(4, 64)));
        assert!(
            stray_root.validate().is_err(),
            "the receipts container must be part of the protected set"
        );
        let mut unsorted = receipt(vec![object(1, 64), object(2, 64)], false);
        unsorted.objects.reverse();
        assert!(unsorted.validate().is_err());

        // Two identical blocks dedup to one protected object.
        let deduped = receipt(vec![object(1, 64), object(1, 64)], false);
        assert_eq!(deduped.objects.len(), 1);

        let good = receipt(vec![object(1, 64), object(2, 64)], true);
        assert_eq!(good.objects.len(), 2);
        assert!(good.receipts.is_some());
        assert_eq!(
            good,
            OrphanReceipt::decode(&good.encode().unwrap()).unwrap()
        );
    }

    /// WRITE-006: the protection is durable and idempotent by digest, releasing
    /// it happens exactly once against the recorded bytes, and a protected
    /// object can never reach the collectable set.
    #[tokio::test]
    async fn protection_is_durable_idempotent_and_never_collectable() {
        let store = MemoryControlStore::new();
        let keys = Keys::new(&VOLUME);
        let record = receipt(vec![object(1, 64), object(2, 64), object(3, 64)], true);
        assert!(
            record_orphan_receipt(&store, &keys, &record).await.unwrap(),
            "first write records the protection"
        );
        assert!(
            !record_orphan_receipt(&store, &keys, &record).await.unwrap(),
            "the same digest is idempotent"
        );
        assert_eq!(
            read_orphan_receipt(&store, &keys, &record.operation_id)
                .await
                .unwrap()
                .unwrap(),
            record
        );
        assert_eq!(
            scan_orphan_receipts(&store, &keys).await.unwrap(),
            vec![record.clone()]
        );

        // The same OperationId with another payload is refused.
        let mut conflicting = record.clone();
        conflicting.reason = "another reason".into();
        let error = record_orphan_receipt(&store, &keys, &conflicting)
            .await
            .unwrap_err();
        assert!(
            matches!(error, WriteError::OperationIdMismatch(_)),
            "{error}"
        );
        assert_eq!(
            read_orphan_receipt(&store, &keys, &record.operation_id)
                .await
                .unwrap()
                .unwrap(),
            record,
            "the conflicting write changed nothing"
        );

        // Protection keeps the objects out of the collectable set.
        let protected = protected_object_ids(std::iter::once(&record));
        assert_eq!(protected.len(), 3);
        let candidates = protected.clone();
        let plan = plan_collection(&candidates, &protected);
        assert!(plan.collectable.is_empty());
        assert_eq!(plan.blocked_by_protection, protected);
        ensure_not_collected(&plan, &protected).unwrap();
        let unsafe_plan = CollectionPlan {
            collectable: protected.clone(),
            blocked_by_protection: BTreeSet::new(),
        };
        let error = ensure_not_collected(&unsafe_plan, &protected).unwrap_err();
        assert!(error.to_string().contains("protected"), "{error}");

        // Releasing is keyed on the recorded bytes and happens exactly once.
        let release = resolve_orphan_receipt(
            &store,
            &keys,
            &record.operation_id,
            OrphanResolution::Abandoned,
        )
        .await
        .unwrap();
        assert_eq!(release.resolution, OrphanResolution::Abandoned);
        assert_eq!(release.released_objects.len(), 3);
        assert_eq!(release.resolution.as_str(), "abandoned");
        assert!(
            read_orphan_receipt(&store, &keys, &record.operation_id)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            resolve_orphan_receipt(
                &store,
                &keys,
                &record.operation_id,
                OrphanResolution::Committed,
            )
            .await
            .is_err(),
            "protection is released exactly once"
        );
        assert!(
            scan_orphan_receipts(&store, &keys)
                .await
                .unwrap()
                .is_empty()
        );

        // After the release the same objects are ordinary candidates.
        let plan = plan_collection(&candidates, &BTreeSet::new());
        assert_eq!(plan.collectable, candidates);
        assert!(plan.blocked_by_protection.is_empty());
    }
}
