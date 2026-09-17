//! Exact candidate closure partitioning and RetainBatch construction.

use std::collections::{BTreeMap, BTreeSet};

use sha2::{Digest, Sha256};

use crate::native_base::wire::bnct::{Id16, RetentionReceipt};
use crate::native_base::wire::refs::{Hash32, ObjectId, ObjectRef};
use crate::native_base::wire::uvarint::Writer;

use super::index::{BuiltObjectIndex, DEFAULT_INVENTORY_LEAF_TARGET, build_object_index};
use super::{LifecycleError, LifecycleResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectOrigin {
    /// Candidate-owned object.  Only the workspace and current build domain
    /// are permitted in one P1 publication.
    Owned(Id16),
    /// Dependency of an already permanent PublishedRevision.
    Published(Hash32),
}

#[derive(Debug, Clone)]
pub struct CandidateClosure {
    pub manifest: ObjectRef,
    pub physical_inventory: BuiltObjectIndex,
    /// Exact origin for the manifest, inventory container, and every target
    /// listed by the physical inventory.
    pub origins: BTreeMap<ObjectId, ObjectOrigin>,
    /// Published source views the verification certificate authorized.
    pub authorized_sources: BTreeSet<Hash32>,
    /// Reported separately from new retention work (RET-022).
    pub scanned_metadata_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RetentionMetrics {
    pub new_retained_objects: u64,
    pub new_retained_bytes: u64,
    pub scanned_metadata_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionPlan {
    pub by_domain: BTreeMap<Id16, Vec<ObjectRef>>,
    pub metrics: RetentionMetrics,
}

fn insert_exact(
    objects: &mut BTreeMap<ObjectId, ObjectRef>,
    object: ObjectRef,
) -> LifecycleResult<()> {
    if let Some(previous) = objects.insert(object.object_id, object.clone())
        && previous != object
    {
        return Err(LifecycleError::Retention(format!(
            "ObjectId {:02x?} resolves to different ObjectRefs",
            object.object_id
        )));
    }
    Ok(())
}

/// Partition the verified physical closure by origin domain. Dependencies
/// coming from authorized PublishedRevisions are already permanent and are
/// intentionally omitted from new batches.
pub fn prepare_retention(
    closure: &CandidateClosure,
    owned_domains: &[Id16],
) -> LifecycleResult<RetentionPlan> {
    if owned_domains.is_empty() || owned_domains.len() > 2 {
        return Err(LifecycleError::Retention(
            "P1 publication requires one or two owned domains".into(),
        ));
    }
    let owned: BTreeSet<Id16> = owned_domains.iter().copied().collect();
    if owned.len() != owned_domains.len() {
        return Err(LifecycleError::Retention(
            "owned domain list contains duplicates".into(),
        ));
    }
    if closure
        .physical_inventory
        .entries
        .iter()
        .any(|object| object.object_id == closure.manifest.object_id)
    {
        return Err(LifecycleError::Retention(
            "physical inventory must exclude the manifest itself".into(),
        ));
    }

    let mut required = BTreeMap::new();
    insert_exact(&mut required, closure.manifest.clone())?;
    insert_exact(
        &mut required,
        closure.physical_inventory.root.object.clone(),
    )?;
    for object in &closure.physical_inventory.entries {
        insert_exact(&mut required, object.clone())?;
    }
    let required_ids: BTreeSet<_> = required.keys().copied().collect();
    let origin_ids: BTreeSet<_> = closure.origins.keys().copied().collect();
    if required_ids != origin_ids {
        return Err(LifecycleError::Retention(format!(
            "origin map is not exact: {} required objects, {} origin entries",
            required_ids.len(),
            origin_ids.len()
        )));
    }

    let mut by_domain: BTreeMap<Id16, Vec<ObjectRef>> = BTreeMap::new();
    let mut metrics = RetentionMetrics {
        scanned_metadata_bytes: closure.scanned_metadata_bytes,
        ..RetentionMetrics::default()
    };
    for (object_id, object) in required {
        match closure.origins.get(&object_id).unwrap() {
            ObjectOrigin::Owned(domain_id) => {
                if !owned.contains(domain_id) {
                    return Err(LifecycleError::Retention(format!(
                        "object {:02x?} comes from an unowned sibling private domain",
                        object_id
                    )));
                }
                metrics.new_retained_objects += 1;
                metrics.new_retained_bytes = metrics
                    .new_retained_bytes
                    .checked_add(object.object_len)
                    .ok_or_else(|| {
                        LifecycleError::LimitExceeded("retained byte sum overflow".into())
                    })?;
                by_domain.entry(*domain_id).or_default().push(object);
            }
            ObjectOrigin::Published(storage_view_id) => {
                if !closure.authorized_sources.contains(storage_view_id) {
                    return Err(LifecycleError::Retention(format!(
                        "object {:02x?} cites an unauthorized PublishedRevision",
                        object_id
                    )));
                }
            }
        }
    }
    for objects in by_domain.values_mut() {
        objects.sort_by_key(|object| object.object_id);
    }
    Ok(RetentionPlan { by_domain, metrics })
}

/// Stable digest bound into `RetentionReceipt.verified_subset_digest`.
pub fn retained_subset_digest(domain_id: &Id16, objects: &[ObjectRef]) -> Hash32 {
    let mut hasher = Sha256::new();
    hasher.update(b"BrewFS.RetainBatch.v1\0");
    hasher.update(domain_id);
    hasher.update((objects.len() as u64).to_le_bytes());
    for object in objects {
        let mut w = Writer::new();
        object.encode_into(&mut w);
        let encoded = w.into_bytes();
        hasher.update((encoded.len() as u64).to_le_bytes());
        hasher.update(encoded);
    }
    hasher.finalize().into()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetainBatchArtifact {
    pub domain_id: Id16,
    pub index: BuiltObjectIndex,
    pub verified_subset_digest: Hash32,
}

/// Build one domain-local RetainBatch.  Its own index ObjectRef is omitted;
/// the future authority receipt's RootRef protects it without self-hashing.
pub fn build_retain_batch(
    domain_id: Id16,
    objects: &[ObjectRef],
    index_object_id: ObjectId,
    key: Vec<u8>,
) -> LifecycleResult<RetainBatchArtifact> {
    if objects.is_empty() {
        return Err(LifecycleError::Retention(
            "empty domains do not need RetainBatch records".into(),
        ));
    }
    let index = build_object_index(objects, index_object_id, key, DEFAULT_INVENTORY_LEAF_TARGET)?;
    let verified_subset_digest = retained_subset_digest(&domain_id, &index.entries);
    Ok(RetainBatchArtifact {
        domain_id,
        index,
        verified_subset_digest,
    })
}

impl RetainBatchArtifact {
    pub fn receipt(
        &self,
        retention_seq: u64,
        operation_id: Id16,
        candidate_view: Hash32,
    ) -> RetentionReceipt {
        RetentionReceipt {
            domain_id: self.domain_id,
            retention_seq,
            operation_id,
            candidate_view,
            retained_objects: self.index.root.clone(),
            // The outer authority reference is the non-cyclic retention edge
            // for the index container itself.  No ObjectRef to this object is
            // present in its leaf list.
            evidence_root: self.index.root.clone(),
            verified_subset_digest: self.verified_subset_digest,
        }
    }
}
