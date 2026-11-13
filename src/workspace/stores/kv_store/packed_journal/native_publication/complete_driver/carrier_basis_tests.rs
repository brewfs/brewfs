//! Assertions on actual production final packets and durable successors.
//! Real Redis/TiKV test entrypoints reuse the original VFS/capture/factory.

use super::*;
use crate::workspace::stores::kv_store::packed_carrier_basis::{
    PackedCarrierBasis, packed_carrier_basis_key, packed_carrier_claim_key,
};

const CONFLICT: &[u8] = b"counterfactual-existing-carrier-basis";

fn is_carrier_key(key: &[u8]) -> bool {
    key.starts_with(b"packed/sealed-carrier/") || key.starts_with(b"packed/carrier-claim/")
}

pub(super) fn assert_no_standalone_writes(writes: &[KvWrite], final_packet: bool) {
    for write in writes {
        let key = match write {
            KvWrite::Put { key, .. } | KvWrite::Delete { key } => key,
        };
        assert!(
            !is_carrier_key(key) || final_packet,
            "carrier descriptor/claim escaped the actual final publication CAS"
        );
    }
}

fn put_value<'a>(writes: &'a [KvWrite], key: &[u8]) -> &'a [u8] {
    let values = writes
        .iter()
        .filter_map(|write| match write {
            KvWrite::Put { key: actual, value } if actual.as_slice() == key => {
                Some(value.as_slice())
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        values.len(),
        1,
        "actual final packet omitted or duplicated a required write"
    );
    values[0]
}

pub(super) fn assert_packet(checks: &[KvCheck], writes: &[KvWrite]) {
    assert_no_standalone_writes(writes, true);
    let mut confirmation_keys = checks
        .iter()
        .map(|check| check.key.clone())
        .collect::<std::collections::BTreeSet<_>>();
    for write in writes {
        confirmation_keys.insert(match write {
            KvWrite::Put { key, .. } | KvWrite::Delete { key } => key.clone(),
        });
    }
    assert!(
        confirmation_keys.len() <= 64,
        "exact unknown-response confirmation exceeds the existing native proof cap"
    );
    let records = writes
        .iter()
        .filter_map(|write| match write {
            KvWrite::Put { key, value } if key.starts_with(JOURNAL_PREFIX) => {
                PackedJournalRecord::decode(value)
                    .ok()
                    .filter(|record| record.phase == PackedJournalPhase::Committed)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(records.len(), 1);
    let record = &records[0];
    let target = record.commit_target.as_ref().unwrap();
    let plan = record
        .native_rebind
        .as_ref()
        .unwrap()
        .publication
        .as_ref()
        .unwrap();
    let descriptor_key = packed_carrier_basis_key(plan.carrier_layer_id);
    let claim_key = packed_carrier_claim_key(plan.carrier_layer_id);
    let descriptor = put_value(writes, &descriptor_key).to_vec();
    let claim = put_value(writes, &claim_key).to_vec();
    let basis =
        PackedCarrierBasis::decode_pair(&target.base_revision, &Some(descriptor), &Some(claim))
            .unwrap()
            .unwrap();
    for key in [&descriptor_key, &claim_key] {
        let matched = checks
            .iter()
            .filter(|check| check.key == *key)
            .collect::<Vec<_>>();
        assert_eq!(matched.len(), 1);
        assert_eq!(
            matched[0].expected, None,
            "carrier keys require exact absence in the final CAS"
        );
    }
    let source: LayerRecord = decode_open_value(
        put_value(writes, &hot_layer_key(record.guard.expected_head_layer_id)),
        OPEN_RECORD_MAX_BYTES,
    )
    .unwrap();
    let carrier: LayerRecord = decode_open_value(
        put_value(writes, &hot_layer_key(plan.carrier_layer_id)),
        OPEN_RECORD_MAX_BYTES,
    )
    .unwrap();
    let head: LayerRecord = decode_open_value(
        put_value(writes, &hot_layer_key(target.head_layer_id)),
        OPEN_RECORD_MAX_BYTES,
    )
    .unwrap();
    assert_eq!(basis.source_binding, *target);
    assert_eq!(basis.registry_incarnation, record.source.staging_id);
    assert_eq!(basis.carrier_revision, plan.carrier_revision());
    assert_eq!(
        basis.native_sealed_source_revision,
        BaseRevision {
            layer_id: source.layer_id,
            sealed_version: source.sealed_version.unwrap(),
            root_hash: source.root_hash.unwrap(),
        }
    );
    assert_eq!(
        basis.native_sealed_source_revision.layer_id,
        record.guard.expected_head_layer_id
    );
    assert_eq!(
        basis.native_sealed_source_revision.sealed_version,
        plan.source_sealed_version
    );
    assert_ne!(basis.carrier_revision, basis.native_sealed_source_revision);
    assert_eq!(source.state, LayerState::Sealed);
    assert_eq!(carrier.state, LayerState::Sealed);
    assert_eq!(carrier.parent_layer_id, None);
    assert_eq!(head.parent_layer_id, Some(carrier.layer_id));
    assert_eq!(carrier.root_hash, Some(plan.carrier_root_hash));
    assert_eq!(
        put_value(writes, &packed_current_key(target.workspace_id)),
        target.encode().unwrap()
    );
    assert_eq!(
        put_value(
            writes,
            &packed_history_key(target.workspace_id, target.binding.binding_version)
        ),
        target.encode().unwrap()
    );
    assert!(
        !put_value(
            writes,
            &registry::registry_root_key(record.source.staging_id)
        )
        .is_empty()
    );
    assert!(
        checks
            .iter()
            .any(|check| { check.key.as_slice() == CONTROL_KEY && check.expected.is_some() })
    );
    assert!(writes.iter().all(|write| match write {
        KvWrite::Put { key, .. } | KvWrite::Delete { key } => key.as_slice() != CONTROL_KEY,
    }));
    assert!(!put_value(writes, TOPOLOGY_GENERATION_KEY).is_empty());
    assert!(!put_value(writes, &hot_workspace_key(target.workspace_id)).is_empty());
    assert!(!put_value(writes, PACKED_ROOT_GENERATION_KEY).is_empty());
    assert!(!put_value(writes, LAYER_INVENTORY_GENERATION_KEY).is_empty());
    assert!(writes.iter().any(|write| matches!(write,
        KvWrite::Put { key, .. } if key.starts_with(b"packed/native-hold/"))));
    assert!(
        checks
            .iter()
            .any(|check| check.key == packed_claim_key(target.workspace_id)
                && check.expected.as_deref() == Some(PACKED_CLAIM))
    );
}

pub(super) async fn assert_committed<B: WorkspaceKvBackend>(
    backend: &FinalDelivery<B>,
    target: &PackedLowerBindingRecord,
    staged: &PackedJournalRecord,
    source_root: [u8; 32],
) {
    let descriptor_key = packed_carrier_basis_key(target.base_revision.layer_id);
    let claim_key = packed_carrier_claim_key(target.base_revision.layer_id);
    let descriptor = backend.get(&descriptor_key).await.unwrap();
    let claim = backend.get(&claim_key).await.unwrap();
    let basis = PackedCarrierBasis::decode_pair(&target.base_revision, &descriptor, &claim)
        .unwrap()
        .unwrap();
    assert_eq!(basis.source_binding, *target);
    assert_eq!(basis.registry_incarnation, staged.source.staging_id);
    assert_eq!(
        basis.native_sealed_source_revision.layer_id,
        staged.guard.expected_head_layer_id
    );
    assert_eq!(basis.native_sealed_source_revision.root_hash, source_root);
    assert_ne!(basis.native_sealed_source_revision, target.base_revision);
    let packet = backend.final_writes.lock().unwrap().clone().unwrap();
    assert_eq!(
        descriptor.as_deref(),
        Some(put_value(&packet, &descriptor_key))
    );
    assert_eq!(claim.as_deref(), Some(put_value(&packet, &claim_key)));
    assert_eq!(backend.final_calls.load(Ordering::SeqCst), 1);
    if backend.mode.load(Ordering::SeqCst) == Delivery::LostReply as u8 {
        let reads = backend.confirmation_reads.lock().unwrap().clone();
        assert!(reads.is_empty(), "confirmation must not issue point reads");
        assert_eq!(backend.confirmation_calls.load(Ordering::SeqCst), 1);
        let (proof, deadline) = backend.confirmation_proof.lock().unwrap().clone().unwrap();
        let (original, original_deadline) = backend.final_checks.lock().unwrap().clone().unwrap();
        assert_eq!(
            deadline, original_deadline,
            "confirmation cannot renew the original deadline"
        );
        let mut expected = original
            .into_iter()
            .map(|check| (check.key, check.expected))
            .collect::<std::collections::BTreeMap<_, _>>();
        for write in packet {
            match write {
                KvWrite::Put { key, value } => {
                    expected.insert(key, Some(value));
                }
                KvWrite::Delete { key } => {
                    expected.insert(key, None);
                }
            }
        }
        assert!(
            expected.len() > journal_point_limits().max_records,
            "actual final successor must exercise the old point-read tier limit"
        );
        let actual = proof
            .into_iter()
            .map(|check| (check.key, check.expected))
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(
            actual, expected,
            "timed no-op CAS must prove every exact successor together"
        );
    }
}

pub(super) async fn inject_descriptor_conflict<B: WorkspaceKvBackend>(
    backend: &B,
    checks: &[KvCheck],
    writes: &[KvWrite],
) {
    let record = writes
        .iter()
        .find_map(|write| match write {
            KvWrite::Put { key, value } if key.starts_with(JOURNAL_PREFIX) => {
                PackedJournalRecord::decode(value).ok()
            }
            _ => None,
        })
        .unwrap();
    let carrier = record.commit_target.unwrap().base_revision.layer_id;
    let key = packed_carrier_basis_key(carrier);
    assert!(
        checks
            .iter()
            .any(|check| check.key == key && check.expected.is_none())
    );
    assert!(
        backend
            .compare_and_swap(
                &[KvCheck {
                    key: key.clone(),
                    expected: None
                }],
                &[KvWrite::Put {
                    key,
                    value: CONFLICT.to_vec()
                }]
            )
            .await
            .unwrap()
    );
}

pub(super) async fn assert_uncommitted<B: WorkspaceKvBackend>(
    backend: &FinalDelivery<B>,
    guard: &HeadGuard,
    old: &PackedLowerBindingRecord,
    target: &PackedLowerBindingRecord,
    staged: &PackedJournalRecord,
    conflicted: bool,
) {
    let descriptor = backend
        .get(&packed_carrier_basis_key(target.base_revision.layer_id))
        .await
        .unwrap();
    assert_eq!(descriptor, conflicted.then(|| CONFLICT.to_vec()));
    assert_eq!(
        backend
            .get(&packed_carrier_claim_key(target.base_revision.layer_id))
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        backend
            .get(&hot_layer_key(target.base_revision.layer_id))
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        backend
            .get(&hot_layer_key(target.head_layer_id))
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        backend
            .get(&packed_history_key(
                target.workspace_id,
                target.binding.binding_version
            ))
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        backend
            .get(&packed_current_key(guard.workspace_id))
            .await
            .unwrap(),
        Some(old.encode().unwrap())
    );
    let actual = PackedJournalRecord::decode(
        &backend
            .get(&journal_key(staged.journal_id))
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(actual, *staged);
    let source: LayerRecord = decode_open_value(
        &backend
            .get(&hot_layer_key(guard.expected_head_layer_id))
            .await
            .unwrap()
            .unwrap(),
        OPEN_RECORD_MAX_BYTES,
    )
    .unwrap();
    assert_eq!(source.state, LayerState::Sealing);
}

#[tokio::test]
#[ignore = "requires isolated BREWFS_TEST_REDIS_URL; actual final CAS false"]
async fn real_redis_carrier_basis_final_cas_false_has_no_partial_write_or_second_final() {
    redis(Delivery::CarrierFinalFalse).await;
}

#[tokio::test]
#[ignore = "requires isolated BREWFS_TEST_TIKV_PD_ENDPOINTS; actual final CAS false"]
async fn real_tikv_carrier_basis_final_cas_false_has_no_partial_write_or_second_final() {
    tikv(Delivery::CarrierFinalFalse).await;
}

#[tokio::test]
#[ignore = "requires isolated BREWFS_TEST_REDIS_URL; actual factory counterfactual"]
async fn real_redis_carrier_basis_counterfactual_native_revision_rejected_before_final() {
    redis(Delivery::CarrierCounterfactual).await;
}

#[tokio::test]
#[ignore = "requires isolated BREWFS_TEST_TIKV_PD_ENDPOINTS; actual factory counterfactual"]
async fn real_tikv_carrier_basis_counterfactual_native_revision_rejected_before_final() {
    tikv(Delivery::CarrierCounterfactual).await;
}
