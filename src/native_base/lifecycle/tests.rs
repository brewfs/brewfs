use std::collections::{BTreeMap, BTreeSet};

use sha2::{Digest, Sha256};

use super::cleanup::{CloseStartRequest, close_domain_start};
use super::fork::{FastForwardRequest, ForkRequest, fast_forward, fork_from_published};
use super::index::{build_object_index, open_object_index};
use super::manifest::{KvNamespace, SnapshotManifest, open_manifest};
use super::options::{LegacyRetentionOptions, reject_legacy_retention_options};
use super::publish::{
    DomainRetentionInput, PublicationOutcome, PublicationRequest, abort_publication,
    complete_publication, decode_published_revision, publish_candidate,
};
use super::retention::{
    CandidateClosure, ObjectOrigin, build_retain_batch, prepare_retention, retained_subset_digest,
};
use super::seal::{
    DrainPlanBatch, RecoveryAction, RemoteDurability, begin_seal, commit_drain_batch,
    drain_plan_digest, finish_data_drain, mark_candidate_verified, mark_retention_prepared,
    quiesce_with_batches, recovery_action,
};
use crate::native_base::wire::bnct::{
    ControlRecord, DomainKind, DomainState, HeadRef, KvBaseRetention, NativePublicationJournal,
    NativeWorkspaceHead, ObjectRegistration, OwnershipDomain, PlanEntry, PublicationPhase,
    PublicationResult, PublishedRevision, RegistrationState, RetentionPolicy, SnapshotRef,
    WorkspaceHeadState,
};
use crate::native_base::wire::container::{Codec, ObjectKind};
use crate::native_base::wire::refs::{ObjectRef, PageAddress, PageKind, RootRef};
use crate::native_base::write::domain::decode_domain;
use crate::native_base::write::keys::Keys;
use crate::native_base::write::memory::MemoryControlStore;
use crate::native_base::write::records::HeadState;
use crate::native_base::write::store::{ControlStore, Txn};

fn object(id: u8, kind: u8, key: &str) -> ObjectRef {
    ObjectRef {
        object_id: [id; 16],
        kind,
        object_len: 128 + u64::from(id),
        full_hash: Sha256::digest([id; 7]).into(),
        key: key.as_bytes().to_vec(),
    }
}

fn address(page_kind: PageKind, seed: u8) -> PageAddress {
    PageAddress {
        offset: 64,
        stored_len: 64,
        raw_len: 64,
        codec: Codec::None,
        page_kind,
        level: 0,
        entry_count: 1,
        stored_digest: [seed; 32],
    }
}

fn root(id: u8) -> RootRef {
    RootRef {
        object: object(
            id,
            ObjectKind::PagedInventory.as_u8(),
            &format!("inventory/{id}.brfin"),
        ),
        address: address(PageKind::InventoryIndex, id),
    }
}

fn snapshot(volume_id: [u8; 16], id: u8) -> SnapshotRef {
    SnapshotRef {
        volume_id,
        logical_revision: [id; 32],
        manifest: object(
            id,
            ObjectKind::SnapshotManifest.as_u8(),
            &format!("manifest/{id}.brfsm"),
        ),
    }
}

fn domain(
    volume_id: [u8; 16],
    domain_id: [u8; 16],
    kind: DomainKind,
    generation: u64,
) -> OwnershipDomain {
    OwnershipDomain {
        domain_id,
        volume_id,
        namespace_id: [44; 16],
        domain_kind: kind,
        owner_id: [45; 16],
        owner_generation: generation,
        state: DomainState::Active,
        entity_version: 1,
        inventory_seq: 100,
        retention_seq: 0,
        outstanding_attempts: 0,
        open_operations: 0,
        accepted_ticket_end: None,
        close_ref: None,
    }
}

async fn put(store: &dyn ControlStore, key: Vec<u8>, value: Vec<u8>) {
    store
        .run(Txn::new().check_absent(key.clone()).put(key, value))
        .await
        .unwrap();
}

async fn seed_verified(
    store: &dyn ControlStore,
    keys: &Keys,
    domain_id: [u8; 16],
    object: &ObjectRef,
    seq: u64,
) {
    let registration = ObjectRegistration {
        object_ref: object.clone(),
        domain_id,
        upload_plan_hash: [seq as u8; 32],
        registration_seq: seq,
        attempt_generation: 1,
        state: RegistrationState::Verified,
    };
    put(
        store,
        keys.object(&domain_id, &object.object_id),
        ControlRecord::ObjectRegistration(registration).encode(),
    )
    .await;
    put(
        store,
        keys.registry(&[44; 16], &object.key),
        object.object_id.to_vec(),
    )
    .await;
}

#[test]
fn manifest_and_object_inventory_roundtrip_without_self_reference() {
    let loose = object(1, 6, "loose/1");
    let pack = object(2, ObjectKind::DataPack.as_u8(), "pack/2.brfdp");
    let seal = object(3, ObjectKind::DataSeal.as_u8(), "seal/3.brfds");
    let inventory = build_object_index(
        &[loose.clone(), pack.clone(), seal.clone()],
        [4; 16],
        b"inventory/4.brfin".to_vec(),
        96,
    )
    .unwrap();
    assert_eq!(
        open_object_index(&inventory.root, &inventory.bytes).unwrap(),
        vec![loose, pack, seal]
    );
    assert!(
        inventory
            .entries
            .iter()
            .all(|entry| entry.object_id != inventory.root.object.object_id)
    );

    let manifest = SnapshotManifest {
        volume_id: [9; 16],
        storage_namespace_id: [8; 16],
        chunk_size: 1024 * 1024,
        block_size: 4096,
        required_features: 0,
        logical_revision: [7; 32],
        namespace_digest: [6; 32],
        binding_digest: [5; 32],
        namespace: KvNamespace {
            layer_id: [4; 16],
            sealed_version: 11,
        },
        data_seal: RootRef {
            object: object(3, ObjectKind::DataSeal.as_u8(), "seal/3.brfds"),
            address: address(PageKind::TableRootDirectory, 3),
        },
        physical_inventory: inventory.root,
        file_count: 2,
        directory_count: 1,
        total_logical_bytes: 8192,
        created_at_ns: 123,
    };
    let built = manifest
        .build([5; 16], b"manifest/5.brfsm".to_vec())
        .unwrap();
    assert_eq!(open_manifest(&built.object, &built.bytes).unwrap(), built);
    let mut corrupt = built.bytes.clone();
    corrupt[70] ^= 1;
    assert!(open_manifest(&built.object, &corrupt).is_err());
}

#[test]
fn exact_retention_partitions_owned_domains_and_rejects_siblings() {
    let workspace = [10; 16];
    let build = [11; 16];
    let sibling = [12; 16];
    let loose_b = object(1, 6, "loose/b");
    let pack = object(2, 1, "pack/p");
    let seal = object(3, 2, "seal/s");
    let inventory = build_object_index(
        &[loose_b.clone(), pack.clone(), seal.clone()],
        [4; 16],
        b"inventory/i".to_vec(),
        256,
    )
    .unwrap();
    let manifest = object(5, 4, "manifest/m");
    let mut origins = BTreeMap::new();
    origins.insert(loose_b.object_id, ObjectOrigin::Owned(workspace));
    origins.insert(pack.object_id, ObjectOrigin::Published([77; 32]));
    origins.insert(seal.object_id, ObjectOrigin::Owned(build));
    origins.insert(inventory.root.object.object_id, ObjectOrigin::Owned(build));
    origins.insert(manifest.object_id, ObjectOrigin::Owned(build));
    let closure = CandidateClosure {
        manifest,
        physical_inventory: inventory,
        origins,
        authorized_sources: BTreeSet::from([[77; 32]]),
        scanned_metadata_bytes: 999,
    };
    let plan = prepare_retention(&closure, &[workspace, build]).unwrap();
    assert_eq!(plan.by_domain[&workspace], vec![loose_b]);
    assert_eq!(plan.by_domain[&build].len(), 3); // manifest + inventory + seal
    assert_eq!(plan.metrics.new_retained_objects, 4);
    assert_eq!(plan.metrics.scanned_metadata_bytes, 999);

    let mut bad = closure.clone();
    bad.origins
        .insert(bad.manifest.object_id, ObjectOrigin::Owned(sibling));
    assert!(prepare_retention(&bad, &[workspace, build]).is_err());
}

#[test]
fn drain_batches_enforce_entry_and_payload_limits_before_persistence() {
    let template = PlanEntry {
        admission_ticket: 1,
        inode: 1,
        mutation_order: 1,
        logical_offset: 0,
        logical_len: 1,
        operation_id: [1; 16],
        payload_digest: [2; 32],
        source_token: Vec::new(),
    };
    let mut too_many = Vec::new();
    for i in 0..=super::seal::MAX_DRAIN_BATCH_ENTRIES {
        let mut entry = template.clone();
        entry.operation_id = [(i + 1) as u8; 16];
        entry.mutation_order = i as u64 + 1;
        too_many.push(entry);
    }
    assert!(drain_plan_digest(&too_many).is_err());
    let mut too_large = template;
    too_large.source_token = vec![0; super::seal::MAX_DRAIN_BATCH_BYTES + 1];
    assert!(drain_plan_digest(&[too_large]).is_err());
}

#[tokio::test]
async fn bounded_drain_rejects_forgery_and_noop_write_barrier_then_recovers() {
    let store = MemoryControlStore::new();
    let volume = [1; 16];
    let workspace = [2; 16];
    let keys = Keys::new(&volume);
    let head = HeadState {
        head: HeadRef {
            head_id: [3; 16],
            epoch: 4,
            commit_seq: 5,
        },
        writer_generation: 6,
        write_domain_id: [7; 16],
    };
    let view = NativeWorkspaceHead {
        workspace_id: workspace,
        head: head.head.clone(),
        base: snapshot(volume, 8),
        writer_generation: 6,
        write_domain_id: [7; 16],
        visible_delta_count: 1,
        open_orphan_count: 0,
        orphan_carry_digest: None,
        state: WorkspaceHeadState::Running,
        entity_version: 1,
    };
    put(&store, keys.head(&workspace), head.encode()).await;
    put(
        &store,
        keys.workspace_view(&workspace),
        ControlRecord::NativeWorkspaceHead(view.clone()).encode(),
    )
    .await;
    let operation = [9; 16];
    begin_seal(&store, &keys, operation, workspace, &head, &view, 6, 7)
        .await
        .unwrap();
    begin_seal(&store, &keys, operation, workspace, &head, &view, 6, 7)
        .await
        .unwrap();
    let entry = PlanEntry {
        admission_ticket: 7,
        inode: 42,
        mutation_order: 1,
        logical_offset: 0,
        logical_len: 4096,
        operation_id: [10; 16],
        payload_digest: [11; 32],
        source_token: b"durable-spool-token".to_vec(),
    };
    let batch_root = root(12);
    let batches = [DrainPlanBatch {
        batch_id: 1,
        plan: batch_root.clone(),
        entries: vec![entry.clone()],
    }];
    quiesce_with_batches(
        &store,
        &keys,
        operation,
        6,
        Some(batch_root.clone()),
        &batches,
    )
    .await
    .unwrap();
    quiesce_with_batches(&store, &keys, operation, 6, Some(batch_root), &batches)
        .await
        .unwrap();
    let mut forged = entry.clone();
    forged.admission_ticket = 1;
    assert!(
        commit_drain_batch(&store, &keys, operation, 1, 6, &[forged], true, [13; 32])
            .await
            .is_err()
    );
    assert!(
        commit_drain_batch(
            &store,
            &keys,
            operation,
            1,
            6,
            &[entry.clone()],
            false,
            [13; 32]
        )
        .await
        .is_err()
    );
    let committed = commit_drain_batch(
        &store,
        &keys,
        operation,
        1,
        6,
        &[entry.clone()],
        true,
        [13; 32],
    )
    .await
    .unwrap();
    assert_eq!(
        committed.plan_digest,
        drain_plan_digest(&[entry.clone()]).unwrap()
    );
    assert_eq!(
        commit_drain_batch(&store, &keys, operation, 1, 6, &[entry], true, [13; 32])
            .await
            .unwrap(),
        committed
    );
    finish_data_drain(&store, &keys, operation, 6, &head)
        .await
        .unwrap();
    finish_data_drain(&store, &keys, operation, 6, &head)
        .await
        .unwrap();
    let candidate = snapshot(volume, 14);
    let evidence = root(15);
    assert!(
        mark_candidate_verified(
            &store,
            &keys,
            operation,
            6,
            candidate.clone(),
            evidence.clone(),
            RemoteDurability::NoopTestBarrier,
            true
        )
        .await
        .is_err()
    );
    let head_key = keys.head(&workspace);
    let stable_head_bytes = store.get(&head_key).await.unwrap().unwrap();
    let mut stale_replacement = head.clone();
    stale_replacement.head.epoch += 1;
    store
        .run(
            Txn::new()
                .check_bytes(head_key.clone(), stable_head_bytes.clone())
                .put(head_key.clone(), stale_replacement.encode()),
        )
        .await
        .unwrap();
    assert!(
        mark_candidate_verified(
            &store,
            &keys,
            operation,
            6,
            candidate.clone(),
            evidence.clone(),
            RemoteDurability::ExactReadback,
            true,
        )
        .await
        .is_err()
    );
    store
        .run(
            Txn::new()
                .check_bytes(head_key.clone(), stale_replacement.encode())
                .put(head_key, stable_head_bytes),
        )
        .await
        .unwrap();
    mark_candidate_verified(
        &store,
        &keys,
        operation,
        6,
        candidate.clone(),
        evidence.clone(),
        RemoteDurability::ExactReadback,
        true,
    )
    .await
    .unwrap();
    mark_candidate_verified(
        &store,
        &keys,
        operation,
        6,
        candidate,
        evidence.clone(),
        RemoteDurability::ExactReadback,
        true,
    )
    .await
    .unwrap();
    mark_retention_prepared(&store, &keys, operation, 6, evidence.clone())
        .await
        .unwrap();
    mark_retention_prepared(&store, &keys, operation, 6, evidence)
        .await
        .unwrap();
    assert_eq!(
        recovery_action(&store, &keys, &operation).await.unwrap(),
        RecoveryAction::VerifyOrPublish
    );
    assert_eq!(
        abort_publication(&store, &keys, &operation).await.unwrap(),
        None
    );
    let view_after_abort = super::seal::decode_workspace_view(
        &store
            .get(&keys.workspace_view(&workspace))
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(view_after_abort.state, WorkspaceHeadState::Running);
    assert_eq!(
        recovery_action(&store, &keys, &operation).await.unwrap(),
        RecoveryAction::ReturnAborted
    );
}

#[tokio::test]
async fn every_persisted_seal_phase_has_a_defined_recovery_action() {
    let volume = [16; 16];
    let keys = Keys::new(&volume);
    let operation = [17; 16];
    let cases = [
        (PublicationPhase::Prepare, RecoveryAction::ResumeQuiesce),
        (PublicationPhase::Quiesced, RecoveryAction::ResumeDrain),
        (
            PublicationPhase::DataDrained,
            RecoveryAction::RebuildCandidate,
        ),
        (
            PublicationPhase::CandidateVerified,
            RecoveryAction::VerifyOrPublish,
        ),
        (
            PublicationPhase::RetentionPrepared,
            RecoveryAction::VerifyOrPublish,
        ),
        (
            PublicationPhase::PublishedRetained,
            RecoveryAction::InstallPublishedResult,
        ),
        (
            PublicationPhase::Completed,
            RecoveryAction::ReturnCompletedResult,
        ),
        (PublicationPhase::Aborted, RecoveryAction::ReturnAborted),
    ];
    for (phase, expected) in cases {
        let store = MemoryControlStore::new();
        assert!(recovery_action(&store, &keys, &operation).await.is_err());
        let committed_result = matches!(
            phase,
            PublicationPhase::PublishedRetained | PublicationPhase::Completed
        )
        .then(|| PublicationResult {
            snapshot: snapshot(volume, 18),
            new_head: Some(HeadRef {
                head_id: [19; 16],
                epoch: 2,
                commit_seq: 0,
            }),
            publication_id: [20; 16],
        });
        let has_drained = matches!(
            phase,
            PublicationPhase::DataDrained
                | PublicationPhase::CandidateVerified
                | PublicationPhase::RetentionPrepared
                | PublicationPhase::PublishedRetained
                | PublicationPhase::Completed
        );
        let has_candidate = matches!(
            phase,
            PublicationPhase::CandidateVerified
                | PublicationPhase::RetentionPrepared
                | PublicationPhase::PublishedRetained
                | PublicationPhase::Completed
        );
        let has_retention = matches!(
            phase,
            PublicationPhase::RetentionPrepared
                | PublicationPhase::PublishedRetained
                | PublicationPhase::Completed
        );
        put(
            &store,
            keys.publication_journal(&operation),
            ControlRecord::NativePublicationJournal(NativePublicationJournal {
                operation_id: operation,
                workspace_id: None,
                expected_head: None,
                expected_base: None,
                owner_generation: 1,
                phase,
                accepted_ticket_end: 0,
                drain_plan: None,
                batch_count: 0,
                completed_count: 0,
                fixed_commit_seq: has_drained.then_some(0),
                candidate: has_candidate.then(|| snapshot(volume, 18)),
                verification_evidence: has_candidate.then(|| root(21)),
                retention_evidence: has_retention.then(|| root(22)),
                committed_result,
            })
            .encode(),
        )
        .await;
        assert_eq!(
            recovery_action(&store, &keys, &operation).await.unwrap(),
            expected
        );
    }
}

#[tokio::test]
async fn two_domain_publication_is_atomic_exact_and_idempotent() {
    let store = MemoryControlStore::new();
    let volume = [20; 16];
    let workspace_id = [21; 16];
    let workspace_domain = [22; 16];
    let build_domain = [23; 16];
    let keys = Keys::new(&volume);

    let overwritten_a = object(24, 6, "loose/overwritten-a");
    let loose_b = object(25, 6, "loose/published-b");
    let pack = object(26, 1, "pack/mixed.brfdp");
    let seal_object = object(27, 2, "seal/mixed.brfds");
    let inventory = build_object_index(
        &[loose_b.clone(), pack.clone(), seal_object.clone()],
        [28; 16],
        b"inventory/mixed.brfin".to_vec(),
        128,
    )
    .unwrap();
    let manifest_model = SnapshotManifest {
        volume_id: volume,
        storage_namespace_id: [29; 16],
        chunk_size: 1024 * 1024,
        block_size: 4096,
        required_features: 0,
        logical_revision: [30; 32],
        namespace_digest: [31; 32],
        binding_digest: [32; 32],
        namespace: KvNamespace {
            layer_id: [33; 16],
            sealed_version: 7,
        },
        data_seal: RootRef {
            object: seal_object.clone(),
            address: address(PageKind::TableRootDirectory, 27),
        },
        physical_inventory: inventory.root.clone(),
        file_count: 1,
        directory_count: 1,
        total_logical_bytes: 4096,
        created_at_ns: 1234,
    };
    let manifest = manifest_model
        .build([34; 16], b"manifest/mixed.brfsm".to_vec())
        .unwrap();
    let mut origins = BTreeMap::new();
    origins.insert(loose_b.object_id, ObjectOrigin::Owned(workspace_domain));
    origins.insert(pack.object_id, ObjectOrigin::Owned(build_domain));
    origins.insert(seal_object.object_id, ObjectOrigin::Owned(build_domain));
    origins.insert(
        inventory.root.object.object_id,
        ObjectOrigin::Owned(build_domain),
    );
    origins.insert(manifest.object.object_id, ObjectOrigin::Owned(build_domain));
    let plan = prepare_retention(
        &CandidateClosure {
            manifest: manifest.object.clone(),
            physical_inventory: inventory.clone(),
            origins,
            authorized_sources: BTreeSet::new(),
            scanned_metadata_bytes: 8080,
        },
        &[workspace_domain, build_domain],
    )
    .unwrap();
    assert_eq!(plan.by_domain[&workspace_domain], vec![loose_b.clone()]);
    assert!(!plan.by_domain[&workspace_domain].contains(&overwritten_a));
    let workspace_batch = build_retain_batch(
        workspace_domain,
        &plan.by_domain[&workspace_domain],
        [35; 16],
        b"retain/workspace.brfin".to_vec(),
    )
    .unwrap();
    let build_batch = build_retain_batch(
        build_domain,
        &plan.by_domain[&build_domain],
        [36; 16],
        b"retain/build.brfin".to_vec(),
    )
    .unwrap();
    assert!(
        workspace_batch
            .index
            .entries
            .iter()
            .all(|entry| entry.object_id != workspace_batch.index.root.object.object_id)
    );

    let workspace_record = domain(volume, workspace_domain, DomainKind::Workspace, 9);
    let build_record = domain(volume, build_domain, DomainKind::Build, 9);
    put(
        &store,
        keys.domain(&workspace_domain),
        ControlRecord::OwnershipDomain(workspace_record.clone()).encode(),
    )
    .await;
    put(
        &store,
        keys.domain(&build_domain),
        ControlRecord::OwnershipDomain(build_record.clone()).encode(),
    )
    .await;
    seed_verified(&store, &keys, workspace_domain, &overwritten_a, 1).await;
    for (seq, object) in workspace_batch
        .index
        .entries
        .iter()
        .chain(std::iter::once(&workspace_batch.index.root.object))
        .enumerate()
    {
        seed_verified(&store, &keys, workspace_domain, object, 10 + seq as u64).await;
    }
    for (seq, object) in build_batch
        .index
        .entries
        .iter()
        .chain(std::iter::once(&build_batch.index.root.object))
        .enumerate()
    {
        seed_verified(&store, &keys, build_domain, object, 20 + seq as u64).await;
    }

    let old_base = snapshot(volume, 37);
    let head = HeadState {
        head: HeadRef {
            head_id: [38; 16],
            epoch: 2,
            commit_seq: 99,
        },
        writer_generation: 9,
        write_domain_id: workspace_domain,
    };
    let view = NativeWorkspaceHead {
        workspace_id,
        head: head.head.clone(),
        base: old_base.clone(),
        writer_generation: 9,
        write_domain_id: workspace_domain,
        visible_delta_count: 5,
        open_orphan_count: 1,
        orphan_carry_digest: Some([39; 32]),
        state: WorkspaceHeadState::Sealing,
        entity_version: 4,
    };
    put(&store, keys.head(&workspace_id), head.encode()).await;
    put(
        &store,
        keys.workspace_view(&workspace_id),
        ControlRecord::NativeWorkspaceHead(view.clone()).encode(),
    )
    .await;
    let operation_id = [40; 16];
    let publication_id = [41; 16];
    let candidate = SnapshotRef {
        volume_id: volume,
        logical_revision: manifest.manifest.logical_revision,
        manifest: manifest.object.clone(),
    };
    let journal = NativePublicationJournal {
        operation_id,
        workspace_id: Some(workspace_id),
        expected_head: Some(head.head.clone()),
        expected_base: Some(old_base),
        owner_generation: 9,
        phase: PublicationPhase::RetentionPrepared,
        accepted_ticket_end: 12,
        drain_plan: None,
        batch_count: 0,
        completed_count: 0,
        fixed_commit_seq: Some(99),
        candidate: Some(candidate),
        verification_evidence: Some(root(42)),
        retention_evidence: Some(build_batch.index.root.clone()),
        committed_result: None,
    };
    put(
        &store,
        keys.publication_journal(&operation_id),
        ControlRecord::NativePublicationJournal(journal).encode(),
    )
    .await;
    let request = PublicationRequest {
        operation_id,
        publication_id,
        workspace_id,
        expected_head: head.clone(),
        expected_view: view.clone(),
        new_head_id: [43; 16],
        manifest,
        physical_inventory: inventory,
        publication_evidence: build_batch.index.root.clone(),
        domains: vec![
            DomainRetentionInput {
                expected_owner_generation: 9,
                expected_retention_seq: 0,
                batch: workspace_batch,
            },
            DomainRetentionInput {
                expected_owner_generation: 9,
                expected_retention_seq: 0,
                batch: build_batch,
            },
        ],
        source_revisions: Vec::new(),
        published_dependencies: Vec::new(),
        kv_base: KvBaseRetention {
            volume_id: volume,
            layer_id: [33; 16],
            sealed_version: 7,
            logical_revision: [30; 32],
            first_publication_id: publication_id,
        },
        retained_at_ns: 555,
    };

    // If close wins on either domain, absolutely no receipt/publication/head
    // effect is allowed to leak from the attempted two-domain transaction.
    let build_key = keys.domain(&build_domain);
    let active_bytes = store.get(&build_key).await.unwrap().unwrap();
    let mut draining = decode_domain(&active_bytes).unwrap();
    draining.state = DomainState::Draining;
    store
        .run(
            Txn::new()
                .check_bytes(build_key.clone(), active_bytes.clone())
                .put(
                    build_key.clone(),
                    ControlRecord::OwnershipDomain(draining).encode(),
                ),
        )
        .await
        .unwrap();
    assert!(publish_candidate(&store, &keys, &request).await.is_err());
    assert!(
        store
            .get(&keys.published_revision(&request.manifest.object.full_hash))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        HeadState::decode(&store.get(&keys.head(&workspace_id)).await.unwrap().unwrap()).unwrap(),
        head
    );
    let draining_bytes = store.get(&build_key).await.unwrap().unwrap();
    store
        .run(
            Txn::new()
                .check_bytes(build_key.clone(), draining_bytes)
                .put(
                    build_key,
                    ControlRecord::OwnershipDomain(build_record).encode(),
                ),
        )
        .await
        .unwrap();

    // A cleaned/reused physical key cannot be adopted under another
    // ObjectId, even when the candidate supplies a content-compatible ref.
    let registry_key = keys.registry(&[44; 16], &loose_b.key);
    let registry_bytes = store.get(&registry_key).await.unwrap().unwrap();
    store
        .run(
            Txn::new()
                .check_bytes(registry_key.clone(), registry_bytes.clone())
                .put(registry_key.clone(), [99; 16].to_vec()),
        )
        .await
        .unwrap();
    assert!(publish_candidate(&store, &keys, &request).await.is_err());
    assert!(
        store
            .get(&keys.published_revision(&request.manifest.object.full_hash))
            .await
            .unwrap()
            .is_none()
    );
    store
        .run(
            Txn::new()
                .check_bytes(registry_key.clone(), [99; 16].to_vec())
                .put(registry_key, registry_bytes),
        )
        .await
        .unwrap();

    let result = match publish_candidate(&store, &keys, &request).await.unwrap() {
        PublicationOutcome::Published(result) => result,
        other => panic!("unexpected {other:?}"),
    };
    assert_eq!(
        result.snapshot.manifest.full_hash,
        request.manifest.object.full_hash
    );
    let new_head =
        HeadState::decode(&store.get(&keys.head(&workspace_id)).await.unwrap().unwrap()).unwrap();
    assert_eq!(new_head.write_domain_id, workspace_domain);
    assert_eq!(new_head.head.epoch, head.head.epoch + 1);
    let new_view = super::seal::decode_workspace_view(
        &store
            .get(&keys.workspace_view(&workspace_id))
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(new_view.open_orphan_count, 1);
    assert_eq!(new_view.orphan_carry_digest, Some([39; 32]));
    assert_eq!(
        new_view.base.manifest.full_hash,
        request.manifest.object.full_hash
    );
    for id in [workspace_domain, build_domain] {
        let d = decode_domain(&store.get(&keys.domain(&id)).await.unwrap().unwrap()).unwrap();
        assert_eq!(d.retention_seq, 1);
        assert!(
            store
                .get(&keys.retention_receipt(&id, 1))
                .await
                .unwrap()
                .is_some()
        );
    }
    assert!(
        store
            .get(&keys.kv_base_retention(&[33; 16], 7))
            .await
            .unwrap()
            .is_some()
    );
    assert!(matches!(
        publish_candidate(&store, &keys, &request).await.unwrap(),
        PublicationOutcome::AlreadyPublished(_)
    ));
    let mut changed = request.clone();
    changed.new_head_id = [99; 16];
    assert!(publish_candidate(&store, &keys, &changed).await.is_err());
    assert_eq!(
        abort_publication(&store, &keys, &operation_id)
            .await
            .unwrap(),
        Some(result.clone())
    );
    assert_eq!(
        complete_publication(&store, &keys, &operation_id)
            .await
            .unwrap(),
        result
    );
    let published = super::publish::decode_published_revision(
        &store
            .get(&keys.published_revision(&request.manifest.object.full_hash))
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    let forked = fork_from_published(
        &store,
        &keys,
        &ForkRequest {
            operation_id: [50; 16],
            workspace_id: [51; 16],
            owner_id: [52; 16],
            owner_generation: 1,
            new_head_id: [53; 16],
            new_domain_id: [54; 16],
            namespace_id: [29; 16],
            source: published,
        },
    )
    .await
    .unwrap();
    assert_eq!(forked.view.base, result.snapshot);
    assert!(
        request
            .physical_inventory
            .entries
            .iter()
            .any(|object| object.kind == 6)
    );
    assert!(
        request
            .physical_inventory
            .entries
            .iter()
            .any(|object| object.kind == ObjectKind::DataPack.as_u8())
    );

    // In the opposite serialization order, publication commits first and
    // close_start must freeze the incremented retention sequence.
    let published_domain = decode_domain(
        &store
            .get(&keys.domain(&build_domain))
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    let close = close_domain_start(
        &store,
        &keys,
        &CloseStartRequest {
            domain_id: build_domain,
            expected_owner_generation: published_domain.owner_generation,
            expected_entity_version: published_domain.entity_version,
            expected_inventory_seq: published_domain.inventory_seq,
            expected_retention_seq: published_domain.retention_seq,
            accepted_ticket_end: 13,
        },
    )
    .await
    .unwrap();
    assert_eq!(close.state, DomainState::Draining);
    assert_eq!(close.retention_seq, 1);
}

#[tokio::test]
async fn one_hundred_forks_are_control_records_and_modified_target_will_not_fast_forward() {
    let store = MemoryControlStore::new();
    let volume = [70; 16];
    let keys = Keys::new(&volume);
    let mut source_manifest = object(73, 4, "manifest/source");
    source_manifest.full_hash = [71; 32];
    let source = PublishedRevision {
        volume_id: volume,
        storage_view_id: [71; 32],
        logical_revision: [72; 32],
        manifest: source_manifest,
        publication_id: [74; 16],
        retained_at_ns: 1,
        retention_policy: RetentionPolicy::Forever,
        evidence_root: root(75),
    };
    put(
        &store,
        keys.published_revision(&source.storage_view_id),
        ControlRecord::PublishedRevision(source.clone()).encode(),
    )
    .await;
    let mut first = None;
    for i in 0..100u8 {
        let outcome = fork_from_published(
            &store,
            &keys,
            &ForkRequest {
                operation_id: [i.wrapping_add(1); 16],
                workspace_id: [i.wrapping_add(101); 16],
                owner_id: [80; 16],
                owner_generation: 1,
                new_head_id: [i.wrapping_add(2); 16],
                new_domain_id: [i.wrapping_add(151); 16],
                namespace_id: [81; 16],
                source: source.clone(),
            },
        )
        .await
        .unwrap();
        first.get_or_insert(outcome);
    }
    // One source PublishedRevision plus five bounded control rows per fork;
    // no per-file metadata/data was copied.
    assert_eq!(store.scan(b"nb2/").await.unwrap().len(), 1 + 5 * 100);

    let first = first.unwrap();
    let view_key = keys.workspace_view(&first.workspace_id);
    let view_bytes = store.get(&view_key).await.unwrap().unwrap();
    let mut modified = first.view.clone();
    modified.visible_delta_count = 1;
    modified.entity_version += 1;
    store
        .run(Txn::new().check_bytes(view_key.clone(), view_bytes).put(
            view_key,
            ControlRecord::NativeWorkspaceHead(modified.clone()).encode(),
        ))
        .await
        .unwrap();
    let mut newer_manifest = object(84, 4, "manifest/newer");
    newer_manifest.full_hash = [82; 32];
    let newer = PublishedRevision {
        storage_view_id: [82; 32],
        logical_revision: [83; 32],
        manifest: newer_manifest,
        publication_id: [85; 16],
        ..source.clone()
    };
    put(
        &store,
        keys.published_revision(&newer.storage_view_id),
        ControlRecord::PublishedRevision(newer.clone()).encode(),
    )
    .await;
    assert!(
        fast_forward(
            &store,
            &keys,
            &FastForwardRequest {
                operation_id: [86; 16],
                workspace_id: first.workspace_id,
                expected_head: first.head,
                expected_view: modified,
                expected_fork_base: first.view.base,
                source: newer.clone(),
                new_head_id: [87; 16],
            }
        )
        .await
        .is_err()
    );

    let clean = fork_from_published(
        &store,
        &keys,
        &ForkRequest {
            operation_id: [201; 16],
            workspace_id: [202; 16],
            owner_id: [80; 16],
            owner_generation: 1,
            new_head_id: [204; 16],
            new_domain_id: [251; 16],
            namespace_id: [81; 16],
            source,
        },
    )
    .await
    .unwrap();
    let fast_forward_request = FastForwardRequest {
        operation_id: [205; 16],
        workspace_id: clean.workspace_id,
        expected_head: clean.head,
        expected_view: clean.view.clone(),
        expected_fork_base: clean.view.base,
        source: newer.clone(),
        new_head_id: [206; 16],
    };
    let advanced = fast_forward(&store, &keys, &fast_forward_request)
        .await
        .unwrap();
    assert_eq!(advanced.view.base.logical_revision, newer.logical_revision);
    assert_eq!(advanced.view.base.manifest.full_hash, newer.storage_view_id);
    assert_eq!(advanced.head.head.epoch, 2);
    assert_eq!(
        fast_forward(&store, &keys, &fast_forward_request)
            .await
            .unwrap(),
        advanced
    );
}

/// RET-002: a newer PublishedRevision takes over the workspace's base
/// pointer, and the superseded revision keeps its facts. Removing the alias
/// (the head/view pointer) cannot revoke the permanently retained revision
/// rows - they are separate control records, not sub-keys of the alias.
///
/// In the native model "latest/alias" is the workspace head + view pointer;
/// there is no legacy `latest`/`alias` KV row.
#[tokio::test]
async fn replacing_the_latest_alias_keeps_the_old_published_revision() {
    let store = MemoryControlStore::new();
    let volume = [90; 16];
    let keys = Keys::new(&volume);

    let mut old_manifest = object(91, 4, "manifest/old");
    old_manifest.full_hash = [92; 32];
    let old = PublishedRevision {
        volume_id: volume,
        storage_view_id: [92; 32],
        logical_revision: [93; 32],
        manifest: old_manifest,
        publication_id: [94; 16],
        retained_at_ns: 1,
        retention_policy: RetentionPolicy::Forever,
        evidence_root: root(95),
    };
    put(
        &store,
        keys.published_revision(&old.storage_view_id),
        ControlRecord::PublishedRevision(old.clone()).encode(),
    )
    .await;

    let forked = fork_from_published(
        &store,
        &keys,
        &ForkRequest {
            operation_id: [96; 16],
            workspace_id: [97; 16],
            owner_id: [98; 16],
            owner_generation: 1,
            new_head_id: [99; 16],
            new_domain_id: [100; 16],
            namespace_id: [101; 16],
            source: old.clone(),
        },
    )
    .await
    .unwrap();
    assert_eq!(forked.view.base.manifest.full_hash, old.storage_view_id);

    let mut new_manifest = object(102, 4, "manifest/new");
    new_manifest.full_hash = [103; 32];
    let newer = PublishedRevision {
        storage_view_id: [103; 32],
        logical_revision: [104; 32],
        manifest: new_manifest,
        publication_id: [105; 16],
        retained_at_ns: 2,
        ..old.clone()
    };
    put(
        &store,
        keys.published_revision(&newer.storage_view_id),
        ControlRecord::PublishedRevision(newer.clone()).encode(),
    )
    .await;

    let advanced = fast_forward(
        &store,
        &keys,
        &FastForwardRequest {
            operation_id: [106; 16],
            workspace_id: forked.workspace_id,
            expected_head: forked.head.clone(),
            expected_view: forked.view.clone(),
            expected_fork_base: forked.view.base.clone(),
            source: newer.clone(),
            new_head_id: [107; 16],
        },
    )
    .await
    .unwrap();
    assert_eq!(
        advanced.view.base.manifest.full_hash, newer.storage_view_id,
        "the workspace now aliases the newer revision"
    );
    assert_eq!(advanced.view.base.logical_revision, newer.logical_revision);

    let old_bytes = store
        .get(&keys.published_revision(&old.storage_view_id))
        .await
        .unwrap()
        .unwrap();
    let new_bytes = store
        .get(&keys.published_revision(&newer.storage_view_id))
        .await
        .unwrap()
        .unwrap();
    assert_ne!(old_bytes, new_bytes);

    // The alias disappears (the workspace is unbound); the revision facts
    // must survive byte for byte.
    store
        .run(
            Txn::new()
                .delete(keys.head(&forked.workspace_id))
                .delete(keys.workspace_view(&forked.workspace_id)),
        )
        .await
        .unwrap();
    assert_eq!(
        store.get(&keys.head(&forked.workspace_id)).await.unwrap(),
        None
    );
    assert_eq!(
        store
            .get(&keys.published_revision(&old.storage_view_id))
            .await
            .unwrap()
            .unwrap(),
        old_bytes,
        "the superseded revision is not deleted or rewritten"
    );
    assert_eq!(
        store
            .get(&keys.published_revision(&newer.storage_view_id))
            .await
            .unwrap()
            .unwrap(),
        new_bytes
    );
    let decoded_old = decode_published_revision(&old_bytes).unwrap();
    let decoded_new = decode_published_revision(&new_bytes).unwrap();
    assert_eq!(decoded_old, old);
    assert_eq!(decoded_new, newer);
    assert_eq!(decoded_old.retention_policy, RetentionPolicy::Forever);
    assert_eq!(decoded_new.retention_policy, RetentionPolicy::Forever);
    assert_ne!(decoded_old.storage_view_id, decoded_new.storage_view_id);
    assert_ne!(decoded_old.publication_id, decoded_new.publication_id);
}

/// RET-023: each origin domain gets its own RetainBatch, and its receipt's
/// evidence root enumerates exactly the objects that domain retained. No
/// global PublishedRevision scan is involved, and one domain's evidence
/// never exposes the other domain's control objects. The evidence root
/// object itself is never inside its own list (no self-hash).
#[test]
fn each_domain_receipt_enumerates_only_its_own_evidence() {
    let domain_a = [110; 16];
    let domain_b = [111; 16];
    let a_objects = vec![object(112, 6, "a/one"), object(113, 6, "a/two")];
    let b_objects = vec![object(114, 6, "b/one")];

    let a_batch =
        build_retain_batch(domain_a, &a_objects, [115; 16], b"retain/a".to_vec()).unwrap();
    let b_batch =
        build_retain_batch(domain_b, &b_objects, [116; 16], b"retain/b".to_vec()).unwrap();
    let a_receipt = a_batch.receipt(1, [117; 16], [118; 32]);
    let b_receipt = b_batch.receipt(1, [119; 16], [120; 32]);

    assert_eq!(a_receipt.domain_id, domain_a);
    assert_eq!(b_receipt.domain_id, domain_b);
    assert_eq!(a_receipt.evidence_root, a_batch.index.root);
    assert_eq!(b_receipt.evidence_root, b_batch.index.root);
    assert_ne!(a_receipt.evidence_root, b_receipt.evidence_root);
    assert_eq!(a_receipt.retained_objects, a_batch.index.root);
    assert_eq!(b_receipt.retained_objects, b_batch.index.root);

    // Every receipt is enumerable from its domain-local batch bytes alone.
    let a_entries = open_object_index(&a_receipt.evidence_root, &a_batch.index.bytes).unwrap();
    let b_entries = open_object_index(&b_receipt.evidence_root, &b_batch.index.bytes).unwrap();
    assert_eq!(a_entries, a_objects);
    assert_eq!(b_entries, b_objects);
    assert!(
        a_entries
            .iter()
            .all(|object| !b_objects.iter().any(|other| other == object)),
        "domain A's evidence never exposes domain B's objects"
    );
    assert!(
        b_entries
            .iter()
            .all(|object| !a_objects.iter().any(|other| other == object)),
        "domain B's evidence never exposes domain A's objects"
    );

    // The index container is protected by the receipt's outer RootRef; it is
    // deliberately absent from its own leaf list.
    for batch in [&a_batch, &b_batch] {
        assert!(
            batch
                .index
                .entries
                .iter()
                .all(|object| object.object_id != batch.index.root.object.object_id),
            "a RetainBatch never lists its own index object"
        );
    }

    // The receipt's verified digest binds exactly this domain's subset.
    assert_eq!(
        a_receipt.verified_subset_digest,
        retained_subset_digest(&domain_a, &a_entries)
    );
    assert_eq!(
        b_receipt.verified_subset_digest,
        retained_subset_digest(&domain_b, &b_entries)
    );
    assert_ne!(
        a_receipt.verified_subset_digest,
        b_receipt.verified_subset_digest
    );
}

#[test]
fn retired_retention_options_fail_closed_without_claiming_runtime_wiring() {
    assert!(reject_legacy_retention_options(&LegacyRetentionOptions::default()).is_ok());
    assert!(
        reject_legacy_retention_options(&LegacyRetentionOptions {
            retention_ttl_seconds: Some(60),
            published_gc: Some(true),
            read_retention_leases: Some(false),
        })
        .is_err()
    );
}
