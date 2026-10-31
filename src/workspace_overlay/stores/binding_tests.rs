//! Public binding contracts. No injected binding-authority stand-in is used.
//! New APIs require an API compile gate before meaningful behavior red/green.

mod real_open_tests;
mod real_publication_tests;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

use async_trait::async_trait;
use tokio::sync::Mutex;
use uuid::Uuid;

use super::database::{SqliteWorkspaceStore, StoreFailpoint};
use super::kv_backend::{KvCheck, KvEntry, KvReadLimits, KvWrite, WorkspaceKvBackend};
use super::kv_store::KvWorkspaceStore;
use super::redis::RedisWorkspaceBackend;
use super::tikv::TiKvWorkspaceBackend;
use crate::cadapter::client::ObjectClient;
use crate::cadapter::localfs::LocalFsBackend;
use crate::workspace_overlay::catalog::*;
use crate::workspace_overlay::error::WorkspaceError;
use crate::workspace_overlay::ids::{LayerId, LeaseId, WorkspaceId};
use crate::workspace_overlay::model::{BaseRevision, LayerRecord, LayerState};
use crate::workspace_overlay::packed_v3::wire005::{
    AuthenticatedV3Snapshot, V3IndexReader, V3MountBudget, V3ProducerOptions, V3SnapshotProducer,
};
use crate::workspace_overlay::packed_v3::{
    AccessProfile, GroupMeta, GroupMetaEntry, GroupMetaExtent, PackedCodec, PackedFrameInput,
    PackedGroupInput, SizeClass, SizeClassTable,
};
use crate::workspace_overlay::publish::binding::{
    InstallPackedLowerBinding, PackedLowerBindingRecord, PublishPackedLowerBinding,
    VerifiedPackedLower,
};

pub(crate) async fn packed() -> (
    tempfile::TempDir,
    ObjectClient<LocalFsBackend>,
    AuthenticatedV3Snapshot,
    VerifiedPackedLower,
    Vec<u8>,
) {
    packed_with_snapshot_id([9; 32]).await
}

pub(crate) async fn packed_with_snapshot_id(
    snapshot_id: [u8; 32],
) -> (
    tempfile::TempDir,
    ObjectClient<LocalFsBackend>,
    AuthenticatedV3Snapshot,
    VerifiedPackedLower,
    Vec<u8>,
) {
    packed_with_snapshot_id_and_inodes(snapshot_id, &[400]).await
}

async fn packed_with_snapshot_id_and_inodes(
    snapshot_id: [u8; 32],
    inodes: &[u64],
) -> (
    tempfile::TempDir,
    ObjectClient<LocalFsBackend>,
    AuthenticatedV3Snapshot,
    VerifiedPackedLower,
    Vec<u8>,
) {
    packed_with_snapshot_id_and_inodes_under(snapshot_id, inodes, None).await
}

pub(crate) async fn packed_for_process_restart(
    parent: &std::path::Path,
) -> (
    tempfile::TempDir,
    ObjectClient<LocalFsBackend>,
    AuthenticatedV3Snapshot,
    VerifiedPackedLower,
    Vec<u8>,
) {
    packed_with_snapshot_id_and_inodes_under([9; 32], &[400], Some(parent)).await
}

async fn packed_with_snapshot_id_and_inodes_under(
    snapshot_id: [u8; 32],
    inodes: &[u64],
    parent: Option<&std::path::Path>,
) -> (
    tempfile::TempDir,
    ObjectClient<LocalFsBackend>,
    AuthenticatedV3Snapshot,
    VerifiedPackedLower,
    Vec<u8>,
) {
    assert!(!inodes.is_empty());
    assert!(inodes.iter().all(|inode| *inode > 1));
    assert!(inodes.windows(2).all(|pair| pair[0] < pair[1]));
    let temp = match parent {
        Some(parent) => tempfile::tempdir_in(parent).unwrap(),
        None => tempfile::tempdir().unwrap(),
    };
    let client = ObjectClient::new(LocalFsBackend::new(temp.path().join("objects")));
    let payload: Vec<u8> = (0..4096).map(|i| (i % 251 + 1) as u8).collect();
    let group = PackedGroupInput {
        group_id: 1,
        parent_dir_key: [7; 32],
        metadata: GroupMeta::new(
            inodes
                .iter()
                .enumerate()
                .map(|(slot, &inode)| GroupMetaEntry {
                    name: if slot == 0 {
                        b"nonzero".to_vec()
                    } else {
                        format!("nonzero-{inode:020}").into_bytes()
                    },
                    inode,
                    kind: 1,
                    mode: 0o100644,
                    uid: 1,
                    gid: 2,
                    rdev: 0,
                    nlink: 1,
                    atime_ns: 0,
                    mtime_ns: 0,
                    ctime_ns: 0,
                    size: payload.len() as u64,
                    flags: 0,
                    inline_data: Arc::from([]),
                    extents: vec![GroupMetaExtent {
                        file_offset: 0,
                        logical_len: payload.len() as u32,
                        frame_ordinal: 0,
                        raw_offset: 0,
                        raw_len: payload.len() as u32,
                    }],
                })
                .collect(),
        )
        .unwrap()
        .encode()
        .unwrap(),
        frame_ordinals: vec![0],
        entry_count: inodes.len() as u32,
        file_count: inodes.len() as u32,
        layout_profile: AccessProfile::RandomSmallFile,
    };
    let mut producer = V3SnapshotProducer::new(
        client.clone(),
        temp.path(),
        "g10c".into(),
        V3ProducerOptions {
            snapshot_id,
            root_dir_key: [7; 32],
            root_inode: 1,
            profile: AccessProfile::RandomSmallFile,
            size_classes: SizeClassTable::default(),
            build_policy: Default::default(),
            metadata_codec: PackedCodec::Raw,
            data_codec: PackedCodec::Raw,
        },
    )
    .await
    .unwrap();
    producer
        .add_container(
            1,
            &[group],
            &[PackedFrameInput {
                raw: payload.clone(),
                size_class: SizeClass::Tiny,
                codec: 0,
                first_file_slot: 0,
                last_file_slot: (inodes.len() - 1) as u32,
            }],
            &[1],
        )
        .await
        .unwrap();
    let reference = producer.finish().await.unwrap();
    let snapshot = AuthenticatedV3Snapshot::open(&client, &reference)
        .await
        .unwrap();
    let reader = V3IndexReader::new(client.clone(), 0);
    let proof = VerifiedPackedLower::from_authenticated_snapshot(&snapshot, &reader)
        .await
        .unwrap();
    assert_eq!(
        proof.highest_inode(),
        i64::try_from(*inodes.last().unwrap()).unwrap()
    );
    (temp, client, snapshot, proof, payload)
}

pub(crate) async fn request<S: WorkspaceStore>(
    store: &S,
    lower: VerifiedPackedLower,
) -> InstallPackedLowerBinding {
    store.initialize_workspace_schema().await.unwrap();
    let workspace = store
        .create_volume_root(CreateVolumeRoot {
            upper_bucket: None,
            volume_format: "workspace-v1".into(),
            schema_version: 1,
            volume_id: Uuid::new_v4(),
            workspace_id: WorkspaceId::new(),
            root_layer_id: LayerId::new(),
            writable_layer_id: LayerId::new(),
            owner_id: None,
        })
        .await
        .unwrap();
    let lease = store
        .acquire_lease(AcquireLease {
            workspace_id: workspace.workspace_id,
            lease_id: LeaseId::new(),
            holder_generation: 7,
            ttl_ns: 30_000_000_000,
        })
        .await
        .unwrap();
    let expected_layers: [LayerRecord; 2] = store
        .load_layer_chain(workspace.head_layer_id)
        .await
        .unwrap()
        .try_into()
        .unwrap();
    InstallPackedLowerBinding {
        guard: HeadGuard {
            workspace_id: workspace.workspace_id,
            expected_head_layer_id: workspace.head_layer_id,
            expected_head_epoch: workspace.head_epoch,
            lease_id: lease.lease_id,
            holder_generation: lease.holder_generation,
        },
        expected_base: BaseRevision {
            layer_id: expected_layers[1].layer_id,
            sealed_version: expected_layers[1].sealed_version.unwrap(),
            root_hash: expected_layers[1].root_hash.unwrap(),
        },
        expected_layers,
        expected_binding: None,
        lower,
    }
}

fn installed_guard(
    request: &InstallPackedLowerBinding,
    record: &PackedLowerBindingRecord,
) -> HeadGuard {
    HeadGuard {
        expected_head_epoch: record.head_epoch,
        ..request.guard.clone()
    }
}

async fn contract<S: WorkspaceStore>(store: &S, expected_mount_support: bool) {
    let (_temp, _client, _snapshot, proof, _payload) = packed().await;
    let request = request(store, proof).await;
    assert_eq!(
        store
            .load_packed_binding_record(request.guard.clone())
            .await
            .unwrap(),
        None
    );
    let record = store
        .install_packed_lower_binding(request.clone())
        .await
        .unwrap();
    let guard = installed_guard(&request, &record);
    assert_eq!(record.binding.manifest, *request.lower.manifest_reference());
    assert_eq!(record.base_revision, request.expected_base);
    assert_eq!(record.head_epoch, request.guard.expected_head_epoch + 1);
    assert_eq!(
        store
            .load_workspace(guard.workspace_id)
            .await
            .unwrap()
            .head_epoch,
        record.head_epoch
    );
    assert_eq!(
        store
            .load_layer(guard.expected_head_layer_id)
            .await
            .unwrap()
            .next_sequence,
        request.expected_layers[0].next_sequence + 1
    );
    assert_eq!(
        store
            .load_packed_lower_binding(guard.clone())
            .await
            .unwrap(),
        Some(record.binding.clone())
    );
    assert_eq!(
        store
            .load_packed_binding_record(guard.clone())
            .await
            .unwrap(),
        Some(record.clone())
    );
    assert_eq!(
        store
            .load_packed_binding_version(guard.workspace_id, 1)
            .await
            .unwrap(),
        Some(record.clone())
    );
    assert_eq!(store.allocate_id("inode").await.unwrap(), 401);
    assert_eq!(store.allocate_id("inode").await.unwrap(), 402);
    assert_eq!(
        store.supports_packed_workspace_mount(),
        expected_mount_support
    );
    assert!(matches!(
        store
            .load_packed_binding_record(request.guard.clone())
            .await,
        Err(WorkspaceError::Fenced)
    ));
    assert!(matches!(
        store
            .apply_versioned_mutation(VersionedMutation::empty(
                request.guard.clone(),
                request.expected_layers.clone(),
                4096
            ))
            .await,
        Err(WorkspaceError::Fenced)
    ));
    assert!(matches!(
        store
            .apply_versioned_mutation(VersionedMutation::empty(
                guard.clone(),
                request.expected_layers.clone(),
                4096
            ))
            .await,
        Err(WorkspaceError::Busy)
    ));
    store
        .release_lease(ReleaseLease {
            lease_id: guard.lease_id,
            holder_generation: guard.holder_generation,
        })
        .await
        .unwrap();
    assert!(matches!(
        store.load_packed_binding_record(guard.clone()).await,
        Err(WorkspaceError::Fenced)
    ));
    assert_eq!(
        store
            .load_packed_binding_version(guard.workspace_id, 1)
            .await
            .unwrap(),
        Some(record)
    );
}

async fn publish_same_head<S: WorkspaceStore>(store: &S) {
    let (_temp, _client, _snapshot, proof, _payload) = packed().await;
    let request = request(store, proof).await;
    let first = store
        .install_packed_lower_binding(request.clone())
        .await
        .unwrap();
    let (_replacement_temp, _replacement_client, _replacement_snapshot, replacement, _payload) =
        packed_with_snapshot_id([10; 32]).await;
    let expected_layers: [LayerRecord; 2] = store
        .load_layer_chain(first.head_layer_id)
        .await
        .unwrap()
        .try_into()
        .unwrap();
    let publication = PublishPackedLowerBinding {
        guard: installed_guard(&request, &first),
        expected_layers,
        expected_base: request.expected_base.clone(),
        expected_binding: first.clone(),
        lower: replacement,
    };
    let second = store
        .publish_packed_lower_binding(publication.clone())
        .await
        .unwrap();
    let committed_workspace = store.load_workspace(second.workspace_id).await.unwrap();
    let committed_head = store.load_layer(second.head_layer_id).await.unwrap();
    assert_eq!(store.allocate_id("inode").await.unwrap(), 401);
    assert_eq!(
        store
            .publish_packed_lower_binding(publication.clone())
            .await
            .unwrap(),
        second
    );
    assert_eq!(
        store.load_workspace(second.workspace_id).await.unwrap(),
        committed_workspace
    );
    assert_eq!(
        store.load_layer(second.head_layer_id).await.unwrap(),
        committed_head
    );
    assert_eq!(second.binding.binding_version, 2);
    assert_eq!(second.head_epoch, first.head_epoch + 1);
    assert_eq!(second.highest_inode, first.highest_inode);
    assert_eq!(
        store
            .load_packed_binding_version(second.workspace_id, 1)
            .await
            .unwrap(),
        Some(first)
    );
    assert_eq!(
        store
            .load_packed_binding_version(second.workspace_id, 2)
            .await
            .unwrap(),
        Some(second.clone())
    );
    let guard = HeadGuard {
        expected_head_epoch: second.head_epoch,
        workspace_id: second.workspace_id,
        expected_head_layer_id: second.head_layer_id,
        lease_id: request.guard.lease_id,
        holder_generation: request.guard.holder_generation,
    };
    assert_eq!(
        store.load_packed_binding_record(guard).await.unwrap(),
        Some(second)
    );
    assert_eq!(store.allocate_id("inode").await.unwrap(), 402);
}

#[tokio::test]
async fn public_sqlite_binding_same_head_publication_is_versioned_and_atomic() {
    publish_same_head(
        &SqliteWorkspaceStore::connect("sqlite::memory:")
            .await
            .unwrap(),
    )
    .await;
}

#[tokio::test]
async fn public_kv_binding_same_head_publication_is_versioned_and_atomic() {
    let budget = V3MountBudget::defaults();
    publish_same_head(
        &KvWorkspaceStore::new(ClockBackend::default()).with_packed_reader_pin_budget(budget),
    )
    .await;
}

async fn publication_request<S: WorkspaceStore>(store: &S) -> PublishPackedLowerBinding {
    publication_request_with_inodes(store, &[400]).await
}

async fn publication_request_with_inodes<S: WorkspaceStore>(
    store: &S,
    replacement_inodes: &[u64],
) -> PublishPackedLowerBinding {
    let (_objects, _client, _snapshot, lower, _payload) = packed().await;
    let installation = request(store, lower).await;
    let first = store
        .install_packed_lower_binding(installation.clone())
        .await
        .unwrap();
    let (_replacement_objects, _client, _snapshot, lower, _payload) =
        packed_with_snapshot_id_and_inodes([13; 32], replacement_inodes).await;
    PublishPackedLowerBinding {
        guard: installed_guard(&installation, &first),
        expected_layers: store
            .load_layer_chain(first.head_layer_id)
            .await
            .unwrap()
            .try_into()
            .unwrap(),
        expected_base: installation.expected_base,
        expected_binding: first,
        lower,
    }
}

async fn publication_inode_expansion_reserves_unissued_range<S: WorkspaceStore>(store: &S) {
    let publication = publication_request_with_inodes(store, &[401, 450]).await;
    assert_eq!(publication.expected_binding.highest_inode, 400);
    assert_eq!(publication.lower.highest_inode(), 450);
    let target = store
        .publish_packed_lower_binding(publication.clone())
        .await
        .unwrap();
    assert_eq!(target.highest_inode, 450);
    assert_eq!(target.binding.binding_version, 2);
    assert_eq!(target.head_epoch, publication.guard.expected_head_epoch + 1);
    let workspace = store.load_workspace(target.workspace_id).await.unwrap();
    let head = store.load_layer(target.head_layer_id).await.unwrap();
    let mut expected_head = publication.expected_layers[0].clone();
    expected_head.next_sequence += 1;
    assert_eq!(head, expected_head);
    assert_eq!(
        store
            .load_layer(publication.expected_base.layer_id)
            .await
            .unwrap(),
        publication.expected_layers[1]
    );
    assert_eq!(
        store
            .load_packed_binding_version(target.workspace_id, 1)
            .await
            .unwrap(),
        Some(publication.expected_binding.clone())
    );
    assert_eq!(
        store
            .load_packed_binding_version(target.workspace_id, 2)
            .await
            .unwrap(),
        Some(target.clone())
    );
    let target_guard = HeadGuard {
        expected_head_epoch: target.head_epoch,
        ..publication.guard.clone()
    };
    assert_eq!(
        store
            .load_packed_binding_record(target_guard)
            .await
            .unwrap(),
        Some(target.clone())
    );
    assert_eq!(store.allocate_id("inode").await.unwrap(), 451);
    // The first-publication reservation predicate must not be replayed after
    // a successful commit: the same request can be recognized after issuance.
    assert_eq!(
        store
            .publish_packed_lower_binding(publication)
            .await
            .unwrap(),
        target
    );
    assert_eq!(
        store.load_workspace(target.workspace_id).await.unwrap(),
        workspace
    );
    assert_eq!(store.load_layer(target.head_layer_id).await.unwrap(), head);
    assert_eq!(
        store
            .load_packed_binding_version(target.workspace_id, 3)
            .await
            .unwrap(),
        None
    );
    assert_eq!(store.allocate_id("inode").await.unwrap(), 452);
}

async fn assert_inode_expansion_unpublished<S: WorkspaceStore>(
    store: &S,
    publication: &PublishPackedLowerBinding,
) {
    let workspace = store
        .load_workspace(publication.guard.workspace_id)
        .await
        .unwrap();
    assert_eq!(workspace.head_epoch, publication.guard.expected_head_epoch);
    assert_eq!(
        workspace.head_layer_id,
        publication.guard.expected_head_layer_id
    );
    for layer in &publication.expected_layers {
        assert_eq!(store.load_layer(layer.layer_id).await.unwrap(), *layer);
    }
    assert_eq!(
        store
            .load_packed_binding_record(publication.guard.clone())
            .await
            .unwrap(),
        Some(publication.expected_binding.clone())
    );
    assert_eq!(
        store
            .load_packed_binding_version(publication.guard.workspace_id, 1)
            .await
            .unwrap(),
        Some(publication.expected_binding.clone())
    );
    assert_eq!(
        store
            .load_packed_binding_version(publication.guard.workspace_id, 2)
            .await
            .unwrap(),
        None
    );
}

async fn publication_inode_expansion_rejects_issued_ids<S: WorkspaceStore>(store: &S) {
    // The replacement is produced and authenticated with actual inode 401,
    // which collides with the ID issued below, and a highest inode of 450.
    let publication = publication_request_with_inodes(store, &[401, 450]).await;
    assert_eq!(publication.expected_binding.highest_inode, 400);
    assert_eq!(store.allocate_id("inode").await.unwrap(), 401);
    let workspace = store
        .load_workspace(publication.guard.workspace_id)
        .await
        .unwrap();
    assert!(matches!(
        store
            .publish_packed_lower_binding(publication.clone())
            .await,
        Err(WorkspaceError::UnsupportedCapability(
            "packed lower expansion overlaps issued native inode IDs"
        ))
    ));
    assert_eq!(
        store
            .load_workspace(publication.guard.workspace_id)
            .await
            .unwrap(),
        workspace
    );
    assert_inode_expansion_unpublished(store, &publication).await;
    // A rejected publication must preserve the allocator instead of silently
    // moving its floor to 451 after the collision has already happened.
    assert_eq!(store.allocate_id("inode").await.unwrap(), 402);
}

#[tokio::test]
async fn public_sqlite_binding_publication_inode_expansion_reserves_unissued_range() {
    publication_inode_expansion_reserves_unissued_range(
        &SqliteWorkspaceStore::connect("sqlite::memory:")
            .await
            .unwrap(),
    )
    .await;
}

#[tokio::test]
async fn public_kv_binding_publication_inode_expansion_reserves_unissued_range() {
    let budget = V3MountBudget::defaults();
    publication_inode_expansion_reserves_unissued_range(
        &KvWorkspaceStore::new(ClockBackend::default()).with_packed_reader_pin_budget(budget),
    )
    .await;
}

#[tokio::test]
async fn public_sqlite_binding_publication_inode_expansion_rejects_issued_ids() {
    publication_inode_expansion_rejects_issued_ids(
        &SqliteWorkspaceStore::connect("sqlite::memory:")
            .await
            .unwrap(),
    )
    .await;
}

#[tokio::test]
async fn public_kv_binding_publication_inode_expansion_rejects_issued_ids() {
    let budget = V3MountBudget::defaults();
    publication_inode_expansion_rejects_issued_ids(
        &KvWorkspaceStore::new(ClockBackend::default()).with_packed_reader_pin_budget(budget),
    )
    .await;
}

#[tokio::test]
async fn public_kv_binding_publication_inode_expansion_fences_issuance_at_cas() {
    let budget = V3MountBudget::defaults();
    let backend = ClockBackend::default();
    let store = KvWorkspaceStore::new(backend.clone()).with_packed_reader_pin_budget(budget);
    let publication = publication_request_with_inodes(&store, &[401, 450]).await;
    let workspace = store
        .load_workspace(publication.guard.workspace_id)
        .await
        .unwrap();
    let mut expected_records = backend.records.lock().await.clone();
    assert_eq!(
        decode_clock_record::<i64>(expected_records.get(b"alloc/inode".as_slice()).unwrap()),
        401
    );
    // Simulate a completed inode allocation after the consistent read but
    // before the publication's atomic exact-key checks. Its only write is
    // the same allocator increment performed by the public allocate_id API.
    backend.allocate_inode_at_cas.store(true, Ordering::SeqCst);
    assert!(matches!(
        store
            .publish_packed_lower_binding(publication.clone())
            .await,
        Err(WorkspaceError::Busy)
    ));
    assert!(!backend.allocate_inode_at_cas.load(Ordering::SeqCst));
    expected_records.insert(b"alloc/inode".to_vec(), encode_clock_record(&402i64));
    assert_eq!(*backend.records.lock().await, expected_records);
    assert_eq!(
        store
            .load_workspace(publication.guard.workspace_id)
            .await
            .unwrap(),
        workspace
    );
    assert_inode_expansion_unpublished(&store, &publication).await;
    assert_eq!(store.allocate_id("inode").await.unwrap(), 402);
}

#[tokio::test]
async fn public_sqlite_binding_publication_response_loss_retries_after_reopen_without_writes() {
    let catalog = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}",
        catalog.path().join("lost-response.sqlite").display()
    );
    let store = SqliteWorkspaceStore::connect(&url).await.unwrap();
    let publication = publication_request(&store).await;
    let target = publication.record().unwrap();
    store.set_failpoint(StoreFailpoint::AfterPublicationCommit);
    assert!(matches!(
        store
            .publish_packed_lower_binding(publication.clone())
            .await,
        Err(WorkspaceError::Backend(_))
    ));
    let workspace = store.load_workspace(target.workspace_id).await.unwrap();
    let head = store.load_layer(target.head_layer_id).await.unwrap();
    assert_eq!(workspace.head_epoch, target.head_epoch);
    assert_eq!(
        head.next_sequence,
        publication.expected_layers[0].next_sequence + 1
    );
    assert_eq!(
        store
            .load_packed_binding_version(target.workspace_id, 2)
            .await
            .unwrap(),
        Some(target.clone())
    );
    drop(store);
    let reopened = SqliteWorkspaceStore::connect(&url).await.unwrap();
    assert_eq!(
        reopened
            .publish_packed_lower_binding(publication.clone())
            .await
            .unwrap(),
        target
    );
    assert_eq!(
        reopened
            .publish_packed_lower_binding(publication)
            .await
            .unwrap(),
        target
    );
    assert_eq!(
        reopened.load_workspace(target.workspace_id).await.unwrap(),
        workspace
    );
    assert_eq!(
        reopened.load_layer(target.head_layer_id).await.unwrap(),
        head
    );
    assert_eq!(
        reopened
            .load_packed_binding_version(target.workspace_id, 3)
            .await
            .unwrap(),
        None
    );
    assert_eq!(reopened.allocate_id("inode").await.unwrap(), 401);
}

#[tokio::test]
async fn public_kv_binding_publication_response_loss_retries_without_writes() {
    let backend = ClockBackend::default();
    let budget = V3MountBudget::defaults();
    let store =
        KvWorkspaceStore::new(backend.clone()).with_packed_reader_pin_budget(budget.clone());
    let publication = publication_request(&store).await;
    let target = publication.record().unwrap();
    backend.lose_response_at_cas.store(true, Ordering::SeqCst);
    assert!(matches!(
        store
            .publish_packed_lower_binding(publication.clone())
            .await,
        Err(WorkspaceError::Backend(_))
    ));
    let durable = backend.records.lock().await.clone();
    assert_eq!(
        store
            .load_packed_binding_version(target.workspace_id, 2)
            .await
            .unwrap(),
        Some(target.clone())
    );
    drop(store);
    let reopened = KvWorkspaceStore::new(backend.clone()).with_packed_reader_pin_budget(budget);
    assert_eq!(
        reopened
            .publish_packed_lower_binding(publication.clone())
            .await
            .unwrap(),
        target
    );
    assert_eq!(
        reopened
            .publish_packed_lower_binding(publication)
            .await
            .unwrap(),
        target
    );
    assert_eq!(*backend.records.lock().await, durable);
    assert_eq!(
        reopened
            .load_packed_binding_version(target.workspace_id, 3)
            .await
            .unwrap(),
        None
    );
    assert_eq!(reopened.allocate_id("inode").await.unwrap(), 401);
}

#[tokio::test]
async fn public_kv_binding_publication_retry_fences_mutation_after_consistent_read() {
    let backend = ClockBackend::default();
    let budget = V3MountBudget::defaults();
    let store =
        KvWorkspaceStore::new(backend.clone()).with_packed_reader_pin_budget(budget.clone());
    let publication = publication_request(&store).await;
    let target = store
        .publish_packed_lower_binding(publication.clone())
        .await
        .unwrap();
    backend.mutate_head_at_cas.store(true, Ordering::SeqCst);
    assert!(matches!(
        store.publish_packed_lower_binding(publication).await,
        Err(WorkspaceError::Busy)
    ));
    assert_eq!(
        store
            .load_packed_binding_version(target.workspace_id, 2)
            .await
            .unwrap(),
        Some(target.clone())
    );
    assert_eq!(
        store
            .load_packed_binding_version(target.workspace_id, 3)
            .await
            .unwrap(),
        None
    );
    assert_eq!(store.allocate_id("inode").await.unwrap(), 401);
}

async fn publication_retry_rejects_changed_request_and_mutation<S: WorkspaceStore>(store: &S) {
    use crate::workspace_overlay::model::DentryDelta;
    let publication = publication_request(store).await;
    let target = store
        .publish_packed_lower_binding(publication.clone())
        .await
        .unwrap();
    let mut wrong_holder = publication.clone();
    wrong_holder.guard.holder_generation += 1;
    assert!(matches!(
        store.publish_packed_lower_binding(wrong_holder).await,
        Err(WorkspaceError::Fenced)
    ));
    let mut wrong_predecessor = publication.clone();
    wrong_predecessor
        .expected_binding
        .binding
        .manifest
        .key
        .push_str("/different");
    assert!(matches!(
        store.publish_packed_lower_binding(wrong_predecessor).await,
        Err(WorkspaceError::Busy)
    ));
    let mut wrong_base = publication.clone();
    wrong_base.expected_layers[1].created_at_ns += 1;
    assert!(matches!(
        store.publish_packed_lower_binding(wrong_base).await,
        Err(WorkspaceError::Busy)
    ));
    let mut wrong_head = publication.clone();
    wrong_head.expected_layers[0].created_at_ns += 1;
    assert!(matches!(
        store.publish_packed_lower_binding(wrong_head).await,
        Err(WorkspaceError::Busy)
    ));
    let target_guard = HeadGuard {
        expected_head_epoch: target.head_epoch,
        ..publication.guard.clone()
    };
    store
        .apply_namespace_mutation(NamespaceMutation {
            guard: target_guard.clone(),
            dentries: vec![DentryDelta::put(
                target.head_layer_id,
                1,
                b"changed-after-publication".to_vec(),
                400,
                0,
                0,
            )],
            inodes: vec![],
        })
        .await
        .unwrap();
    let changed_head = store.load_layer(target.head_layer_id).await.unwrap();
    assert!(matches!(
        store.publish_packed_lower_binding(publication).await,
        Err(WorkspaceError::Busy)
    ));
    assert_eq!(
        store.load_layer(target.head_layer_id).await.unwrap(),
        changed_head
    );
    assert_eq!(
        store
            .load_packed_binding_record(target_guard)
            .await
            .unwrap(),
        Some(target.clone())
    );
    assert_eq!(
        store
            .load_packed_binding_version(target.workspace_id, 3)
            .await
            .unwrap(),
        None
    );
    assert_eq!(store.allocate_id("inode").await.unwrap(), 401);
}

#[tokio::test]
async fn public_sqlite_binding_publication_retry_rejects_changed_request_and_mutation() {
    publication_retry_rejects_changed_request_and_mutation(
        &SqliteWorkspaceStore::connect("sqlite::memory:")
            .await
            .unwrap(),
    )
    .await;
}

#[tokio::test]
async fn public_kv_binding_publication_retry_rejects_changed_request_and_mutation() {
    let budget = V3MountBudget::defaults();
    publication_retry_rejects_changed_request_and_mutation(
        &KvWorkspaceStore::new(ClockBackend::default()).with_packed_reader_pin_budget(budget),
    )
    .await;
}

#[tokio::test]
async fn public_sqlite_binding_publication_retry_requires_live_lease() {
    for release in [false, true] {
        let store = SqliteWorkspaceStore::connect("sqlite::memory:")
            .await
            .unwrap();
        let publication = publication_request(&store).await;
        let target = store
            .publish_packed_lower_binding(publication.clone())
            .await
            .unwrap();
        let head = store.load_layer(target.head_layer_id).await.unwrap();
        if release {
            store
                .release_lease(ReleaseLease {
                    lease_id: publication.guard.lease_id,
                    holder_generation: publication.guard.holder_generation,
                })
                .await
                .unwrap();
        } else {
            store
                .renew_lease(RenewLease {
                    lease_id: publication.guard.lease_id,
                    holder_generation: publication.guard.holder_generation,
                    ttl_ns: 1,
                })
                .await
                .unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        assert!(matches!(
            store.publish_packed_lower_binding(publication).await,
            Err(WorkspaceError::Fenced)
        ));
        assert_eq!(store.load_layer(target.head_layer_id).await.unwrap(), head);
        assert_eq!(
            store
                .load_packed_binding_version(target.workspace_id, 2)
                .await
                .unwrap(),
            Some(target)
        );
        assert_eq!(store.allocate_id("inode").await.unwrap(), 401);
    }
}

#[tokio::test]
async fn public_kv_binding_publication_retry_requires_live_lease_and_commit_deadline() {
    for expiry_mode in 0..3 {
        let backend = ClockBackend::default();
        let budget = V3MountBudget::defaults();
        let store = KvWorkspaceStore::new(backend.clone()).with_packed_reader_pin_budget(budget);
        let publication = publication_request(&store).await;
        let target = store
            .publish_packed_lower_binding(publication.clone())
            .await
            .unwrap();
        let head = store.load_layer(target.head_layer_id).await.unwrap();
        match expiry_mode {
            0 => {
                store
                    .release_lease(ReleaseLease {
                        lease_id: publication.guard.lease_id,
                        holder_generation: publication.guard.holder_generation,
                    })
                    .await
                    .unwrap();
            }
            1 => {
                backend.now.fetch_add(30_000_000_001, Ordering::SeqCst);
            }
            _ => {
                backend
                    .advance_at_cas
                    .store(30_000_000_001, Ordering::SeqCst);
            }
        }
        assert!(matches!(
            store.publish_packed_lower_binding(publication).await,
            Err(WorkspaceError::Fenced)
        ));
        assert_eq!(store.load_layer(target.head_layer_id).await.unwrap(), head);
        assert_eq!(
            store
                .load_packed_binding_version(target.workspace_id, 2)
                .await
                .unwrap(),
            Some(target)
        );
        assert_eq!(store.allocate_id("inode").await.unwrap(), 401);
    }
}

#[tokio::test]
async fn public_kv_binding_publication_cas_failure_leaves_old_view() {
    let budget = V3MountBudget::defaults();
    let backend = ClockBackend::default();
    let store = KvWorkspaceStore::new(backend.clone()).with_packed_reader_pin_budget(budget);
    let (_temp, _client, _snapshot, proof, _payload) = packed().await;
    let request = request(&store, proof).await;
    let first = store
        .install_packed_lower_binding(request.clone())
        .await
        .unwrap();
    let (_replacement_temp, _replacement_client, _replacement_snapshot, replacement, _payload) =
        packed_with_snapshot_id([12; 32]).await;
    let expected_layers: [LayerRecord; 2] = store
        .load_layer_chain(first.head_layer_id)
        .await
        .unwrap()
        .try_into()
        .unwrap();
    let mut stale = PublishPackedLowerBinding {
        guard: installed_guard(&request, &first),
        expected_layers: expected_layers.clone(),
        expected_base: request.expected_base.clone(),
        expected_binding: first.clone(),
        lower: replacement.clone(),
    };
    stale.guard.expected_head_epoch -= 1;
    assert!(matches!(
        store.publish_packed_lower_binding(stale).await,
        Err(WorkspaceError::Fenced)
    ));
    backend.fail_at_cas.store(true, Ordering::SeqCst);
    let publication = PublishPackedLowerBinding {
        guard: installed_guard(&request, &first),
        expected_layers,
        expected_base: request.expected_base.clone(),
        expected_binding: first.clone(),
        lower: replacement,
    };
    assert!(matches!(
        store.publish_packed_lower_binding(publication).await,
        Err(WorkspaceError::Backend(_))
    ));
    assert_eq!(
        store
            .load_packed_binding_record(installed_guard(&request, &first))
            .await
            .unwrap(),
        Some(first.clone())
    );
    assert_eq!(
        store
            .load_packed_binding_version(first.workspace_id, 2)
            .await
            .unwrap(),
        None
    );
    assert_eq!(store.allocate_id("inode").await.unwrap(), 401);
}

#[tokio::test]
async fn public_sqlite_binding_publication_stale_and_failpoint_leave_old_view() {
    let temp = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}",
        temp.path().join("publication.sqlite").display()
    );
    let store = SqliteWorkspaceStore::connect(&url).await.unwrap();
    let (_temp, _client, _snapshot, proof, _payload) = packed().await;
    let request = request(&store, proof).await;
    let first = store
        .install_packed_lower_binding(request.clone())
        .await
        .unwrap();
    let (_replacement_temp, _replacement_client, _replacement_snapshot, replacement, _payload) =
        packed_with_snapshot_id([11; 32]).await;
    let expected_layers: [LayerRecord; 2] = store
        .load_layer_chain(first.head_layer_id)
        .await
        .unwrap()
        .try_into()
        .unwrap();
    let mut publication = PublishPackedLowerBinding {
        guard: installed_guard(&request, &first),
        expected_layers,
        expected_base: request.expected_base.clone(),
        expected_binding: first.clone(),
        lower: replacement,
    };
    publication.guard.expected_head_epoch -= 1;
    assert!(matches!(
        store
            .publish_packed_lower_binding(publication.clone())
            .await,
        Err(WorkspaceError::Fenced)
    ));
    assert_eq!(
        store
            .load_packed_binding_record(installed_guard(&request, &first))
            .await
            .unwrap(),
        Some(first.clone())
    );
    publication.guard.expected_head_epoch = first.head_epoch;
    store.set_failpoint(StoreFailpoint::BeforeCommit);
    assert!(matches!(
        store
            .publish_packed_lower_binding(publication.clone())
            .await,
        Err(WorkspaceError::Backend(_))
    ));
    store.set_failpoint(StoreFailpoint::Disabled);
    assert_eq!(
        store
            .load_packed_binding_record(installed_guard(&request, &first))
            .await
            .unwrap(),
        Some(first.clone())
    );
    assert_eq!(
        store
            .load_packed_binding_version(first.workspace_id, 2)
            .await
            .unwrap(),
        None
    );
    assert_eq!(store.allocate_id("inode").await.unwrap(), 401);
    let second = store
        .publish_packed_lower_binding(publication)
        .await
        .unwrap();
    drop(store);
    let reopened = SqliteWorkspaceStore::connect(&url).await.unwrap();
    let published_guard = HeadGuard {
        expected_head_epoch: second.head_epoch,
        workspace_id: second.workspace_id,
        expected_head_layer_id: second.head_layer_id,
        lease_id: request.guard.lease_id,
        holder_generation: request.guard.holder_generation,
    };
    assert_eq!(
        reopened
            .load_packed_binding_record(published_guard)
            .await
            .unwrap(),
        Some(second.clone())
    );
    assert_eq!(
        reopened
            .load_packed_binding_version(second.workspace_id, 1)
            .await
            .unwrap(),
        Some(first)
    );
    assert_eq!(
        reopened
            .load_packed_binding_version(second.workspace_id, 2)
            .await
            .unwrap(),
        Some(second)
    );
}

#[tokio::test]
async fn public_sqlite_binding_atomic_epoch_sequence_allocator_and_history() {
    let store = SqliteWorkspaceStore::connect("sqlite::memory:")
        .await
        .unwrap();
    contract(&store, false).await;
}

#[tokio::test]
async fn public_authenticated_inode_ceiling_rejects_missing_and_key_value_substitution() {
    use crate::workspace_overlay::packed_v3::wire005::{
        V3IndexPage, V3ObjectKind, V3ObjectRef, V3RootKind,
    };
    let (_objects, client, snapshot, _proof, _payload) = packed().await;
    let root = snapshot.root(V3RootKind::Inodes);
    let bytes = snapshot
        .read_root(&client, V3RootKind::Inodes)
        .await
        .unwrap();
    let mut page = V3IndexPage::decode(root, &bytes).unwrap();
    let record = page.records.last_mut().unwrap();
    record.first_key = 401u64.to_be_bytes().to_vec();
    record.last_key = record.first_key.clone();
    let substituted = page.encode().unwrap();
    let replacement = V3ObjectRef::from_bytes(
        "substituted-inodes".into(),
        V3ObjectKind::InodeIndex,
        &substituted,
    )
    .unwrap();
    client
        .put_object(&replacement.key, &substituted)
        .await
        .unwrap();
    let mut manifest = snapshot.manifest().clone();
    manifest.roots[V3RootKind::Inodes as usize] = replacement;
    let manifest_bytes = manifest.encode().unwrap();
    let manifest_ref = V3ObjectRef::from_bytes(
        "substituted-manifest".into(),
        V3ObjectKind::Manifest,
        &manifest_bytes,
    )
    .unwrap();
    let changed = AuthenticatedV3Snapshot::decode(&manifest_ref, &manifest_bytes).unwrap();
    assert!(
        VerifiedPackedLower::from_authenticated_snapshot(
            &changed,
            &V3IndexReader::new(client.clone(), 0)
        )
        .await
        .is_err()
    );
    client.delete_object(&root.key).await.unwrap();
    assert!(
        VerifiedPackedLower::from_authenticated_snapshot(&snapshot, &V3IndexReader::new(client, 0))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn public_authenticated_inode_ceiling_uses_deep_rightmost_path() {
    use crate::workspace_overlay::packed_v3::wire005::{
        V3IndexBuilder, V3IndexPage, V3IndexRecord, V3IndexValue, V3InodeLocation, V3ObjectKind,
        V3ObjectRef, V3RootKind,
    };
    let (objects, client, snapshot, _proof, _payload) = packed().await;
    let bytes = snapshot
        .read_root(&client, V3RootKind::Inodes)
        .await
        .unwrap();
    let page = V3IndexPage::decode(snapshot.root(V3RootKind::Inodes), &bytes).unwrap();
    let V3IndexValue::Leaf(value) = &page.records[0].value else {
        panic!("one leaf expected")
    };
    let mut location = V3InodeLocation::decode_value(value).unwrap();
    let mut builder = V3IndexBuilder::new(
        client.clone(),
        V3ObjectKind::InodeIndex,
        "deep/inodes".into(),
        2,
        16 * 1024,
    )
    .unwrap();
    // Routing-only fixture: this authenticates an index ceiling, not a
    // publishable group/namespace dependency closure.
    for inode in 2u64..40 {
        location.hot.inode = inode;
        let key = inode.to_be_bytes().to_vec();
        builder
            .push(V3IndexRecord {
                first_key: key.clone(),
                last_key: key,
                value: V3IndexValue::Leaf(location.encode_value().unwrap()),
            })
            .await
            .unwrap();
    }
    let root = builder.finish().await.unwrap();
    let mut manifest = snapshot.manifest().clone();
    manifest.roots[V3RootKind::Inodes as usize] = root.clone();
    let bytes = manifest.encode().unwrap();
    let reference =
        V3ObjectRef::from_bytes("deep/manifest".into(), V3ObjectKind::Manifest, &bytes).unwrap();
    let snapshot = AuthenticatedV3Snapshot::decode(&reference, &bytes).unwrap();
    let root_bytes = snapshot
        .read_root(&client, V3RootKind::Inodes)
        .await
        .unwrap();
    let page = V3IndexPage::decode(&root, &root_bytes).unwrap();
    assert!(page.height >= 3);
    let proof = VerifiedPackedLower::from_authenticated_snapshot(
        &snapshot,
        &V3IndexReader::new(client.clone(), 0),
    )
    .await
    .unwrap();
    assert_eq!(proof.highest_inode(), 39);
    // Remove every inode page except the authenticated rightmost route. A
    // namespace scan would then fail; the bounded route still succeeds.
    let mut keep = std::collections::BTreeSet::new();
    let mut reference = root;
    loop {
        keep.insert(reference.key.clone());
        let bytes = crate::workspace_overlay::packed_v3::wire005::read_v3_page(
            &client,
            &reference,
            256 * 1024,
        )
        .await
        .unwrap();
        let page = V3IndexPage::decode(&reference, &bytes).unwrap();
        match &page.records.last().unwrap().value {
            V3IndexValue::Leaf(_) => break,
            V3IndexValue::Child {
                reference: child, ..
            } => reference = child.clone(),
        }
    }
    for entry in std::fs::read_dir(objects.path().join("objects/deep/inodes/6")).unwrap() {
        let path = entry.unwrap().path();
        let key = format!(
            "deep/inodes/6/{}",
            path.file_name().unwrap().to_str().unwrap()
        );
        if !keep.contains(&key) {
            client.delete_object(&key).await.unwrap();
        }
    }
    assert_eq!(
        VerifiedPackedLower::from_authenticated_snapshot(&snapshot, &V3IndexReader::new(client, 0))
            .await
            .unwrap()
            .highest_inode(),
        39
    );
}

#[tokio::test]
async fn public_kv_binding_atomic_epoch_sequence_allocator_and_history() {
    let budget = V3MountBudget::defaults();
    contract(
        &KvWorkspaceStore::new(ClockBackend::default()).with_packed_reader_pin_budget(budget),
        true,
    )
    .await;
}

#[tokio::test]
async fn public_kv_claimed_binding_missing_pointer_never_falls_back_to_native() {
    let budget = V3MountBudget::defaults();
    let backend = ClockBackend::default();
    let store = KvWorkspaceStore::new(backend.clone()).with_packed_reader_pin_budget(budget);
    let (_objects, _client, _snapshot, proof, _payload) = packed().await;
    let request = request(&store, proof).await;
    let record = store
        .install_packed_lower_binding(request.clone())
        .await
        .unwrap();
    let guard = installed_guard(&request, &record);
    let key = format!("packed/v3/current/{}", guard.workspace_id).into_bytes();
    let expected = backend.get(&key).await.unwrap();
    assert!(
        backend
            .compare_and_swap(
                &[KvCheck {
                    key: key.clone(),
                    expected
                }],
                &[KvWrite::Delete { key }]
            )
            .await
            .unwrap()
    );
    assert!(matches!(
        store.load_packed_lower_binding(guard.clone()).await,
        Err(WorkspaceError::CorruptMetadata(_))
    ));
    assert_eq!(
        store
            .load_packed_binding_version(guard.workspace_id, 1)
            .await
            .unwrap(),
        Some(record)
    );
}

async fn same_epoch_mutation<S: WorkspaceStore>(store: &S) {
    use crate::workspace_overlay::model::DentryDelta;
    let (_objects, _client, _snapshot, proof, _payload) = packed().await;
    let request = request(store, proof).await;
    let mut fenced = request.clone();
    fenced.guard.holder_generation += 1;
    assert!(matches!(
        store.install_packed_lower_binding(fenced).await,
        Err(WorkspaceError::Fenced)
    ));
    store
        .apply_namespace_mutation(NamespaceMutation {
            guard: request.guard.clone(),
            dentries: vec![DentryDelta::put(
                request.guard.expected_head_layer_id,
                1,
                b"changed".to_vec(),
                2,
                0,
                0,
            )],
            inodes: vec![],
        })
        .await
        .unwrap();
    assert!(matches!(
        store.install_packed_lower_binding(request.clone()).await,
        Err(WorkspaceError::Busy)
    ));
    assert_eq!(
        store
            .load_workspace(request.guard.workspace_id)
            .await
            .unwrap()
            .head_epoch,
        request.guard.expected_head_epoch
    );
    assert_eq!(
        store
            .load_packed_binding_record(request.guard.clone())
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        store
            .load_packed_binding_version(request.guard.workspace_id, 1)
            .await
            .unwrap(),
        None
    );
    assert_eq!(store.allocate_id("inode").await.unwrap(), 2);
}

#[tokio::test]
async fn public_sqlite_binding_requires_initial_history_anchor_for_current_version() {
    let store = SqliteWorkspaceStore::connect("sqlite::memory:")
        .await
        .unwrap();
    let (_objects, _client, _snapshot, proof, _payload) = packed().await;
    let request = request(&store, proof).await;
    let first = store
        .install_packed_lower_binding(request.clone())
        .await
        .unwrap();
    let guard = installed_guard(&request, &first);
    let mut current = first.clone();
    current.binding.binding_version = 2;
    store
        .rewrite_packed_binding_for_test(request.guard.workspace_id, &current, true)
        .await
        .unwrap();

    // The current pointer alone is insufficient: the version-1 history anchor
    // is the durable proof that this binding lineage was initialized safely.
    assert!(matches!(
        store.load_packed_binding_record(guard).await,
        Err(WorkspaceError::CorruptMetadata(_))
    ));
}

#[tokio::test]
async fn public_sqlite_binding_same_epoch_mutation_does_not_replay_or_reserve_ids() {
    same_epoch_mutation(
        &SqliteWorkspaceStore::connect("sqlite::memory:")
            .await
            .unwrap(),
    )
    .await;
}

#[tokio::test]
async fn public_kv_binding_same_epoch_mutation_does_not_replay_or_reserve_ids() {
    let budget = V3MountBudget::defaults();
    same_epoch_mutation(
        &KvWorkspaceStore::new(ClockBackend::default()).with_packed_reader_pin_budget(budget),
    )
    .await;
}

#[tokio::test]
async fn public_kv_binding_corrupt_both_records_fails_closed() {
    let budget = V3MountBudget::defaults();
    let backend = ClockBackend::default();
    let store = KvWorkspaceStore::new(backend.clone()).with_packed_reader_pin_budget(budget);
    let (_objects, _client, _snapshot, proof, _payload) = packed().await;
    let request = request(&store, proof).await;
    let record = store
        .install_packed_lower_binding(request.clone())
        .await
        .unwrap();
    let guard = installed_guard(&request, &record);
    let current = format!("packed/v3/current/{}", guard.workspace_id).into_bytes();
    let history = format!("packed/v3/history/{}/{:016x}", guard.workspace_id, 1u64).into_bytes();
    let bytes = backend.get(&current).await.unwrap().unwrap();
    let mut corrupt = bytes.clone();
    corrupt[80] ^= 1;
    assert!(
        backend
            .compare_and_swap(
                &[
                    KvCheck {
                        key: current.clone(),
                        expected: Some(bytes.clone())
                    },
                    KvCheck {
                        key: history.clone(),
                        expected: Some(bytes)
                    }
                ],
                &[
                    KvWrite::Put {
                        key: current,
                        value: corrupt.clone()
                    },
                    KvWrite::Put {
                        key: history,
                        value: corrupt
                    },
                ]
            )
            .await
            .unwrap()
    );
    assert!(matches!(
        store.load_packed_lower_binding(guard.clone()).await,
        Err(WorkspaceError::CorruptMetadata(_))
    ));
    assert!(matches!(
        store
            .load_packed_binding_version(guard.workspace_id, 1)
            .await,
        Err(WorkspaceError::CorruptMetadata(_))
    ));
}

#[tokio::test]
async fn public_sqlite_binding_precommit_failure_is_all_old_and_reopen_is_all_new() {
    let temp = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}", temp.path().join("catalog.sqlite").display());
    let store = SqliteWorkspaceStore::connect(&url).await.unwrap();
    let (_objects, _client, _snapshot, proof, _payload) = packed().await;
    let request = request(&store, proof).await;
    store.set_failpoint(StoreFailpoint::BeforeCommit);
    assert!(matches!(
        store.install_packed_lower_binding(request.clone()).await,
        Err(WorkspaceError::Backend(_))
    ));
    store.set_failpoint(StoreFailpoint::Disabled);
    let reopened = SqliteWorkspaceStore::connect(&url).await.unwrap();
    assert_eq!(
        reopened
            .load_workspace(request.guard.workspace_id)
            .await
            .unwrap()
            .head_epoch,
        request.guard.expected_head_epoch
    );
    assert_eq!(
        reopened
            .load_layer(request.guard.expected_head_layer_id)
            .await
            .unwrap(),
        request.expected_layers[0]
    );
    assert_eq!(
        reopened
            .load_packed_binding_record(request.guard.clone())
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        reopened
            .load_packed_binding_version(request.guard.workspace_id, 1)
            .await
            .unwrap(),
        None
    );
    let record = reopened
        .install_packed_lower_binding(request.clone())
        .await
        .unwrap();
    drop(store);
    drop(reopened);
    let reopened = SqliteWorkspaceStore::connect(&url).await.unwrap();
    assert_eq!(
        reopened
            .load_packed_binding_record(installed_guard(&request, &record))
            .await
            .unwrap(),
        Some(record.clone())
    );
    assert_eq!(
        reopened
            .load_layer(request.guard.expected_head_layer_id)
            .await
            .unwrap()
            .next_sequence,
        2
    );
    assert_eq!(reopened.allocate_id("inode").await.unwrap(), 401);
}

#[tokio::test]
async fn public_sqlite_binding_two_publishers_preserve_winner_without_replay() {
    let temp = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}", temp.path().join("race.sqlite").display());
    let first = SqliteWorkspaceStore::connect(&url).await.unwrap();
    let second = SqliteWorkspaceStore::connect(&url).await.unwrap();
    let (_objects, _client, _snapshot, proof, _payload) = packed().await;
    let request = request(&first, proof).await;
    let (a, b) = tokio::join!(
        first.install_packed_lower_binding(request.clone()),
        second.install_packed_lower_binding(request.clone())
    );
    let record = match (a, b) {
        (Ok(record), Err(WorkspaceError::Fenced | WorkspaceError::Busy))
        | (Err(WorkspaceError::Fenced | WorkspaceError::Busy), Ok(record)) => record,
        results => panic!("exactly one publisher must win: {results:?}"),
    };
    assert_eq!(
        first
            .load_packed_binding_version(request.guard.workspace_id, 1)
            .await
            .unwrap(),
        Some(record.clone())
    );
    assert_eq!(
        first
            .load_workspace(request.guard.workspace_id)
            .await
            .unwrap()
            .head_epoch,
        record.head_epoch
    );
    assert_eq!(second.allocate_id("inode").await.unwrap(), 401);
}

#[tokio::test]
async fn public_sqlite_binding_rejects_issued_inode_and_stale_base_without_writes() {
    let store = SqliteWorkspaceStore::connect("sqlite::memory:")
        .await
        .unwrap();
    let (_objects, _client, _snapshot, proof, _payload) = packed().await;
    let request = request(&store, proof).await;
    let mut wrong = request.clone();
    wrong.expected_base.root_hash[0] ^= 1;
    assert!(store.install_packed_lower_binding(wrong).await.is_err());
    assert_eq!(store.allocate_id("inode").await.unwrap(), 2);
    assert!(matches!(
        store.install_packed_lower_binding(request.clone()).await,
        Err(WorkspaceError::UnsupportedCapability(_))
    ));
    assert_eq!(
        store
            .load_packed_binding_record(request.guard.clone())
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        store
            .load_layer(request.guard.expected_head_layer_id)
            .await
            .unwrap(),
        request.expected_layers[0]
    );
}

#[tokio::test]
async fn public_kv_binding_cas_clock_expiry_and_backend_failure_leave_all_old() {
    for expiry in [false, true] {
        let backend = ClockBackend::default();
        let budget = V3MountBudget::defaults();
        let store = KvWorkspaceStore::new(backend.clone()).with_packed_reader_pin_budget(budget);
        let (_objects, _client, _snapshot, proof, _payload) = packed().await;
        let request = request(&store, proof).await;
        if expiry {
            backend
                .advance_at_cas
                .store(30_000_000_000, Ordering::SeqCst);
        } else {
            backend.fail_at_cas.store(true, Ordering::SeqCst);
        }
        let result = store.install_packed_lower_binding(request.clone()).await;
        if expiry {
            assert!(matches!(result, Err(WorkspaceError::Fenced)));
        } else {
            assert!(matches!(result, Err(WorkspaceError::Backend(_))));
        }
        assert_eq!(
            store
                .load_workspace(request.guard.workspace_id)
                .await
                .unwrap()
                .head_epoch,
            request.guard.expected_head_epoch
        );
        assert_eq!(
            store
                .load_layer(request.guard.expected_head_layer_id)
                .await
                .unwrap(),
            request.expected_layers[0]
        );
        assert_eq!(
            store
                .load_packed_binding_version(request.guard.workspace_id, 1)
                .await
                .unwrap(),
            None
        );
        assert_eq!(store.allocate_id("inode").await.unwrap(), 2);
    }
}

#[tokio::test]
async fn public_sqlite_persisted_catalog_authority_executes_authenticated_lower_bytes() {
    use crate::chunk::read_plan::{WorkspaceReadPlanProvider, execute_unified_into};
    use crate::chunk::{BlockKey, BlockStore, ChunkLayout};
    use crate::workspace_overlay::meta_layer::{
        CatalogPackedBindingAuthority, WorkspaceMetaLayer, WorkspacePackedBindingAuthority,
    };
    use crate::workspace_overlay::model::ViewContext;
    use crate::workspace_overlay::packed_v3::PackedV3ReadonlyMeta;
    struct NoUpper;
    #[async_trait]
    impl BlockStore for NoUpper {
        async fn write_fresh_range(
            &self,
            _key: BlockKey,
            _offset: u64,
            _bytes: &[u8],
        ) -> anyhow::Result<u64> {
            anyhow::bail!("unexpected upper write")
        }
        async fn read_range(
            &self,
            _key: BlockKey,
            _offset: u64,
            _bytes: &mut [u8],
        ) -> anyhow::Result<()> {
            anyhow::bail!("unexpected upper read")
        }
        async fn delete_range(&self, _key: BlockKey, _count: u64) -> anyhow::Result<()> {
            anyhow::bail!("unexpected upper delete")
        }
    }
    let store = Arc::new(
        SqliteWorkspaceStore::connect("sqlite::memory:")
            .await
            .unwrap(),
    );
    let (_objects, client, snapshot, proof, payload) = packed().await;
    let request = request(store.as_ref(), proof).await;
    let record = store
        .install_packed_lower_binding(request.clone())
        .await
        .unwrap();
    let guard = installed_guard(&request, &record);
    let authority = Arc::new(CatalogPackedBindingAuthority(store.clone()));
    authority.validate(&guard, &record.binding).await.unwrap();
    let mut wrong = record.binding.clone();
    wrong.manifest.digest[0] ^= 1;
    assert!(matches!(
        authority.validate(&guard, &wrong).await,
        Err(WorkspaceError::Fenced)
    ));
    let lower = Arc::new(PackedV3ReadonlyMeta::from_v3(client, snapshot, 4096, 0));
    let meta = WorkspaceMetaLayer::with_chunk_size(
        store.clone(),
        ViewContext {
            workspace_id: guard.workspace_id,
            head_layer_id: guard.expected_head_layer_id,
            head_epoch: guard.expected_head_epoch,
            lease_id: guard.lease_id,
            holder_generation: guard.holder_generation,
        },
        4096,
    )
    .with_packed_v3_lower(
        record.binding,
        lower,
        authority,
        Arc::new(NoUpper),
        ChunkLayout {
            chunk_size: 4096,
            block_size: 4096,
        },
    )
    .unwrap();
    let prepared = meta
        .prepare_unified_read(400, 0, 19, 64)
        .await
        .unwrap()
        .unwrap();
    let mut bytes = vec![0; 64];
    execute_unified_into(prepared.fetcher.as_ref(), 19, &prepared.plan, &mut bytes)
        .await
        .unwrap();
    assert_eq!(bytes, payload[19..83]);
    store
        .release_lease(ReleaseLease {
            lease_id: guard.lease_id,
            holder_generation: guard.holder_generation,
        })
        .await
        .unwrap();
    assert!(meta.prepare_unified_read(400, 0, 19, 64).await.is_err());
}

#[tokio::test]
#[ignore = "requires BREWFS_TEST_REDIS_URL"]
async fn public_real_redis_binding_contract() {
    let budget = V3MountBudget::defaults();
    let backend = RedisWorkspaceBackend::connect(
        &std::env::var("BREWFS_TEST_REDIS_URL").unwrap(),
        &format!("g10c-{}", Uuid::new_v4().simple()),
    )
    .await
    .unwrap();
    contract(
        &KvWorkspaceStore::new(backend).with_packed_reader_pin_budget(budget),
        true,
    )
    .await;
}

#[tokio::test]
#[ignore = "requires BREWFS_TEST_TIKV_PD_ENDPOINTS"]
async fn public_real_tikv_binding_contract() {
    let budget = V3MountBudget::defaults();
    let endpoints = std::env::var("BREWFS_TEST_TIKV_PD_ENDPOINTS")
        .unwrap()
        .split(',')
        .map(str::to_owned)
        .collect();
    let backend = TiKvWorkspaceBackend::connect_with_budget(
        endpoints,
        &format!("g10c-{}", Uuid::new_v4().simple()),
        budget.clone(),
    )
    .await
    .unwrap();
    contract(
        &KvWorkspaceStore::new(backend).with_packed_reader_pin_budget(budget),
        true,
    )
    .await;
}

#[derive(Clone)]
struct ClockBackend {
    records: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
    now: Arc<AtomicI64>,
    advance_at_cas: Arc<AtomicI64>,
    fail_at_cas: Arc<AtomicBool>,
    lose_response_at_cas: Arc<AtomicBool>,
    mutate_head_at_cas: Arc<AtomicBool>,
    allocate_inode_at_cas: Arc<AtomicBool>,
}
// Reuse the unchanged real KV fixture without exposing its private type.
pub(crate) fn packed_open_preparation_test_backend() -> impl WorkspaceKvBackend {
    ClockBackend::default()
}

impl Default for ClockBackend {
    fn default() -> Self {
        Self {
            records: Default::default(),
            now: Arc::new(AtomicI64::new(1_000_000_000)),
            advance_at_cas: Default::default(),
            fail_at_cas: Default::default(),
            lose_response_at_cas: Default::default(),
            mutate_head_at_cas: Default::default(),
            allocate_inode_at_cas: Default::default(),
        }
    }
}
// Test actors must preserve the BWSKV001 envelope used by kv_store::encode.
// Bare bincode bytes would model catalog corruption rather than a mutation.
fn decode_clock_record<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> T {
    let payload = bytes
        .strip_prefix(b"BWSKV001")
        .expect("ClockBackend actor requires a valid KV envelope");
    bincode::deserialize(payload).unwrap()
}

fn encode_clock_record<T: serde::Serialize>(value: &T) -> Vec<u8> {
    let mut bytes = b"BWSKV001".to_vec();
    bytes.extend_from_slice(&bincode::serialize(value).unwrap());
    bytes
}

impl ClockBackend {
    async fn cas(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        deadline: Option<i64>,
    ) -> Result<bool, WorkspaceError> {
        self.cas_with_authentication_limits(checks, writes, deadline, None)
            .await
    }

    async fn cas_with_authentication_limits(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        deadline: Option<i64>,
        authentication_limits: Option<crate::workspace_overlay::stores::kv_backend::KvReadLimits>,
    ) -> Result<bool, WorkspaceError> {
        let mut records = self.records.lock().await;
        if deadline.is_some() && self.allocate_inode_at_cas.swap(false, Ordering::SeqCst) {
            let key = b"alloc/inode".to_vec();
            let current: i64 = decode_clock_record(records.get(&key).unwrap());
            records.insert(key, encode_clock_record(&current.checked_add(1).unwrap()));
        }
        if deadline.is_some() && self.mutate_head_at_cas.swap(false, Ordering::SeqCst) {
            let head_key = checks
                .iter()
                .find(|check| {
                    check.key.starts_with(b"layer/")
                        && records.get(&check.key).is_some_and(|raw| {
                            decode_clock_record::<LayerRecord>(raw).state == LayerState::Writable
                        })
                })
                .expect("publication actor requires the exact writable head entity")
                .key
                .clone();
            let mut head: LayerRecord = decode_clock_record(records.get(&head_key).unwrap());
            head.next_sequence += 1;
            records.insert(head_key, encode_clock_record(&head));
        }
        if let Some(limits) = authentication_limits {
            let mut authentication_total = 0usize;
            for check in checks {
                let value_bytes = records.get(&check.key).map_or(0, Vec::len);
                authentication_total = authentication_total
                    .checked_add(check.key.len())
                    .and_then(|bytes| bytes.checked_add(value_bytes))
                    .ok_or_else(|| {
                        WorkspaceError::InvalidReadPlan(
                            "fixture authentication byte count overflow".into(),
                        )
                    })?;
                let response_bytes = check
                    .key
                    .len()
                    .checked_add(value_bytes)
                    .and_then(|bytes| bytes.checked_add(16))
                    .ok_or_else(|| {
                        WorkspaceError::InvalidReadPlan(
                            "fixture authentication response byte count overflow".into(),
                        )
                    })?;
                if value_bytes > limits.max_value_bytes
                    || authentication_total > limits.max_total_bytes
                    || response_bytes > limits.max_response_bytes
                {
                    return Err(WorkspaceError::InvalidReadPlan(
                        "fixture authentication snapshot exceeds byte limits".into(),
                    ));
                }
            }
        }
        if checks
            .iter()
            .any(|check| records.get(&check.key) != check.expected.as_ref())
        {
            return Ok(false);
        }
        if let Some(deadline) = deadline {
            self.now.fetch_add(
                self.advance_at_cas.swap(0, Ordering::SeqCst),
                Ordering::SeqCst,
            );
            if self.now.load(Ordering::SeqCst) >= deadline {
                return Err(WorkspaceError::Fenced);
            }
            if self.fail_at_cas.swap(false, Ordering::SeqCst) {
                return Err(WorkspaceError::Backend("injected precommit failure".into()));
            }
        }
        for write in writes {
            match write {
                KvWrite::Put { key, value } => {
                    records.insert(key.clone(), value.clone());
                }
                KvWrite::Delete { key } => {
                    records.remove(key);
                }
            }
        }
        if deadline.is_some() && self.lose_response_at_cas.swap(false, Ordering::SeqCst) {
            return Err(WorkspaceError::Backend(
                "injected committed response loss".into(),
            ));
        }
        Ok(true)
    }
}
#[async_trait]
impl WorkspaceKvBackend for ClockBackend {
    fn name(&self) -> &'static str {
        "g10c-clock-test"
    }
    fn supports_consistent_reads(&self) -> bool {
        true
    }
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, WorkspaceError> {
        Ok(self.records.lock().await.get(key).cloned())
    }
    async fn get_many_consistent(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>, WorkspaceError> {
        let records = self.records.lock().await;
        Ok(keys.iter().map(|key| records.get(key).cloned()).collect())
    }
    async fn get_many_consistent_with_time(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        let records = self.records.lock().await;
        Ok((
            keys.iter().map(|key| records.get(key).cloned()).collect(),
            self.now.load(Ordering::SeqCst),
        ))
    }
    async fn get_many_consistent_with_time_bounded(
        &self,
        keys: &[Vec<u8>],
        limits: KvReadLimits,
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        limits.validate_keys(keys)?;
        let records = self.records.lock().await;
        // This in-memory fixture checks its exact snapshot before cloning any
        // stored value. It does not emulate a remote unbounded-read fallback.
        let mut total = 0usize;
        for key in keys {
            let bytes = records.get(key).map_or(0, Vec::len);
            total = total
                .checked_add(key.len())
                .and_then(|sum| sum.checked_add(bytes))
                .ok_or_else(|| WorkspaceError::Backend("fixture byte count overflow".into()))?;
            if bytes > limits.max_value_bytes
                || total > limits.max_total_bytes
                || total > limits.max_response_bytes
            {
                return Err(WorkspaceError::Backend(
                    "fixture bounded read exceeded".into(),
                ));
            }
        }
        Ok((
            keys.iter().map(|key| records.get(key).cloned()).collect(),
            self.now.load(Ordering::SeqCst),
        ))
    }
    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<KvEntry>, WorkspaceError> {
        Ok(self
            .records
            .lock()
            .await
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| KvEntry {
                key: key.clone(),
                value: value.clone(),
            })
            .collect())
    }
    async fn compare_and_swap(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
    ) -> Result<bool, WorkspaceError> {
        self.cas(checks, writes, None).await
    }
    async fn scan_prefix_page_with_byte_limits(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limits: KvReadLimits,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        limits.validate_scan_page(prefix, after)?;
        let records = self.records.lock().await;
        let selected = || {
            records
                .range(prefix.to_vec()..)
                .take_while(|(key, _)| key.starts_with(prefix))
                .filter(|(key, _)| after.is_none_or(|after| key.as_slice() > after))
                .take(limits.max_records.min(limits.max_data_requests))
        };
        let mut total = 0usize;
        let mut response = 16usize;
        for (key, value) in selected() {
            total = total
                .checked_add(key.len())
                .and_then(|sum| sum.checked_add(value.len()))
                .ok_or(WorkspaceError::Busy)?;
            response = response
                .checked_add(32)
                .and_then(|sum| sum.checked_add(key.len()))
                .and_then(|sum| sum.checked_add(value.len()))
                .ok_or(WorkspaceError::Busy)?;
            if key.len() > limits.max_key_bytes
                || value.len() > limits.max_value_bytes
                || total > limits.max_total_bytes
                || response > limits.max_response_bytes
            {
                return Err(WorkspaceError::InvalidReadPlan(
                    "clock fixture keyset page exceeds limits before cloning".into(),
                ));
            }
        }
        Ok(selected()
            .map(|(key, value)| KvEntry {
                key: key.clone(),
                value: value.clone(),
            })
            .collect())
    }
    async fn compare_and_swap_before(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        expires_at_ns: i64,
    ) -> Result<bool, WorkspaceError> {
        self.cas(checks, writes, Some(expires_at_ns)).await
    }
    async fn authenticate_checks_before_bounded(
        &self,
        checks: &[KvCheck],
        expires_at_ns: i64,
        limits: crate::workspace_overlay::stores::kv_backend::KvReadLimits,
    ) -> Result<bool, WorkspaceError> {
        crate::workspace_overlay::stores::kv_backend::validate_bounded_authentication_checks(
            checks, limits,
        )?;
        crate::workspace_overlay::stores::kv_backend::validate_cas_time_window(
            None,
            Some(expires_at_ns),
        )?;
        self.cas_with_authentication_limits(checks, &[], Some(expires_at_ns), Some(limits))
            .await
    }

    async fn server_time_ns(&self) -> Result<i64, WorkspaceError> {
        Ok(self.now.load(Ordering::SeqCst))
    }
}
