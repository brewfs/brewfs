//! Exercises the actual shared placement prepare/execute path, including its
//! 32 MiB caller allocation limit and opposite logical/physical frame order.
use super::super::{
    SnapshotBudgetLimits, SnapshotBudgetPool, SnapshotFrameDirectoryPage, SnapshotIndexPage,
    SnapshotIndexReader, SnapshotIndexRecord, SnapshotIndexValue, SnapshotMountBudget,
};
use super::*;
use crate::cadapter::localfs::LocalFsBackend;
use crate::cadapter::read_observer::{
    Engine, Ledger, Origin, Phase, ReadClass, ReadContext, ReadObserver,
};
use crate::chunk::read_plan::execute_unified_into;
use crate::workspace::packed::{GroupMetaEntry, GroupMetaExtent, PackedFrameDescriptor, SizeClass};
use sha2::{Digest, Sha256};
use std::sync::Arc;

async fn index(
    client: &ObjectClient<LocalFsBackend>,
    kind: SnapshotObjectKind,
    key: &str,
    records: Vec<SnapshotIndexRecord>,
) -> SnapshotObjectRef {
    let bytes = SnapshotIndexPage {
        kind,
        height: 0,
        records,
    }
    .encode()
    .unwrap();
    let reference = SnapshotObjectRef::from_bytes(key.into(), kind, &bytes).unwrap();
    client.put_object(key, &bytes).await.unwrap();
    reference
}
fn leaf(key: Vec<u8>, value: Vec<u8>) -> SnapshotIndexRecord {
    SnapshotIndexRecord {
        first_key: key.clone(),
        last_key: key,
        value: SnapshotIndexValue::Leaf(value),
    }
}

#[tokio::test]
async fn shared_placement_prepare_executes_reverse_and_revisited_frames_with_one_raw_frame_capacity()
 {
    for engine in [Engine::Native, Engine::Packed] {
        for sequence in [vec![(1, 0), (0, 0)], vec![(0, 0), (1, 0), (0, 32)]] {
            let dir = tempfile::tempdir().unwrap();
            let observer = Arc::new(ReadObserver::default());
            let client = ObjectClient::new(LocalFsBackend::new(dir.path())).with_read_observer(
                observer.clone(),
                engine,
                Phase::Runtime,
                Origin::Demand,
            );
            let mut roots = Vec::new();
            for (ordinal, kind) in ROOT_KINDS.iter().copied().enumerate() {
                roots.push(
                    index(
                        &client,
                        kind.object_kind(),
                        &format!("root-{ordinal}"),
                        vec![],
                    )
                    .await,
                );
            }
            let mut body = vec![1u8; 64];
            body.extend([0u8; 32]);
            body.extend([2u8; 64]);
            let bytes =
                encode_snapshot_object(SnapshotObjectKind::GroupContainer, &body, 1024).unwrap();
            let container = SnapshotObjectRef::from_bytes(
                "container".into(),
                SnapshotObjectKind::GroupContainer,
                &bytes,
            )
            .unwrap();
            client.put_object(&container.key, &bytes).await.unwrap();
            let page = SnapshotFrameDirectoryPage {
                container_digest: container.digest,
                container_len: container.object_len,
                profile: AccessProfile::RandomSmallFile,
                size_classes: SizeClassTable::default(),
                frame_policy: Default::default(),
                first_ordinal: 0,
                frames: (0..2)
                    .map(|ordinal| PackedFrameDescriptor {
                        frame_ordinal: ordinal,
                        object_offset: 4096 + u64::from(ordinal) * 96,
                        stored_len: 64,
                        raw_len: 64,
                        first_file_slot: 0,
                        last_file_slot: 0,
                        size_class: SizeClass::Tiny,
                        codec: 0,
                        frame_digest: Sha256::digest(vec![ordinal as u8 + 1; 64])[..16]
                            .try_into()
                            .unwrap(),
                    })
                    .collect(),
            };
            let page_bytes = page.encode().unwrap();
            let page_ref = SnapshotObjectRef::from_bytes(
                "fd".into(),
                SnapshotObjectKind::FrameDirectory,
                &page_bytes,
            )
            .unwrap();
            client.put_object(&page_ref.key, &page_bytes).await.unwrap();
            roots[SnapshotRootKind::Containers as usize] = index(
                &client,
                SnapshotObjectKind::ContainerIndex,
                "containers",
                vec![leaf(
                    0u32.to_be_bytes().to_vec(),
                    container.encode_value().unwrap(),
                )],
            )
            .await;
            roots[SnapshotRootKind::Frames as usize] = index(
                &client,
                SnapshotObjectKind::FrameIndex,
                "frames",
                (0u32..2)
                    .map(|ordinal| {
                        let mut key = 0u32.to_be_bytes().to_vec();
                        key.extend(ordinal.to_be_bytes());
                        leaf(key, page_ref.encode_value().unwrap())
                    })
                    .collect(),
            )
            .await;
            let manifest = SnapshotManifest {
                snapshot_id: [1; 32],
                root_dir_key: [2; 32],
                root_inode: 1,
                group_dentry_count: 0,
                profile: AccessProfile::RandomSmallFile,
                size_classes: SizeClassTable::default(),
                build: Default::default(),
                roots: roots.try_into().unwrap(),
                source: None,
            };
            let bytes = manifest.encode().unwrap();
            let reference = SnapshotObjectRef::from_bytes(
                "manifest".into(),
                SnapshotObjectKind::Manifest,
                &bytes,
            )
            .unwrap();
            let snapshot = AuthenticatedSnapshot::decode(&reference, &bytes).unwrap();
            let mut limits = SnapshotBudgetLimits::default();
            limits.bytes[SnapshotBudgetPool::Raw as usize] = 64;
            limits.bytes[SnapshotBudgetPool::Plans as usize] = 4 << 20;
            let budget = SnapshotMountBudget::new(limits).unwrap();
            let reader = SnapshotIndexReader::with_budget(client.clone(), 0, budget.clone());
            let length = sequence.len() * 32;
            let entry = GroupMetaEntry {
                name: b"file".to_vec(),
                inode: 2,
                kind: 1,
                mode: 0o100644,
                uid: 0,
                gid: 0,
                rdev: 0,
                nlink: 1,
                atime_ns: 0,
                mtime_ns: 0,
                ctime_ns: 0,
                size: length as u64,
                flags: 0,
                inline_data: Arc::from([]),
                extents: sequence
                    .iter()
                    .enumerate()
                    .map(|(i, &(ordinal, raw_offset))| GroupMetaExtent {
                        file_offset: (i * 32) as u64,
                        logical_len: 32,
                        frame_ordinal: ordinal,
                        raw_offset,
                        raw_len: 64,
                    })
                    .collect(),
            };
            let repeated_entry = entry.clone();
            let tag = ReadContext {
                engine,
                phase: Phase::Runtime,
                origin: Origin::Demand,
                class: ReadClass::PackedPayload,
            };
            let guard = observer.start(
                Ledger::LogicalOperation,
                ReadContext {
                    class: ReadClass::LogicalRead,
                    ..tag
                },
                length as u64,
            );
            let prepared = snapshot
                .prepare_placement_read_observed(
                    &client,
                    &reader,
                    SnapshotReadRange { offset: 0, length },
                    32 << 20,
                    guard.delivery_token(),
                    SnapshotReadPlacementInput {
                        group_id: 7,
                        container_ordinal: 0,
                        entry,
                        placement: None,
                        owner: Arc::new(()),
                    },
                )
                .await
                .unwrap();
            assert!(
                budget.state().used[SnapshotBudgetPool::Plans as usize]
                    < budget.capacity(SnapshotBudgetPool::Plans)
            );
            assert!(
                !observer
                    .snapshot()
                    .rows
                    .contains_key(&(Ledger::BackendBody, tag)),
                "prepare authenticates routes without fetching future payloads"
            );
            let mut output = vec![0u8; length];
            tokio::time::timeout(
                std::time::Duration::from_secs(2),
                execute_unified_into(prepared.fetcher.as_ref(), 0, &prepared.plan, &mut output),
            )
            .await
            .unwrap()
            .unwrap();
            let expected = sequence
                .iter()
                .flat_map(|&(ordinal, _)| vec![ordinal as u8 + 1; 32])
                .collect::<Vec<_>>();
            assert_eq!(output, expected);
            guard.deliver(length as u64);
            drop(prepared);
            let state = observer.snapshot();
            assert_eq!(
                state.rows[&(Ledger::BackendBody, tag)].success,
                sequence.len() as u64
            );
            let raw = state.raw[&tag];
            assert_eq!(
                (
                    raw.decoded_raw,
                    raw.requested_union,
                    raw.copied_union,
                    raw.delivered_union
                ),
                (
                    sequence.len() as u64 * 64,
                    length as u64,
                    length as u64,
                    length as u64
                )
            );
            assert!(budget.state().peak[SnapshotBudgetPool::Raw as usize] <= 64);
            // Both operations use the complete shared placement executor.
            // The old per-read executor either opens two bodies or rejects
            // the second reader under this one-frame mount capacity.
            let first_guard = observer.start(
                Ledger::LogicalOperation,
                ReadContext {
                    class: ReadClass::LogicalRead,
                    ..tag
                },
                32,
            );
            let second_guard = observer.start(
                Ledger::LogicalOperation,
                ReadContext {
                    class: ReadClass::LogicalRead,
                    ..tag
                },
                32,
            );
            let first = snapshot
                .prepare_placement_read_observed(
                    &client,
                    &reader,
                    SnapshotReadRange {
                        offset: 0,
                        length: 32,
                    },
                    32 << 20,
                    first_guard.delivery_token(),
                    SnapshotReadPlacementInput {
                        group_id: 7,
                        container_ordinal: 0,
                        entry: repeated_entry.clone(),
                        placement: None,
                        owner: Arc::new(()),
                    },
                )
                .await
                .unwrap();
            let second = snapshot
                .prepare_placement_read_observed(
                    &client,
                    &reader,
                    SnapshotReadRange {
                        offset: 0,
                        length: 32,
                    },
                    32 << 20,
                    second_guard.delivery_token(),
                    SnapshotReadPlacementInput {
                        group_id: 7,
                        container_ordinal: 0,
                        entry: repeated_entry,
                        placement: None,
                        owner: Arc::new(()),
                    },
                )
                .await
                .unwrap();
            let before = observer.snapshot();
            let mut first_output = [0u8; 32];
            let mut second_output = [0u8; 32];
            let (first_result, second_result) =
                tokio::time::timeout(std::time::Duration::from_secs(2), async {
                    tokio::join!(
                        execute_unified_into(
                            first.fetcher.as_ref(),
                            0,
                            &first.plan,
                            &mut first_output
                        ),
                        execute_unified_into(
                            second.fetcher.as_ref(),
                            0,
                            &second.plan,
                            &mut second_output
                        )
                    )
                })
                .await
                .unwrap();
            first_result.unwrap();
            second_result.unwrap();
            assert_eq!(first_output, second_output);
            first_guard.deliver(32);
            second_guard.deliver(32);
            drop(first);
            drop(second);
            let after = observer.snapshot();
            assert_eq!(
                after.rows[&(Ledger::BackendBody, tag)].success
                    - before.rows[&(Ledger::BackendBody, tag)].success,
                1
            );
            assert_eq!(
                after.raw[&tag].decoded_raw - before.raw[&tag].decoded_raw,
                64
            );
            assert_eq!(
                after.raw[&tag].delivered_union - before.raw[&tag].delivered_union,
                32
            );
            reader.close().await;
            drop(reader);
            assert_eq!(budget.state().used, [0; 8]);
        }
    }
}
