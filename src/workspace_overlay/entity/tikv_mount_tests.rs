//! Real TiKV upper and catalog, shared native/packed adapters, and real S3.
use std::sync::Arc;

use super::{
    EntityError,
    experiment::{EntityExperiment, FreshMountOptions},
    model::EntityKind,
    namespace::{CreateRequest, Credentials},
    resolver::ImmutableLowerData,
    test_objects,
    tikv::TiKvEntityBackend,
    writeback::WritebackConfig,
};
use crate::cadapter::{client::ObjectClient, s3::S3Backend};
use crate::workspace_overlay::packed_v3::{
    AccessProfile, GroupMeta, GroupMetaEntry, GroupMetaExtent, PackedCodec, PackedFrameInput,
    PackedGroupInput, SizeClass, SizeClassTable,
    wire005::{V3MountBudget, V3ProducerOptions, V3SnapshotProducer},
};
use crate::workspace_overlay::stores::{kv_store::KvWorkspaceStore, tikv::TiKvWorkspaceBackend};
use uuid::Uuid;

async fn packed_manifest(objects: S3Backend, payload: &[u8]) -> String {
    let spool = tempfile::tempdir().unwrap();
    let group = PackedGroupInput {
        group_id: 1,
        parent_dir_key: [7; 32],
        metadata: GroupMeta::new(vec![GroupMetaEntry {
            name: b"file".to_vec(),
            inode: 400,
            kind: 1,
            mode: 0o100644,
            uid: 0,
            gid: 0,
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
        }])
        .unwrap()
        .encode()
        .unwrap(),
        frame_ordinals: vec![0],
        entry_count: 1,
        file_count: 1,
        layout_profile: AccessProfile::RandomSmallFile,
    };
    let mut producer = V3SnapshotProducer::new(
        ObjectClient::new(objects),
        spool.path(),
        format!("entity-tikv-test/{}", Uuid::new_v4()),
        V3ProducerOptions {
            snapshot_id: [9; 32],
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
                raw: payload.to_vec(),
                size_class: SizeClass::Tiny,
                codec: 0,
                first_file_slot: 0,
                last_file_slot: 0,
            }],
            &[1],
        )
        .await
        .unwrap();
    producer.finish().await.unwrap().key
}

#[tokio::test]
#[ignore = "requires isolated PD/TiKV and RustFS; ENTITY_DATA_TIKV=true tools/testing/run_entity_data_tests.sh"]
async fn tikv_entity_both_lowers_keep_cow_and_writeback_contract() {
    let endpoints = std::env::var("BREWFS_TEST_TIKV_PD_ENDPOINTS")
        .unwrap()
        .split(',')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let source = tempfile::tempdir().unwrap();
    let payload = b"frozen native source";
    std::fs::write(source.path().join("file"), payload).unwrap();
    let objects = test_objects::backend().await;
    let manifest = packed_manifest(objects.clone(), payload).await;
    let credentials = Credentials {
        uid: 0,
        gid: 0,
        groups: vec![],
        umask: 0,
    };
    for packed in [false, true] {
        for asynchronous in [false, true] {
            let volume = Uuid::new_v4();
            let spool = tempfile::tempdir().unwrap();
            let budget = V3MountBudget::defaults();
            let catalog = TiKvWorkspaceBackend::connect_with_budget(
                endpoints.clone(),
                &format!("entity-tikv-{}", Uuid::new_v4()),
                budget.clone(),
            )
            .await
            .unwrap();
            let backend = Arc::new(
                TiKvEntityBackend::connect(endpoints.clone(), volume, None)
                    .await
                    .unwrap(),
            );
            let factory = EntityExperiment::from_backends(
                backend,
                Arc::new(KvWorkspaceStore::new(catalog).with_packed_reader_pin_budget(budget)),
                objects.clone(),
                volume,
            )
            .unwrap();
            let options = FreshMountOptions {
                namespace_id: Some(Uuid::new_v4()),
                lease_ttl_ns: 60_000_000_000,
                chunk_size: 8,
                writeback: asynchronous.then(|| WritebackConfig {
                    directory: spool.path().into(),
                    capacity_bytes: 1 << 20,
                    min_free_bytes: 0,
                }),
            };
            let mount = if packed {
                factory.create_fresh_from_manifest(&manifest, options).await
            } else {
                factory
                    .create_fresh_from_native(Some(source.path()), None, options)
                    .await
            }
            .unwrap();
            let handle = Arc::new(
                mount
                    .namespace()
                    .open(1, b"file", 6, &credentials)
                    .await
                    .unwrap(),
            );
            assert_eq!(
                mount
                    .data()
                    .read(handle.clone(), 0, payload.len())
                    .await
                    .unwrap(),
                payload
            );
            let mut readers = tokio::task::JoinSet::new();
            for _ in 0..8 {
                let mount = mount.clone();
                let handle = handle.clone();
                readers.spawn(async move {
                    for _ in 0..16 {
                        assert_eq!(
                            mount
                                .data()
                                .read(handle.clone(), 0, payload.len())
                                .await
                                .unwrap(),
                            payload
                        );
                    }
                });
            }
            for _ in 0..4 {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                mount.renew(60_000_000_000).await.unwrap();
            }
            while let Some(result) = readers.join_next().await {
                result.unwrap();
            }
            mount
                .data()
                .write(
                    handle.clone(),
                    7,
                    bytes::Bytes::from_static(b"UPPER!"),
                    false,
                )
                .await
                .unwrap();
            mount.data().fsync(handle.clone()).await.unwrap();
            mount.renew(60_000_000_000).await.unwrap();
            assert_eq!(
                mount
                    .data()
                    .read(handle.clone(), 0, payload.len())
                    .await
                    .unwrap(),
                b"frozen UPPER! source"
            );
            assert_eq!(
                mount
                    .namespace()
                    .lower
                    .read_range(handle.ino(), 0, payload.len())
                    .await
                    .unwrap(),
                payload
            );
            drop(handle);
            mount
                .namespace()
                .unlink(1, b"file".to_vec(), false, credentials.clone())
                .await
                .unwrap();
            assert!(matches!(
                mount.namespace().open(1, b"file", 4, &credentials).await,
                Err(EntityError::NotFound)
            ));
            mount.orderly_close().await.unwrap();
            let status = mount.data().writeback_status().await.unwrap();
            assert_eq!((status.pending_objects, status.pending_bytes), (0, 0));
            factory.shutdown().await.unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires isolated PD/TiKV and RustFS"]
async fn tikv_entity_concurrent_small_writes_keep_renewal_and_uploads_live() {
    let endpoints = std::env::var("BREWFS_TEST_TIKV_PD_ENDPOINTS")
        .unwrap()
        .split(',')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let source = tempfile::tempdir().unwrap();
    std::fs::write(source.path().join("file"), b"lower").unwrap();
    let objects = test_objects::backend().await;
    let manifest = packed_manifest(objects.clone(), b"lower").await;
    let credentials = Credentials {
        uid: 0,
        gid: 0,
        groups: vec![],
        umask: 0,
    };
    let request = |parent, name: Vec<u8>, kind| CreateRequest {
        parent,
        name,
        kind,
        mode: 0o777,
        rdev: 0,
        symlink_target: None,
    };
    for packed in [true, false] {
        for asynchronous in [false, true] {
            let volume = Uuid::new_v4();
            let spool = tempfile::tempdir().unwrap();
            let budget = V3MountBudget::defaults();
            let catalog = TiKvWorkspaceBackend::connect_with_budget(
                endpoints.clone(),
                &format!("entity-writes-{}", Uuid::new_v4()),
                budget.clone(),
            )
            .await
            .unwrap();
            let backend = Arc::new(
                TiKvEntityBackend::connect(endpoints.clone(), volume, None)
                    .await
                    .unwrap(),
            );
            let factory = EntityExperiment::from_backends(
                backend,
                Arc::new(KvWorkspaceStore::new(catalog).with_packed_reader_pin_budget(budget)),
                objects.clone(),
                volume,
            )
            .unwrap();
            let options = FreshMountOptions {
                namespace_id: Some(Uuid::new_v4()),
                lease_ttl_ns: 60_000_000_000,
                chunk_size: 4 << 20,
                writeback: asynchronous.then(|| WritebackConfig {
                    directory: spool.path().into(),
                    capacity_bytes: 32 << 20,
                    min_free_bytes: 0,
                }),
            };
            let mount = if packed {
                factory.create_fresh_from_manifest(&manifest, options).await
            } else {
                factory
                    .create_fresh_from_native(Some(source.path()), None, options)
                    .await
            }
            .unwrap();
            let mut directories = Vec::new();
            for directory in 0..64 {
                directories.push(
                    mount
                        .namespace()
                        .create(
                            request(
                                1,
                                format!("d{directory:03}").into_bytes(),
                                EntityKind::Directory,
                            ),
                            credentials.clone(),
                        )
                        .await
                        .unwrap()
                        .ino,
                );
            }
            let directories = Arc::new(directories);
            let barrier = Arc::new(tokio::sync::Barrier::new(9));
            let mut writers = tokio::task::JoinSet::new();
            for worker in 0..8 {
                let mount = mount.clone();
                let directories = directories.clone();
                let credentials = credentials.clone();
                let barrier = barrier.clone();
                writers.spawn(async move {
                    barrier.wait().await;
                    for sequence in 0..125 {
                        let id = worker + sequence * 8;
                        let (_, handle) = mount
                            .namespace()
                            .create_open(
                                request(
                                    directories[id % 64],
                                    format!("f{id:04}").into_bytes(),
                                    EntityKind::File,
                                ),
                                credentials.clone(),
                                6,
                            )
                            .await?;
                        let handle = Arc::new(handle);
                        let mut content = vec![(id % 251) as u8; 16 << 10];
                        content[..8].copy_from_slice(&(id as u64).to_le_bytes());
                        let payload = bytes::Bytes::from(content);
                        mount
                            .data()
                            .write(handle.clone(), 0, payload.clone(), false)
                            .await?;
                        mount.data().fsync(handle.clone()).await?;
                        if mount.data().read(handle.clone(), 0, payload.len()).await? != payload {
                            return Err(EntityError::Corrupt(
                                "concurrent write readback mismatch".into(),
                            ));
                        }
                        mount.data().flush_release(handle).await?;
                    }
                    Ok::<_, EntityError>(())
                });
            }
            barrier.wait().await;
            // Match the CLI heartbeat and the cloud's 1000-file workload.
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut errors = Vec::new();
            let mut renewals = 0;
            while !writers.is_empty() {
                tokio::select! {
                    result = writers.join_next() => match result.unwrap() {
                        Ok(Ok(())) => {},
                        Ok(Err(error)) => errors.push(format!("write: {error:?}")),
                        Err(error) => errors.push(format!("writer task: {error}")),
                    },
                    _ = interval.tick() => {
                        renewals += 1;
                        if let Err(error) = mount.renew(60_000_000_000).await {
                            errors.push(format!("renew: {error:?}"));
                        }
                    },
                }
            }
            // Join all operations and close the clients even on a failure,
            // retaining the first concrete backend errors for diagnosis.
            let drained = mount.orderly_close().await;
            let status = mount.data().writeback_status().await;
            let shutdown = factory.shutdown().await;
            assert!(
                errors.is_empty(),
                "packed={packed} asynchronous={asynchronous}: {errors:?}"
            );
            assert!(renewals > 0);
            drained.unwrap();
            let status = status.unwrap();
            assert_eq!((status.pending_objects, status.pending_bytes), (0, 0));
            shutdown.unwrap();
        }
    }
}
