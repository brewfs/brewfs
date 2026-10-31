use super::*;
use crate::workspace_overlay::entity::local_test_objects::DurableLocalObjects as LocalFsBackend;
use crate::workspace_overlay::entity::{
    data::EntityData,
    keys::EntityKeys,
    lifecycle::Lifecycle,
    memory::MemoryEntityBackend,
    namespace::{Credentials, EntityNamespace},
    writeback::WritebackConfig,
};

#[tokio::test]
#[ignore = "requires isolated Redis and RustFS; run tools/testing/run_entity_data_tests.sh"]
async fn native_entity_mount_bootstrap_redis_rustfs_sync_and_writeback() {
    use crate::workspace_overlay::entity::{
        experiment::{FreshMountOptions, RedisEntityExperiment},
        test_objects,
    };
    let source = tempfile::tempdir().unwrap();
    let spool = tempfile::tempdir().unwrap();
    std::fs::write(source.path().join("file"), b"frozen native source").unwrap();
    let objects = test_objects::backend().await;
    for asynchronous in [false, true] {
        let factory = RedisEntityExperiment::connect(
            &std::env::var("BREWFS_TEST_REDIS_URL").unwrap(),
            &format!("entity-native-test-{}", Uuid::new_v4()),
            Uuid::new_v4(),
            objects.clone(),
        )
        .await
        .unwrap();
        let mount = factory
            .create_fresh_from_native(
                Some(source.path()),
                None,
                FreshMountOptions {
                    namespace_id: Some(Uuid::new_v4()),
                    lease_ttl_ns: 60_000_000_000,
                    chunk_size: 8,
                    writeback: asynchronous.then(|| WritebackConfig {
                        directory: spool.path().into(),
                        capacity_bytes: 1 << 20,
                        min_free_bytes: 0,
                    }),
                },
            )
            .await
            .unwrap();
        let handle = Arc::new(
            mount
                .namespace()
                .open(
                    1,
                    b"file",
                    6,
                    &Credentials {
                        uid: 0,
                        gid: 0,
                        groups: vec![],
                        umask: 0,
                    },
                )
                .await
                .unwrap(),
        );
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
        assert_eq!(
            mount.data().read(handle.clone(), 0, 20).await.unwrap(),
            b"frozen UPPER! source"
        );
        mount.data().fsync(handle.clone()).await.unwrap();
        drop(handle);
        mount.orderly_close().await.unwrap();
        assert_eq!(
            mount
                .data()
                .writeback_status()
                .await
                .unwrap()
                .pending_objects,
            0
        );
    }
}

#[tokio::test]
async fn native_lower_captures_hardlinks_symlinks_and_copy_on_write() {
    let source = tempfile::tempdir().unwrap();
    let remote = tempfile::tempdir().unwrap();
    let spool = tempfile::tempdir().unwrap();
    std::fs::write(source.path().join("file"), b"native immutable bytes").unwrap();
    std::fs::hard_link(source.path().join("file"), source.path().join("alias")).unwrap();
    std::os::unix::fs::symlink("file", source.path().join("link")).unwrap();
    let backend = Arc::new(MemoryEntityBackend::new(1000));
    let objects = Arc::new(LocalFsBackend::new(remote.path()));
    let volume = Uuid::new_v4();
    let lower = Arc::new(
        NativeLower::capture(backend.clone(), objects.clone(), volume, source.path(), 8)
            .await
            .unwrap(),
    );
    let entry = lower.dentry(1, b"file").await.unwrap().unwrap();
    assert_eq!(
        lower.dentry(1, b"alias").await.unwrap(),
        Some(entry.clone())
    );
    let EntityDentry::Present { ino, .. } = entry else {
        panic!("native dentry")
    };
    assert_eq!(lower.inode(ino).await.unwrap().unwrap().nlink, 2);
    assert_eq!(lower.read_range(ino, 3, 12).await.unwrap(), b"ive immutabl");
    // Captured data is independent of later source mutations.
    std::fs::write(source.path().join("file"), b"changed source").unwrap();
    assert_eq!(lower.read_range(ino, 0, 6).await.unwrap(), b"native");
    for asynchronous in [false, true] {
        let keys = EntityKeys::new(volume, Uuid::new_v4());
        let lifecycle = Lifecycle::new(backend.clone(), keys.clone());
        lifecycle.provision(lower.binding()).await.unwrap();
        let session = lifecycle.acquire(Uuid::new_v4(), 1_000_000).await.unwrap();
        let namespace = EntityNamespace::new(backend.clone(), lower.clone(), keys, session);
        let data = EntityData::new_with_writeback(
            namespace.clone(),
            objects.clone(),
            8,
            asynchronous.then(|| WritebackConfig {
                directory: spool.path().into(),
                capacity_bytes: 1 << 20,
                min_free_bytes: 0,
            }),
        )
        .await
        .unwrap();
        let handle = Arc::new(
            namespace
                .open(
                    1,
                    b"file",
                    6,
                    &Credentials {
                        uid: 0,
                        gid: 0,
                        groups: vec![],
                        umask: 0,
                    },
                )
                .await
                .unwrap(),
        );
        data.write(
            handle.clone(),
            7,
            bytes::Bytes::from_static(b"UPPER"),
            false,
        )
        .await
        .unwrap();
        assert_eq!(
            data.read(handle.clone(), 0, 16).await.unwrap(),
            b"native UPPERable"
        );
        data.truncate(handle.clone(), 9).await.unwrap();
        assert_eq!(
            data.read(handle.clone(), 0, 30).await.unwrap(),
            b"native UP"
        );
        drop(handle);
        data.drain_remote(std::time::Duration::from_secs(5))
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn native_lower_missing_metadata_and_corrupt_objects_are_errors() {
    let source = tempfile::tempdir().unwrap();
    let remote = tempfile::tempdir().unwrap();
    std::fs::write(source.path().join("file"), b"base").unwrap();
    let backend = Arc::new(MemoryEntityBackend::new(1000));
    let objects = Arc::new(LocalFsBackend::new(remote.path()));
    let volume = Uuid::new_v4();
    let lower = NativeLower::capture(backend.clone(), objects.clone(), volume, source.path(), 8)
        .await
        .unwrap();
    let EntityDentry::Present { ino, .. } = lower.dentry(1, b"file").await.unwrap().unwrap() else {
        panic!("dentry")
    };
    let chunk: NativeObject = lower.row(&format!("chunk/{ino}/0")).await.unwrap().unwrap();
    objects
        .put_object(&object_key(volume, &chunk.sha256), b"bad!")
        .await
        .unwrap();
    assert!(matches!(
        lower.read_range(ino, 0, 4).await,
        Err(EntityError::Corrupt(_))
    ));
    let key = row_key(
        volume,
        lower.manifest.rows.get(&format!("inode/{ino}")).unwrap(),
    );
    let old = backend.read(std::slice::from_ref(&key)).await.unwrap().rows[0].clone();
    let authority = TransactionAuthority::from_validated(
        vec![RowCheck {
            key: key.clone(),
            expected: old,
        }],
        None,
    )
    .unwrap();
    backend
        .commit(
            &authority,
            &EntityPlan {
                checks: vec![],
                writes: vec![RowWrite::Delete { key }],
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        lower.inode(ino).await,
        Err(EntityError::Corrupt(_))
    ));
}
