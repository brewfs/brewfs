use brewfs::chunk::{
    SliceDesc,
    store::{BlockStore, InMemoryBlockStore},
};
use brewfs::meta::store::{MetaError, RetryReason};
use brewfs::{Config, DatabaseConfig, DatabaseMetaStore, DatabaseType, MetaStore, RedisMetaStore};

fn config(database: DatabaseType) -> Config {
    Config {
        database: DatabaseConfig {
            db_config: database,
        },
        cache: Default::default(),
        client: Default::default(),
        compact: Default::default(),
    }
}

async fn sqlite() -> (tempfile::TempDir, DatabaseMetaStore) {
    let temp = tempfile::tempdir().unwrap();
    let store = DatabaseMetaStore::from_config(config(DatabaseType::Sqlite {
        url: format!(
            "sqlite://{}?mode=rwc",
            temp.path().join("metadata.db").display()
        ),
    }))
    .await
    .unwrap();
    store.initialize().await.unwrap();
    (temp, store)
}

async fn redis_store() -> RedisMetaStore {
    let url = std::env::var("BREWFS_REDIS_TEST_URL").expect("isolated Redis test URL required");
    let client = redis::Client::open(url.clone()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    redis::cmd("FLUSHDB")
        .query_async::<()>(&mut conn)
        .await
        .unwrap();
    RedisMetaStore::from_config(config(DatabaseType::Redis { url }))
        .await
        .unwrap()
}

async fn claimed_output_cannot_be_published<M: MetaStore>(store: &M, deleted: bool, chunk: u64) {
    let blocks = InMemoryBlockStore::new();
    let old = SliceDesc {
        slice_id: chunk * 10,
        chunk_id: chunk,
        offset: 0,
        length: 3,
    };
    let new = SliceDesc {
        slice_id: old.slice_id + 1,
        ..old
    };
    store.append_slice(chunk, old).await.unwrap();
    blocks
        .write_fresh_range((old.slice_id, 0), 0, b"old")
        .await
        .unwrap();
    store
        .record_uncommitted_slice(new.slice_id, chunk, new.length, "compact_heavy")
        .await
        .unwrap();
    blocks
        .write_fresh_range((new.slice_id, 0), 0, b"new")
        .await
        .unwrap();
    // Advance the cutoff beyond the freshly recorded second on both backends.
    let claimed = store
        .cleanup_orphan_uncommitted_slices(-1, 100)
        .await
        .unwrap();
    assert!(claimed.contains(&(new.slice_id, new.length)));
    if deleted {
        blocks.delete_range((new.slice_id, 0), 1).await.unwrap();
        store
            .delete_uncommitted_slices(&[new.slice_id])
            .await
            .unwrap();
    }
    let delayed = SliceDesc::encode_delayed_data(&[old], &[old.slice_id]);
    let result = store
        .replace_slices_for_compact_with_version(chunk, &[new], &delayed, &[old])
        .await;
    assert!(
        matches!(
            result,
            Err(MetaError::ContinueRetry(RetryReason::CompactConflict))
        ),
        "a GC claim must fence publication, including after the reservation is deleted: {result:?}"
    );
    if !deleted {
        blocks.delete_range((new.slice_id, 0), 1).await.unwrap();
        store
            .delete_uncommitted_slices(&[new.slice_id])
            .await
            .unwrap();
    }
    assert_eq!(store.get_slices(chunk).await.unwrap(), vec![old]);
    let mut actual = [0; 3];
    blocks
        .read_range((old.slice_id, 0), 0, &mut actual)
        .await
        .unwrap();
    assert_eq!(&actual, b"old");
}

#[tokio::test]
async fn sqlite_compaction_rejects_gc_selected_output() {
    let (_temp, store) = sqlite().await;
    claimed_output_cannot_be_published(&store, false, 7001).await;
}

#[tokio::test]
async fn sqlite_compaction_rejects_gc_deleted_output() {
    let (_temp, store) = sqlite().await;
    claimed_output_cannot_be_published(&store, true, 7002).await;
}

#[tokio::test]
#[serial_test::serial]
#[ignore = "requires isolated Redis via BREWFS_REDIS_TEST_URL"]
async fn redis_compaction_rejects_gc_selected_output() {
    claimed_output_cannot_be_published(&redis_store().await, false, 7001).await;
}

#[tokio::test]
#[serial_test::serial]
#[ignore = "requires isolated Redis via BREWFS_REDIS_TEST_URL"]
async fn redis_compaction_rejects_gc_deleted_output() {
    claimed_output_cannot_be_published(&redis_store().await, true, 7002).await;
}

async fn publication_and_gc_are_exclusive<M: MetaStore>(store: &M) {
    for chunk in 8000..8032 {
        let old = SliceDesc {
            slice_id: chunk * 10,
            chunk_id: chunk,
            offset: 0,
            length: 3,
        };
        let new = SliceDesc {
            slice_id: old.slice_id + 1,
            ..old
        };
        store.append_slice(chunk, old).await.unwrap();
        store
            .record_uncommitted_slice(new.slice_id, chunk, new.length, "compact_heavy")
            .await
            .unwrap();
        let delayed = SliceDesc::encode_delayed_data(&[old], &[old.slice_id]);
        let replacements = [new];
        let expected = [old];
        let (published, claimed) = tokio::join!(
            store.replace_slices_for_compact_with_version(
                chunk,
                &replacements,
                &delayed,
                &expected
            ),
            store.cleanup_orphan_uncommitted_slices(-1, 100),
        );
        let claimed = claimed.unwrap();
        match published {
            Ok(()) => {
                assert!(
                    !claimed.iter().any(|(id, _)| *id == new.slice_id),
                    "GC and publisher both won"
                );
                assert_eq!(store.get_slices(chunk).await.unwrap(), vec![new]);
                assert!(
                    !store
                        .cleanup_orphan_uncommitted_slices(-1, 100)
                        .await
                        .unwrap()
                        .iter()
                        .any(|(id, _)| *id == new.slice_id)
                );
            }
            Err(MetaError::ContinueRetry(RetryReason::CompactConflict)) => {
                assert!(claimed.contains(&(new.slice_id, new.length)));
                assert_eq!(store.get_slices(chunk).await.unwrap(), vec![old]);
                store
                    .delete_uncommitted_slices(&[new.slice_id])
                    .await
                    .unwrap();
            }
            Err(error) => panic!("unexpected publication failure: {error}"),
        }
    }
}

#[tokio::test]
async fn sqlite_compaction_publication_and_gc_are_exclusive() {
    let (_temp, store) = sqlite().await;
    publication_and_gc_are_exclusive(&store).await;
}

#[tokio::test]
#[serial_test::serial]
#[ignore = "requires isolated Redis via BREWFS_REDIS_TEST_URL"]
async fn redis_compaction_publication_and_gc_are_exclusive() {
    publication_and_gc_are_exclusive(&redis_store().await).await;
}

#[tokio::test]
#[ignore = "requires isolated TiKV via BREWFS_TIKV_PD_ENDPOINTS"]
async fn tikv_compaction_gc_claim_fences_publication() {
    let Ok(endpoints) = std::env::var("BREWFS_TIKV_PD_ENDPOINTS") else {
        eprintln!("skip: BREWFS_TIKV_PD_ENDPOINTS is unset");
        return;
    };
    let store = brewfs::TiKvMetaStore::from_config(config(DatabaseType::TiKv {
        pd_endpoints: endpoints.split(',').map(str::to_owned).collect(),
        namespace: format!("fix-240-{}", uuid::Uuid::new_v4()),
    }))
    .await
    .unwrap();
    claimed_output_cannot_be_published(&store, false, 7001).await;
    claimed_output_cannot_be_published(&store, true, 7002).await;
    publication_and_gc_are_exclusive(&store).await;
}

#[tokio::test]
#[ignore = "requires isolated etcd via BREWFS_ETCD_TEST_ENDPOINT"]
async fn etcd_compaction_gc_claim_fences_publication() {
    let endpoint = std::env::var("BREWFS_ETCD_TEST_ENDPOINT").expect("isolated etcd required");
    let mut client = etcd_client::Client::connect([endpoint.clone()], None)
        .await
        .unwrap();
    client
        .delete("", Some(etcd_client::DeleteOptions::new().with_prefix()))
        .await
        .unwrap();
    let store = brewfs::EtcdMetaStore::from_config(config(DatabaseType::Etcd {
        urls: vec![endpoint],
    }))
    .await
    .unwrap();
    claimed_output_cannot_be_published(&store, false, 7001).await;
    claimed_output_cannot_be_published(&store, true, 7002).await;
    publication_and_gc_are_exclusive(&store).await;
}
