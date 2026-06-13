use brewfs::chunk::SliceDesc;
use brewfs::meta::store::{MetaError, RetryReason};
use brewfs::{
    CacheConfig, ClientOptions, CompactConfig, Config, DatabaseConfig, DatabaseType, MetaStore,
    RedisMetaStore,
};
use serial_test::serial;

fn test_config() -> Config {
    Config {
        database: DatabaseConfig {
            db_config: DatabaseType::Redis {
                url: "redis://127.0.0.1:6379/0".to_string(),
            },
        },
        cache: CacheConfig::default(),
        client: ClientOptions::default(),
        compact: CompactConfig::default(),
    }
}

async fn cleanup_test_data() -> Result<(), MetaError> {
    let client = redis::Client::open("redis://127.0.0.1:6379/0")
        .map_err(|e| MetaError::Config(format!("Failed to create Redis client: {e}")))?;
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .map_err(|e| MetaError::Config(format!("Failed to connect to Redis: {e}")))?;

    redis::cmd("FLUSHDB")
        .query_async::<()>(&mut conn)
        .await
        .map_err(|e| MetaError::Internal(format!("Failed to flush Redis DB: {e}")))?;

    Ok(())
}

async fn new_test_store() -> RedisMetaStore {
    cleanup_test_data().await.unwrap();
    RedisMetaStore::from_config(test_config())
        .await
        .expect("Failed to create Redis test store")
}

#[serial]
#[tokio::test]
#[ignore]
async fn redis_versioned_compaction_rejects_changed_slice_set() {
    let store = new_test_store().await;
    let chunk_id = 1002u64;

    let old_slice = SliceDesc {
        slice_id: 601,
        chunk_id,
        offset: 0,
        length: 1024,
    };
    store.append_slice(chunk_id, old_slice).await.unwrap();

    let expected_slices = store.get_slices(chunk_id).await.unwrap();

    let concurrent_slice = SliceDesc {
        slice_id: 602,
        chunk_id,
        offset: 1024,
        length: 1024,
    };
    store
        .append_slice(chunk_id, concurrent_slice)
        .await
        .unwrap();

    let new_slice = SliceDesc {
        slice_id: 603,
        chunk_id,
        offset: 0,
        length: 2048,
    };
    let delayed_data = SliceDesc::encode_delayed_data(&expected_slices, &[old_slice.slice_id]);

    let err = store
        .replace_slices_for_compact_with_version(
            chunk_id,
            &[new_slice],
            &delayed_data,
            &expected_slices,
        )
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        MetaError::ContinueRetry(RetryReason::CompactConflict)
    ));

    let slices_after = store.get_slices(chunk_id).await.unwrap();
    assert_eq!(slices_after.len(), 2);
    assert!(
        slices_after
            .iter()
            .any(|s| s.slice_id == old_slice.slice_id)
    );
    assert!(
        slices_after
            .iter()
            .any(|s| s.slice_id == concurrent_slice.slice_id)
    );
}
