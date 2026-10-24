use brewfs::chunk::cache::{ChunksCache, ChunksCacheConfig};
use brewfs::chunk::cache_integrity::CacheIntegrityMode;
use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::path::Path;

async fn disk_cache(dir: &Path, mode: CacheIntegrityMode) -> ChunksCache {
    ChunksCache::new_with_config(
        ChunksCacheConfig::with_budgets(0, 1024 * 1024, dir.to_path_buf())
            .with_integrity_mode(mode),
    )
    .await
    .unwrap()
}

fn framing_shaped_payload() -> Bytes {
    // Ordinary user bytes which also form a valid SFC1 frame around sixteen Qs.
    Bytes::from(hex::decode("534643311000000051515151515151515151515151515151b391afc5").unwrap())
}

#[tokio::test]
async fn raw_disk_cache_preserves_framing_shaped_payload_on_full_load() {
    let dir = tempfile::tempdir().unwrap();
    let cache = disk_cache(dir.path(), CacheIntegrityMode::None).await;
    let key = "framing-shaped-user-data".to_string();
    let payload = framing_shaped_payload();
    cache.store_to_disk(&key, payload.clone()).await.unwrap();
    assert_eq!(cache.get(&key).await, Some(payload));
}

#[tokio::test]
async fn raw_disk_cache_preserves_framing_shaped_payload_on_range_load() {
    let dir = tempfile::tempdir().unwrap();
    let cache = disk_cache(dir.path(), CacheIntegrityMode::None).await;
    let key = "framing-shaped-user-data".to_string();
    cache
        .store_to_disk(&key, framing_shaped_payload())
        .await
        .unwrap();
    let mut buf = [0; 4];
    assert_eq!(cache.get_range_into(&key, 0, &mut buf).await, Some(4));
    assert_eq!(&buf, b"SFC1");
}

#[tokio::test]
async fn raw_disk_cache_preserves_invalid_framing_and_unaligned_ranges() {
    let dir = tempfile::tempdir().unwrap();
    let cache = disk_cache(dir.path(), CacheIntegrityMode::None).await;
    let key = "invalid-framing-user-data".to_string();
    let mut payload = framing_shaped_payload().to_vec();
    payload[24] ^= 1;
    cache
        .store_to_disk(&key, Bytes::from(payload.clone()))
        .await
        .unwrap();
    let mut buf = [0; 11];
    assert_eq!(cache.get_range_into(&key, 17, &mut buf).await, Some(11));
    assert_eq!(buf, payload[17..]);
    assert_eq!(cache.get(&key).await.as_deref(), Some(payload.as_slice()));
}

#[tokio::test]
async fn disk_cache_mode_changes_cannot_reinterpret_existing_entries() {
    for (first, second) in [
        (CacheIntegrityMode::None, CacheIntegrityMode::Full),
        (CacheIntegrityMode::Full, CacheIntegrityMode::None),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let key = "mode-transition".to_string();
        let payload = framing_shaped_payload();
        let cache = disk_cache(dir.path(), first).await;
        cache.store_to_disk(&key, payload.clone()).await.unwrap();
        drop(cache);

        let cache = disk_cache(dir.path(), second).await;
        let mut buf = [0; 4];
        assert_eq!(cache.get_range_into(&key, 0, &mut buf).await, None);
        assert_eq!(cache.get(&key).await, None);
        cache.store_to_disk(&key, payload.clone()).await.unwrap();
        drop(cache);

        // Reopening either mode must retain the original user bytes.
        for mode in [first, second] {
            let cache = disk_cache(dir.path(), mode).await;
            assert_eq!(cache.get_range_into(&key, 0, &mut buf).await, Some(4));
            assert_eq!(&buf, b"SFC1");
            assert_eq!(cache.get(&key).await, Some(payload.clone()));
        }
    }
}

#[tokio::test]
async fn disk_cache_ignores_ambiguous_legacy_entries_in_both_modes() {
    let dir = tempfile::tempdir().unwrap();
    let key = "legacy-mode-unknown".to_string();
    let legacy_path = dir.path().join(hex::encode(Sha256::digest(key.as_bytes())));
    tokio::fs::write(&legacy_path, framing_shaped_payload())
        .await
        .unwrap();

    for mode in [CacheIntegrityMode::None, CacheIntegrityMode::Full] {
        let cache = disk_cache(dir.path(), mode).await;
        let mut buf = [0; 4];
        assert_eq!(cache.get_range_into(&key, 0, &mut buf).await, None);
        assert_eq!(cache.get(&key).await, None);
    }
}
