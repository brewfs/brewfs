//! Cache contracts through actual entity reads and durable local object I/O.
use super::*;
use crate::workspace::entity::local_test_objects::DurableLocalObjects;

#[tokio::test]
async fn payload_cache_hot_reads_observe_new_writes_truncation_and_fencing() {
    let directory = tempfile::tempdir().unwrap();
    let objects = Arc::new(Objects::new(DurableLocalObjects::new(directory.path())));
    let fixture = attach_objects(
        Arc::new(MemoryEntityBackend::new(1000)),
        EntityKeys::new(Uuid::new_v4(), Uuid::new_v4()),
        1_000_000,
        objects.clone(),
        4096,
    )
    .await;
    let cached = EntityData::new_with_payload_cache(
        fixture.namespace.clone(),
        objects.clone(),
        4096,
        None,
        PayloadCache::new(32 << 10),
    )
    .await
    .unwrap();
    let handle = new_file(&fixture, b"cached").await;
    fixture
        .data
        .write(handle.clone(), 0, Bytes::from(vec![7; 4096]), false)
        .await
        .unwrap();
    fixture.data.fsync(handle.clone()).await.unwrap();
    assert_eq!(objects.puts.load(Ordering::SeqCst), 1);
    let first = cached.read(handle.clone(), 0, 4096).await.unwrap();
    assert_eq!(first, vec![7; 4096]);
    assert_eq!(objects.gets.load(Ordering::SeqCst), 1);
    assert_eq!(cached.read(handle.clone(), 0, 4096).await.unwrap(), first);
    assert_eq!(objects.gets.load(Ordering::SeqCst), 1);

    // A separate EntityData instance publishes a fresh immutable object. The
    // warm reader must follow current metadata rather than a cached inode view.
    fixture
        .data
        .write(handle.clone(), 0, Bytes::from(vec![9; 4096]), false)
        .await
        .unwrap();
    fixture.data.fsync(handle.clone()).await.unwrap();
    assert_eq!(objects.puts.load(Ordering::SeqCst), 2);
    assert_eq!(
        cached.read(handle.clone(), 0, 4096).await.unwrap(),
        vec![9; 4096]
    );
    assert_eq!(objects.gets.load(Ordering::SeqCst), 2);
    assert_eq!(
        cached.read(handle.clone(), 0, 4096).await.unwrap(),
        vec![9; 4096]
    );
    assert_eq!(objects.gets.load(Ordering::SeqCst), 2);
    fixture.data.truncate(handle.clone(), 16).await.unwrap();
    fixture.data.truncate(handle.clone(), 4096).await.unwrap();
    let mut expected = vec![0; 4096];
    expected[..16].fill(9);
    assert_eq!(
        cached.read(handle.clone(), 0, 4096).await.unwrap(),
        expected
    );
    assert_eq!(objects.gets.load(Ordering::SeqCst), 2);
    fixture.backend.set_time(2_000_000).unwrap();
    assert!(matches!(
        cached.read(handle, 0, 16).await,
        Err(EntityError::Fenced)
    ));
}

#[tokio::test]
async fn payload_cache_disabled_retains_no_data_or_negative_authentication_results() {
    let expected = b"authenticated payload";
    for bytes in [
        None,
        Some(Vec::new()),
        Some(expected[..expected.len() - 1].to_vec()),
        Some(vec![b'x'; expected.len()]),
        Some([expected.as_slice(), b"extra"].concat()),
        Some(expected.to_vec()),
    ] {
        let valid = bytes.as_deref() == Some(expected.as_slice());
        let objects = Arc::new(CountingRangeObjects {
            bytes,
            heads: AtomicUsize::new(0),
            gets: AtomicUsize::new(0),
        });
        let fixture = attach_objects(
            Arc::new(MemoryEntityBackend::new(1000)),
            EntityKeys::new(Uuid::new_v4(), Uuid::new_v4()),
            1_000_000,
            objects.clone(),
            4096,
        )
        .await;
        let cache = PayloadCache::new(if valid { 0 } else { 32 << 10 });
        let data = EntityData::new_with_payload_cache(
            fixture.namespace.clone(),
            objects.clone(),
            4096,
            None,
            cache.clone(),
        )
        .await
        .unwrap();
        let descriptor = ObjectDescriptor {
            namespace_id: fixture.keys.namespace_id(),
            slice_id: 1,
            size: expected.len() as u64,
            sha256: Sha256::digest(expected).into(),
        };
        for _ in 0..2 {
            let result = data.read_object(&descriptor).await;
            if valid {
                assert_eq!(result.unwrap(), expected);
            } else {
                assert!(matches!(result, Err(EntityError::Corrupt(_))));
            }
        }
        assert_eq!(objects.gets.load(Ordering::SeqCst), 2);
        assert_eq!(objects.heads.load(Ordering::SeqCst), 0);
        assert_eq!(cache.stats().entries, 0);
    }
}
