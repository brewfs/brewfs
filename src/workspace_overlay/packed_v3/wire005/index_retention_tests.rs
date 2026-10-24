//! Exercise the existing reader APIs and count actual backend range calls.
//! The backend stores test objects in memory; this is not HTTP/kernel/RSS proof.
use super::*;
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Clone, Default)]
struct CountingBackend {
    objects: Arc<Mutex<BTreeMap<String, Vec<u8>>>>,
    range_calls: Arc<AtomicUsize>,
    released: Arc<AtomicBool>,
    release: Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl ObjectBackend for CountingBackend {
    async fn put_object(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
        self.objects
            .lock()
            .unwrap()
            .insert(key.into(), data.to_vec());
        Ok(())
    }

    async fn get_object(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        Ok(self.objects.lock().unwrap().get(key).cloned())
    }

    async fn get_object_range(
        &self,
        key: &str,
        offset: u64,
        buf: &mut [u8],
    ) -> anyhow::Result<usize> {
        self.range_calls.fetch_add(1, Ordering::SeqCst);
        loop {
            let release = self.release.notified();
            tokio::pin!(release);
            release.as_mut().enable();
            if self.released.load(Ordering::Acquire) {
                break;
            }
            release.await;
        }
        let objects = self.objects.lock().unwrap();
        let Some(bytes) = objects.get(key) else {
            return Ok(0);
        };
        let start = usize::try_from(offset)?.min(bytes.len());
        let count = buf.len().min(bytes.len() - start);
        buf[..count].copy_from_slice(&bytes[start..start + count]);
        Ok(count)
    }

    async fn get_etag(&self, _: &str) -> anyhow::Result<String> {
        anyhow::bail!("etag is outside this reader contract")
    }

    async fn delete_object(&self, key: &str) -> anyhow::Result<()> {
        self.objects.lock().unwrap().remove(key);
        Ok(())
    }
}

impl CountingBackend {
    fn open(&self) {
        self.released.store(true, Ordering::Release);
        self.release.notify_waiters();
    }
}

async fn object(backend: &CountingBackend, name: &str, value: u8) -> (V3ObjectRef, u64) {
    let page = V3IndexPage {
        kind: V3ObjectKind::InodeIndex,
        height: 0,
        records: vec![V3IndexRecord {
            first_key: vec![1],
            last_key: vec![1],
            value: V3IndexValue::Leaf(vec![value; 8192]),
        }],
    };
    let bytes = page.encode().unwrap();
    let reference = V3ObjectRef::from_bytes(name.into(), page.kind, &bytes).unwrap();
    // Derive the actual decoded allocation charge rather than using wire size.
    let charge = u64::from(V3IndexPage::decode(&reference, &bytes).unwrap().weight());
    backend.put_object(name, &bytes).await.unwrap();
    (reference, charge)
}

async fn visible_page_owner_charge(
    reader: &V3IndexReader<CountingBackend>,
    references: &[V3ObjectRef],
) -> u64 {
    let mut charge = 0;
    for reference in references {
        let key = (reference.digest, reference.kind, reference.object_len);
        if let Some(page) = reader.pages.get(&key).await {
            charge += u64::from(page.weight());
        }
    }
    // This inspects actual page owners visible through the cache. Moka's
    // approximate weighted_size and process RSS are deliberately not used.
    charge
}

async fn group_tree(backend: &CountingBackend, entries_per_group: u32) -> V3ObjectRef {
    let parent = [1; 32];
    let leaf = V3IndexPage {
        kind: V3ObjectKind::GroupIndex,
        height: 0,
        records: (0..1024)
            .map(|ordinal| {
                let name = format!("g{ordinal:04}").into_bytes();
                let mut key = parent.to_vec();
                key.extend_from_slice(&name);
                let mut last_name = name.clone();
                if entries_per_group > 1 {
                    last_name.push(b'z');
                }
                let mut last_key = parent.to_vec();
                last_key.extend_from_slice(&last_name);
                let group = super::super::V3GroupRef {
                    group_id: ordinal,
                    container_ordinal: 0,
                    parent_dir_key: parent,
                    first_name: name,
                    last_name,
                    meta_offset: V3_HEADER_LEN as u64,
                    meta_stored_len: 1,
                    meta_raw_len: 1,
                    meta_codec: crate::workspace_overlay::packed_v3::PackedCodec::Raw,
                    meta_digest: [1; 32],
                    entry_count: entries_per_group,
                    first_frame: 0,
                    frame_count: 1,
                };
                V3IndexRecord {
                    first_key: key,
                    last_key,
                    value: V3IndexValue::Leaf(group.encode_value().unwrap()),
                }
            })
            .collect(),
    };
    let bytes = leaf.encode().unwrap();
    let reference = V3ObjectRef::from_bytes("groups/leaf".into(), leaf.kind, &bytes).unwrap();
    backend.put_object(&reference.key, &bytes).await.unwrap();
    let root = V3IndexPage {
        kind: leaf.kind,
        height: 1,
        records: vec![V3IndexRecord {
            first_key: leaf.records.first().unwrap().first_key.clone(),
            last_key: leaf.records.last().unwrap().last_key.clone(),
            value: V3IndexValue::Child {
                reference,
                subtree_weight: 1024 * u64::from(entries_per_group),
            },
        }],
    };
    let bytes = root.encode().unwrap();
    let reference = V3ObjectRef::from_bytes("groups/root".into(), root.kind, &bytes).unwrap();
    backend.put_object(&reference.key, &bytes).await.unwrap();
    reference
}

#[tokio::test]
async fn cached_group_descent_does_not_decode_unselected_records() {
    GROUP_WEIGHT_DECODES
        .scope(std::cell::Cell::new(0), async {
            let backend = CountingBackend::default();
            backend.open();
            let root = group_tree(&backend, 1).await;
            let reader = V3IndexReader::new(ObjectClient::new(backend.clone()), 4 << 20);
            let mut key = vec![1; 32];
            key.extend_from_slice(b"g0500");
            let selected = reader.lookup(&root, &key).await.unwrap().unwrap();
            assert_eq!(backend.range_calls.load(Ordering::SeqCst), 2);
            GROUP_WEIGHT_DECODES.with(|count| count.set(0));
            for _ in 0..100 {
                let found = reader.lookup(&root, &key).await.unwrap().unwrap();
                assert_eq!(found, selected);
            }
            let decodes = GROUP_WEIGHT_DECODES.with(|count| count.get());
            let reads = backend.range_calls.load(Ordering::SeqCst) - 2;
            println!(
                "100 cached descents: group weight decodes={decodes}, backend range reads={reads}"
            );
            assert_eq!(reads, 0, "the measured descents must all hit the cache");
            assert_eq!(
                decodes, 0,
                "cached descents must reuse the validated weight"
            );
        })
        .await;
}

#[tokio::test]
async fn cached_group_child_still_checks_every_parent_claim() {
    let backend = CountingBackend::default();
    backend.open();
    let root = group_tree(&backend, 1).await;
    let reader = V3IndexReader::new(ObjectClient::new(backend.clone()), 4 << 20);
    let mut key = vec![1; 32];
    key.extend_from_slice(b"g0500");
    reader.lookup(&root, &key).await.unwrap().unwrap();
    let bytes = backend.get_object(&root.key).await.unwrap().unwrap();
    let parent = V3IndexPage::decode(&root, &bytes).unwrap();
    let record = &parent.records[0];
    let V3IndexValue::Child { reference, .. } = &record.value else {
        unreachable!()
    };
    let expected = ChildExpectation {
        kind: root.kind,
        height: 0,
        first: record.first_key.clone(),
        last: record.last_key.clone(),
        weight: 1024,
    };
    for field in 0..5 {
        let mut wrong = expected.clone();
        match field {
            0 => wrong.kind = V3ObjectKind::InodeIndex,
            1 => wrong.height = 1,
            2 => wrong.first.push(0),
            3 => wrong.last.push(0),
            4 => wrong.weight += 1,
            _ => unreachable!(),
        }
        assert!(matches!(
            reader.load_expected(reference, Some(wrong)).await,
            Err(PackedWireError::Invalid(_))
        ));
    }
    reader
        .load_expected(reference, Some(expected))
        .await
        .unwrap();
    assert_eq!(backend.range_calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn cached_group_weight_tracks_new_digest_and_cache_invalidation() {
    let backend = CountingBackend::default();
    backend.open();
    let old_root = group_tree(&backend, 1).await;
    let reader = V3IndexReader::new(ObjectClient::new(backend.clone()), 4 << 20);
    let mut key = vec![1; 32];
    key.extend_from_slice(b"g0500");
    reader.lookup(&old_root, &key).await.unwrap().unwrap();
    assert_eq!(reader.total_weight(&old_root).await.unwrap(), 1024);

    // Even reuse of an object key cannot reuse an old summary after a new
    // authenticated root is published: cache identity includes its digest.
    let new_root = group_tree(&backend, 2).await;
    assert_eq!(new_root.key, old_root.key);
    assert_ne!(new_root.digest, old_root.digest);
    for expected_reads in [4, 6] {
        let value = reader.lookup(&new_root, &key).await.unwrap().unwrap();
        let group = super::super::V3GroupRef::decode_value(&value).unwrap();
        assert_eq!(group.entry_count, 2);
        assert_eq!(reader.total_weight(&new_root).await.unwrap(), 2048);
        assert_eq!(backend.range_calls.load(Ordering::SeqCst), expected_reads);
        reader.pages.invalidate_all();
        reader.pages.run_pending_tasks().await;
    }
}

#[tokio::test]
async fn existing_reader_never_publishes_an_oversized_page_to_cache() {
    let backend = CountingBackend::default();
    backend.open();
    let (reference, charge) = object(&backend, "oversized-visible", 9).await;
    let cap = 4096;
    assert!(charge > cap);
    let reader = V3IndexReader::new(ObjectClient::new(backend.clone()), cap);
    assert_eq!(
        reader
            .lookup(&reference, &[1])
            .await
            .unwrap()
            .as_ref()
            .map(|value| value.as_ref()),
        Some(&[9; 8192][..])
    );
    assert_eq!(backend.range_calls.load(Ordering::SeqCst), 1);
    assert!(
        visible_page_owner_charge(&reader, &[reference]).await <= cap,
        "an oversized page was visible before asynchronous maintenance"
    );
}

#[tokio::test]
async fn existing_reader_refetches_oversized_page_without_maintenance_or_sleep() {
    let backend = CountingBackend::default();
    backend.open();
    let (reference, charge) = object(&backend, "oversized-refetch", 10).await;
    let reader = V3IndexReader::new(ObjectClient::new(backend.clone()), charge - 1);
    for expected in 1..=2 {
        assert_eq!(
            reader
                .lookup(&reference, &[1])
                .await
                .unwrap()
                .as_ref()
                .map(|value| value.as_ref()),
            Some(&[10; 8192][..])
        );
        assert_eq!(
            backend.range_calls.load(Ordering::SeqCst),
            expected,
            "separate calls reused an over-capacity result as retained cache"
        );
    }
}

#[tokio::test]
async fn existing_reader_cache_owner_sum_stays_bounded_before_maintenance() {
    let backend = CountingBackend::default();
    backend.open();
    let mut references = Vec::new();
    let mut charge = 0;
    for i in 1..=4 {
        let (reference, current) = object(&backend, &format!("aggregate-{i}"), i).await;
        assert!(charge == 0 || charge == current);
        charge = current;
        references.push(reference);
    }
    let cap = charge + 64;
    let reader = V3IndexReader::new(ObjectClient::new(backend.clone()), cap);
    for (i, reference) in references.iter().enumerate() {
        assert_eq!(
            reader
                .lookup(reference, &[1])
                .await
                .unwrap()
                .as_ref()
                .map(|value| value.as_ref()),
            Some(&[(i + 1) as u8; 8192][..])
        );
        assert!(
            visible_page_owner_charge(&reader, &references).await <= cap,
            "multiple visible page owners exceeded the configured cache capacity"
        );
    }
    assert_eq!(backend.range_calls.load(Ordering::SeqCst), 4);
}

#[tokio::test]
async fn capacity_pressure_preserves_hot_pages() {
    let backend = CountingBackend::default();
    backend.open();
    let mut references = Vec::new();
    let mut charge = 0;
    for value in 1..=3 {
        let (reference, bytes) = object(&backend, &format!("hot-retention-{value}"), value).await;
        assert!(charge == 0 || charge == bytes);
        charge = bytes;
        references.push(reference);
    }
    let cap = charge * 2;
    let reader = V3IndexReader::new(ObjectClient::new(backend.clone()), cap);
    for reference in &references[..2] {
        reader.lookup(reference, &[1]).await.unwrap();
    }
    reader.pages.run_pending_tasks().await;
    assert_eq!(visible_page_owner_charge(&reader, &references).await, cap);
    for _ in 0..8 {
        reader.lookup(&references[0], &[1]).await.unwrap();
    }
    reader.pages.run_pending_tasks().await;
    assert_eq!(backend.range_calls.load(Ordering::SeqCst), 2);

    let cold = reader.lookup(&references[2], &[1]).await.unwrap().unwrap();
    assert_eq!(cold.as_ref(), &[3; 8192]);
    reader.pages.run_pending_tasks().await;
    assert!(reader.cache_ownership.owned.load(Ordering::Acquire) <= cap);
    assert_eq!(backend.range_calls.load(Ordering::SeqCst), 3);

    let hot = reader.lookup(&references[0], &[1]).await.unwrap().unwrap();
    assert_eq!(hot.as_ref(), &[1; 8192]);
    assert_eq!(
        backend.range_calls.load(Ordering::SeqCst),
        3,
        "a fitting cold page flushed an unrelated hot page"
    );
    // With no retention space, C is returned to its caller but not cached.
    reader.lookup(&references[2], &[1]).await.unwrap();
    assert_eq!(backend.range_calls.load(Ordering::SeqCst), 4);
    reader.lookup(&references[1], &[1]).await.unwrap();
    assert_eq!(backend.range_calls.load(Ordering::SeqCst), 4);
    assert_eq!(visible_page_owner_charge(&reader, &references).await, cap);
    assert_eq!(reader.cache_ownership.peak.load(Ordering::Acquire), cap);
}

#[tokio::test]
async fn existing_reader_oversized_fetch_still_shares_inflight_result() {
    let backend = CountingBackend::default();
    let (reference, charge) = object(&backend, "oversized-shared", 11).await;
    let reader = V3IndexReader::new(ObjectClient::new(backend.clone()), charge - 1);
    let key = [1];
    let mut calls: Vec<_> = (0..8)
        .map(|_| Box::pin(reader.lookup(&reference, &key)))
        .collect();
    // Every future is known to be pending inside the original reader, with
    // the physical leader blocked. No sleep or timing-based participation.
    for call in &mut calls {
        assert!(futures_util::poll!(call.as_mut()).is_pending());
    }
    assert_eq!(backend.range_calls.load(Ordering::SeqCst), 1);
    backend.open();
    for result in futures_util::future::join_all(calls).await {
        assert_eq!(
            result.unwrap().as_ref().map(|value| value.as_ref()),
            Some(&[11; 8192][..])
        );
    }
    assert_eq!(
        backend.range_calls.load(Ordering::SeqCst),
        1,
        "cache bypass lost the original in-flight singleflight"
    );
    assert_eq!(
        reader
            .lookup(&reference, &[1])
            .await
            .unwrap()
            .as_ref()
            .map(|value| value.as_ref()),
        Some(&[11; 8192][..])
    );
    assert_eq!(
        backend.range_calls.load(Ordering::SeqCst),
        2,
        "completed oversized fetch became retained cache"
    );
}

#[tokio::test]
async fn retention_charge_waits_for_last_cache_wrapper_but_not_weighted_cursor_pin() {
    let backend = CountingBackend::default();
    backend.open();
    let (reference, charge) = object(&backend, "cursor-retention", 12).await;
    let budget = V3MountBudget::defaults();
    let reader = V3IndexReader::with_budget(ObjectClient::new(backend), charge, budget.clone());
    let cursor = reader
        .weighted_cursor(&reference, &[1], &[2], 0, 1)
        .await
        .unwrap();
    let key = (reference.digest, reference.kind, reference.object_len);
    let last_cache_wrapper = reader.pages.get(&key).await.unwrap();
    assert_eq!(reader.cache_ownership.owned.load(Ordering::Acquire), charge);
    let metadata_held = budget.state().used[V3BudgetPool::Metadata as usize];
    assert!(metadata_held >= charge);
    reader.pages.invalidate_all();
    reader.pages.run_pending_tasks().await;
    // Even a removed wrapper still owns its charge until actually dropped.
    assert_eq!(reader.cache_ownership.owned.load(Ordering::Acquire), charge);
    drop(last_cache_wrapper);
    for _ in 0..64 {
        reader.pages.run_pending_tasks().await;
        if reader.cache_ownership.owned.load(Ordering::Acquire) == 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(reader.cache_ownership.owned.load(Ordering::Acquire), 0);
    assert_eq!(
        budget.state().used[V3BudgetPool::Metadata as usize],
        metadata_held
    );
    assert_eq!(
        cursor.stack[0].0.records[0].value,
        V3IndexValue::Leaf(vec![12; 8192])
    );
    drop(cursor);
    for _ in 0..64 {
        reader.pages.run_pending_tasks().await;
        if budget.state().used == [0; 8] {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(budget.state().used, [0; 8]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_distinct_fetches_never_over_admit_cache_owners() {
    let backend = CountingBackend::default();
    backend.open();
    let mut references = Vec::new();
    let mut charge = 0;
    for value in 1..=16 {
        let (reference, bytes) =
            object(&backend, &format!("concurrent-owner-{value}"), value).await;
        charge = bytes;
        references.push(reference);
    }
    let cap = charge * 2;
    let budget = V3MountBudget::defaults();
    let reader = Arc::new(V3IndexReader::with_budget(
        ObjectClient::new(backend.clone()),
        cap,
        budget,
    ));
    let barrier = Arc::new(tokio::sync::Barrier::new(references.len() + 1));
    let mut callers = Vec::new();
    for (index, reference) in references.iter().cloned().enumerate() {
        let reader = reader.clone();
        let barrier = barrier.clone();
        callers.push(tokio::spawn(async move {
            barrier.wait().await;
            assert_eq!(
                reader
                    .lookup(&reference, &[1])
                    .await
                    .unwrap()
                    .as_ref()
                    .map(|value| value.as_ref()),
                Some(&[(index + 1) as u8; 8192][..])
            );
        }));
    }
    barrier.wait().await;
    for caller in callers {
        caller.await.unwrap();
    }
    assert_eq!(backend.range_calls.load(Ordering::SeqCst), references.len());
    assert!(reader.cache_ownership.owned.load(Ordering::Acquire) <= cap);
    let peak = reader.cache_ownership.peak.load(Ordering::Acquire);
    assert!(peak >= charge && peak <= cap);
    assert!(visible_page_owner_charge(&reader, &references).await <= cap);
}
