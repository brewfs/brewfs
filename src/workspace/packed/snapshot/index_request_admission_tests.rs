//! Existing lookup API admission tests, independent of any new owned-result API.
//! Runtime red/green must be run by root; static preparation is not a pass.
use super::*;
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Clone, Default)]
struct Backend {
    objects: Arc<Mutex<BTreeMap<String, Vec<u8>>>>,
    calls: Arc<AtomicUsize>,
    released: Arc<AtomicBool>,
    release: Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl ObjectBackend for Backend {
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
        self.calls.fetch_add(1, Ordering::SeqCst);
        loop {
            let changed = self.release.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.released.load(Ordering::Acquire) {
                break;
            }
            changed.await;
        }
        let objects = self.objects.lock().unwrap();
        let Some(data) = objects.get(key) else {
            return Ok(0);
        };
        let offset = usize::try_from(offset)?.min(data.len());
        let count = buf.len().min(data.len() - offset);
        buf[..count].copy_from_slice(&data[offset..offset + count]);
        Ok(count)
    }

    async fn get_etag(&self, _: &str) -> anyhow::Result<String> {
        anyhow::bail!("not used by authenticated index lookup")
    }

    async fn delete_object(&self, key: &str) -> anyhow::Result<()> {
        self.objects.lock().unwrap().remove(key);
        Ok(())
    }
}

impl Backend {
    fn open(&self) {
        self.released.store(true, Ordering::Release);
        self.release.notify_waiters();
    }
}

async fn fixture(
    open: bool,
) -> (
    Backend,
    SnapshotObjectRef,
    Arc<SnapshotMountBudget>,
    SnapshotIndexReader<Backend>,
) {
    let backend = Backend::default();
    if open {
        backend.open();
    }
    let page = SnapshotIndexPage {
        kind: SnapshotObjectKind::InodeIndex,
        height: 0,
        records: vec![SnapshotIndexRecord {
            first_key: vec![1],
            last_key: vec![1],
            value: SnapshotIndexValue::Leaf(vec![7; 128]),
        }],
    }
    .encode()
    .unwrap();
    let reference = SnapshotObjectRef::from_bytes(
        "request-admission/page".into(),
        SnapshotObjectKind::InodeIndex,
        &page,
    )
    .unwrap();
    backend.put_object(&reference.key, &page).await.unwrap();
    let budget = SnapshotMountBudget::defaults();
    let reader = SnapshotIndexReader::with_budget(
        ObjectClient::new(backend.clone()),
        1 << 20,
        budget.clone(),
    );
    (backend, reference, budget, reader)
}

async fn prime(
    backend: &Backend,
    reference: &SnapshotObjectRef,
    reader: &SnapshotIndexReader<Backend>,
) {
    assert_eq!(
        reader
            .lookup(reference, &[1])
            .await
            .unwrap()
            .as_ref()
            .map(|value| value.as_ref()),
        Some(&[7; 128][..])
    );
    assert_eq!(
        reader
            .lookup(reference, &[1])
            .await
            .unwrap()
            .as_ref()
            .map(|value| value.as_ref()),
        Some(&[7; 128][..])
    );
    assert_eq!(
        backend.calls.load(Ordering::SeqCst),
        1,
        "warm precondition not established"
    );
}

#[tokio::test]
async fn existing_warm_lookup_rejects_closed_mount_before_cache_hit() {
    let (backend, reference, budget, reader) = fixture(true).await;
    prime(&backend, &reference, &reader).await;
    budget.close();
    let result = reader.lookup(&reference, &[1]).await;
    assert!(
        matches!(result, Err(PackedWireError::LimitExceeded(_))),
        "warm cache escaped closed mount admission: {result:?}"
    );
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn existing_warm_lookup_rejects_exhausted_control_and_recovers() {
    let (backend, reference, budget, reader) = fixture(true).await;
    prime(&backend, &reference, &reader).await;
    let owner = budget
        .admit(&[(
            SnapshotBudgetPool::Control,
            budget.capacity(SnapshotBudgetPool::Control),
        )])
        .unwrap();
    let result = reader.lookup(&reference, &[1]).await;
    assert!(
        matches!(result, Err(PackedWireError::LimitExceeded(_))),
        "warm lookup allocated control while pool was exhausted: {result:?}"
    );
    assert_eq!(
        budget.state().used[SnapshotBudgetPool::Control as usize],
        budget.capacity(SnapshotBudgetPool::Control)
    );
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    drop(owner);
    assert_eq!(
        reader
            .lookup(&reference, &[1])
            .await
            .unwrap()
            .as_ref()
            .map(|value| value.as_ref()),
        Some(&[7; 128][..])
    );
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    assert_eq!(budget.state().used[SnapshotBudgetPool::Control as usize], 0);
}

#[tokio::test]
async fn existing_warm_lookup_rejects_exhausted_recipe_and_recovers() {
    let (backend, reference, budget, reader) = fixture(true).await;
    prime(&backend, &reference, &reader).await;
    let owner = budget
        .admit(&[(
            SnapshotBudgetPool::Plans,
            budget.capacity(SnapshotBudgetPool::Plans),
        )])
        .unwrap();
    let result = reader.lookup(&reference, &[1]).await;
    assert!(
        matches!(result, Err(PackedWireError::LimitExceeded(_))),
        "warm lookup recipe escaped full Plans pool: {result:?}"
    );
    assert_eq!(
        budget.state().used[SnapshotBudgetPool::Control as usize],
        0,
        "partial Control charge survived failed atomic admission"
    );
    drop(owner);
    assert_eq!(
        reader
            .lookup(&reference, &[1])
            .await
            .unwrap()
            .as_ref()
            .map(|value| value.as_ref()),
        Some(&[7; 128][..])
    );
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    assert_eq!(budget.state().used[SnapshotBudgetPool::Plans as usize], 0);
}

#[tokio::test]
async fn existing_same_key_pending_follower_owns_request_control() {
    let (backend, reference, budget, reader) = fixture(false).await;
    let mut leader = Box::pin(reader.lookup(&reference, &[1]));
    assert!(futures_util::poll!(leader.as_mut()).is_pending());
    let first = budget.state();
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    let mut follower = Box::pin(reader.lookup(&reference, &[1]));
    assert!(futures_util::poll!(follower.as_mut()).is_pending());
    let second = budget.state();
    assert!(
        second.used[SnapshotBudgetPool::Control as usize]
            > first.used[SnapshotBudgetPool::Control as usize],
        "Moka follower wait allocated without mount Control owner"
    );
    assert!(
        second.used[SnapshotBudgetPool::Plans as usize]
            > first.used[SnapshotBudgetPool::Plans as usize],
        "follower reference/future recipe allocated without independent owner"
    );
    assert_eq!(
        backend.calls.load(Ordering::SeqCst),
        1,
        "admission lost singleflight"
    );
    backend.open();
    let (first_result, second_result) = tokio::join!(leader, follower);
    assert_eq!(
        first_result.unwrap().as_ref().map(|value| value.as_ref()),
        Some(&[7; 128][..])
    );
    assert_eq!(
        second_result.unwrap().as_ref().map(|value| value.as_ref()),
        Some(&[7; 128][..])
    );
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    assert_eq!(budget.state().used[SnapshotBudgetPool::Control as usize], 0);
    assert_eq!(budget.state().used[SnapshotBudgetPool::Plans as usize], 0);
}
