// Existing-public-API candidates. No future bounded method or fake authority.
// Memory read byte accounting is not proof of Redis/TiKV network admission.
use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
struct ReadObservation {
    kind: &'static str,
    keys: Vec<Vec<u8>>,
    value_lengths: Vec<Option<usize>>,
}

#[derive(Clone, Debug, Default)]
struct OpenTransferObservation {
    reads: Vec<ReadObservation>,
    exact_check_value_bytes: usize,
    written_value_bytes: usize,
}

impl OpenTransferObservation {
    fn read_value_bytes(&self) -> usize {
        self.reads.iter().flat_map(|read| read.value_lengths.iter())
            .flatten().copied().try_fold(0usize, |sum, bytes| sum.checked_add(bytes))
            .expect("small fixture encoded value-byte accounting must not overflow")
    }

    fn read_positions(&self) -> usize {
        self.reads.iter().map(|read| read.keys.len()).sum()
    }
}

#[derive(Clone)]
struct ObservedBackend {
    inner: MemoryBackend,
    observed: Arc<Mutex<OpenTransferObservation>>,
}

impl ObservedBackend {
    fn new(inner: MemoryBackend) -> Self {
        Self { inner, observed: Arc::new(Mutex::new(OpenTransferObservation::default())) }
    }

    async fn record_read(&self, kind: &'static str, keys: &[Vec<u8>], values: &[Option<Vec<u8>>]) {
        assert_eq!(keys.len(), values.len(), "actual backend observation must retain all requested positions");
        if keys.iter().any(|key| key.as_slice() == CONTROL_KEY) {
            assert_eq!(kind, "consistent_timed_bounded");
            assert_eq!(keys.len(), 1, "CONTROL header must use a fixed point read");
            let raw = values[0].as_deref().expect("current catalog header is required");
            assert!(raw.len() <= OPEN_RECORD_MAX_BYTES, "CONTROL read must remain a small header");
            assert!(raw.starts_with(CONTROL_MAGIC), "CONTROL read must use current header magic");
            let header = decode_control(raw).expect("CONTROL must decode as the current header");
            assert_eq!(header.schema_version, WORKSPACE_SCHEMA_VERSION);
            assert_eq!(header.catalog_format, CATALOG_FORMAT);
        }
        self.observed.lock().await.reads.push(ReadObservation {
            kind, keys: keys.to_vec(),
            value_lengths: values.iter().map(|value| value.as_ref().map(Vec::len)).collect(),
        });
    }

    async fn record_cas(&self, checks: &[KvCheck], writes: &[KvWrite]) {
        let mut observed = self.observed.lock().await;
        for check in checks {
            observed.exact_check_value_bytes = observed.exact_check_value_bytes
                .checked_add(check.expected.as_ref().map_or(0, Vec::len)).unwrap();
        }
        for write in writes {
            if let KvWrite::Put { value, .. } = write {
                observed.written_value_bytes = observed.written_value_bytes.checked_add(value.len()).unwrap();
            }
        }
    }

    async fn reset(&self) {
        *self.observed.lock().await = OpenTransferObservation::default();
    }

    async fn record_scan(&self, kind: &'static str, entries: &[KvEntry]) {
        self.observed.lock().await.reads.push(ReadObservation {
            kind, keys: entries.iter().map(|entry| entry.key.clone()).collect(),
            value_lengths: entries.iter().map(|entry| Some(entry.value.len())).collect(),
        });
    }

    async fn snapshot(&self) -> OpenTransferObservation {
        self.observed.lock().await.clone()
    }
}

#[async_trait]
impl WorkspaceKvBackend for ObservedBackend {
    fn supports_consistent_reads(&self) -> bool { self.inner.supports_consistent_reads() }
    fn name(&self) -> &'static str { "g10-observed-existing-memory-api" }

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, WorkspaceError> {
        let value = self.inner.get(key).await?;
        // Observe the actual returned Vec without cloning its body or replacing
        // its authority. The probe retains keys and lengths only.
        self.record_read("get", &[key.to_vec()], std::slice::from_ref(&value)).await;
        Ok(value)
    }

    async fn get_many(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>, WorkspaceError> {
        let values = self.inner.get_many(keys).await?;
        self.record_read("get_many", keys, &values).await;
        Ok(values)
    }

    async fn get_many_consistent(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>, WorkspaceError> {
        let values = self.inner.get_many_consistent(keys).await?;
        self.record_read("consistent", keys, &values).await;
        Ok(values)
    }

    async fn get_many_consistent_with_time(&self, keys: &[Vec<u8>]) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        let (values, now) = self.inner.get_many_consistent_with_time(keys).await?;
        self.record_read("consistent_timed", keys, &values).await;
        Ok((values, now))
    }

    async fn get_many_consistent_with_time_bounded(
        &self,
        keys: &[Vec<u8>],
        limits: crate::workspace_overlay::stores::kv_backend::KvReadLimits,
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        let (values, now) = self
            .inner
            .get_many_consistent_with_time_bounded(keys, limits)
            .await?;
        self.record_read("consistent_timed_bounded", keys, &values)
            .await;
        Ok((values, now))
    }

    async fn scan_prefix_page_with_byte_limits(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limits: crate::workspace_overlay::stores::kv_backend::KvReadLimits,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        let entries = self
            .inner
            .scan_prefix_page_with_byte_limits(prefix, after, limits)
            .await?;
        self.record_scan("scan_prefix_page", &entries).await;
        Ok(entries)
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<KvEntry>, WorkspaceError> {
        let entries = self.inner.scan_prefix(prefix).await?;
        self.record_scan("scan_prefix", &entries).await;
        Ok(entries)
    }

    async fn scan_prefix_bounded(&self, prefix: &[u8], max_records: usize) -> Result<Vec<KvEntry>, WorkspaceError> {
        let entries = self.inner.scan_prefix_bounded(prefix, max_records).await?;
        self.record_scan("scan_prefix_bounded", &entries).await;
        Ok(entries)
    }

    async fn compare_and_swap(&self, checks: &[KvCheck], writes: &[KvWrite]) -> Result<bool, WorkspaceError> {
        self.record_cas(checks, writes).await;
        self.inner.compare_and_swap(checks, writes).await
    }

    async fn compare_and_swap_before(&self, checks: &[KvCheck], writes: &[KvWrite], deadline: i64) -> Result<bool, WorkspaceError> {
        self.record_cas(checks, writes).await;
        self.inner.compare_and_swap_before(checks, writes, deadline).await
    }

    async fn server_time_ns(&self) -> Result<i64, WorkspaceError> { self.inner.server_time_ns().await }
}

async fn observed_open(store: &KvWorkspaceStore<ObservedBackend>, target: WorkspaceId) -> OpenTransferObservation {
    store.backend.reset().await;
    let token = store.open_workspace_v3(target, "bounded-target", Duration::from_secs(3600)).await
        .expect("ordinary valid target open must remain usable after unrelated catalog growth");
    assert_eq!(token.workspace_id, target);
    assert_eq!(token.owner_id, "bounded-target");
    assert_eq!(token.state, V3OpenState::Ready);
    assert!(!token.recovery_required, "unrelated journals cannot put the target into recovery");
    let observed = store.backend.snapshot().await;
    assert!(observed.read_positions() > 0, "fixture must actually reach authoritative backend reads");
    assert!(observed.written_value_bytes > 0, "fixture must actually reach sidecar publication CAS");
    observed
}

fn assert_target_open_transfer_does_not_grow(before: &OpenTransferObservation, after: &OpenTransferObservation, case: &str) {
    assert_eq!(after.read_value_bytes(), before.read_value_bytes(),
        "unrelated {case} must not grow target open encoded input bytes: before={before:?}, after={after:?}");
    assert_eq!(after.exact_check_value_bytes, before.exact_check_value_bytes,
        "unrelated {case} must not grow target open exact-CAS upload bytes");
    assert_eq!(after.read_positions(), before.read_positions(),
        "unrelated {case} must not grow target open fixed-key positions");
    assert!(after.reads.iter().any(|read| read.kind == "consistent_timed_bounded"
        && read.keys.len() == 1 && read.keys[0].as_slice() == CONTROL_KEY),
        "target open must authenticate its fixed bounded CONTROL header");
    assert!(!after.reads.iter().any(|read| read.kind.starts_with("scan_prefix")),
        "ordinary target open must not hydrate any global prefix");
}

#[tokio::test]
async fn g10a_kv_unrelated_public_forks_do_not_grow_target_open_transfer() {
    let (store, target, _lease, _guard) = initialized().await;
    store.open_workspace_v3(target.workspace_id, "bounded-target", Duration::from_secs(3600)).await.unwrap();
    let peer = KvWorkspaceStore::new(ObservedBackend::new((*store.backend).clone()));
    let target_before = store.load_workspace(target.workspace_id).await.unwrap();
    let before = observed_open(&peer, target.workspace_id).await;
    for index in 0u128..16 {
        let child = store.create_workspace(CreateWorkspace {
            upper_bucket: None,
            workspace_id: WorkspaceId::from_uuid(id(10_000 + 2 * index)),
            head_layer_id: LayerId::from_uuid(id(10_001 + 2 * index)),
            base_revision: target.fork_base.clone().unwrap(),
            owner_id: Some("unrelated-owner".into()),
        }).await.unwrap();
        assert_eq!(store.load_workspace(child.workspace_id).await.unwrap(), child);
    }
    assert_eq!(store.load_workspace(target.workspace_id).await.unwrap(), target_before,
        "unrelated public fork must not change target workspace authority");
    let after = observed_open(&peer, target.workspace_id).await;
    assert_target_open_transfer_does_not_grow(&before, &after, "forks");
}

#[tokio::test]
async fn g10c_existing_unbounded_timed_api_returns_nonzero_oversized_body_in_full() {
    let backend = MemoryBackend::default();
    let key = b"opaque-existing-unbounded-control".to_vec();
    let payload: Vec<u8> = (0..OPEN_CONTROL_MAX_BYTES + 1).map(|i| (i % 251 + 1) as u8).collect();
    assert!(backend.compare_and_swap(&[KvCheck { key: key.clone(), expected: None }],
        &[KvWrite::Put { key: key.clone(), value: payload.clone() }]).await.unwrap());
    let (values, now) = backend.get_many_consistent_with_time(&[key.clone(), key, b"absent".to_vec()]).await.unwrap();
    assert!(now > 0, "use actual existing backend clock");
    assert_eq!(values.len(), 3);
    assert_eq!(values[0].as_deref(), Some(payload.as_slice()));
    assert_eq!(values[1].as_deref(), Some(payload.as_slice()));
    assert!(values[2].is_none());
    assert_eq!(values.iter().flatten().map(Vec::len).sum::<usize>(), 2 * payload.len(),
        "unbounded historical control counts duplicate returned positions; it is not bounded-open acceptance");
}
