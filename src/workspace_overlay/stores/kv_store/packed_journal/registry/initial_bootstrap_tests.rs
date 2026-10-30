//! Real Redis/TiKV transactions and actual LocalFS create-only objects.
//! Fault wrappers lose genuine replies; they never fabricate success rows.

use super::*;
use crate::cadapter::localfs::LocalFsBackend;
use crate::workspace_overlay::catalog::{AcquireLease, CreateVolumeRoot, WorkspaceStore};
use crate::workspace_overlay::packed_v3::{AccessProfile, PackedCodec, SizeClassTable};
use crate::workspace_overlay::stores::redis::RedisWorkspaceBackend;
use crate::workspace_overlay::stores::tikv::TiKvWorkspaceBackend;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

struct FaultBackend<B: WorkspaceKvBackend> {
    inner: Arc<B>,
    budget: Arc<V3MountBudget>,
    lose_install_reply: AtomicBool,
    lose_dispatch_reply: AtomicBool,
    force_short_pages: AtomicBool,
    installed_mutations: AtomicU64,
    tail_pages: AtomicU64,
}

fn expected(checks: &[KvCheck], key: &[u8]) -> Option<Vec<u8>> {
    checks
        .iter()
        .find(|check| check.key == key)
        .and_then(|check| check.expected.clone())
}

fn written(writes: &[KvWrite], key: &[u8]) -> Option<Vec<u8>> {
    writes.iter().find_map(|write| match write {
        KvWrite::Put { key: found, value } if found == key => Some(value.clone()),
        _ => None,
    })
}

fn install_record(writes: &[KvWrite]) -> Option<InitialBootstrapRecord> {
    writes.iter().find_map(|write| match write {
        KvWrite::Put { key, value } if key.starts_with(b"packed/v3/initial-bootstrap/") => {
            let record = InitialBootstrapRecord::decode(value).unwrap();
            (record.phase == BootstrapPhase::Installed).then_some(record)
        }
        _ => None,
    })
}

fn assert_install_packet(checks: &[KvCheck], writes: &[KvWrite]) {
    let record = install_record(writes)
        .expect("actual first PWB install must write PBIInstalled in its same CAS");
    let binding = record.binding.as_ref().unwrap();
    let prior = InitialBootstrapRecord::decode(
        &expected(checks, &bootstrap_key(binding.workspace_id)).unwrap(),
    )
    .unwrap();
    assert_eq!(prior.phase, BootstrapPhase::Candidate);
    assert_eq!(prior.source_digest, record.source_digest);
    assert_eq!(prior.object_count, record.object_count);
    assert_eq!(
        expected(checks, &packed_current_key(binding.workspace_id)),
        None
    );
    assert_eq!(
        expected(checks, &packed_claim_key(binding.workspace_id)),
        None
    );
    assert_eq!(
        expected(checks, &packed_history_key(binding.workspace_id, 1)),
        None
    );
    for key in [
        packed_current_key(binding.workspace_id),
        packed_history_key(binding.workspace_id, 1),
    ] {
        assert_eq!(written(writes, &key), Some(binding.encode().unwrap()));
    }
    assert_eq!(
        written(writes, &packed_claim_key(binding.workspace_id)).as_deref(),
        Some(PACKED_CLAIM)
    );
    assert_eq!(
        expected(checks, &hot_layer_key(binding.base_revision.layer_id)),
        Some(record.expected_base.clone())
    );
    assert_eq!(
        expected(
            checks,
            &inode_identity_key(binding.base_revision.layer_id, 1)
        ),
        Some(record.expected_root.clone())
    );
    let root_key = registry_root_key(record.incarnation);
    let mapping_key = registry_history_root_key(binding);
    let staged = RootRow::decode(&expected(checks, &root_key).unwrap()).unwrap();
    assert_eq!(staged.state, RootState::Staging);
    assert_eq!(staged.members, record.object_count);
    assert_eq!(staged.pending_puts, 0);
    let root = RootRow::decode(&written(writes, &root_key).unwrap()).unwrap();
    assert_eq!(root.state, RootState::BindingHistory);
    assert_eq!(root.binding.as_ref(), Some(binding));
    assert_eq!(written(writes, &root_key), written(writes, &mapping_key));
    assert_eq!(expected(checks, &mapping_key), None);
    let adopted = writes
        .iter()
        .filter_map(|write| match write {
            KvWrite::Put { key, value } if key.starts_with(b"packed/v3/registry/member/") => {
                let member = MemberRow::decode(value).unwrap();
                assert!(
                    member.adopted && member.retained && !member.pending_put && !member.dispatched
                );
                assert!(member.put_id.is_nil());
                Some(member)
            }
            _ => None,
        })
        .count();
    assert_eq!(adopted as u64, record.object_count);
}

#[async_trait]
impl<B: WorkspaceKvBackend> WorkspaceKvBackend for FaultBackend<B> {
    fn name(&self) -> &'static str {
        self.inner.name()
    }
    fn supports_consistent_reads(&self) -> bool {
        self.inner.supports_consistent_reads()
    }
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, WorkspaceError> {
        self.inner.get(key).await
    }
    async fn get_many(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>, WorkspaceError> {
        self.inner.get_many(keys).await
    }
    async fn get_many_consistent(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>, WorkspaceError> {
        self.inner.get_many_consistent(keys).await
    }
    async fn get_many_consistent_with_time(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        self.inner.get_many_consistent_with_time(keys).await
    }
    async fn get_many_consistent_with_time_bounded(
        &self,
        keys: &[Vec<u8>],
        limits: KvReadLimits,
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        self.inner
            .get_many_consistent_with_time_bounded(keys, limits)
            .await
    }
    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<KvEntry>, WorkspaceError> {
        self.inner.scan_prefix(prefix).await
    }
    async fn scan_prefix_bounded(
        &self,
        prefix: &[u8],
        records: usize,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        self.inner.scan_prefix_bounded(prefix, records).await
    }
    async fn scan_prefix_with_byte_limits(
        &self,
        prefix: &[u8],
        limits: KvReadLimits,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        self.inner
            .scan_prefix_with_byte_limits(prefix, limits)
            .await
    }
    async fn scan_prefix_page_with_byte_limits(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        mut limits: KvReadLimits,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        if after.is_some() {
            self.tail_pages.fetch_add(1, Ordering::SeqCst);
        }
        if self.force_short_pages.load(Ordering::SeqCst) {
            limits.max_records = 1;
            limits.max_data_requests = 1;
        }
        self.inner
            .scan_prefix_page_with_byte_limits(prefix, after, limits)
            .await
    }
    async fn compare_and_swap(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
    ) -> Result<bool, WorkspaceError> {
        self.inner.compare_and_swap(checks, writes).await
    }
    async fn compare_and_swap_before(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        before: i64,
    ) -> Result<bool, WorkspaceError> {
        let is_install = writes.iter().any(|write| matches!(write, KvWrite::Put { key, .. } if key.starts_with(b"packed/v3/current/")));
        if is_install {
            assert_install_packet(checks, writes);
            assert!(self.budget.state().used[V3BudgetPool::Metadata as usize] >= 8 << 20);
        }
        let dispatch = writes.iter().any(|write| match write {
            KvWrite::Put { key, value } if key.starts_with(b"packed/v3/registry/member/") => {
                let member = MemberRow::decode(value).unwrap();
                member.pending_put && member.dispatched && !member.adopted
            }
            _ => false,
        });
        let applied = self
            .inner
            .compare_and_swap_before(checks, writes, before)
            .await?;
        if applied && is_install {
            self.installed_mutations.fetch_add(1, Ordering::SeqCst);
            if self.lose_install_reply.swap(false, Ordering::SeqCst) {
                return Err(WorkspaceError::Backend(
                    "injected lost actual PBI installation reply".into(),
                ));
            }
        }
        if applied && dispatch && self.lose_dispatch_reply.swap(false, Ordering::SeqCst) {
            return Err(WorkspaceError::Backend(
                "injected lost actual bootstrap dispatch reply".into(),
            ));
        }
        Ok(applied)
    }
    async fn compare_and_swap_in_time_window(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        lower: Option<i64>,
        upper: Option<i64>,
    ) -> Result<bool, WorkspaceError> {
        self.inner
            .compare_and_swap_in_time_window(checks, writes, lower, upper)
            .await
    }
    async fn authenticate_checks_before_bounded(
        &self,
        checks: &[KvCheck],
        expires_at_ns: i64,
        limits: crate::workspace_overlay::stores::kv_backend::KvReadLimits,
    ) -> Result<bool, WorkspaceError> {
        crate::workspace_overlay::stores::kv_backend::validate_bounded_authentication_checks(
            checks, limits,
        )?;
        crate::workspace_overlay::stores::kv_backend::validate_cas_time_window(
            None,
            Some(expires_at_ns),
        )?;
        self.inner
            .authenticate_checks_before_bounded(checks, expires_at_ns, limits)
            .await
    }

    async fn server_time_ns(&self) -> Result<i64, WorkspaceError> {
        self.inner.server_time_ns().await
    }
    async fn shutdown_metadata_backend(&self) -> Result<(), WorkspaceError> {
        self.inner.shutdown_metadata_backend().await
    }
}

struct ObservedObjects<B: WorkspaceKvBackend> {
    inner: LocalFsBackend,
    metadata: Arc<FaultBackend<B>>,
    puts: Arc<AtomicU64>,
    lose_remote_reply: Arc<AtomicBool>,
}
impl<B: WorkspaceKvBackend> Clone for ObservedObjects<B> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            metadata: self.metadata.clone(),
            puts: self.puts.clone(),
            lose_remote_reply: self.lose_remote_reply.clone(),
        }
    }
}

#[async_trait]
impl<B: WorkspaceKvBackend> ObjectBackend for ObservedObjects<B> {
    async fn put_object(&self, _: &str, _: &[u8]) -> anyhow::Result<()> {
        anyhow::bail!("bootstrap must use create-only PUT")
    }
    async fn put_object_create_only(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
        let reference = super::super::upload_backend::typed_reference(key, data).unwrap();
        let rows = self
            .metadata
            .inner
            .scan_prefix(b"packed/v3/initial-bootstrap/")
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        let record = InitialBootstrapRecord::decode(&rows[0].value).unwrap();
        assert_eq!(record.phase, BootstrapPhase::Building);
        assert!(key.starts_with(&format!("{}/", record.prefix())));
        let object = ObjectRow::decode(
            &self
                .metadata
                .inner
                .get(&registry_object_key(&reference))
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        let member = MemberRow::decode(
            &self
                .metadata
                .inner
                .get(&registry_member_key(&reference, record.incarnation))
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        let root = RootRow::decode(
            &self
                .metadata
                .inner
                .get(&registry_root_key(record.incarnation))
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(object.reference, reference);
        assert_eq!(object.state, ObjectState::Live);
        assert_eq!(object.pending_puts, 1);
        assert_eq!(object.memberships, 1);
        assert!(member.pending_put && member.dispatched && member.retained && !member.adopted);
        assert!(!member.put_id.is_nil());
        assert_eq!(member.reference, reference);
        assert_eq!(root.state, RootState::Staging);
        assert_eq!(root.members, record.object_count);
        assert_eq!(root.pending_puts, 1);
        assert!(
            self.metadata.budget.state().used[V3BudgetPool::Metadata as usize] >= 12 << 20,
            "physical request must retain driver, producer scratch, and operation owners"
        );
        let occurrence = PackedJournalObject::decode(
            &self
                .metadata
                .inner
                .get(&object_key(record.journal_id, member.ordinal))
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert!(!occurrence.uploaded && !occurrence.readback_recorded);
        assert!(
            self.metadata
                .inner
                .get(&packed_current_key(record.workspace_id))
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            self.metadata
                .inner
                .get(&journal_key(record.journal_id))
                .await
                .unwrap()
                .is_none(),
            "first bootstrap cannot fabricate PPJ authority"
        );
        self.puts.fetch_add(1, Ordering::SeqCst);
        self.inner.put_object_create_only(key, data).await?;
        if self.lose_remote_reply.swap(false, Ordering::SeqCst) {
            anyhow::bail!("injected lost actual create-only PUT reply");
        }
        Ok(())
    }
    async fn get_object(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        self.inner.get_object(key).await
    }
    async fn get_object_stream(&self, key: &str) -> anyhow::Result<Option<ObjectByteStream>> {
        self.inner.get_object_stream(key).await
    }
    async fn get_object_stream_observed(
        &self,
        key: &str,
        expected: Option<u64>,
        context: ReadContext,
        observer: Arc<ReadObserver>,
    ) -> anyhow::Result<Option<ObjectByteStream>> {
        self.inner
            .get_object_stream_observed(key, expected, context, observer)
            .await
    }
    async fn get_object_range(
        &self,
        key: &str,
        offset: u64,
        buf: &mut [u8],
    ) -> anyhow::Result<usize> {
        self.inner.get_object_range(key, offset, buf).await
    }
    async fn get_object_range_stream(
        &self,
        key: &str,
        offset: u64,
        length: u64,
    ) -> anyhow::Result<ObjectByteStream> {
        self.inner
            .get_object_range_stream(key, offset, length)
            .await
    }
    async fn get_object_range_stream_observed(
        &self,
        key: &str,
        offset: u64,
        length: u64,
        context: ReadContext,
        observer: Arc<ReadObserver>,
    ) -> anyhow::Result<ObjectByteStream> {
        self.inner
            .get_object_range_stream_observed(key, offset, length, context, observer)
            .await
    }
    async fn get_object_size(&self, key: &str) -> anyhow::Result<Option<u64>> {
        self.inner.get_object_size(key).await
    }
    async fn get_object_size_bounded(&self, key: &str) -> anyhow::Result<Option<u64>> {
        self.inner.get_object_size_bounded(key).await
    }
    async fn get_object_size_bounded_observed(
        &self,
        key: &str,
        context: ReadContext,
        observer: Arc<ReadObserver>,
    ) -> anyhow::Result<Option<u64>> {
        self.inner
            .get_object_size_bounded_observed(key, context, observer)
            .await
    }
    async fn get_etag(&self, key: &str) -> anyhow::Result<String> {
        self.inner.get_etag(key).await
    }
    async fn delete_object(&self, _: &str) -> anyhow::Result<()> {
        panic!("bootstrap and unresolved retry must not issue DELETE")
    }
}

fn options() -> V3ProducerOptions {
    V3ProducerOptions {
        snapshot_id: [3; 32],
        root_dir_key: [4; 32],
        root_inode: 1,
        profile: AccessProfile::RandomSmallFile,
        size_classes: SizeClassTable::default(),
        build_policy: Default::default(),
        metadata_codec: PackedCodec::Raw,
        data_codec: PackedCodec::Raw,
    }
}

async fn fresh_guard<B: WorkspaceKvBackend>(store: &KvWorkspaceStore<B>) -> HeadGuard {
    store.initialize_workspace_schema().await.unwrap();
    let workspace = store
        .create_volume_root(CreateVolumeRoot {
            upper_bucket: None,
            volume_format: "workspace-v1".into(),
            schema_version: 1,
            volume_id: Uuid::new_v4(),
            workspace_id: WorkspaceId::new(),
            root_layer_id: LayerId::new(),
            writable_layer_id: LayerId::new(),
            owner_id: None,
        })
        .await
        .unwrap();
    let lease = store
        .acquire_lease(AcquireLease {
            workspace_id: workspace.workspace_id,
            lease_id: LeaseId::new(),
            holder_generation: 7,
            ttl_ns: 300_000_000_000,
        })
        .await
        .unwrap();
    HeadGuard {
        workspace_id: workspace.workspace_id,
        expected_head_layer_id: workspace.head_layer_id,
        expected_head_epoch: workspace.head_epoch,
        lease_id: lease.lease_id,
        holder_generation: lease.holder_generation,
    }
}

async fn clear_owned_namespace<B: WorkspaceKvBackend>(backend: &B) {
    let entries = backend.scan_prefix(b"").await.unwrap();
    for batch in entries.chunks(16) {
        let checks = batch
            .iter()
            .map(|entry| KvCheck {
                key: entry.key.clone(),
                expected: Some(entry.value.clone()),
            })
            .collect::<Vec<_>>();
        let writes = batch
            .iter()
            .map(|entry| KvWrite::Delete {
                key: entry.key.clone(),
            })
            .collect::<Vec<_>>();
        assert!(backend.compare_and_swap(&checks, &writes).await.unwrap());
    }
    assert!(backend.scan_prefix(b"").await.unwrap().is_empty());
}

async fn budget_idle(budget: &V3MountBudget) {
    for _ in 0..100 {
        if budget.state().used == [0; 8] {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        budget.state().used,
        [0; 8],
        "actual producer/backend owners must drain"
    );
}

async fn assert_quarantine<B: WorkspaceKvBackend>(
    backend: &B,
    guard: &HeadGuard,
) -> InitialBootstrapRecord {
    let record = InitialBootstrapRecord::decode(
        &backend
            .get(&bootstrap_key(guard.workspace_id))
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(record.phase, BootstrapPhase::Building);
    assert_eq!(record.object_count, 1);
    let root = RootRow::decode(
        &backend
            .get(&registry_root_key(record.incarnation))
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(root.state, RootState::Staging);
    assert_eq!(root.pending_puts, 1);
    assert_eq!(root.members, 1);
    let reference = V3ObjectRef::decode_value(
        &backend
            .get(&registry_reverse_key(record.incarnation, 0))
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    let object = ObjectRow::decode(
        &backend
            .get(&registry_object_key(&reference))
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    let member = MemberRow::decode(
        &backend
            .get(&registry_member_key(&reference, record.incarnation))
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(object.pending_puts, 1);
    assert_eq!(object.memberships, 1);
    assert!(member.pending_put && member.dispatched && member.retained);
    assert!(
        backend
            .get(&packed_current_key(guard.workspace_id))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        backend
            .get(&packed_claim_key(guard.workspace_id))
            .await
            .unwrap()
            .is_none()
    );
    record
}

async fn contract<B: WorkspaceKvBackend>(backend: Arc<FaultBackend<B>>) {
    let scratch = tempfile::tempdir().unwrap();
    let object_directory = tempfile::tempdir().unwrap();
    let puts = Arc::new(AtomicU64::new(0));
    let lose_remote_reply = Arc::new(AtomicBool::new(false));
    let client = ObjectClient::new(ObservedObjects {
        inner: LocalFsBackend::new(object_directory.path()),
        metadata: backend.clone(),
        puts: puts.clone(),
        lose_remote_reply: lose_remote_reply.clone(),
    });
    let budget = backend.budget.clone();
    let store = Arc::new(
        KvWorkspaceStore::from_arc(backend.clone()).with_packed_reader_pin_budget(budget.clone()),
    );
    let guard = fresh_guard(store.as_ref()).await;
    let allocator_key = hot_allocator_key("inode");
    let original_allocator = backend.get(&allocator_key).await.unwrap();
    assert!(
        backend
            .compare_and_swap(
                &[KvCheck {
                    key: allocator_key.clone(),
                    expected: original_allocator.clone()
                }],
                &[put(allocator_key.clone(), &3i64).unwrap()]
            )
            .await
            .unwrap()
    );
    assert!(
        store
            .bootstrap_initial_packed_lower(
                guard.clone(),
                client.clone(),
                scratch.path().into(),
                options(),
                budget.clone(),
                CancellationToken::new()
            )
            .await
            .is_err()
    );
    assert_eq!(puts.load(Ordering::SeqCst), 0);
    assert!(
        backend
            .get(&bootstrap_key(guard.workspace_id))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        backend
            .compare_and_swap(
                &[KvCheck {
                    key: allocator_key.clone(),
                    expected: Some(encode(&3i64).unwrap())
                }],
                &[KvWrite::Put {
                    key: allocator_key,
                    value: original_allocator.unwrap()
                }]
            )
            .await
            .unwrap()
    );
    let chain = store
        .load_layer_chain(guard.expected_head_layer_id)
        .await
        .unwrap();
    let root_key = inode_identity_key(chain[1].layer_id, 1);
    let root: InodeDelta = decode_required(&backend.get(&root_key).await.unwrap()).unwrap();
    let mut extra = root.clone();
    extra.ino = 2;
    extra.sequence = 2;
    let extra_key = inode_key(&extra);
    assert!(
        backend
            .compare_and_swap(
                &[KvCheck {
                    key: extra_key.clone(),
                    expected: None
                }],
                &[put(extra_key.clone(), &extra).unwrap()]
            )
            .await
            .unwrap()
    );
    backend.force_short_pages.store(true, Ordering::SeqCst);
    assert!(
        store
            .bootstrap_initial_packed_lower(
                guard.clone(),
                client.clone(),
                scratch.path().into(),
                options(),
                budget.clone(),
                CancellationToken::new()
            )
            .await
            .is_err()
    );
    assert!(
        backend.tail_pages.load(Ordering::SeqCst) > 0,
        "real short base page needs a following actual page"
    );
    backend.force_short_pages.store(false, Ordering::SeqCst);
    assert_eq!(puts.load(Ordering::SeqCst), 0);
    assert!(
        backend
            .get(&bootstrap_key(guard.workspace_id))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        backend
            .compare_and_swap(
                &[KvCheck {
                    key: extra_key.clone(),
                    expected: Some(encode(&extra).unwrap())
                }],
                &[KvWrite::Delete { key: extra_key }]
            )
            .await
            .unwrap()
    );

    backend.lose_install_reply.store(true, Ordering::SeqCst);
    let error = store
        .bootstrap_initial_packed_lower(
            guard.clone(),
            client.clone(),
            scratch.path().into(),
            options(),
            budget.clone(),
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("lost actual PBI installation reply")
    );
    assert_eq!(backend.installed_mutations.load(Ordering::SeqCst), 1);
    let installed = InitialBootstrapRecord::decode(
        &backend
            .get(&bootstrap_key(guard.workspace_id))
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(installed.phase, BootstrapPhase::Installed);
    let binding = installed.binding.clone().unwrap();
    assert_eq!(binding.highest_inode, 1);
    assert!(installed.object_count > 1);
    assert_eq!(puts.load(Ordering::SeqCst), installed.object_count);
    assert_eq!(
        backend
            .get(&packed_current_key(guard.workspace_id))
            .await
            .unwrap(),
        Some(binding.encode().unwrap())
    );
    assert!(
        backend
            .get(&journal_key(installed.journal_id))
            .await
            .unwrap()
            .is_none()
    );
    let before_reopen = puts.load(Ordering::SeqCst);
    let fresh = Arc::new(
        KvWorkspaceStore::from_arc(backend.clone()).with_packed_reader_pin_budget(budget.clone()),
    );
    assert_eq!(
        fresh
            .bootstrap_initial_packed_lower(
                guard.clone(),
                client.clone(),
                scratch.path().into(),
                options(),
                budget.clone(),
                CancellationToken::new()
            )
            .await
            .unwrap(),
        binding
    );
    assert_eq!(
        puts.load(Ordering::SeqCst),
        before_reopen,
        "installed reopen must audit actual bytes without reissuing PUTs"
    );
    assert_eq!(backend.installed_mutations.load(Ordering::SeqCst), 1);
    let packet = fresh
        .initial_packed_bootstrap_checks(&guard, &binding)
        .await
        .unwrap();
    assert!(
        packet
            .iter()
            .any(|check| check.key == bootstrap_key(guard.workspace_id))
    );
    assert!(
        packet
            .iter()
            .any(|check| check.key == packed_history_key(guard.workspace_id, 1))
    );
    assert!(
        !packet
            .iter()
            .any(|check| check.key == hot_layer_key(binding.head_layer_id)
                || check.key == packed_current_key(binding.workspace_id)),
        "immutable source packet cannot freeze mutable native phases"
    );
    let snapshot = AuthenticatedV3Snapshot::open(&client, &binding.binding.manifest)
        .await
        .unwrap();
    assert_eq!(
        snapshot.manifest().source.as_ref().unwrap().root,
        root_attributes(&root)
    );
    let raw_installed = backend
        .get(&bootstrap_key(guard.workspace_id))
        .await
        .unwrap()
        .unwrap();
    assert!(
        backend
            .compare_and_swap(
                &[KvCheck {
                    key: bootstrap_key(guard.workspace_id),
                    expected: Some(raw_installed.clone())
                }],
                &[KvWrite::Delete {
                    key: bootstrap_key(guard.workspace_id)
                }]
            )
            .await
            .unwrap()
    );
    assert!(
        fresh
            .initial_packed_bootstrap_checks(&guard, &binding)
            .await
            .is_err(),
        "PWB/namespace bytes cannot replace durable PBI source authority"
    );
    assert!(
        backend
            .compare_and_swap(
                &[KvCheck {
                    key: bootstrap_key(guard.workspace_id),
                    expected: None
                }],
                &[KvWrite::Put {
                    key: bootstrap_key(guard.workspace_id),
                    value: raw_installed
                }]
            )
            .await
            .unwrap()
    );
    budget_idle(&budget).await;
    clear_owned_namespace(backend.inner.as_ref()).await;

    for dispatch_unknown in [false, true] {
        let guard = fresh_guard(store.as_ref()).await;
        let before = puts.load(Ordering::SeqCst);
        lose_remote_reply.store(!dispatch_unknown, Ordering::SeqCst);
        backend
            .lose_dispatch_reply
            .store(dispatch_unknown, Ordering::SeqCst);
        assert!(
            store
                .bootstrap_initial_packed_lower(
                    guard.clone(),
                    client.clone(),
                    scratch.path().into(),
                    options(),
                    budget.clone(),
                    CancellationToken::new()
                )
                .await
                .is_err()
        );
        assert_eq!(
            puts.load(Ordering::SeqCst),
            before + u64::from(!dispatch_unknown)
        );
        let quarantined = assert_quarantine(backend.inner.as_ref(), &guard).await;
        let root_before = backend
            .get(&registry_root_key(quarantined.incarnation))
            .await
            .unwrap();
        let retry = Arc::new(
            KvWorkspaceStore::from_arc(backend.clone())
                .with_packed_reader_pin_budget(budget.clone()),
        );
        assert!(
            retry
                .bootstrap_initial_packed_lower(
                    guard.clone(),
                    client.clone(),
                    scratch.path().into(),
                    options(),
                    budget.clone(),
                    CancellationToken::new()
                )
                .await
                .is_err()
        );
        assert_eq!(
            puts.load(Ordering::SeqCst),
            before + u64::from(!dispatch_unknown),
            "unresolved reserve/dispatch/PUT cannot authorize a physical retry"
        );
        assert_eq!(
            backend
                .get(&registry_root_key(quarantined.incarnation))
                .await
                .unwrap(),
            root_before
        );
        assert_quarantine(backend.inner.as_ref(), &guard).await;
        budget_idle(&budget).await;
        clear_owned_namespace(backend.inner.as_ref()).await;
    }
    eprintln!(
        "packed-v3 real native root + before-PUT registry + atomic initial install + lost reply/reopen + short-page/nonempty/allocator refusal + remote/dispatch quarantine passed; backend={}",
        backend.name()
    );
}

async fn isolated<B: WorkspaceKvBackend>(backend: B) {
    let backend = Arc::new(backend);
    let worker = Arc::new(FaultBackend {
        inner: backend.clone(),
        budget: V3MountBudget::defaults(),
        lose_install_reply: AtomicBool::new(false),
        lose_dispatch_reply: AtomicBool::new(false),
        force_short_pages: AtomicBool::new(false),
        installed_mutations: AtomicU64::new(0),
        tail_pages: AtomicU64::new(0),
    });
    let result = tokio::spawn(async move { contract(worker).await }).await;
    clear_owned_namespace(backend.as_ref()).await;
    backend.shutdown_metadata_backend().await.unwrap();
    match result {
        Ok(()) => {}
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Err(error) => panic!("initial bootstrap contract cancelled: {error}"),
    }
}

#[tokio::test]
#[ignore = "requires BREWFS_TEST_REDIS_URL; real root/objects, isolated UUID namespace"]
async fn real_redis_initial_packed_bootstrap_source_staging_unknown_reopen_and_quarantine() {
    let url = std::env::var("BREWFS_TEST_REDIS_URL").expect("BREWFS_TEST_REDIS_URL");
    isolated(
        RedisWorkspaceBackend::connect(
            &url,
            &format!("packed-v3-initial-bootstrap-{}", Uuid::new_v4()),
        )
        .await
        .unwrap(),
    )
    .await;
}

#[tokio::test]
#[ignore = "requires BREWFS_TEST_TIKV_PD_ENDPOINTS; real root/objects, isolated UUID namespace"]
async fn real_tikv_initial_packed_bootstrap_source_staging_unknown_reopen_and_quarantine() {
    let endpoints = std::env::var("BREWFS_TEST_TIKV_PD_ENDPOINTS")
        .expect("BREWFS_TEST_TIKV_PD_ENDPOINTS")
        .split(',')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    isolated(
        TiKvWorkspaceBackend::connect(
            endpoints,
            &format!("packed-v3-initial-bootstrap-{}", Uuid::new_v4()),
        )
        .await
        .unwrap(),
    )
    .await;
}
