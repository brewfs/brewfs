//! Real producer fixtures exercise physical authentication, not publication
//! authority or semantic namespace closure.

#[path = "index_context_tests.rs"]
mod index_context_tests;

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::cadapter::client::{ObjectBackend, ObjectByteStream, ObjectClient};
use crate::cadapter::localfs::LocalFsBackend;
use crate::workspace::packed::snapshot::{
    AuthenticatedSnapshot, SnapshotBudgetPool, SnapshotBuildPolicy, SnapshotColdAttributes,
    SnapshotIndexBuilder, SnapshotIndexPage, SnapshotIndexReader, SnapshotIndexValue,
    SnapshotManifest, SnapshotMountBudget, SnapshotObjectKind, SnapshotObjectRef, SnapshotProducer,
    SnapshotProducerOptions, SnapshotRootAttributes, SnapshotRootKind, SnapshotXattr,
};
use crate::workspace::packed::wire::{PackedResult, PackedWireError};
use crate::workspace::packed::{
    AccessProfile, PackedCodec, PackedFileInput, SizeClassTable, pack_group_files_with_policy,
};

use super::physical::{
    SnapshotPhysicalAuditLimits, SnapshotPhysicalDependencyAudit,
    audit_snapshot_physical_dependencies, finish_after_close,
};

pub(super) struct Fixture {
    pub(super) objects: tempfile::TempDir,
    pub(super) client: ObjectClient<LocalFsBackend>,
    pub(super) reference: SnapshotObjectRef,
    pub(super) manifest: SnapshotManifest,
}

#[tokio::test]
async fn physical_audit_rejects_termination_during_close_acknowledgement() {
    use futures_util::FutureExt;

    for close_budget in [false, true] {
        let fixture = fixture(PackedCodec::Raw, false, 4).await;
        let scratch = scratch();
        let budget = SnapshotMountBudget::defaults();
        let cancel = CancellationToken::new();
        let verified = audit_snapshot_physical_dependencies(
            &fixture.client,
            &fixture.reference,
            scratch.path(),
            budget.clone(),
            limits(),
            cancel.clone(),
        )
        .await
        .unwrap();
        // The result is a real producer graph audit. Hold the final close
        // acknowledgment so termination happens after verification and before
        // the publication-side caller can observe a successful return.
        let (sender, receiver) = tokio::sync::oneshot::channel::<()>();
        let finishing = finish_after_close(
            Ok(verified),
            async {
                receiver.await.unwrap();
                Ok(())
            },
            &budget,
            &cancel,
        );
        tokio::pin!(finishing);
        assert!(finishing.as_mut().now_or_never().is_none());
        if close_budget {
            budget.close();
        } else {
            cancel.cancel();
        }
        sender.send(()).unwrap();
        let result = finishing.await;
        assert!(
            result.is_err(),
            "terminated audit escaped the close acknowledgment boundary"
        );
        drop(result);
        assert_scratch_clean(scratch.path());
        assert_eq!(budget.state().used, [0; 8]);
        assert_eq!(budget.state().closed, close_budget);
    }
}

pub(super) fn limits() -> SnapshotPhysicalAuditLimits {
    SnapshotPhysicalAuditLimits {
        max_objects: 256,
        max_authenticated_bytes: 4 << 20,
        max_requested_bytes: 4 << 20,
        max_edges: 512,
        max_leaf_records: 512,
        max_disk_bytes: 256 << 10,
        sqlite_cache_bytes: 64 << 10,
        chunk_bytes: 4096,
    }
}

fn options(codec: PackedCodec, inline_data: bool) -> SnapshotProducerOptions {
    SnapshotProducerOptions {
        snapshot_id: [1; 32],
        root_dir_key: [2; 32],
        root_inode: 1,
        profile: AccessProfile::RandomSmallFile,
        size_classes: SizeClassTable::default(),
        build_policy: SnapshotBuildPolicy {
            inline_data,
            ..Default::default()
        },
        metadata_codec: codec,
        data_codec: codec,
    }
}

fn root_attributes() -> SnapshotRootAttributes {
    SnapshotRootAttributes {
        inode: 1,
        size: 4096,
        blocks: 8,
        mode: 0o040755,
        uid: 0,
        gid: 0,
        nlink: 2,
        atime_ns: 0,
        mtime_ns: 0,
        ctime_ns: 0,
    }
}

pub(super) async fn fixture(codec: PackedCodec, inline_data: bool, count: usize) -> Fixture {
    let objects = tempfile::tempdir().unwrap();
    let producer_scratch = tempfile::tempdir().unwrap();
    let client = ObjectClient::new(LocalFsBackend::new(objects.path()));
    let options = options(codec, inline_data);
    let files = (0..count)
        .map(|ordinal| PackedFileInput {
            name: format!("file-{ordinal:04}").into_bytes(),
            inode: ordinal as u64 + 2,
            kind: 1,
            mode: 0o100644,
            uid: 0,
            gid: 0,
            rdev: 0,
            nlink: 1,
            atime_ns: 0,
            mtime_ns: 0,
            ctime_ns: 0,
            flags: 0,
            data: vec![ordinal as u8 + 1; 4096],
        })
        .collect();
    let (group, frames) = pack_group_files_with_policy(
        1,
        options.root_dir_key,
        files,
        options.profile,
        options.size_classes,
        options.build_policy,
    )
    .unwrap();
    let mut producer = SnapshotProducer::new(
        client.clone(),
        producer_scratch.path(),
        "physical".into(),
        options,
    )
    .await
    .unwrap();
    producer.set_root_attributes(root_attributes()).unwrap();
    producer
        .add_container(1, &[group], &frames, &[1])
        .await
        .unwrap();
    for inode in 2..count as u64 + 2 {
        producer.set_inode_blocks(inode, 8).await.unwrap();
    }
    producer
        .add_cold_attributes(&SnapshotColdAttributes {
            inode: 2,
            symlink_target: None,
            xattrs: vec![SnapshotXattr {
                name: b"user.test".to_vec(),
                value: b"physical fixture".to_vec(),
            }],
            acl: vec![],
        })
        .await
        .unwrap();
    let reference = producer.finish().await.unwrap();
    let snapshot = AuthenticatedSnapshot::open(&client, &reference)
        .await
        .unwrap();
    assert_eq!(
        std::fs::read_dir(producer_scratch.path()).unwrap().count(),
        0
    );
    Fixture {
        objects,
        client,
        reference,
        manifest: snapshot.manifest().clone(),
    }
}

fn scratch() -> tempfile::TempDir {
    let scratch = tempfile::tempdir().unwrap();
    std::fs::write(scratch.path().join("existing-evidence"), b"keep").unwrap();
    scratch
}

fn assert_scratch_clean(scratch: &Path) {
    assert_eq!(std::fs::read_dir(scratch).unwrap().count(), 1);
    assert_eq!(
        std::fs::read(scratch.join("existing-evidence")).unwrap(),
        b"keep"
    );
}

fn assert_released(scratch: &Path, budget: &SnapshotMountBudget) {
    assert_eq!(budget.state().used, [0; 8], "audit retained memory owners");
    assert_scratch_clean(scratch);
}

fn object_bytes(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn visit(root: &Path, result: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for entry in std::fs::read_dir(root).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                visit(&entry.path(), result);
            } else {
                result.insert(entry.path(), std::fs::read(entry.path()).unwrap());
            }
        }
    }
    let mut result = BTreeMap::new();
    visit(root, &mut result);
    result
}

async fn page(fixture: &Fixture, reference: &SnapshotObjectRef) -> SnapshotIndexPage {
    let bytes = fixture
        .client
        .get_object(&reference.key)
        .await
        .unwrap()
        .unwrap();
    SnapshotIndexPage::decode(reference, &bytes).unwrap()
}

async fn leaf_refs(fixture: &Fixture, kind: SnapshotRootKind) -> Vec<SnapshotObjectRef> {
    let page = page(fixture, &fixture.manifest.roots[kind as usize]).await;
    assert_eq!(page.height, 0);
    page.records
        .iter()
        .map(|record| {
            let SnapshotIndexValue::Leaf(value) = &record.value else {
                panic!("fixture should have one small leaf page")
            };
            SnapshotObjectRef::decode_value(value).unwrap()
        })
        .collect()
}

async fn audit(
    fixture: &Fixture,
    bounded: SnapshotPhysicalAuditLimits,
) -> PackedResult<SnapshotPhysicalDependencyAudit> {
    let scratch = scratch();
    let budget = SnapshotMountBudget::defaults();
    let result = audit_snapshot_physical_dependencies(
        &fixture.client,
        &fixture.reference,
        scratch.path(),
        budget.clone(),
        bounded,
        CancellationToken::new(),
    )
    .await;
    assert_scratch_clean(scratch.path());
    if result.is_err() {
        assert_eq!(budget.state().used, [0; 8]);
    } else {
        // The successful public result owns its manifest-reference allocation.
        // Only that Roots reservation may remain while the result is live.
        let state = budget.state();
        assert!(state.used[SnapshotBudgetPool::Roots as usize] > 0);
        for pool in SnapshotBudgetPool::ALL {
            if pool != SnapshotBudgetPool::Roots {
                assert_eq!(
                    state.used[pool as usize],
                    0,
                    "retained {} owner",
                    pool.name()
                );
            }
        }
    }
    assert!(!budget.state().closed);
    result
}

// Reads still use real LocalFS. Forbidden methods prevent audit from silently
// falling back to whole GET, legacy size, or any object publication operation.
#[derive(Clone)]
struct ReadOnlyLocal {
    inner: LocalFsBackend,
    heads: Arc<AtomicU64>,
    ranges: Arc<AtomicU64>,
    maximum_range: Arc<AtomicU64>,
    pause: Option<Arc<PauseRange>>,
}

#[derive(Default)]
struct PauseRange {
    entered: Notify,
    once: AtomicBool,
}

impl ReadOnlyLocal {
    fn new(root: &Path) -> Self {
        Self {
            inner: LocalFsBackend::new(root),
            heads: Arc::new(AtomicU64::new(0)),
            ranges: Arc::new(AtomicU64::new(0)),
            maximum_range: Arc::new(AtomicU64::new(0)),
            pause: None,
        }
    }

    async fn before_range(&self, length: u64) {
        self.ranges.fetch_add(1, Ordering::SeqCst);
        self.maximum_range.fetch_max(length, Ordering::SeqCst);
        if let Some(pause) = &self.pause
            && !pause.once.swap(true, Ordering::SeqCst)
        {
            pause.entered.notify_one();
            futures_util::future::pending::<()>().await;
        }
    }
}

#[async_trait]
impl ObjectBackend for ReadOnlyLocal {
    async fn put_object(&self, _: &str, _: &[u8]) -> anyhow::Result<()> {
        panic!("physical audit must not publish objects")
    }
    async fn put_object_create_only(&self, _: &str, _: &[u8]) -> anyhow::Result<()> {
        panic!("physical audit must not create objects")
    }
    async fn get_object(&self, _: &str) -> anyhow::Result<Option<Vec<u8>>> {
        panic!("physical audit must not issue whole GET")
    }
    async fn get_object_size(&self, _: &str) -> anyhow::Result<Option<u64>> {
        panic!("physical audit must not use legacy size fallback")
    }
    async fn get_object_size_bounded(&self, key: &str) -> anyhow::Result<Option<u64>> {
        self.heads.fetch_add(1, Ordering::SeqCst);
        self.inner.get_object_size_bounded(key).await
    }
    async fn get_object_range(
        &self,
        key: &str,
        offset: u64,
        bytes: &mut [u8],
    ) -> anyhow::Result<usize> {
        self.before_range(bytes.len() as u64).await;
        self.inner.get_object_range(key, offset, bytes).await
    }
    async fn get_object_range_stream(
        &self,
        key: &str,
        offset: u64,
        length: u64,
    ) -> anyhow::Result<ObjectByteStream> {
        self.before_range(length).await;
        self.inner
            .get_object_range_stream(key, offset, length)
            .await
    }
    async fn get_etag(&self, _: &str) -> anyhow::Result<String> {
        panic!("physical audit must not use etag fallback")
    }
    async fn delete_object(&self, _: &str) -> anyhow::Result<()> {
        panic!("physical audit must not delete objects")
    }
}

#[tokio::test]
async fn physical_audit_authenticates_raw_zstd_and_inline_without_object_writes() {
    for codec in [PackedCodec::Raw, PackedCodec::Zstd] {
        for inline in [false, true] {
            let fixture = fixture(codec, inline, 4).await;
            let before = object_bytes(fixture.objects.path());
            let expected_bytes = before.values().map(|bytes| bytes.len() as u64).sum::<u64>();
            let backend = ReadOnlyLocal::new(fixture.objects.path());
            let client = ObjectClient::new(backend.clone());
            let scratch = scratch();
            let budget = SnapshotMountBudget::defaults();
            let result = audit_snapshot_physical_dependencies(
                &client,
                &fixture.reference,
                scratch.path(),
                budget.clone(),
                limits(),
                CancellationToken::new(),
            )
            .await
            .unwrap();
            assert_eq!(result.manifest_reference(), &fixture.reference);
            assert_ne!(result.inventory_digest(), [0; 32]);
            let counts = result.counts();
            assert_eq!(counts.objects, if inline { 11 } else { 12 });
            assert_eq!(counts.objects, before.len() as u64);
            assert_eq!(counts.authenticated_bytes, expected_bytes);
            assert_eq!(counts.requested_bytes, expected_bytes);
            assert_eq!(counts.leaf_records, if inline { 15 } else { 16 });
            assert_eq!(counts.edges, counts.objects);
            assert_eq!(
                fixture.manifest.build.inline_dentries,
                if inline { 4 } else { 0 }
            );
            assert_eq!(fixture.manifest.build.requested_metadata_codec, codec as u8);
            assert_eq!(fixture.manifest.build.requested_data_codec, codec as u8);
            assert!(backend.heads.load(Ordering::SeqCst) >= 2 * counts.objects);
            assert!(backend.ranges.load(Ordering::SeqCst) > counts.objects);
            assert!(backend.maximum_range.load(Ordering::SeqCst) <= 4096);
            assert_eq!(object_bytes(fixture.objects.path()), before);
            assert_scratch_clean(scratch.path());
            // Changing range size must not alter the physical inventory proof.
            let mut smaller = limits();
            smaller.chunk_bytes = 1024;
            let repeated = audit(&fixture, smaller).await.unwrap();
            assert_eq!(repeated.inventory_digest(), result.inventory_digest());
            assert_eq!(repeated.counts(), counts);
            drop(result);
            assert_released(scratch.path(), &budget);
        }
    }
}

#[tokio::test]
async fn physical_audit_rejects_missing_gc_fd_cold_source_index_and_manifest() {
    let fixture = fixture(PackedCodec::Raw, false, 4).await;
    let mut missing = vec![
        fixture.reference.clone(),
        fixture
            .manifest
            .source
            .as_ref()
            .unwrap()
            .allocations
            .clone(),
        fixture.manifest.roots[SnapshotRootKind::Inodes as usize].clone(),
    ];
    for root in [
        SnapshotRootKind::Containers,
        SnapshotRootKind::Frames,
        SnapshotRootKind::ColdAttributes,
    ] {
        missing.extend(leaf_refs(&fixture, root).await);
    }
    assert_eq!(missing.len(), 6);
    for reference in missing {
        let path = fixture.objects.path().join(&reference.key);
        let bytes = std::fs::read(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        assert!(
            audit(&fixture, limits()).await.is_err(),
            "missing {:?} accepted",
            reference.kind
        );
        std::fs::write(path, bytes).unwrap();
    }
}

#[tokio::test]
async fn physical_audit_rejects_appended_suffixes_on_metadata_and_payload_objects() {
    let fixture = fixture(PackedCodec::Zstd, false, 4).await;
    let mut references = vec![
        fixture.reference.clone(),
        fixture
            .manifest
            .source
            .as_ref()
            .unwrap()
            .allocations
            .clone(),
    ];
    for root in [
        SnapshotRootKind::Containers,
        SnapshotRootKind::Frames,
        SnapshotRootKind::ColdAttributes,
    ] {
        references.extend(leaf_refs(&fixture, root).await);
    }
    for reference in references {
        let path = fixture.objects.path().join(&reference.key);
        let bytes = std::fs::read(&path).unwrap();
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"unreferenced suffix")
            .unwrap();
        assert!(
            audit(&fixture, limits()).await.is_err(),
            "suffix on {:?} accepted",
            reference.kind
        );
        std::fs::write(path, bytes).unwrap();
    }
}

#[tokio::test]
async fn physical_audit_authenticates_same_length_metadata_and_payload_tail_bytes() {
    use crate::workspace::packed::snapshot::SNAPSHOT_FOOTER_LEN;

    let fixture = fixture(PackedCodec::Raw, false, 4).await;
    let mut references = vec![
        fixture
            .manifest
            .source
            .as_ref()
            .unwrap()
            .allocations
            .clone(),
    ];
    for root in [
        SnapshotRootKind::Containers,
        SnapshotRootKind::Frames,
        SnapshotRootKind::ColdAttributes,
    ] {
        references.extend(leaf_refs(&fixture, root).await);
    }
    for reference in references {
        let path = fixture.objects.path().join(&reference.key);
        let original = std::fs::read(&path).unwrap();
        let mut altered = original.clone();
        // For GC this is beyond the directory and compressed metadata: a
        // summary-only fetch cannot detect the damaged frame payload tail.
        let body_tail = altered.len() - SNAPSHOT_FOOTER_LEN - 1;
        altered[body_tail] ^= 1;
        std::fs::write(&path, &altered).unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            reference.object_len
        );
        let error = audit(&fixture, limits()).await.unwrap_err();
        assert!(
            matches!(error, PackedWireError::HashMismatch { .. }),
            "same-length {:?} corruption: {error}",
            reference.kind
        );
        assert!(
            error.to_string().contains("hash mismatch"),
            "corruption failed for an unrelated reason: {error}"
        );
        std::fs::write(path, original).unwrap();
    }
}

async fn replace_manifest(fixture: &mut Fixture, manifest: SnapshotManifest, key: &str) {
    let bytes = manifest.encode().unwrap();
    let reference =
        SnapshotObjectRef::from_bytes(key.into(), SnapshotObjectKind::Manifest, &bytes).unwrap();
    AuthenticatedSnapshot::decode(&reference, &bytes).unwrap();
    fixture
        .client
        .put_object_create_only(key, &bytes)
        .await
        .unwrap();
    fixture.manifest = manifest;
    fixture.reference = reference;
}

async fn rebuild_inode_root(fixture: &mut Fixture, prefix: String) -> SnapshotObjectRef {
    let old = &fixture.manifest.roots[SnapshotRootKind::Inodes as usize];
    let leaves = page(fixture, old).await;
    assert_eq!(leaves.height, 0);
    let mut builder = SnapshotIndexBuilder::new(
        fixture.client.clone(),
        SnapshotObjectKind::InodeIndex,
        prefix,
        2,
        16 * 1024,
    )
    .unwrap();
    for record in leaves.records {
        builder.push(record).await.unwrap();
    }
    let root = builder.finish().await.unwrap();
    let mut manifest = fixture.manifest.clone();
    manifest.roots[SnapshotRootKind::Inodes as usize] = root.clone();
    replace_manifest(fixture, manifest, "physical/rebuilt-manifest").await;
    root
}

#[tokio::test]
async fn physical_audit_checks_index_children_outside_the_maximum_inode_route() {
    let mut fixture = fixture(PackedCodec::Raw, false, 4).await;
    let original = audit(&fixture, limits()).await.unwrap();
    let root = rebuild_inode_root(&mut fixture, "physical/small-fanout".into()).await;
    let branch = page(&fixture, &root).await;
    assert_eq!(branch.height, 1);
    assert_eq!(branch.records.len(), 2);
    let complete = audit(&fixture, limits()).await.unwrap();
    assert_eq!(complete.counts().objects, original.counts().objects + 2);
    assert_eq!(complete.counts().edges, original.counts().edges + 2);
    assert_eq!(
        complete.counts().leaf_records,
        original.counts().leaf_records
    );
    assert!(object_bytes(fixture.objects.path()).len() as u64 > complete.counts().objects);
    let SnapshotIndexValue::Child {
        reference: left, ..
    } = &branch.records[0].value
    else {
        panic!("small fanout must create a left child")
    };
    fixture.client.delete_object(&left.key).await.unwrap();
    assert_eq!(
        SnapshotIndexReader::new(fixture.client.clone(), 0)
            .maximum_inode(&root)
            .await
            .unwrap(),
        Some(5)
    );
    assert!(audit(&fixture, limits()).await.is_err());
}

#[tokio::test]
async fn physical_audit_rejects_one_key_referenced_by_distinct_self_consistent_index_pages() {
    let mut fixture = fixture(PackedCodec::Raw, false, 4).await;
    let root = rebuild_inode_root(&mut fixture, "physical/conflicting-children".into()).await;
    let mut branch = page(&fixture, &root).await;
    let SnapshotIndexValue::Child {
        reference: right, ..
    } = &branch.records[1].value
    else {
        panic!("right child expected")
    };
    let right = right.clone();
    let right_bytes = fixture
        .client
        .get_object(&right.key)
        .await
        .unwrap()
        .unwrap();
    SnapshotIndexPage::decode(&right, &right_bytes).unwrap();
    let SnapshotIndexValue::Child {
        reference: left, ..
    } = &mut branch.records[0].value
    else {
        panic!("left child expected")
    };
    let left_bytes = fixture.client.get_object(&left.key).await.unwrap().unwrap();
    SnapshotIndexPage::decode(left, &left_bytes).unwrap();
    assert_ne!(left.digest, right.digest);
    left.key = right.key;
    // Both descriptors derive from genuine builder output and remain locally
    // self-consistent; one physical key cannot satisfy their two identities.
    SnapshotIndexPage::decode(left, &left_bytes).unwrap();
    let bytes = branch.encode().unwrap();
    let root = SnapshotObjectRef::from_bytes(
        "physical/conflicting-root".into(),
        SnapshotObjectKind::InodeIndex,
        &bytes,
    )
    .unwrap();
    fixture
        .client
        .put_object_create_only(&root.key, &bytes)
        .await
        .unwrap();
    let mut manifest = fixture.manifest.clone();
    manifest.roots[SnapshotRootKind::Inodes as usize] = root.clone();
    replace_manifest(&mut fixture, manifest, "physical/conflicting-manifest").await;
    assert_eq!(
        SnapshotIndexReader::new(fixture.client.clone(), 0)
            .maximum_inode(&root)
            .await
            .unwrap(),
        Some(5)
    );
    let error = audit(&fixture, limits()).await.unwrap_err();
    assert!(
        matches!(error, PackedWireError::Invalid(_)),
        "unexpected conflict error: {error}"
    );
}

#[tokio::test]
async fn physical_audit_enforces_object_byte_request_edge_and_leaf_quotas() {
    let fixture = fixture(PackedCodec::Raw, false, 4).await;
    let actual = audit(&fixture, limits()).await.unwrap().counts();
    for quota in 0..5 {
        let mut bounded = limits();
        match quota {
            0 => bounded.max_objects = actual.objects - 1,
            1 => bounded.max_authenticated_bytes = actual.authenticated_bytes - 1,
            2 => bounded.max_requested_bytes = actual.requested_bytes - 1,
            3 => bounded.max_edges = actual.edges - 1,
            _ => bounded.max_leaf_records = actual.leaf_records - 1,
        }
        let error = audit(&fixture, bounded).await.unwrap_err();
        assert!(
            matches!(error, PackedWireError::LimitExceeded(_)),
            "quota {quota}: {error}"
        );
    }
}

#[tokio::test]
async fn physical_audit_enforces_disk_quota_while_registering_real_long_key_dependencies() {
    let mut fixture = fixture(PackedCodec::Raw, false, 16).await;
    let prefix = format!("physical/{}", vec!["k".repeat(96); 20].join("/"));
    rebuild_inode_root(&mut fixture, prefix).await;
    audit(&fixture, limits()).await.unwrap();
    let mut bounded = limits();
    bounded.max_disk_bytes = 16 << 10;
    let error = audit(&fixture, bounded).await.unwrap_err();
    assert!(
        matches!(error, PackedWireError::LimitExceeded(_)),
        "disk quota: {error}"
    );
}

#[tokio::test]
async fn physical_audit_cancellation_and_budget_close_release_inflight_scratch_and_memory() {
    let fixture = fixture(PackedCodec::Raw, false, 4).await;
    for close_budget in [false, true] {
        let scratch = scratch();
        let budget = SnapshotMountBudget::defaults();
        let pause = Arc::new(PauseRange::default());
        let mut backend = ReadOnlyLocal::new(fixture.objects.path());
        backend.pause = Some(pause.clone());
        let client = ObjectClient::new(backend);
        let cancel = CancellationToken::new();
        let task_reference = fixture.reference.clone();
        let task_scratch = scratch.path().to_owned();
        let task_budget = budget.clone();
        let task_cancel = cancel.clone();
        let task = tokio::spawn(async move {
            audit_snapshot_physical_dependencies(
                &client,
                &task_reference,
                &task_scratch,
                task_budget,
                limits(),
                task_cancel,
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(5), pause.entered.notified())
            .await
            .unwrap();
        assert!(budget.state().used.iter().any(|used| *used > 0));
        assert!(std::fs::read_dir(scratch.path()).unwrap().count() > 1);
        if close_budget {
            budget.close();
        } else {
            cancel.cancel();
        }
        let error = tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        if close_budget {
            assert!(
                matches!(error, PackedWireError::LimitExceeded(_)),
                "budget close: {error}"
            );
        } else {
            assert!(
                matches!(error, PackedWireError::Backend(_)),
                "cancel: {error}"
            );
        }
        assert_released(scratch.path(), &budget);
        assert_eq!(budget.state().closed, close_budget);
    }
}

#[cfg(target_os = "linux")]
async fn external_fixture(all_hole: bool) -> Fixture {
    use crate::workspace::packed::snapshot::CapturedSourceLayout;
    use crate::workspace::packed::{GroupMeta, PackedGroupInput};

    let source = tempfile::tempdir().unwrap();
    let path = source.path().join("external");
    let mut file = std::fs::File::create(&path).unwrap();
    if !all_hole {
        file.write_all(&[37; 4096]).unwrap();
    }
    file.set_len(8 << 20).unwrap();
    file.sync_all().unwrap();
    let capture_scratch = tempfile::tempdir().unwrap();
    let producer_scratch = tempfile::tempdir().unwrap();
    let objects = tempfile::tempdir().unwrap();
    let client = ObjectClient::new(LocalFsBackend::new(objects.path()));
    let options = options(PackedCodec::Zstd, false);
    let mut captured = CapturedSourceLayout::capture_with_policy(
        &path,
        capture_scratch.path(),
        2,
        options.profile,
        options.size_classes,
        options.build_policy,
    )
    .await
    .unwrap();
    assert_eq!(captured.data_bytes(), if all_hole { 0 } else { 4096 });
    let entry = captured.entry().clone();
    let blocks = captured.source_blocks();
    let mut producer = SnapshotProducer::new(
        client.clone(),
        producer_scratch.path(),
        "external-physical".into(),
        options,
    )
    .await
    .unwrap();
    producer.set_root_attributes(root_attributes()).unwrap();
    let chunks = producer.add_external_source(&mut captured).await.unwrap();
    assert_eq!(chunks, if all_hole { 0 } else { 1 });
    let group = PackedGroupInput {
        group_id: 1,
        parent_dir_key: [2; 32],
        metadata: GroupMeta::new(vec![entry]).unwrap().encode().unwrap(),
        frame_ordinals: vec![],
        entry_count: 1,
        file_count: 1,
        layout_profile: AccessProfile::RandomSmallFile,
    };
    producer
        .add_container(1, &[group], &[], &[1])
        .await
        .unwrap();
    producer.set_inode_blocks(2, blocks).await.unwrap();
    producer
        .add_cold_attributes(&SnapshotColdAttributes {
            inode: 2,
            symlink_target: None,
            xattrs: vec![SnapshotXattr {
                name: b"user.test".to_vec(),
                value: b"external".to_vec(),
            }],
            acl: vec![],
        })
        .await
        .unwrap();
    let reference = producer.finish().await.unwrap();
    let snapshot = AuthenticatedSnapshot::open(&client, &reference)
        .await
        .unwrap();
    drop(captured);
    assert_eq!(
        std::fs::read_dir(capture_scratch.path()).unwrap().count(),
        0
    );
    assert_eq!(
        std::fs::read_dir(producer_scratch.path()).unwrap().count(),
        0
    );
    Fixture {
        objects,
        client,
        reference,
        manifest: snapshot.manifest().clone(),
    }
}

#[cfg(target_os = "linux")]
async fn external_extent_root(fixture: &Fixture) -> SnapshotObjectRef {
    use crate::workspace::packed::snapshot::SnapshotPlacement;
    let selectors = page(
        fixture,
        &fixture.manifest.roots[SnapshotRootKind::LargePlacements as usize],
    )
    .await;
    let SnapshotIndexValue::Leaf(value) = &selectors.records[0].value else {
        panic!("external selector should be a leaf")
    };
    let SnapshotPlacement::External { extents, .. } =
        SnapshotPlacement::decode(value, 2, 8 << 20).unwrap()
    else {
        panic!("external selector expected")
    };
    extents
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn physical_audit_authenticates_all_hole_and_external_ld_le_dependencies() {
    for all_hole in [true, false] {
        let fixture = external_fixture(all_hole).await;
        let result = audit(&fixture, limits()).await.unwrap();
        let objects = object_bytes(fixture.objects.path());
        assert_eq!(result.counts().objects, objects.len() as u64);
        assert_eq!(result.counts().objects, if all_hole { 12 } else { 14 });
        assert_eq!(
            result.counts().authenticated_bytes,
            objects
                .values()
                .map(|bytes| bytes.len() as u64)
                .sum::<u64>()
        );
        let extents = external_extent_root(&fixture).await;
        let extents_page = page(&fixture, &extents).await;
        assert_eq!(extents_page.records.len(), usize::from(!all_hole));
        let containers = leaf_refs(&fixture, SnapshotRootKind::Containers).await;
        assert_eq!(
            containers
                .iter()
                .filter(|r| r.kind == SnapshotObjectKind::LargeData)
                .count(),
            usize::from(!all_hole)
        );
        if all_hole {
            use crate::cadapter::read_observer::{Engine, Origin, Phase, ReadClass};
            let snapshot = AuthenticatedSnapshot::open(&fixture.client, &fixture.reference)
                .await
                .unwrap();
            let budget = crate::workspace::packed::snapshot::SnapshotMountBudget::defaults();
            let observer = budget.read_observer(None).unwrap();
            let client = fixture.client.clone().with_read_observer(
                observer.clone(),
                Engine::Packed,
                Phase::Runtime,
                Origin::Demand,
            );
            let reader = crate::workspace::packed::snapshot::SnapshotIndexReader::with_budget(
                client.clone(),
                0,
                budget,
            );
            for (offset, length) in [(0, 4096), (4093, 17), (8 << 20, 0)] {
                let prepared = snapshot
                    .prepare_inode_read(&client, &reader, 2, offset, length, 4 << 20)
                    .await
                    .unwrap();
                assert_eq!(prepared.plan.segments.len(), usize::from(length != 0));
                if length != 0 {
                    let segment = &prepared.plan.segments[0];
                    assert_eq!(
                        (segment.logical_offset, segment.length),
                        (offset, length as u64)
                    );
                    assert_eq!(segment.source, crate::chunk::read_plan::ReadSource::Hole);
                }
                let mut output = vec![0xff; length];
                crate::chunk::read_plan::execute_unified_into(
                    prepared.fetcher.as_ref(),
                    offset,
                    &prepared.plan,
                    &mut output,
                )
                .await
                .unwrap();
                assert!(output.iter().all(|byte| *byte == 0));
            }
            assert_eq!(
                reader.budget().state().used
                    [crate::workspace::packed::snapshot::SnapshotBudgetPool::Raw as usize],
                0,
            );
            let observed = observer.snapshot();
            assert!(observed.rows.iter().any(|((_, context), counters)| {
                context.class == ReadClass::GroupMetadata && counters.started != 0
            }));
            assert!(observed.rows.iter().all(|((_, context), counters)| {
                !matches!(
                    context.class,
                    ReadClass::PackedPayload | ReadClass::ExternalPayload
                ) || counters.started == 0
            }));
            reader.close().await;
        }
        let mut missing = vec![extents];
        missing.extend(
            containers
                .into_iter()
                .filter(|r| r.kind == SnapshotObjectKind::LargeData),
        );
        for reference in missing {
            let path = fixture.objects.path().join(&reference.key);
            let bytes = std::fs::read(&path).unwrap();
            std::fs::remove_file(&path).unwrap();
            assert!(
                audit(&fixture, limits()).await.is_err(),
                "missing external {:?} accepted",
                reference.kind
            );
            std::fs::write(&path, &bytes).unwrap();
            if reference.kind == SnapshotObjectKind::LargeData {
                std::fs::OpenOptions::new()
                    .append(true)
                    .open(&path)
                    .unwrap()
                    .write_all(b"LD suffix")
                    .unwrap();
                assert!(audit(&fixture, limits()).await.is_err());
                std::fs::write(path, bytes).unwrap();
            }
        }
    }
}
