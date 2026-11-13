//! Explicitly opted-in physical-audit smoke against an isolated local S3
//! service. These tests never construct a catalog publication proof.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::cadapter::client::{ObjectBackend, ObjectByteStream, ObjectClient};
use crate::cadapter::read_observer::{
    Engine, FailureClass, Ledger, Origin, Phase, ReadClass, ReadContext, ReadObserver, Snapshot,
};
use crate::cadapter::s3::{S3Backend, S3Config};
use crate::workspace::packed::PackedCodec;
use crate::workspace::packed::snapshot::{
    SnapshotIndexPage, SnapshotIndexValue, SnapshotMountBudget, SnapshotObjectRef, SnapshotRootKind,
};

use super::physical::audit_snapshot_physical_dependencies;
use super::physical_tests::{Fixture, fixture, limits};

#[derive(Clone)]
struct BoundedS3 {
    inner: S3Backend,
    chunk_bytes: u64,
    ranges: Arc<AtomicU64>,
    requested: Arc<AtomicU64>,
    maximum_range: Arc<AtomicU64>,
}

impl BoundedS3 {
    fn before_range(&self, length: u64) {
        assert!(
            length > 0 && length <= self.chunk_bytes,
            "physical audit issued an unbounded S3 range"
        );
        self.ranges.fetch_add(1, Ordering::SeqCst);
        self.requested.fetch_add(length, Ordering::SeqCst);
        self.maximum_range.fetch_max(length, Ordering::SeqCst);
    }
}

#[async_trait]
impl ObjectBackend for BoundedS3 {
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
        panic!("physical audit must not use the legacy size fallback")
    }
    async fn get_object_range(
        &self,
        key: &str,
        offset: u64,
        bytes: &mut [u8],
    ) -> anyhow::Result<usize> {
        self.before_range(bytes.len() as u64);
        self.inner.get_object_range(key, offset, bytes).await
    }
    async fn get_object_range_stream(
        &self,
        key: &str,
        offset: u64,
        length: u64,
    ) -> anyhow::Result<ObjectByteStream> {
        self.before_range(length);
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
        self.before_range(length);
        // Preserve the real SDK's operation-local HTTP attempt observer.
        self.inner
            .get_object_range_stream_observed(key, offset, length, context, observer)
            .await
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
    async fn get_etag(&self, _: &str) -> anyhow::Result<String> {
        panic!("physical audit must not use etag fallback")
    }
    async fn delete_object(&self, _: &str) -> anyhow::Result<()> {
        panic!("physical audit must not delete objects")
    }
}

fn observed(backend: &BoundedS3) -> (ObjectClient<BoundedS3>, Arc<ReadObserver>) {
    let observer = Arc::new(ReadObserver::default());
    let client = ObjectClient::new(backend.clone()).with_read_observer(
        observer.clone(),
        Engine::Packed,
        Phase::Startup,
        Origin::Demand,
    );
    (client, observer)
}

fn assert_terminal_conservation(observer: &ReadObserver) -> Snapshot {
    let snapshot = observer.snapshot();
    assert!(
        snapshot.http_observed,
        "real S3 HTTP attempts were not observed"
    );
    assert!(!snapshot.overflowed);
    assert!(!snapshot.rows.is_empty());
    for ((_, context), row) in &snapshot.rows {
        assert_eq!(context.engine, Engine::Packed);
        assert_eq!(context.phase, Phase::Startup);
        assert_eq!(context.origin, Origin::Demand);
        assert!(row.conserved());
        assert_eq!(row.inflight, 0);
        assert_eq!(row.cancelled, 0);
        assert_eq!(row.requested_unknown, 0);
        assert_eq!(row.logical_delivered, 0);
    }
    for ledger in [
        Ledger::HttpAttempt,
        Ledger::BackendBody,
        Ledger::ValidatedFetch,
    ] {
        assert!(
            snapshot
                .rows
                .iter()
                .any(|((actual, _), row)| *actual == ledger && row.started > 0)
        );
    }
    snapshot
}

fn local_objects(root: &Path) -> BTreeMap<String, Vec<u8>> {
    fn visit(root: &Path, directory: &Path, objects: &mut BTreeMap<String, Vec<u8>>) {
        for entry in std::fs::read_dir(directory).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                visit(root, &entry.path(), objects);
            } else {
                let path = entry.path();
                let key = path
                    .strip_prefix(root)
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .replace(std::path::MAIN_SEPARATOR, "/");
                objects.insert(key, std::fs::read(path).unwrap());
            }
        }
    }
    let mut objects = BTreeMap::new();
    visit(root, root, &mut objects);
    objects
}

async fn first_leaf_object(fixture: &Fixture, kind: SnapshotRootKind) -> SnapshotObjectRef {
    let root = &fixture.manifest.roots[kind as usize];
    let bytes = fixture.client.get_object(&root.key).await.unwrap().unwrap();
    let page = SnapshotIndexPage::decode(root, &bytes).unwrap();
    assert_eq!(page.height, 0);
    let SnapshotIndexValue::Leaf(value) = &page.records[0].value else {
        panic!("small producer fixture must have a leaf root")
    };
    SnapshotObjectRef::decode_value(value).unwrap()
}

async fn assert_head_zero_payload(
    backend: &BoundedS3,
    reference: &SnapshotObjectRef,
    present: bool,
) {
    let (client, observer) = observed(backend);
    let class = ReadClass::ColdAttributes;
    assert_eq!(
        client
            .typed_object_size(class, &reference.key)
            .await
            .unwrap(),
        present.then_some(reference.object_len)
    );
    let snapshot = assert_terminal_conservation(&observer);
    let context = client.read_context(class).unwrap();
    for ledger in [
        Ledger::HttpAttempt,
        Ledger::BackendBody,
        Ledger::ValidatedFetch,
    ] {
        let row = &snapshot.rows[&(ledger, context)];
        assert_eq!(row.requested, 0, "HEAD counted object length as payload");
        assert_eq!(row.received, 0, "HEAD downloaded object payload");
        if ledger == Ledger::HttpAttempt && !present {
            assert_eq!(row.success, 0);
            assert!(row.failed > 0);
            assert_eq!(row.failure_reasons[&FailureClass::HttpStatus], row.failed);
        } else {
            // Ok(None) remains a successful typed metadata operation even
            // though the real HTTP HEAD attempt ended with 404.
            assert_eq!(row.success, row.started);
            assert_eq!(row.failed, 0);
        }
    }
}

#[tokio::test]
#[ignore = "requires explicit local RustFS endpoint and fresh UUID bucket: BREWFS_PACKED_GRAPH_TEST_S3_ENDPOINT/BUCKET"]
async fn real_s3_physical_audit_authenticates_and_rejects_missing_and_suffix_objects() {
    let endpoint = std::env::var("BREWFS_PACKED_GRAPH_TEST_S3_ENDPOINT")
        .expect("explicit BREWFS_PACKED_GRAPH_TEST_S3_ENDPOINT is required");
    let bucket = std::env::var("BREWFS_PACKED_GRAPH_TEST_S3_BUCKET")
        .expect("explicit BREWFS_PACKED_GRAPH_TEST_S3_BUCKET is required");
    let url = url::Url::parse(&endpoint).expect("graph test endpoint must be a URL");
    assert!(
        endpoint.starts_with("http://127.0.0.1:"),
        "graph test only accepts an explicit loopback HTTP port"
    );
    assert_eq!(url.scheme(), "http");
    assert_eq!(url.host_str(), Some("127.0.0.1"));
    assert!(url.port().is_some());
    assert!(url.username().is_empty() && url.password().is_none());
    assert!(url.path() == "/" && url.query().is_none() && url.fragment().is_none());
    let suffix = bucket
        .strip_prefix("brewfs-graph-test-")
        .expect("graph test bucket must use the isolated test prefix");
    let uuid = uuid::Uuid::parse_str(suffix).expect("graph test bucket must contain a fresh UUID");
    assert_eq!(suffix, uuid.to_string());
    assert!(
        !std::env::var("AWS_ACCESS_KEY_ID")
            .expect("runner AWS_ACCESS_KEY_ID required")
            .is_empty()
    );
    assert!(
        !std::env::var("AWS_SECRET_ACCESS_KEY")
            .expect("runner AWS_SECRET_ACCESS_KEY required")
            .is_empty()
    );

    // The test creates its own newly named bucket through the existing SDK.
    // A create failure terminates before any fixture object can be uploaded.
    let shared = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_sdk_s3::config::Region::new("us-east-1"))
        .load()
        .await;
    let sdk = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::config::Builder::from(&shared)
            .endpoint_url(endpoint.clone())
            .force_path_style(true)
            .retry_config(aws_sdk_s3::config::retry::RetryConfig::standard().with_max_attempts(1))
            .request_checksum_calculation(
                aws_sdk_s3::config::RequestChecksumCalculation::WhenRequired,
            )
            .response_checksum_validation(
                aws_sdk_s3::config::ResponseChecksumValidation::WhenRequired,
            )
            .build(),
    );
    sdk.create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("isolated bucket creation failed; fixture upload aborted");
    let s3 = S3Backend::with_config(S3Config {
        bucket: bucket.clone(),
        region: Some("us-east-1".into()),
        endpoint: Some(endpoint),
        force_path_style: true,
        max_concurrency: 1,
        max_retries: 1,
        ..Default::default()
    })
    .await
    .unwrap();
    let writer = ObjectClient::new(s3.clone());
    let fixture = fixture(PackedCodec::Zstd, false, 4).await;
    let objects = local_objects(fixture.objects.path());
    assert_eq!(objects.len(), 12);
    let expected_bytes = objects
        .values()
        .map(|bytes| bytes.len() as u64)
        .sum::<u64>();
    assert!(
        expected_bytes < 256 << 10,
        "graph smoke fixture unexpectedly grew"
    );
    for (key, bytes) in &objects {
        writer.put_object_create_only(key, bytes).await.unwrap();
    }
    let bounded = BoundedS3 {
        inner: s3,
        chunk_bytes: limits().chunk_bytes as u64,
        ranges: Arc::new(AtomicU64::new(0)),
        requested: Arc::new(AtomicU64::new(0)),
        maximum_range: Arc::new(AtomicU64::new(0)),
    };
    let cold = first_leaf_object(&fixture, SnapshotRootKind::ColdAttributes).await;
    let gc = first_leaf_object(&fixture, SnapshotRootKind::Containers).await;
    assert_head_zero_payload(&bounded, &cold, true).await;

    let (client, observer) = observed(&bounded);
    let scratch = tempfile::tempdir().unwrap();
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
    assert_eq!(result.counts().objects, objects.len() as u64);
    assert_eq!(result.counts().authenticated_bytes, expected_bytes);
    assert_eq!(result.counts().requested_bytes, expected_bytes);
    assert_eq!(result.counts().leaf_records, 16);
    assert_ne!(result.inventory_digest(), [0; 32]);
    let local_scratch = tempfile::tempdir().unwrap();
    let local_budget = SnapshotMountBudget::defaults();
    let local = audit_snapshot_physical_dependencies(
        &fixture.client,
        &fixture.reference,
        local_scratch.path(),
        local_budget.clone(),
        limits(),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(result.inventory_digest(), local.inventory_digest());
    assert_eq!(result.counts(), local.counts());
    let snapshot = assert_terminal_conservation(&observer);
    let http_attempts = snapshot
        .rows
        .iter()
        .filter(|((ledger, _), _)| *ledger == Ledger::HttpAttempt)
        .map(|(_, row)| row.started)
        .sum::<u64>();
    for ledger in [
        Ledger::HttpAttempt,
        Ledger::BackendBody,
        Ledger::ValidatedFetch,
    ] {
        let rows = snapshot
            .rows
            .iter()
            .filter(|((actual, _), _)| *actual == ledger)
            .map(|(_, row)| row)
            .collect::<Vec<_>>();
        assert_eq!(
            rows.iter().map(|row| row.requested).sum::<u64>(),
            expected_bytes
        );
        assert_eq!(
            rows.iter().map(|row| row.received).sum::<u64>(),
            expected_bytes
        );
        assert!(
            rows.iter()
                .all(|row| row.failed == 0 && row.success == row.started)
        );
    }
    assert_eq!(bounded.requested.load(Ordering::SeqCst), expected_bytes);
    assert!(bounded.ranges.load(Ordering::SeqCst) > objects.len() as u64);
    assert!(bounded.maximum_range.load(Ordering::SeqCst) <= limits().chunk_bytes as u64);
    let valid_ranges = bounded.ranges.load(Ordering::SeqCst);
    let valid_maximum_range = bounded.maximum_range.load(Ordering::SeqCst);
    drop(result);
    drop(local);
    assert_eq!(budget.state().used, [0; 8]);
    assert_eq!(local_budget.state().used, [0; 8]);
    assert_eq!(std::fs::read_dir(scratch.path()).unwrap().count(), 0);
    assert_eq!(std::fs::read_dir(local_scratch.path()).unwrap().count(), 0);

    writer.delete_object(&cold.key).await.unwrap();
    assert_head_zero_payload(&bounded, &cold, false).await;
    let (client, observer) = observed(&bounded);
    let error = audit_snapshot_physical_dependencies(
        &client,
        &fixture.reference,
        scratch.path(),
        budget.clone(),
        limits(),
        CancellationToken::new(),
    )
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("physical metadata length differs"),
        "missing cold object was not rejected at physical authentication: {error}"
    );
    let snapshot = assert_terminal_conservation(&observer);
    assert!(
        snapshot
            .rows
            .iter()
            .any(|((ledger, _), row)| *ledger == Ledger::HttpAttempt
                && row.failure_reasons[&FailureClass::HttpStatus] > 0)
    );
    assert_eq!(budget.state().used, [0; 8]);
    assert_eq!(std::fs::read_dir(scratch.path()).unwrap().count(), 0);
    writer
        .put_object_create_only(&cold.key, &objects[&cold.key])
        .await
        .unwrap();

    let mut suffixed = objects[&gc.key].clone();
    suffixed.extend_from_slice(b"graph-audit-unreferenced-suffix");
    writer.put_object(&gc.key, &suffixed).await.unwrap();
    let (client, observer) = observed(&bounded);
    let error = audit_snapshot_physical_dependencies(
        &client,
        &fixture.reference,
        scratch.path(),
        budget.clone(),
        limits(),
        CancellationToken::new(),
    )
    .await
    .unwrap_err();
    assert!(
        error.to_string().contains("physical length"),
        "GC suffix was not rejected at the physical-length boundary: {error}"
    );
    let snapshot = assert_terminal_conservation(&observer);
    assert!(
        snapshot
            .rows
            .iter()
            .any(|((ledger, _), row)| *ledger == Ledger::SemanticValidation && row.failed > 0)
    );
    assert_eq!(budget.state().used, [0; 8]);
    assert_eq!(std::fs::read_dir(scratch.path()).unwrap().count(), 0);
    writer.put_object(&gc.key, &objects[&gc.key]).await.unwrap();

    for key in objects.keys() {
        writer.delete_object(key).await.unwrap();
    }
    sdk.delete_bucket().bucket(&bucket).send().await.unwrap();
    println!(
        "real_s3_physical_audit: objects=12 authenticated_bytes={expected_bytes} http_attempts={http_attempts} range_requests={valid_ranges} maximum_range_bytes={valid_maximum_range}; missing_cold=rejected gc_suffix=rejected bucket=removed"
    );
}
