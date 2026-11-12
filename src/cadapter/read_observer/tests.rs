//! Isolated tests for the common observer candidate; not run or type-checked.
use super::*;
use async_trait::async_trait;
use futures_util::{StreamExt, stream};

#[derive(Clone)]
enum Response {
    Complete(Vec<u8>),
    Short(Vec<u8>),
    Excess(Vec<u8>),
    StartError,
    PartialThenErrors,
    PendingStart,
    PendingBody,
}

#[derive(Clone)]
struct ScriptedBackend(Response);

#[async_trait]
impl ObjectBackend for ScriptedBackend {
    async fn put_object(&self, _: &str, _: &[u8]) -> Result<()> {
        panic!("readonly observer must not write")
    }
    async fn get_object(&self, _: &str) -> Result<Option<Vec<u8>>> {
        panic!("readonly observer must not perform full GET")
    }
    async fn get_object_range(&self, _: &str, _: u64, _: &mut [u8]) -> Result<usize> {
        panic!("test requires the actual stream path")
    }
    async fn get_object_range_stream(
        &self,
        key: &str,
        offset: u64,
        length: u64,
    ) -> Result<ObjectByteStream> {
        // Intentionally misleading key: classification must come from the
        // selected typed context, not string matching a payload-looking key.
        assert_eq!((key, offset, length), ("looks-like-data/frame", 10, 8));
        match &self.0 {
            Response::Complete(bytes) | Response::Short(bytes) | Response::Excess(bytes) => {
                let split = bytes.len().min(3);
                let chunks = vec![
                    Ok(Bytes::copy_from_slice(&bytes[..split])),
                    Ok(Bytes::copy_from_slice(&bytes[split..])),
                ];
                Ok(Box::pin(stream::iter(chunks)))
            }
            Response::StartError => anyhow::bail!("scripted start failure"),
            Response::PartialThenErrors => Ok(Box::pin(stream::iter(vec![
                Ok(Bytes::from_static(b"abcd")),
                Err(anyhow::anyhow!("first stream failure")),
                Err(anyhow::anyhow!("second stream failure must not be polled")),
                Ok(Bytes::from_static(b"unreachable")),
            ]))),
            Response::PendingStart => futures_util::future::pending().await,
            Response::PendingBody => Ok(Box::pin(
                stream::once(async { Ok(Bytes::from_static(b"abc")) }).chain(stream::pending()),
            )),
        }
    }
    async fn get_etag(&self, _: &str) -> Result<String> {
        panic!("readonly observer must not request an etag")
    }
    async fn delete_object(&self, _: &str) -> Result<()> {
        panic!("readonly observer must not delete")
    }
}

fn tag(phase: Phase) -> ReadContext {
    ReadContext {
        engine: Engine::Packed,
        phase,
        class: ReadClass::GroupMetadata,
        origin: Origin::Demand,
    }
}

fn row(observer: &ReadObserver, ledger: Ledger, context: ReadContext) -> Counters {
    let snapshot = observer.snapshot();
    assert!(!snapshot.overflowed);
    assert!(snapshot.rows.values().all(Counters::conserved));
    snapshot.rows[&(ledger, context)].clone()
}

fn stats_metric(
    observer: &Arc<ReadObserver>,
    name: &str,
    ledger: Ledger,
    context: ReadContext,
) -> u64 {
    let stats = crate::vfs::stats::FsStats::new();
    assert!(stats.set_extension(Arc::new(ReadStatsExtension(Arc::clone(observer)))));
    let rendered = stats.render();
    assert!(rendered.contains("brewfs_fuse_read_ops_total 0"));
    assert!(rendered.contains("brewfs_object_read_counters_valid 1"));
    let prefix = format!(
        "brewfs_object_read_{name}{{layer=\"{}\",engine=\"{}\",phase=\"{}\",kind=\"{}\",origin=\"{}\"}} ",
        ledger.label(),
        context.engine.label(),
        context.phase.label(),
        context.class.label(),
        context.origin.label()
    );
    let values: Vec<_> = rendered
        .lines()
        .filter_map(|line| line.strip_prefix(&prefix))
        .collect();
    assert_eq!(
        values.len(),
        1,
        "metric absent or duplicated: {prefix}\n{rendered}"
    );
    values[0].parse().unwrap()
}

async fn verified(
    response: Response,
    observer: &Arc<ReadObserver>,
    context: ReadContext,
    authentication_ok: bool,
) -> Result<Vec<u8>> {
    exact_verified(
        &ObjectClient::new(ScriptedBackend(response)),
        observer,
        context,
        VerifiedReadRequest {
            key: "looks-like-data/frame",
            offset: 10,
            length: 8,
            allocation_limit: 8,
        },
        |bytes| {
            if authentication_ok {
                Ok(bytes.to_vec())
            } else {
                Err((
                    FailureClass::Authentication,
                    anyhow::anyhow!("scripted bad digest"),
                ))
            }
        },
    )
    .await
}

#[tokio::test]
async fn successful_typed_stream_is_exported_by_actual_fs_stats() {
    let observer = Arc::new(ReadObserver::default());
    let context = tag(Phase::Runtime);
    assert_eq!(
        verified(
            Response::Complete(b"abcdefgh".to_vec()),
            &observer,
            context,
            true
        )
        .await
        .unwrap(),
        b"abcdefgh"
    );
    assert_eq!(
        stats_metric(&observer, "started_total", Ledger::BackendBody, context),
        1
    );
    assert_eq!(
        stats_metric(&observer, "successful_total", Ledger::BackendBody, context),
        1
    );
    assert_eq!(
        stats_metric(
            &observer,
            "received_body_bytes_total",
            Ledger::BackendBody,
            context
        ),
        8
    );
    assert_eq!(
        stats_metric(
            &observer,
            "successful_total",
            Ledger::ValidatedFetch,
            context
        ),
        1
    );
    // No whole-object GET is possible, and payload-looking names do not
    // generate any payload bucket.
    assert!(
        observer
            .snapshot()
            .rows
            .keys()
            .all(
                |(ledger, context)| context.class == ReadClass::GroupMetadata
                    || (*ledger == Ledger::LogicalOperation
                        && context.class == ReadClass::StatsSnapshot
                        && context.origin == Origin::StatsObserver)
            )
    );
}

#[tokio::test]
async fn complete_backend_body_with_bad_digest_is_validation_failure() {
    let observer = Arc::new(ReadObserver::default());
    let context = tag(Phase::Runtime);
    assert!(
        verified(
            Response::Complete(b"abcdefgh".to_vec()),
            &observer,
            context,
            false
        )
        .await
        .is_err()
    );
    assert_eq!(row(&observer, Ledger::BackendBody, context).success, 1);
    let fetch = row(&observer, Ledger::ValidatedFetch, context);
    assert_eq!((fetch.failed, fetch.received_failed), (1, 8));
    assert_eq!(fetch.failure_reasons[&FailureClass::Authentication], 1);
    assert_eq!(
        stats_metric(&observer, "failed_total", Ledger::ValidatedFetch, context),
        1
    );
}

#[tokio::test]
async fn short_and_excess_bodies_keep_transport_and_validation_distinct() {
    let context = tag(Phase::Runtime);
    for (response, reason, received, transport_success, transport_cancelled) in [
        (
            Response::Short(b"abc".to_vec()),
            FailureClass::ShortBody,
            3,
            1,
            0,
        ),
        (
            Response::Excess(b"abcdefghij".to_vec()),
            FailureClass::ExcessBody,
            10,
            0,
            1,
        ),
    ] {
        let observer = Arc::new(ReadObserver::default());
        assert!(verified(response, &observer, context, true).await.is_err());
        let body = row(&observer, Ledger::BackendBody, context);
        assert_eq!(
            (body.success, body.cancelled, body.received),
            (transport_success, transport_cancelled, received)
        );
        assert_eq!(
            row(&observer, Ledger::ValidatedFetch, context).failure_reasons[&reason],
            1
        );
        assert_eq!(
            stats_metric(
                &observer,
                "received_body_bytes_total",
                Ledger::BackendBody,
                context
            ),
            received
        );
    }
}

#[tokio::test]
async fn start_failure_and_partial_body_error_count_one_terminal_failure() {
    let context = tag(Phase::Runtime);
    for (response, received) in [(Response::StartError, 0), (Response::PartialThenErrors, 4)] {
        let observer = Arc::new(ReadObserver::default());
        assert!(verified(response, &observer, context, true).await.is_err());
        let body = row(&observer, Ledger::BackendBody, context);
        assert_eq!(
            (
                body.started,
                body.failed,
                body.inflight,
                body.received_failed
            ),
            (1, 1, 0, received)
        );
        assert_eq!(row(&observer, Ledger::ValidatedFetch, context).failed, 1);
        assert_eq!(
            stats_metric(&observer, "failed_total", Ledger::BackendBody, context),
            1
        );
    }
}

#[tokio::test]
async fn a_body_is_fused_after_its_first_error() {
    let observer = Arc::new(ReadObserver::default());
    let context = tag(Phase::Runtime);
    let client = ObjectClient::new(ScriptedBackend(Response::PartialThenErrors));
    let mut body = range_stream(&client, &observer, context, "looks-like-data/frame", 10, 8)
        .await
        .unwrap();
    assert_eq!(body.next().await.unwrap().unwrap(), b"abcd".as_slice());
    assert!(body.next().await.unwrap().is_err());
    assert!(body.next().await.is_none());
    assert!(body.next().await.is_none());
    drop(body);
    assert_eq!(row(&observer, Ledger::BackendBody, context).failed, 1);
}

#[tokio::test]
async fn cancellation_before_headers_and_after_a_body_chunk_is_exported() {
    let context = tag(Phase::Runtime);
    for (response, received) in [(Response::PendingStart, 0), (Response::PendingBody, 3)] {
        let observer = Arc::new(ReadObserver::default());
        let client = ObjectClient::new(ScriptedBackend(response));
        let mut future = Box::pin(exact_verified(
            &client,
            &observer,
            context,
            VerifiedReadRequest {
                key: "looks-like-data/frame",
                offset: 10,
                length: 8,
                allocation_limit: 8,
            },
            |bytes| Ok(bytes.to_vec()),
        ));
        assert!(futures_util::poll!(future.as_mut()).is_pending());
        drop(future);
        for ledger in [Ledger::BackendBody, Ledger::ValidatedFetch] {
            let counters = row(&observer, ledger, context);
            assert_eq!(
                (
                    counters.cancelled,
                    counters.inflight,
                    counters.received_cancelled
                ),
                (1, 0, received)
            );
            assert_eq!(
                stats_metric(&observer, "cancelled_total", ledger, context),
                1
            );
        }
    }
}

#[tokio::test]
async fn dropping_an_unpolled_future_starts_no_backend_call() {
    let observer = Arc::new(ReadObserver::default());
    let client = ObjectClient::new(ScriptedBackend(Response::PendingStart));
    let future = range_stream(
        &client,
        &observer,
        tag(Phase::Runtime),
        "looks-like-data/frame",
        10,
        8,
    );
    drop(future);
    assert!(observer.snapshot().rows.is_empty());
}

#[tokio::test]
async fn dropping_a_returned_unconsumed_body_is_cancelled() {
    let observer = Arc::new(ReadObserver::default());
    let context = tag(Phase::Runtime);
    let client = ObjectClient::new(ScriptedBackend(Response::Complete(b"abcdefgh".to_vec())));
    drop(
        range_stream(&client, &observer, context, "looks-like-data/frame", 10, 8)
            .await
            .unwrap(),
    );
    assert_eq!(
        stats_metric(&observer, "cancelled_total", Ledger::BackendBody, context),
        1
    );
}

#[tokio::test]
async fn snapshot_during_stream_is_coherent_and_startup_survives_stats_installation() {
    let observer = Arc::new(ReadObserver::default());
    let startup = tag(Phase::Startup);
    assert!(
        verified(
            Response::Complete(b"abcdefgh".to_vec()),
            &observer,
            startup,
            true
        )
        .await
        .is_ok()
    );
    let runtime = tag(Phase::Runtime);
    let client = ObjectClient::new(ScriptedBackend(Response::PendingBody));
    let mut body = range_stream(&client, &observer, runtime, "looks-like-data/frame", 10, 8)
        .await
        .unwrap();
    assert_eq!(body.next().await.unwrap().unwrap().len(), 3);
    let counters = row(&observer, Ledger::BackendBody, runtime);
    assert_eq!(
        (
            counters.started,
            counters.inflight,
            counters.received_inflight
        ),
        (1, 1, 3)
    );
    assert_eq!(
        stats_metric(&observer, "successful_total", Ledger::BackendBody, startup),
        1
    );
    assert_eq!(
        stats_metric(
            &observer,
            "received_inflight_body_bytes",
            Ledger::BackendBody,
            runtime
        ),
        3
    );
    drop(body);
    assert_eq!(
        row(&observer, Ledger::BackendBody, runtime).received_cancelled,
        3
    );
}

#[tokio::test]
async fn one_complete_operation_fails_after_an_earlier_successful_span() {
    let observer = Arc::new(ReadObserver::default());
    let context = tag(Phase::Runtime);
    let logical = ReadContext {
        class: ReadClass::LogicalRead,
        ..context
    };
    let operation = observer.start(Ledger::LogicalOperation, logical, 16);
    assert!(
        verified(
            Response::Complete(b"abcdefgh".to_vec()),
            &observer,
            context,
            true
        )
        .await
        .is_ok()
    );
    assert!(
        verified(
            Response::Complete(b"abcdefgh".to_vec()),
            &observer,
            context,
            false
        )
        .await
        .is_err()
    );
    operation.fail(FailureClass::Authentication);
    assert_eq!(
        stats_metric(
            &observer,
            "logical_delivered_bytes_total",
            Ledger::LogicalOperation,
            logical
        ),
        0
    );
    assert_eq!(row(&observer, Ledger::BackendBody, context).success, 2);
    let operation = observer.start(Ledger::LogicalOperation, logical, 8);
    operation.deliver(8);
    assert_eq!(
        stats_metric(
            &observer,
            "logical_delivered_bytes_total",
            Ledger::LogicalOperation,
            logical
        ),
        8
    );
}

#[test]
fn missing_adapter_capabilities_do_not_invent_zero_http_or_raw_counters() {
    let observer = Arc::new(ReadObserver::default());
    let stats = crate::vfs::stats::FsStats::new();
    assert!(stats.set_extension(Arc::new(ReadStatsExtension(observer))));
    let rendered = stats.render();
    assert!(rendered.contains("brewfs_object_read_http_attempts_observed 0"));
    assert!(rendered.contains("brewfs_object_read_http_body_bytes_observed 0"));
    assert!(rendered.contains("brewfs_object_read_raw_union_observed 0"));
    assert!(!rendered.contains("layer=\"http_attempt\""));
    assert!(!rendered.contains("raw_overfetch_bytes_total"));
}

#[test]
fn raw_union_separates_compression_request_overlap_and_failed_delivery() {
    // Wire body can be smaller than requested logical bytes. Raw overscan is
    // still measured in decoded bytes, rather than wire minus logical.
    let wire_body = 50u64;
    let mut coverage =
        RawCoverage::new(1024, RawCoverage::required_tracking_bytes(1024).unwrap()).unwrap();
    coverage.request(10, 80).unwrap();
    coverage.request(40, 70).unwrap();
    coverage.copied(10, 80).unwrap();
    coverage.copied(40, 70).unwrap();
    let completed = coverage.summary(true);
    assert_eq!(
        (
            completed.decoded_raw,
            completed.requested_union,
            completed.copied_union
        ),
        (1024, 100, 100)
    );
    assert_eq!(
        (
            completed.requested_overfetch,
            completed.copied_overfetch,
            completed.delivered_union
        ),
        (924, 924, 100)
    );
    assert!(wire_body < completed.delivered_union);
    let failed = coverage.summary(false);
    assert_eq!(
        (
            failed.copied_union,
            failed.delivered_union,
            failed.undelivered_decoded_raw
        ),
        (100, 0, 1024)
    );
    // A fresh decode is a fresh unit. Reuse inside one unit does not multiply
    // decoded bytes or count overlapping copied extents twice.
    assert_eq!(
        coverage.tracking_bytes(),
        RawCoverage::required_tracking_bytes(1024).unwrap()
    );
}

#[test]
fn raw_coverage_is_bounded_before_allocation_and_rejects_invalid_ranges() {
    assert!(RawCoverage::new(8 * 1024 * 1024 + 1, u64::MAX).is_err());
    assert!(
        RawCoverage::new(
            1024,
            RawCoverage::required_tracking_bytes(1024).unwrap() - 1
        )
        .is_err()
    );
    let mut coverage =
        RawCoverage::new(1024, RawCoverage::required_tracking_bytes(1024).unwrap()).unwrap();
    assert!(coverage.request(u64::MAX, 2).is_err());
    assert!(coverage.copied(1000, 25).is_err());
    assert!(
        coverage.copied(0, 1).is_err(),
        "copied range must belong to authenticated requested union"
    );
    coverage.request(0, 1).unwrap();
    coverage.copied(0, 1).unwrap();
    assert_eq!(coverage.summary(true).delivered_union, 1);
}

#[derive(Clone)]
struct PageBackend(Bytes);

#[async_trait]
impl ObjectBackend for PageBackend {
    async fn put_object(&self, _: &str, _: &[u8]) -> Result<()> {
        panic!("readonly")
    }
    async fn get_object(&self, _: &str) -> Result<Option<Vec<u8>>> {
        panic!("full GET forbidden")
    }
    async fn get_object_range(&self, _: &str, _: u64, _: &mut [u8]) -> Result<usize> {
        panic!("stream only")
    }
    async fn get_object_range_stream(
        &self,
        _: &str,
        offset: u64,
        length: u64,
    ) -> Result<ObjectByteStream> {
        assert_eq!(offset, 0);
        assert_eq!(length, self.0.len() as u64);
        Ok(Box::pin(stream::iter(vec![
            Ok(self.0.slice(..37)),
            Ok(self.0.slice(37..)),
        ])))
    }
    async fn get_object_range_stream_observed(
        &self,
        key: &str,
        offset: u64,
        length: u64,
        context: ReadContext,
        _: Arc<ReadObserver>,
    ) -> Result<ObjectByteStream> {
        assert_eq!(
            context,
            ReadContext {
                class: ReadClass::GroupIndex,
                ..tag(Phase::Runtime)
            }
        );
        self.get_object_range_stream(key, offset, length).await
    }
    async fn get_etag(&self, _: &str) -> Result<String> {
        panic!("HEAD forbidden")
    }
    async fn delete_object(&self, _: &str) -> Result<()> {
        panic!("readonly")
    }
}

#[cfg(feature = "workspace")]
#[tokio::test]
async fn actual_ip06_reader_exports_typed_page_buckets_and_schema_failures() {
    use crate::workspace::packed::snapshot::{
        SnapshotIndexPage, SnapshotIndexReader, SnapshotObjectKind, SnapshotObjectRef,
        encode_snapshot_object,
    };
    let context = ReadContext {
        class: ReadClass::GroupIndex,
        ..tag(Phase::Runtime)
    };
    for valid in [true, false] {
        let bytes = if valid {
            SnapshotIndexPage {
                kind: SnapshotObjectKind::GroupIndex,
                height: 0,
                records: Vec::new(),
            }
            .encode()
            .unwrap()
        } else {
            encode_snapshot_object(SnapshotObjectKind::GroupIndex, b"IP05", 256 * 1024).unwrap()
        };
        let reference = SnapshotObjectRef::from_bytes(
            "misleading-data/frame".into(),
            SnapshotObjectKind::GroupIndex,
            &bytes,
        )
        .unwrap();
        let observer = Arc::new(ReadObserver::default());
        let client = ObjectClient::new(PageBackend(Bytes::from(bytes))).with_read_observer(
            Arc::clone(&observer),
            Engine::Packed,
            Phase::Runtime,
            Origin::Demand,
        );
        assert!(client.get_object("misleading-data/frame").await.is_err());
        assert!(
            client
                .get_object_size("misleading-data/frame")
                .await
                .is_err()
        );
        let reader = SnapshotIndexReader::new(client, 0);
        let result = reader.lookup(&reference, b"one").await;
        assert_eq!(result.is_ok(), valid);
        assert_eq!(
            stats_metric(&observer, "successful_total", Ledger::BackendBody, context),
            1
        );
        assert_eq!(
            stats_metric(
                &observer,
                if valid {
                    "successful_total"
                } else {
                    "failed_total"
                },
                Ledger::ValidatedFetch,
                context
            ),
            1
        );
        assert!(
            observer
                .snapshot()
                .rows
                .keys()
                .all(|(ledger, context)| context.class == ReadClass::GroupIndex
                    || (*ledger == Ledger::LogicalOperation
                        && context.class == ReadClass::StatsSnapshot
                        && context.origin == Origin::StatsObserver))
        );
    }
}

#[test]
fn real_raw_leases_commit_only_after_the_complete_operation() {
    let observer = Arc::new(ReadObserver::default());
    let mut tag = tag(Phase::Runtime);
    tag.class = ReadClass::PackedPayload;
    let guard = observer.start(
        Ledger::LogicalOperation,
        ReadContext {
            class: ReadClass::LogicalRead,
            ..tag
        },
        200,
    );
    let delivery = guard.delivery_token().unwrap();
    let required = RawCoverage::required_tracking_bytes(1024).unwrap();
    let mut first = RawLease::new(1024, required, delivery.clone(), tag).unwrap();
    first.request(10, 100).unwrap();
    first.request(40, 100).unwrap();
    first.copied(10, 100).unwrap();
    first.copied(40, 100).unwrap();
    drop(first);
    assert!(
        observer.snapshot().raw.is_empty(),
        "a completed chunk does not commit full-read delivery"
    );
    let mut inline_tag = tag;
    inline_tag.class = ReadClass::InlinePayload;
    let mut inline = RawLease::new(
        30,
        RawCoverage::required_tracking_bytes(30).unwrap(),
        delivery,
        inline_tag,
    )
    .unwrap();
    inline.request(0, 30).unwrap();
    inline.copied(0, 30).unwrap();
    guard.fail(FailureClass::Generation);
    drop(inline);
    let snapshot = observer.snapshot();
    assert_eq!(
        (
            snapshot.raw[&tag].decoded_raw,
            snapshot.raw[&tag].copied_union,
            snapshot.raw[&tag].delivered_union
        ),
        (1024, 130, 0)
    );
    assert_eq!(snapshot.raw[&inline_tag].delivered_union, 0);
    let stats = crate::vfs::stats::FsStats::new();
    stats.set_extension(Arc::new(ReadStatsExtension(observer.clone())));
    let rendered = stats.render();
    assert!(rendered.contains("brewfs_object_read_raw_union_observed 1"));
    assert!(rendered.contains("kind=\"stats_snapshot\",origin=\"stats_observer\""));
    assert_eq!(
        row(
            &observer,
            Ledger::LogicalOperation,
            ReadContext {
                class: ReadClass::StatsSnapshot,
                origin: Origin::StatsObserver,
                ..tag
            }
        )
        .success,
        1
    );

    let guard = observer.start(
        Ledger::LogicalOperation,
        ReadContext {
            class: ReadClass::LogicalRead,
            ..tag
        },
        4,
    );
    let mut lease = RawLease::new(
        4,
        RawCoverage::required_tracking_bytes(4).unwrap(),
        guard.delivery_token().unwrap(),
        tag,
    )
    .unwrap();
    lease.request(0, 4).unwrap();
    lease.copied(0, 4).unwrap();
    guard.deliver(4);
    drop(lease); // delivery may finish before the last owned output is dropped
    assert_eq!(observer.snapshot().raw[&tag].delivered_union, 4);
}

#[cfg(feature = "workspace")]
#[derive(Clone)]
struct MapBackend(Arc<std::collections::HashMap<String, Vec<u8>>>);
#[cfg(feature = "workspace")]
#[async_trait]
impl ObjectBackend for MapBackend {
    async fn put_object(&self, _: &str, _: &[u8]) -> Result<()> {
        panic!("readonly")
    }
    async fn get_object(&self, _: &str) -> Result<Option<Vec<u8>>> {
        panic!("whole GET forbidden")
    }
    async fn get_object_range(&self, _: &str, _: u64, _: &mut [u8]) -> Result<usize> {
        panic!("stream only")
    }
    async fn get_object_range_stream(
        &self,
        key: &str,
        offset: u64,
        length: u64,
    ) -> Result<ObjectByteStream> {
        assert_eq!(offset, 0);
        let bytes = self.0.get(key).unwrap();
        assert_eq!(length, bytes.len() as u64);
        Ok(Box::pin(stream::iter(vec![Ok(Bytes::copy_from_slice(
            bytes,
        ))])))
    }
    async fn get_etag(&self, _: &str) -> Result<String> {
        panic!("etag forbidden")
    }
    async fn delete_object(&self, _: &str) -> Result<()> {
        panic!("readonly")
    }
}
#[cfg(feature = "workspace")]
#[tokio::test]
async fn selected_child_fences_fail_fetch_validation_and_cached_resolution_separately() {
    use crate::workspace::packed::snapshot::{
        SnapshotIndexPage, SnapshotIndexReader, SnapshotIndexRecord, SnapshotIndexValue,
        SnapshotObjectKind, SnapshotObjectRef,
    };
    let leaf = SnapshotIndexPage {
        kind: SnapshotObjectKind::InodeIndex,
        height: 0,
        records: vec![SnapshotIndexRecord {
            first_key: vec![b'a'],
            last_key: vec![b'b'],
            value: SnapshotIndexValue::Leaf(vec![1]),
        }],
    }
    .encode()
    .unwrap();
    let leaf_ref =
        SnapshotObjectRef::from_bytes("leaf".into(), SnapshotObjectKind::InodeIndex, &leaf)
            .unwrap();
    let parent = SnapshotIndexPage {
        kind: SnapshotObjectKind::InodeIndex,
        height: 1,
        records: vec![SnapshotIndexRecord {
            first_key: vec![b'a'],
            last_key: vec![b'c'],
            value: SnapshotIndexValue::Child {
                reference: leaf_ref.clone(),
                subtree_weight: 1,
            },
        }],
    }
    .encode()
    .unwrap();
    let parent_ref =
        SnapshotObjectRef::from_bytes("parent".into(), SnapshotObjectKind::InodeIndex, &parent)
            .unwrap();
    let backend = MapBackend(Arc::new(
        [("leaf".into(), leaf), ("parent".into(), parent)]
            .into_iter()
            .collect(),
    ));
    let observer = Arc::new(ReadObserver::default());
    let tag = ReadContext {
        class: ReadClass::InodeIndex,
        ..tag(Phase::Runtime)
    };
    let client = ObjectClient::new(backend.clone()).with_read_observer(
        observer.clone(),
        tag.engine,
        tag.phase,
        tag.origin,
    );
    let reader = SnapshotIndexReader::new(client, 1024 * 1024);
    assert!(reader.lookup(&parent_ref, b"b").await.is_err());
    let fetch = row(&observer, Ledger::ValidatedFetch, tag);
    assert_eq!((fetch.started, fetch.success, fetch.failed), (2, 1, 1));
    assert_eq!(fetch.failure_reasons[&FailureClass::Schema], 1);
    assert_eq!(row(&observer, Ledger::SemanticValidation, tag).failed, 1);

    let observer = Arc::new(ReadObserver::default());
    let client = ObjectClient::new(backend).with_read_observer(
        observer.clone(),
        tag.engine,
        tag.phase,
        tag.origin,
    );
    let reader = SnapshotIndexReader::new(client, 1024 * 1024);
    assert!(reader.lookup(&leaf_ref, b"b").await.unwrap().is_some()); // independently valid cached object
    assert!(reader.lookup(&parent_ref, b"b").await.is_err());
    assert_eq!(row(&observer, Ledger::ValidatedFetch, tag).failed, 0);
    assert_eq!(row(&observer, Ledger::SemanticValidation, tag).failed, 1);
    assert_eq!(observer.snapshot().events[&(tag, ReadEvent::CacheHit)], 1);
}

#[cfg(feature = "workspace")]
#[test]
fn maximum_enum_snapshot_fits_default_output_with_a_normal_read_reply() {
    use crate::workspace::packed::snapshot::{
        SnapshotBudgetLimits, SnapshotBudgetPool, SnapshotMountBudget,
    };
    let mut limits = SnapshotBudgetLimits::default();
    let stats_bytes = ReadObserver::MAX_RENDER_BYTES
        + crate::vfs::stats::FsStats::BASE_RENDER_MAX_BYTES
        + SnapshotMountBudget::STATS_EXTENSION_RENDER_MAX_BYTES;
    let required = stats_bytes as u64
        + limits.max_read_bytes as u64
        + 2 * SnapshotMountBudget::REPLY_ALLOCATION_ALLOWANCE_BYTES;
    assert!(required <= limits.bytes[SnapshotBudgetPool::Output as usize]);
    // Exercise the exact minimum as well as proving that defaults admit it.
    limits.bytes[SnapshotBudgetPool::Output as usize] = required;
    let budget = SnapshotMountBudget::new(limits).unwrap();
    budget.validate_frame_capability(8 << 20).unwrap();
    let owner = budget
        .admit(&[(
            crate::workspace::packed::snapshot::SnapshotBudgetPool::Roots,
            ReadObserver::MEMORY_BOUND_BYTES,
        )])
        .unwrap();
    let observer = Arc::new(ReadObserver::with_memory_owner(owner));
    {
        let mut state = observer.state.lock().unwrap();
        for engine in Engine::ALL {
            for phase in Phase::ALL {
                for class in ReadClass::ALL {
                    for origin in Origin::ALL {
                        let context = ReadContext {
                            engine,
                            phase,
                            class,
                            origin,
                        };
                        for ledger in Ledger::ALL {
                            // Maximum numeral width is reachable after saturating counters;
                            // validity=0 still has to render as a complete finite snapshot.
                            let row = Counters {
                                started: u64::MAX,
                                received: u64::MAX,
                                requested: u64::MAX,
                                failure_reasons: FailureReasons([u64::MAX; 9]),
                                terminal_latency: Latency {
                                    count: u64::MAX,
                                    sum_nanoseconds: u64::MAX,
                                    buckets: [u64::MAX; 32],
                                },
                                ..Counters::default()
                            };
                            state.rows.insert((ledger, context), row);
                        }
                        for event in ReadEvent::ALL {
                            state.events.insert((context, event), u64::MAX);
                        }
                        for work in ReadWork::ALL {
                            state.work.insert(
                                (context, work),
                                Latency {
                                    count: u64::MAX,
                                    sum_nanoseconds: u64::MAX,
                                    buckets: [u64::MAX; 32],
                                },
                            );
                        }
                        state.raw.insert(
                            context,
                            RawSummary {
                                decoded_raw: u64::MAX,
                                requested_union: u64::MAX,
                                copied_union: u64::MAX,
                                delivered_union: u64::MAX,
                                requested_overfetch: u64::MAX,
                                copied_overfetch: u64::MAX,
                                undelivered_decoded_raw: u64::MAX,
                            },
                        );
                    }
                }
            }
        }
        assert_eq!(
            state.rows.len(),
            Ledger::ALL.len() * ReadContext::MAX_CONTEXTS
        );
        assert_eq!(
            state.work.len(),
            ReadWork::ALL.len() * ReadContext::MAX_CONTEXTS
        );
        assert_eq!(
            state.events.len(),
            ReadEvent::ALL.len() * ReadContext::MAX_CONTEXTS
        );
        assert_eq!(state.raw.len(), ReadContext::MAX_CONTEXTS);
    }
    assert_eq!(observer.render_max_bytes(), ReadObserver::MAX_RENDER_BYTES);
    let stats = crate::vfs::stats::FsStats::new();
    stats.set_extension(Arc::new(ReadStatsExtension(observer.clone())));
    let limit = stats
        .render_allocation_limit()
        .checked_add(SnapshotMountBudget::STATS_EXTENSION_RENDER_MAX_BYTES)
        .unwrap();
    assert_eq!(limit, stats_bytes);
    let normal = budget.output(budget.max_read_bytes()).unwrap();
    let normal_reply = budget
        .admit(&[(
            crate::workspace::packed::snapshot::SnapshotBudgetPool::Output,
            SnapshotMountBudget::REPLY_ALLOCATION_ALLOWANCE_BYTES,
        )])
        .unwrap();
    let snapshot = budget
        .admit(&[(
            crate::workspace::packed::snapshot::SnapshotBudgetPool::Output,
            limit as u64 + SnapshotMountBudget::REPLY_ALLOCATION_ALLOWANCE_BYTES,
        )])
        .unwrap();
    assert_eq!(
        budget.state().used[SnapshotBudgetPool::Output as usize],
        required
    );
    let text = stats.render_bounded(limit).unwrap();
    assert!(text.len() <= limit);
    assert!(text.capacity() <= limit);
    for line in text
        .lines()
        .filter(|line| line.starts_with("brewfs_object_read_"))
    {
        assert!(
            line.len() < 208,
            "maximum metric line exceeded its width: {}",
            line.len() + 1
        );
    }
    println!(
        "max_enum_rows={} max_enum_work={} max_enum_events={} max_enum_raw={} rendered={} capacity={} admitted_limit={} output_used={} output_capacity={}",
        Ledger::ALL.len() * ReadContext::MAX_CONTEXTS,
        ReadWork::ALL.len() * ReadContext::MAX_CONTEXTS,
        ReadEvent::ALL.len() * ReadContext::MAX_CONTEXTS,
        ReadContext::MAX_CONTEXTS,
        text.len(),
        text.capacity(),
        limit,
        budget.state().used
            [crate::workspace::packed::snapshot::SnapshotBudgetPool::Output as usize],
        budget.capacity(crate::workspace::packed::snapshot::SnapshotBudgetPool::Output)
    );
    drop(text);
    drop(snapshot);
    drop(normal_reply);
    drop(normal);
    drop(stats);
    drop(observer);
    assert_eq!(budget.state().used, [0; 8]);
}

struct WholeBackend(Response);
#[async_trait]
impl ObjectBackend for WholeBackend {
    async fn put_object(&self, _: &str, _: &[u8]) -> Result<()> {
        unreachable!()
    }
    async fn get_object(&self, _: &str) -> Result<Option<Vec<u8>>> {
        panic!("actual body stream required")
    }
    async fn get_object_range(&self, _: &str, _: u64, _: &mut [u8]) -> Result<usize> {
        panic!("whole GET must not become Range")
    }
    async fn get_object_stream(&self, _: &str) -> Result<Option<ObjectByteStream>> {
        match &self.0 {
            Response::Complete(bytes) | Response::Short(bytes) | Response::Excess(bytes) => {
                Ok(Some(Box::pin(stream::iter(vec![Ok(
                    Bytes::copy_from_slice(bytes),
                )]))))
            }
            Response::PartialThenErrors => Ok(Some(Box::pin(stream::iter(vec![
                Ok(Bytes::from_static(b"abcd")),
                Err(anyhow::anyhow!("reset")),
            ])))),
            Response::PendingBody => Ok(Some(Box::pin(
                stream::once(async { Ok(Bytes::from_static(b"abc")) }).chain(stream::pending()),
            ))),
            Response::PendingStart => futures_util::future::pending().await,
            Response::StartError => anyhow::bail!("headers failed"),
        }
    }
    async fn get_etag(&self, _: &str) -> Result<String> {
        unreachable!()
    }
    async fn delete_object(&self, _: &str) -> Result<()> {
        unreachable!()
    }
}
fn native_whole_client(
    response: Response,
    observer: Arc<ReadObserver>,
) -> ObjectClient<WholeBackend> {
    ObjectClient::new(WholeBackend(response)).with_read_observer(
        observer,
        Engine::Native,
        Phase::Runtime,
        Origin::Demand,
    )
}
fn native_tag() -> ReadContext {
    ReadContext {
        engine: Engine::Native,
        phase: Phase::Runtime,
        class: ReadClass::NativePayload,
        origin: Origin::Demand,
    }
}

#[tokio::test]
async fn native_unknown_full_get_retains_shape_and_explicit_unknown_request_length() {
    let observer = Arc::new(ReadObserver::default());
    let client = native_whole_client(Response::Complete(b"abcdefgh".to_vec()), observer.clone());
    assert_eq!(
        client
            .typed_full(ReadClass::NativePayload, "any-key", None, 8, Ok)
            .await
            .unwrap()
            .unwrap(),
        b"abcdefgh"
    );
    for ledger in [Ledger::BackendBody, Ledger::ValidatedFetch] {
        let row = row(&observer, ledger, native_tag());
        assert_eq!(
            (
                row.success,
                row.requested,
                row.requested_unknown,
                row.received
            ),
            (1, 0, 1, 8)
        );
    }
    let mut rendered = String::new();
    observer.render_into(&mut rendered);
    assert!(rendered.contains(
        "requested_body_unknown_operations_total{layer=\"backend_body\",engine=\"native\""
    ));
}

#[tokio::test]
async fn native_known_length_and_partial_failed_full_body_keep_independent_ledgers() {
    let observer = Arc::new(ReadObserver::default());
    let client = native_whole_client(Response::Short(b"abcd".to_vec()), observer.clone());
    assert!(
        client
            .typed_full(ReadClass::NativePayload, "k", Some(8), 8, Ok)
            .await
            .is_err()
    );
    let body = row(&observer, Ledger::BackendBody, native_tag());
    let fetch = row(&observer, Ledger::ValidatedFetch, native_tag());
    assert_eq!(
        (body.success, body.received, body.requested_unknown),
        (1, 4, 0)
    );
    assert_eq!(fetch.failure_reasons[&FailureClass::ShortBody], 1);
    let failed = Arc::new(ReadObserver::default());
    let client = native_whole_client(Response::PartialThenErrors, failed.clone());
    assert!(
        client
            .typed_full(ReadClass::NativePayload, "k", None, 8, Ok)
            .await
            .is_err()
    );
    for ledger in [Ledger::BackendBody, Ledger::ValidatedFetch] {
        let row = row(&failed, ledger, native_tag());
        assert_eq!(
            (row.failed, row.received_failed, row.requested_unknown),
            (1, 4, 1)
        );
    }
}

#[tokio::test]
async fn native_full_body_cancellation_and_mount_local_phase_are_exact() {
    let observer = Arc::new(ReadObserver::default());
    let phase = Arc::new(std::sync::atomic::AtomicU8::new(0));
    let client = native_whole_client(Response::PendingBody, observer.clone())
        .with_phase_control(phase.clone());
    let future = client.typed_full(ReadClass::NativePayload, "k", None, 8, Ok);
    let mut future = Box::pin(future);
    assert!(matches!(
        futures_util::poll!(future.as_mut()),
        std::task::Poll::Pending
    ));
    drop(future);
    let mut startup = native_tag();
    startup.phase = Phase::Startup;
    for ledger in [Ledger::BackendBody, Ledger::ValidatedFetch] {
        let row = row(&observer, ledger, startup);
        assert_eq!((row.cancelled, row.received_cancelled), (1, 3));
    }
    phase.store(1, std::sync::atomic::Ordering::Release);
    assert_eq!(
        client.read_context(ReadClass::NativePayload).unwrap().phase,
        Phase::Runtime
    );
    let independent = native_whole_client(
        Response::Complete(vec![]),
        Arc::new(ReadObserver::default()),
    );
    assert_eq!(
        independent
            .read_context(ReadClass::NativePayload)
            .unwrap()
            .phase,
        Phase::Runtime
    );
}

#[tokio::test]
async fn native_explicit_origin_is_per_call_and_does_not_require_backend_clone() {
    let observer = Arc::new(ReadObserver::default());
    let phase = Arc::new(std::sync::atomic::AtomicU8::new(0));
    let client = native_whole_client(Response::Complete(b"abcdefgh".to_vec()), observer.clone())
        .with_phase_control(phase.clone());
    let bytes = client
        .typed_full_with_origin(
            ReadClass::NativePayload,
            Origin::Prefetch,
            "same-native-block",
            None,
            8,
            |bytes| {
                let _work = client.measure_read_work_with_origin(
                    ReadClass::NativePayload,
                    Origin::Prefetch,
                    ReadWork::Authentication,
                );
                let validation = client
                    .begin_validation_with_origin(ReadClass::NativePayload, Origin::Prefetch)
                    .unwrap();
                client.read_event_with_origin(
                    ReadClass::NativePayload,
                    Origin::Prefetch,
                    ReadEvent::FetchLeader,
                );
                validation.succeed();
                Ok(bytes)
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(bytes, b"abcdefgh");
    let mut prefetch = native_tag();
    prefetch.phase = Phase::Startup;
    prefetch.origin = Origin::Prefetch;
    for ledger in [Ledger::BackendBody, Ledger::ValidatedFetch] {
        assert_eq!(row(&observer, ledger, prefetch).success, 1);
    }
    assert_eq!(
        client
            .read_context(ReadClass::NativePayload)
            .unwrap()
            .origin,
        Origin::Demand
    );
    assert_eq!(
        row(&observer, Ledger::SemanticValidation, prefetch).success,
        1
    );
    let snapshot = observer.snapshot();
    assert_eq!(
        snapshot.work[&(prefetch, ReadWork::Authentication)].count,
        1
    );
    assert_eq!(snapshot.events[&(prefetch, ReadEvent::FetchLeader)], 1);
    let mut default_startup = prefetch;
    default_startup.origin = Origin::Demand;
    assert!(
        !snapshot
            .work
            .contains_key(&(default_startup, ReadWork::Authentication))
    );
    phase.store(1, std::sync::atomic::Ordering::Release);
    client
        .typed_full(ReadClass::NativePayload, "same-native-block", None, 8, Ok)
        .await
        .unwrap()
        .unwrap();
    for ledger in [Ledger::BackendBody, Ledger::ValidatedFetch] {
        assert_eq!(row(&observer, ledger, native_tag()).success, 1);
        assert_eq!(row(&observer, ledger, prefetch).success, 1);
    }
}
